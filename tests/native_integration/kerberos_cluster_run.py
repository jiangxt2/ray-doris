"""Run a scoped Doris/HDFS/KDC/two-worker Kerberos integration fixture."""

import atexit
import hashlib
import ipaddress
import json
import os
import pathlib
import secrets
import signal
import socket
import subprocess
import tempfile
import time
import xml.sax.saxutils

import pyarrow as pa
import pyarrow.parquet as pq

repo = pathlib.Path(__file__).resolve().parents[2]
source = pathlib.Path(os.environ["RD_HADOOP_SOURCE"]).resolve()
evidence = pathlib.Path(os.environ["RD_KERBEROS_EVIDENCE"]).resolve()
if not (source / "SOURCE.json").is_file() or not evidence.is_dir():
    raise RuntimeError("verified Hadoop sources and an existing evidence directory are required")
root = pathlib.Path(tempfile.mkdtemp(prefix="rd-krb-cluster-"))


def cleanup_temporary_root():
    if root.exists():
        result = subprocess.run(["trash", str(root)], check=False)
        if result.returncode:
            raise RuntimeError("temporary test credentials could not be cleaned")


atexit.register(cleanup_temporary_root)
signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit("timeout")))
fixture = root / "fixture"
fixture.mkdir()
(fixture / "conf").mkdir()
(fixture / "state").mkdir()
tag = secrets.token_hex(5)
project = "rdkrb" + tag
image = "ray-doris-kerberos-it:" + tag
realm = "RD" + secrets.token_hex(4).upper() + ".TEST"
server = "hdfs/hdfs-probe@" + realm
reader = "reader@" + realm
web = "HTTP/hdfs-probe@" + realm
password = secrets.token_urlsafe(20)
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
port = sock.getsockname()[1]
sock.close()
docker_env = {
    k: v
    for k, v in os.environ.items()
    if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")
}
log = (evidence / "commands.log").open("w")


def run(args, *, env=None, timeout=300, capture=False, check=True):
    log.write("command: " + " ".join(args[:4]) + " ...\n")
    log.flush()
    try:
        result = subprocess.run(
            args,
            cwd=repo,
            env=env or docker_env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE if capture else log,
            stderr=subprocess.STDOUT if not capture else log,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("integration command timed out; see evidence") from None
    if check and result.returncode:
        raise RuntimeError("integration command failed; see evidence")
    return result


def render_xml(values):
    return (
        "<configuration>"
        + "".join(
            "<property><name>"
            + xml.sax.saxutils.escape(key)
            + "</name><value>"
            + xml.sax.saxutils.escape(value)
            + "</value></property>"
            for key, value in values.items()
        )
        + "</configuration>"
    )


def memory_bytes(value):
    value = value.strip()
    for suffix, multiplier in (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024), ("B", 1)):
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * multiplier
    raise RuntimeError("unsupported Docker memory measurement")


def wait_for_resources():
    total = int(run(["docker", "info", "--format", "{{.MemTotal}}"], capture=True).stdout)
    deadline = time.monotonic() + 900
    announced = False
    ready_samples = 0
    while True:
        stats = run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
            capture=True,
            timeout=30,
        )
        entries = [json.loads(line) for line in stats.stdout.splitlines()]
        used = sum(memory_bytes(entry["MemUsage"].split("/")[0]) for entry in entries)
        cpu = sum(float(entry["CPUPerc"].rstrip("%")) for entry in entries)
        if total - used >= 11 * 1024**3 and cpu < 200:
            ready_samples += 1
            if ready_samples >= 3:
                return
        else:
            ready_samples = 0
        if not announced:
            print("Waiting for other Docker work to release resources", flush=True)
            announced = True
        if time.monotonic() >= deadline:
            raise RuntimeError("insufficient Docker resources for the isolated suite")
        time.sleep(15)


def krbconf(host):
    return f"""[libdefaults]
 default_realm = {realm}
 dns_lookup_kdc = false
 dns_lookup_realm = false
 dns_canonicalize_hostname = false
 rdns = false
 renew_lifetime = 5m
[realms]
 {realm} = {{
  kdc = {host}:{port}
 }}
"""


(root / "krb5-host.conf").write_text(krbconf("127.0.0.1"))
(fixture / "krb5.conf").write_text(krbconf("host.docker.internal"))
(root / "kdc.conf").write_text(
    f"""[kdcdefaults]
 kdc_ports = {port}
 kdc_tcp_ports = {port}
[realms]
 {realm} = {{
  database_name = {root}/principal
  key_stash_file = {root}/stash
  acl_file = {root}/kadm5.acl
  max_life = 20m
  max_renewable_life = 1h
 }}
"""
)
kdc_env = dict(
    os.environ,
    KRB5_CONFIG=str(root / "krb5-host.conf"),
    KRB5_KDC_PROFILE=str(root / "kdc.conf"),
)
conf = fixture / "conf"
(conf / "core-site.xml").write_text(
    render_xml(
        {
            "fs.defaultFS": "hdfs://hdfs-probe:8020",
            "hadoop.security.authentication": "kerberos",
            "hadoop.security.authorization": "true",
        }
    )
)
(conf / "hdfs-site.xml").write_text(
    render_xml(
        {
            "dfs.replication": "1",
            "dfs.block.access.token.enable": "true",
            "dfs.namenode.name.dir": "file:///fixture/state/name",
            "dfs.datanode.data.dir": "file:///fixture/state/data",
            "dfs.namenode.kerberos.principal": server,
            "dfs.namenode.keytab.file": "/fixture/server.keytab",
            "dfs.datanode.kerberos.principal": server,
            "dfs.datanode.keytab.file": "/fixture/server.keytab",
            "dfs.datanode.address": "0.0.0.0:9866",
            "dfs.web.authentication.kerberos.principal": web,
            "dfs.web.authentication.kerberos.keytab": "/fixture/server.keytab",
            "dfs.namenode.kerberos.internal.spnego.principal": web,
            "dfs.http.policy": "HTTPS_ONLY",
            "dfs.namenode.https-address": "0.0.0.0:9871",
            "dfs.datanode.https.address": "0.0.0.0:9865",
            "dfs.data.transfer.protection": "authentication",
            "dfs.namenode.safemode.threshold-pct": "0",
        }
    )
)
(conf / "ssl-server.xml").write_text(
    render_xml(
        {
            "ssl.server.keystore.location": "/fixture/state/server.jks",
            "ssl.server.keystore.password": password,
            "ssl.server.keystore.keypassword": password,
        }
    )
)
(fixture / "hdfs-server.sh").write_text("""#!/bin/bash
set -euo pipefail
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
keytool -genkeypair -alias hdfs -keyalg RSA -storepass "$KEYSTORE_PASS" \
  -keypass "$KEYSTORE_PASS" -keystore /fixture/state/server.jks \
  -dname CN=hdfs-probe -validity 1 >/dev/null 2>&1
hdfs namenode -format -nonInteractive > /fixture/state/format.log 2>&1
hdfs namenode > /fixture/state/namenode.log 2>&1 & nn=$!
hdfs datanode > /fixture/state/datanode.log 2>&1 & dn=$!
trap 'kill "$nn" "$dn" 2>/dev/null || true' EXIT
wait -n "$nn" "$dn"
exit 1
""")
(fixture / "hms-only.sh").write_text("""#!/bin/bash
set -euo pipefail
awk '
  /^# start hdfs$/ { skip = 1; next }
  /^# start mysql/ {
    skip = 0
    print "cp /fixture/conf/core-site.xml $HADOOP_CONF_DIR/core-site.xml"
    print "cp /fixture/conf/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml"
    print "sed -i s@hdfs://hms:9000@hdfs://hdfs-probe:8020@g $HIVE_CONF_DIR/hive-site.xml"
  }
  !skip && !/--service hiveserver2/ { print }
' /usr/local/sbin/start.sh > /tmp/rd-hms-only.sh
grep -q -- '--service metastore' /tmp/rd-hms-only.sh
exec bash /tmp/rd-hms-only.sh
""")
for number in range(4):
    pq.write_table(
        pa.table({"id": pa.array(range(number * 10, number * 10 + 10), type=pa.int32())}),
        fixture / f"part-{number}.parquet",
    )

# Select a non-overlapping task-specific subnet and reserve it before building.
existing_subnets = []
network_ids = run(["docker", "network", "ls", "-q"], capture=True).stdout.split()
for network_id in network_ids:
    networks = json.loads(run(["docker", "network", "inspect", network_id], capture=True).stdout)
    for network in networks:
        for item in network.get("IPAM", {}).get("Config") or []:
            if item.get("Subnet"):
                existing_subnets.append(ipaddress.ip_network(item["Subnet"]))
for octet in range(256):
    candidate = ipaddress.ip_network(f"10.248.{octet}.0/24")
    if not any(
        candidate.version == current.version and candidate.overlaps(current)
        for current in existing_subnets
    ):
        break
else:
    raise RuntimeError("no isolated test subnet is available")
subnet = str(candidate)
network_name = project + "net"


def node_ip(index):
    return str(candidate.network_address + index)


fe_config = (
    (repo / "tests/integration/docker/fe.conf").read_text().replace("172.16.127.0/24", subnet)
)
be_config = (
    (repo / "tests/integration/docker/be.conf").read_text().replace("172.16.127.0/24", subnet)
)
(fixture / "fe.conf").write_text(fe_config)
(fixture / "be.conf").write_text(be_config)
mount = str(fixture) + ":/fixture"
java = "-Djava.security.krb5.conf=/fixture/krb5.conf"
hadoop_env = {
    "JAVA_HOME": "/usr/local/jdk",
    "HADOOP_HOME": "/opt/hadoop-3.3.0",
    "HADOOP_INSTALL": "/opt/hadoop-3.3.0",
    "HADOOP_COMMON_HOME": "/opt/hadoop-3.3.0",
    "HADOOP_HDFS_HOME": "/opt/hadoop-3.3.0",
    "HADOOP_MAPRED_HOME": "/opt/hadoop-3.3.0",
    "HADOOP_CONF_DIR": "/fixture/conf",
    "HADOOP_HEAPSIZE": "384",
    "JAVA_TOOL_OPTIONS": java,
    "KRB5CCNAME": "FILE:/fixture/admin.cache",
    "KEYSTORE_PASS": password,
}
ray_env = {
    "PYTHONPATH": "/workspace/src",
    "HADOOP_CONF_DIR": "/fixture/conf",
    "KRB5_CONFIG": "/fixture/krb5.conf",
    "JAVA_TOOL_OPTIONS": java,
    "RAY_DORIS_HDFS_KEYTAB_ROOT": "/fixture",
    "RD_KRB_REALM": realm,
}
services = {
    "hdfs": {
        "image": "apache/gravitino-ci:hive-0.1.20",
        "hostname": "hdfs-probe",
        "entrypoint": ["bash", "/fixture/hdfs-server.sh"],
        "environment": hadoop_env,
        "volumes": [mount],
        "mem_limit": "2g",
        "cpus": 1,
        "networks": {"test": {"ipv4_address": node_ip(4), "aliases": ["hdfs-probe"]}},
    },
    "hms": {
        "image": "apache/gravitino-ci:hive-0.1.20",
        "hostname": "hms",
        "entrypoint": ["bash", "/fixture/hms-only.sh"],
        "environment": {
            "HIVE_RUNTIME_VERSION": "hive3",
            "JAVA_TOOL_OPTIONS": java,
            "KRB5_CONFIG": "/fixture/krb5.conf",
            "KRB5CCNAME": "FILE:/fixture/admin.cache",
        },
        "volumes": [mount],
        "mem_limit": "2560m",
        "cpus": 1,
        "networks": {"test": {"ipv4_address": node_ip(5)}},
    },
    "fe": {
        "image": "apache/doris:fe-4.0.6",
        "hostname": "fe",
        "environment": {
            "FE_SERVERS": f"fe1:{node_ip(2)}:9010",
            "FE_ID": "1",
            "JAVA_TOOL_OPTIONS": java,
        },
        "volumes": [mount, str(fixture / "fe.conf") + ":/opt/apache-doris/fe/conf/fe.conf"],
        "mem_limit": "2560m",
        "cpus": 1,
        "networks": {"test": {"ipv4_address": node_ip(2)}},
    },
    "be": {
        "image": "apache/doris:be-4.0.6",
        "hostname": "be",
        "environment": {
            "FE_SERVERS": f"fe1:{node_ip(2)}:9010",
            "BE_ADDR": f"{node_ip(3)}:9050",
            "JAVA_TOOL_OPTIONS": java,
        },
        "volumes": [
            mount,
            str(fixture / "be.conf") + ":/opt/apache-doris/be/conf/be.conf",
            str(fixture / "krb5.conf") + ":/etc/krb5.conf:ro",
        ],
        "mem_limit": "3g",
        "cpus": 1,
        "networks": {"test": {"ipv4_address": node_ip(3)}},
    },
}
for index, name in enumerate(("ray-head", "ray-worker-a", "ray-worker-b"), start=6):
    command = (
        ["ray", "start", "--head", "--port=6379", "--num-cpus=0", "--include-dashboard=false"]
        if name == "ray-head"
        else ["ray", "start", "--address=ray-head:6379", "--num-cpus=1"]
    )
    services[name] = {
        "image": image,
        "user": "0:0",
        "command": command + ["--object-store-memory=134217728", "--block"],
        "environment": ray_env,
        "volumes": [
            mount,
            str(repo / "src") + ":/workspace/src:ro",
            str(repo / "tests") + ":/workspace/tests:ro",
        ],
        "mem_limit": "1536m",
        "cpus": 0.5 if name == "ray-head" else 1,
        "shm_size": "256m",
        "networks": {"test": {"ipv4_address": node_ip(index)}},
    }
compose_file = root / "compose.json"
compose_file.write_text(
    json.dumps(
        {"services": services, "networks": {"test": {"external": True, "name": network_name}}}
    )
)
compose = ["docker", "compose", "-p", project, "-f", str(compose_file)]
kdc = None
try:
    run(["docker", "image", "ls", "--filter", "dangling=true"])
    run(["docker", "system", "df"])
    run(
        [
            "docker",
            "network",
            "create",
            "--label",
            f"rd.kerberos.run={tag}",
            "--subnet",
            subnet,
            network_name,
        ]
    )
    network_probe = run(
        [
            "docker",
            "run",
            "--rm",
            "--pull",
            "never",
            "--name",
            project + "probe",
            "--label",
            f"rd.kerberos.run={tag}",
            "--network",
            network_name,
            "--ip",
            node_ip(9),
            "--memory",
            "256m",
            "--cpus",
            "0.25",
            "--entrypoint",
            "bash",
            "apache/gravitino-ci:hive-0.1.20",
            "-c",
            "hostname -i",
        ],
        capture=True,
        timeout=30,
    )
    if node_ip(9) not in network_probe.stdout.split():
        raise RuntimeError("isolated test network address verification failed")
    wait_for_resources()
    print("Building isolated test runtime", flush=True)
    existing_image = run(["docker", "image", "inspect", image], capture=True, check=False)
    if existing_image.returncode == 0:
        raise RuntimeError("test image tag already exists")
    run(
        [
            "docker",
            "build",
            "--label",
            f"rd.kerberos.run={tag}",
            "--resource",
            "memory=1536m",
            "--resource",
            "cpu-quota=50000",
            "--build-context",
            f"hadoop-native-source={source}",
            "-t",
            image,
            "-f",
            "tests/native_integration/ray.Dockerfile",
            ".",
        ],
        timeout=900,
    )
    wait_for_resources()
    krb = "/opt/homebrew/opt/krb5/"
    run(
        [krb + "sbin/kdb5_util", "create", "-s", "-r", realm, "-P", secrets.token_urlsafe(20)],
        env=kdc_env,
    )
    for principal in (server, web, reader):
        args = "addprinc -randkey " + ("-maxlife 60s " if principal == reader else "") + principal
        run([krb + "sbin/kadmin.local", "-r", realm, "-q", args], env=kdc_env)
        target = fixture / ("reader.keytab" if principal == reader else "server.keytab")
        run(
            [krb + "sbin/kadmin.local", "-r", realm, "-q", f"ktadd -k {target} {principal}"],
            env=kdc_env,
        )
    kdc = subprocess.Popen(
        [krb + "sbin/krb5kdc", "-n", "-r", realm], env=kdc_env, stdout=log, stderr=log
    )
    time.sleep(0.5)
    if kdc.poll() is not None:
        raise RuntimeError("KDC failed to start")
    run(
        [
            krb + "bin/kinit",
            "-k",
            "-t",
            str(fixture / "server.keytab"),
            "-c",
            str(fixture / "admin.cache"),
            server,
        ],
        env=kdc_env,
    )
    fingerprint = hashlib.sha256()
    for path in sorted((repo / "src/ray_doris").glob("*.py")):
        fingerprint.update(path.read_bytes())
    fingerprint.update((repo / "tests/native_integration/kerberos_native_smoke.py").read_bytes())
    fingerprint.update(pathlib.Path(__file__).read_bytes())
    fingerprint.update((repo / "tests/native_integration/ray.Dockerfile").read_bytes())
    fingerprint.update((repo / "pyproject.toml").read_bytes())
    fingerprint.update((source / "SOURCE.json").read_bytes())
    (evidence / "source-fingerprint.txt").write_text(fingerprint.hexdigest() + "\n")
    print("Starting dedicated Doris, HDFS, and Ray services", flush=True)
    run(
        compose
        + [
            "up",
            "-d",
            "--no-build",
            "hdfs",
            "fe",
            "ray-head",
            "ray-worker-a",
            "ray-worker-b",
        ],
        timeout=300,
    )
    deadline = time.monotonic() + 180
    while True:
        result = run(
            compose + ["exec", "-T", "hdfs", "/opt/hadoop-3.3.0/bin/hdfs", "dfsadmin", "-report"],
            capture=True,
            check=False,
            timeout=20,
        )
        if result.returncode == 0 and "Live datanodes (1)" in result.stdout:
            break
        if time.monotonic() > deadline:
            raise RuntimeError("HDFS readiness timed out")
        time.sleep(3)
    for command in (["-mkdir", "-p", "/lake/data"], ["-chown", "reader", "/lake/data"]):
        run(compose + ["exec", "-T", "hdfs", "/opt/hadoop-3.3.0/bin/hdfs", "dfs", *command])
    for number in range(4):
        run(
            compose
            + [
                "exec",
                "-T",
                "hdfs",
                "/opt/hadoop-3.3.0/bin/hdfs",
                "dfs",
                "-put",
                f"/fixture/part-{number}.parquet",
                "/lake/data/",
            ]
        )
    print("Waiting for the dedicated FE before starting BE", flush=True)
    deadline = time.monotonic() + 240
    check_fe = (
        "import pymysql; "
        "c=pymysql.connect(host='fe',port=9030,user='root',connect_timeout=2); "
        "q=c.cursor(); q.execute('SHOW FRONTENDS'); "
        "assert q.fetchall(); c.close()"
    )
    while True:
        result = run(
            compose + ["exec", "-T", "ray-head", "python", "-c", check_fe],
            capture=True,
            check=False,
            timeout=15,
        )
        if result.returncode == 0:
            break
        if time.monotonic() > deadline:
            raise RuntimeError("FE readiness timed out")
        time.sleep(3)
    print("Starting dedicated Doris BE and Hive Metastore", flush=True)
    run(compose + ["up", "-d", "--no-build", "be", "hms"], timeout=180)
    print("Running Doris catalog, multi-worker read, and ticket renewal checks", flush=True)
    run(
        compose
        + [
            "exec",
            "-T",
            "ray-head",
            "bash",
            "-c",
            'export CLASSPATH="/fixture/conf:$(/opt/hadoop/bin/hadoop classpath --glob)"; '
            "exec python -u /workspace/tests/native_integration/kerberos_native_smoke.py",
        ],
        timeout=600,
    )
    print("Kerberos HDFS multi-worker suite passed", flush=True)
finally:
    try:
        run(compose + ["logs", "--no-color"], check=False, timeout=45)
        run(compose + ["down", "--volumes", "--remove-orphans"], check=False, timeout=90)
        owned_containers = run(
            ["docker", "ps", "-aq", "--filter", f"label=rd.kerberos.run={tag}"],
            capture=True,
        )
        for container_id in owned_containers.stdout.split():
            run(["docker", "rm", "-f", container_id], timeout=30)
        owned_networks = run(
            ["docker", "network", "ls", "-q", "--filter", f"label=rd.kerberos.run={tag}"],
            capture=True,
        )
        for network_id in owned_networks.stdout.split():
            run(["docker", "network", "rm", network_id], timeout=30)
        if run(
            ["docker", "network", "ls", "-q", "--filter", f"label=rd.kerberos.run={tag}"],
            capture=True,
        ).stdout.strip():
            raise RuntimeError("isolated test network cleanup incomplete")
        owned_image = run(
            [
                "docker",
                "image",
                "inspect",
                image,
                "--format",
                '{{index .Config.Labels "rd.kerberos.run"}}',
            ],
            capture=True,
            check=False,
        )
        if owned_image.returncode == 0 and owned_image.stdout.strip() == tag:
            run(["docker", "image", "rm", image], timeout=30)
        project_filter = "label=com.docker.compose.project=" + project
        for kind, command in (
            ("container", ["docker", "ps", "-aq"]),
            ("network", ["docker", "network", "ls", "-q"]),
            ("volume", ["docker", "volume", "ls", "-q"]),
        ):
            remaining = run(command + ["--filter", project_filter], capture=True)
            if remaining.stdout.strip():
                raise RuntimeError("isolated test " + kind + " cleanup incomplete")
        run(["docker", "image", "ls", "--filter", "dangling=true"], check=False)
        run(["docker", "system", "df"], check=False)
    finally:
        if kdc is not None and kdc.poll() is None:
            kdc.terminate()
            kdc.wait(timeout=10)
        for name in ("format.log", "namenode.log", "datanode.log"):
            path = fixture / "state" / name
            if path.exists():
                (evidence / ("hdfs-" + name)).write_bytes(path.read_bytes())
        log.close()
        cleanup_temporary_root()
        atexit.unregister(cleanup_temporary_root)
