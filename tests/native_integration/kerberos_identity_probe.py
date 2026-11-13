"""Probe isolation between two real HDFS ticket-cache identities in one JVM."""

import os
import pathlib
import secrets
import signal
import socket
import subprocess
import time
import xml.sax.saxutils

root = pathlib.Path(os.environ["RD_HDFS_IDENTITY_DIR"]).resolve()
evidence = pathlib.Path(os.environ["RD_HDFS_IDENTITY_EVIDENCE"]).resolve()
if not root.is_dir() or any(root.iterdir()) or not evidence.is_dir():
    raise RuntimeError("the fixture directory must exist and be empty; evidence must exist")
realm = "RD" + secrets.token_hex(4).upper() + ".TEST"
tag = secrets.token_hex(5)
container = "rd-kerb-ident-" + tag
network = "rd-kerb-ident-" + tag
krb = "/opt/homebrew/opt/krb5/"
s = socket.socket()
s.bind(("127.0.0.1", 0))
port = s.getsockname()[1]
s.close()
config = root / "conf"
config.mkdir(exist_ok=True)
(root / "state").mkdir(exist_ok=True)


def krb_conf(host):
    return f"""[libdefaults]
 default_realm = {realm}
 dns_lookup_kdc = false
 dns_lookup_realm = false
 rdns = false
 dns_canonicalize_hostname = false
[realms]
 {realm} = {{
  kdc = {host}:{port}
 }}
"""


(root / "krb5-host.conf").write_text(krb_conf("127.0.0.1"))
(root / "krb5.conf").write_text(krb_conf("host.docker.internal"))
(root / "kdc.conf").write_text(
    f"""[kdcdefaults]
 kdc_ports = {port}
 kdc_tcp_ports = {port}
[realms]
 {realm} = {{
  database_name = {root}/principal
  key_stash_file = {root}/stash
  acl_file = {root}/kadm5.acl
  max_life = 5m
  max_renewable_life = 30m
 }}
"""
)
env = dict(
    os.environ, KRB5_CONFIG=str(root / "krb5-host.conf"), KRB5_KDC_PROFILE=str(root / "kdc.conf")
)
docker_env = {
    k: v
    for k, v in os.environ.items()
    if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")
}
log = (evidence / "hdfs-identity.log").open("w")


def run(args, *, use_env=env, check=True, timeout=30):
    try:
        result = subprocess.run(
            args,
            env=use_env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("command timed out: " + args[0]) from None
    if check and result.returncode:
        raise RuntimeError("failed command " + args[0] + " output: " + result.stdout[-2500:])
    return result


def render_xml(props):
    return (
        "<configuration>"
        + "".join(
            "<property><name>"
            + xml.sax.saxutils.escape(k)
            + "</name><value>"
            + xml.sax.saxutils.escape(v)
            + "</value></property>"
            for k, v in props.items()
        )
        + "</configuration>"
    )


server_principal = "hdfs/hdfs-probe@" + realm
web_principal = "HTTP/hdfs-probe@" + realm
password = secrets.token_urlsafe(20)
(config / "core-site.xml").write_text(
    render_xml(
        {
            "fs.defaultFS": "hdfs://hdfs-probe:8020",
            "hadoop.security.authentication": "kerberos",
            "hadoop.security.authorization": "true",
        }
    )
)
(config / "hdfs-site.xml").write_text(
    render_xml(
        {
            "dfs.replication": "1",
            "dfs.block.access.token.enable": "true",
            "dfs.namenode.name.dir": "file:///fixture/state/name",
            "dfs.datanode.data.dir": "file:///fixture/state/data",
            "dfs.namenode.kerberos.principal": server_principal,
            "dfs.namenode.keytab.file": "/fixture/server.keytab",
            "dfs.datanode.kerberos.principal": server_principal,
            "dfs.datanode.keytab.file": "/fixture/server.keytab",
            "dfs.datanode.address": "0.0.0.0:9866",
            "dfs.web.authentication.kerberos.principal": web_principal,
            "dfs.web.authentication.kerberos.keytab": "/fixture/server.keytab",
            "dfs.namenode.kerberos.internal.spnego.principal": web_principal,
            "dfs.http.policy": "HTTPS_ONLY",
            "dfs.namenode.https-address": "0.0.0.0:9871",
            "dfs.datanode.https.address": "0.0.0.0:9865",
            "dfs.data.transfer.protection": "authentication",
            "dfs.namenode.safemode.threshold-pct": "0",
            "dfs.permissions.enabled": "true",
        }
    )
)
(config / "ssl-server.xml").write_text(
    render_xml(
        {
            "ssl.server.keystore.location": "/fixture/state/server.jks",
            "ssl.server.keystore.password": password,
            "ssl.server.keystore.keypassword": password,
        }
    )
)
(root / "input.txt").write_text("isolation-check")
(root / "IdentityProbe.java").write_text(
    """import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
public class IdentityProbe {
  public static void main(String[] args) throws Exception {
    Configuration base = new Configuration();
    UserGroupInformation.setConfiguration(base);
    Configuration ac = new Configuration(base), bc = new Configuration(base);
    ac.set("hadoop.security.kerberos.ticket.cache.path", "/fixture/alice.cache");
    bc.set("hadoop.security.kerberos.ticket.cache.path", "/fixture/bob.cache");
    URI uri = URI.create("hdfs://hdfs-probe:8020");
    FileSystem a = FileSystem.get(uri, ac, args[0]);
    FileSystem b = FileSystem.get(uri, bc, args[1]);
    try (var stream = a.open(new Path("/alice/input.txt"))) {
      if (stream.read() != 'i') throw new RuntimeException("alice data mismatch");
    }
    boolean denied = false;
    try (var stream = b.open(new Path("/alice/input.txt"))) { stream.read(); }
    catch (java.io.IOException e) {
      if (!e.toString().contains("AccessControlException")) throw e;
      denied = true;
    }
    if (!denied) throw new RuntimeException("bob accessed alice data");
    try (var stream = a.open(new Path("/alice/input.txt"))) {
      if (stream.read() != 'i') throw new RuntimeException("alice after bob mismatch");
    }
    System.out.println("same-JVM HDFS: alice allowed, bob denied, alice still allowed");
  }
}""".replace("try (var stream", "try (org.apache.hadoop.fs.FSDataInputStream stream")
)
(root / "run.sh").write_text("""#!/bin/bash
set -euo pipefail
export JAVA_HOME=/usr/local/jdk HADOOP_HOME=/opt/hadoop-3.3.0 HADOOP_CONF_DIR=/fixture/conf
export HADOOP_INSTALL="$HADOOP_HOME" HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME" HADOOP_MAPRED_HOME="$HADOOP_HOME"
export JAVA_TOOL_OPTIONS=-Djava.security.krb5.conf=/fixture/krb5.conf
export HADOOP_HEAPSIZE=384 HADOOP_ROOT_LOGGER=ERROR,console KRB5CCNAME=FILE:/fixture/admin.cache
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
keytool -genkeypair -alias hdfs -keyalg RSA -storepass "$KEYSTORE_PASS" \
  -keypass "$KEYSTORE_PASS" -keystore /fixture/state/server.jks \
  -dname CN=hdfs-probe -validity 1 >/dev/null 2>&1
hdfs namenode -format -nonInteractive > /fixture/state/format.log 2>&1
hdfs namenode > /fixture/state/namenode.log 2>&1 &
nn=$!
hdfs datanode > /fixture/state/datanode.log 2>&1 &
dn=$!
trap 'kill "$nn" "$dn" 2>/dev/null || true' EXIT
for n in $(seq 1 75); do
  if hdfs dfs -ls / >/dev/null 2>&1; then break; fi
  kill -0 "$nn" "$dn" 2>/dev/null || { echo "HDFS process exited"; exit 1; }
  sleep 2
done
hdfs dfsadmin -safemode leave >/dev/null
for n in $(seq 1 60); do
  if hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (1)'; then break; fi
  kill -0 "$nn" "$dn" 2>/dev/null || { echo "HDFS process exited"; exit 1; }
  sleep 2
done
hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (1)'
hdfs dfs -mkdir /alice
hdfs dfs -chown alice /alice
hdfs dfs -chmod 700 /alice
hdfs dfs -put /fixture/input.txt /alice/input.txt
cp=$(/opt/hadoop-3.3.0/bin/hadoop classpath --glob)
javac -cp "$cp" /fixture/IdentityProbe.java
java -cp "/fixture:$cp" IdentityProbe "$ALICE_PRINCIPAL" "$BOB_PRINCIPAL"
""")
(root / "run.sh").chmod(0o700)
kdc = None
signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit("timeout")))
try:
    run([krb + "sbin/kdb5_util", "create", "-s", "-r", realm, "-P", secrets.token_urlsafe(24)])
    for principal in (server_principal, web_principal, "alice@" + realm, "bob@" + realm):
        run([krb + "sbin/kadmin.local", "-r", realm, "-q", f"addprinc -randkey {principal}"])
        keytab = root / (
            "server.keytab" if "/" in principal else principal.split("@")[0] + ".keytab"
        )
        run([krb + "sbin/kadmin.local", "-r", realm, "-q", f"ktadd -k {keytab} {principal}"])
    kdc = subprocess.Popen(
        [krb + "sbin/krb5kdc", "-n", "-r", realm], env=env, stdout=log, stderr=log
    )
    time.sleep(0.5)
    if kdc.poll() is not None:
        raise RuntimeError("isolated KDC failed to start")
    for name, principal, keytab in [
        ("admin", server_principal, "server.keytab"),
        ("alice", "alice@" + realm, "alice.keytab"),
        ("bob", "bob@" + realm, "bob.keytab"),
    ]:
        run(
            [
                krb + "bin/kinit",
                "-k",
                "-t",
                str(root / keytab),
                "-c",
                str(root / (name + ".cache")),
                principal,
            ]
        )
    run(
        ["docker", "network", "create", "--label", f"rd.kerberos.run={tag}", network],
        use_env=docker_env,
    )
    print("Starting isolated secure HDFS identity preflight", flush=True)
    command = [
        "docker",
        "run",
        "--rm",
        "--name",
        container,
        "--label",
        f"rd.kerberos.run={tag}",
        "--network",
        network,
        "--hostname",
        "hdfs-probe",
        "--memory",
        "2560m",
        "--cpus",
        "1.5",
        "-v",
        f"{root}:/fixture",
        "-e",
        f"KEYSTORE_PASS={password}",
        "-e",
        f"ALICE_PRINCIPAL=alice@{realm}",
        "-e",
        f"BOB_PRINCIPAL=bob@{realm}",
        "--entrypoint",
        "bash",
        "apache/gravitino-ci:hive-0.1.20",
        "/fixture/run.sh",
    ]
    result = run(command, use_env=docker_env, check=False, timeout=225)
    log.write(result.stdout)
    log.flush()
    if result.returncode:
        raise RuntimeError("HDFS identity preflight failed; see log")
    print(result.stdout.strip(), flush=True)
finally:
    try:
        failures = []
        for kind, listing, removal in (
            ("container", ["docker", "ps", "-aq"], ["docker", "rm", "-f"]),
            ("network", ["docker", "network", "ls", "-q"], ["docker", "network", "rm"]),
        ):
            try:
                command = listing + ["--filter", f"label=rd.kerberos.run={tag}"]
                for owned_id in run(command, use_env=docker_env, timeout=15).stdout.split():
                    run(removal + [owned_id], use_env=docker_env, timeout=15)
                if run(command, use_env=docker_env, timeout=15).stdout.strip():
                    raise RuntimeError("owned resources remain")
            except Exception:
                failures.append(kind)
        if failures:
            raise RuntimeError("unable to verify cleanup: " + ", ".join(failures))
    finally:
        if kdc is not None and kdc.poll() is None:
            kdc.terminate()
            kdc.wait(timeout=10)
        for name in ("format.log", "namenode.log", "datanode.log"):
            path = root / "state" / name
            if path.exists():
                (evidence / ("hdfs-identity-" + name)).write_bytes(path.read_bytes())
        log.close()
    print("Isolated HDFS container, network and KDC stopped", flush=True)
