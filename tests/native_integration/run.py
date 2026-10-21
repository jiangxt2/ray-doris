"""Run the approved cached-image matrix with exact cleanup and durable logs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import signal
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PREFIX = [
    "docker",
    "compose",
    "-p",
    "ray-doris-it",
    "-f",
    str(ROOT / "tests/integration/docker-compose.yml"),
    "-f",
    str(ROOT / "tests/native_integration/docker-compose.yml"),
]


@contextmanager
def cleanup_on_termination(cleanup):
    """Turn SIGTERM into stack unwinding, then finish scoped cleanup once."""

    def terminate(signum, _frame):
        raise SystemExit(128 + signum)

    previous = signal.signal(signal.SIGTERM, terminate)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            cleanup()
        finally:
            signal.signal(signal.SIGTERM, previous)


def verify_cleanup(run, owned_volumes, images):
    """Fail on remaining owned resources or an unavailable Docker daemon."""
    project_filter = "label=com.docker.compose.project=ray-doris-it"
    containers = run(["docker", "ps", "-aq", "--filter", project_filter], capture=True).split()
    networks = run(
        ["docker", "network", "ls", "-q", "--filter", project_filter], capture=True
    ).split()
    volumes = set(run(["docker", "volume", "ls", "--format", "{{.Name}}"], capture=True).split())
    tags = set(
        run(["docker", "image", "ls", "--format", "{{.Repository}}:{{.Tag}}"], capture=True).split()
    )
    remaining = {
        "containers": containers,
        "networks": networks,
        "volumes": sorted(volumes.intersection(owned_volumes)),
        "images": sorted(tags.intersection(images)),
    }
    if any(remaining.values()):
        raise RuntimeError(
            "native integration cleanup left owned resources: " + json.dumps(remaining)
        )
    return remaining


def stream_command(process, out, redactions):
    """Reap this command before outer cleanup can remove its resources."""
    assert process.stdout is not None
    try:
        for line in process.stdout:
            for secret in redactions:
                line = line.replace(secret, "<redacted>")
            out.write(line)
            out.flush()
        return process.wait()
    except BaseException:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        raise
    finally:
        process.stdout.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--versions", nargs="+", default=["4.0.6", "3.0.6.2"])
    parser.add_argument("--logs", required=True)
    parser.add_argument("--hadoop-source", required=True)
    parser.add_argument("--skip-query-regression", action="store_true")
    args = parser.parse_args()
    hadoop_source = Path(args.hadoop_source).resolve()
    if not (hadoop_source / "SOURCE.json").is_file():
        parser.error("prepare the pinned Hadoop JNI source context first")
    if any(v not in ("4.0.6", "3.0.6.2") for v in args.versions):
        parser.error("only the approved cached versions are accepted")
    logs = Path(args.logs).resolve()
    logs.mkdir(parents=True, exist_ok=False)
    env = {
        k: v
        for k, v in os.environ.items()
        if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")
    }
    tag = "native-" + str(int(time.time()))
    env.update(
        NATIVE_RAY_IMAGE="ray-doris-it-ray:" + tag,
        NATIVE_S3_USER="native" + secrets.token_hex(6),
        NATIVE_S3_PASSWORD=secrets.token_hex(24),
        NATIVE_LOG_DIR=str(logs),
        NATIVE_FIXTURE_DIR=str(ROOT / "tests/native_integration"),
        NATIVE_SOURCE_DIR=str(ROOT),
        NATIVE_DORIS_VERSION=args.versions[0],
        NATIVE_FE_IMAGE="ray-doris-it-fe:" + tag,
        NATIVE_BE_IMAGE="ray-doris-it-be:" + tag,
    )
    logfile = logs / "commands.log"
    images = []
    results = []
    owned_volumes = set()

    def run(command, *, check=True, capture=False):
        with logfile.open("a") as out:
            out.write("$ " + " ".join(command) + "\n")
            out.flush()
            if capture:
                result = subprocess.run(
                    command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=out, text=True
                )
                code, output = result.returncode, result.stdout
            else:
                process = subprocess.Popen(
                    command,
                    cwd=ROOT,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                code = stream_command(
                    process, out, [env[key] for key in ["NATIVE_S3_USER", "NATIVE_S3_PASSWORD"]]
                )
                output = ""
        if check and code:
            raise RuntimeError(f"command failed ({code}); see commands.log")
        return output if capture else code

    def compose(*command, check=True):
        return run(PREFIX + list(command), check=check)

    existing = run(
        ["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project=ray-doris-it"],
        capture=True,
    )
    if existing.strip():
        raise RuntimeError("ray-doris-it resources already exist; refusing to overwrite them")
    for volume in ["ray-doris-it_fe-data", "ray-doris-it_be-data"]:
        if run(["docker", "volume", "inspect", volume], check=False) == 0:
            raise RuntimeError("prior Doris test volume exists; refusing to overwrite it")
    fingerprints = hashlib.sha256()
    for directory in [ROOT / "src", ROOT / "tests"]:
        for file in sorted(directory.rglob("*")):
            if file.suffix in (".py", ".yml", ".conf", ".sh", ".Dockerfile"):
                fingerprints.update(str(file.relative_to(ROOT)).encode())
                fingerprints.update(file.read_bytes())
    fingerprints.update((ROOT / "pyproject.toml").read_bytes())
    fingerprints.update((hadoop_source / "SOURCE.json").read_bytes())
    (logs / "hadoop-source.json").write_bytes((hadoop_source / "SOURCE.json").read_bytes())
    (logs / "source-fingerprint.txt").write_text(fingerprints.hexdigest() + "\n")
    run(["docker", "image", "ls", "--filter", "dangling=true"])
    run(["docker", "system", "df"])
    bases = [
        "apache/gravitino-ci:hive-0.1.20",
        "rayproject/ray:2.59.0-py312",
        "apache/spark:3.5.8",
        "minio/minio:RELEASE.2025-09-07T16-13-09Z",
    ]
    bases += [f"apache/doris:{role}-{v}" for v in args.versions for role in ["fe", "be"]]
    metadata = run(["docker", "image", "inspect", *bases], capture=True)
    inspected = json.loads(metadata)
    (logs / "base-images.json").write_text(
        json.dumps(
            [
                {k: image.get(k) for k in ["Id", "RepoTags", "RepoDigests", "Architecture"]}
                for image in inspected
            ],
            indent=2,
        )
    )

    def cleanup():
        # Never remove bases, existing shared images, or global build cache.
        try:
            compose("logs", "--no-color", check=False)
            compose("down", "--volumes", "--remove-orphans", check=False)
            for volume in sorted(owned_volumes):
                run(["docker", "volume", "rm", volume], check=False)
            for name in images:
                run(["docker", "image", "rm", name], check=False)
            run(["docker", "image", "ls", "--filter", "dangling=true"], check=False)
            run(["docker", "system", "df"], check=False)
            remaining = verify_cleanup(run, owned_volumes, images)
            (logs / "cleanup.json").write_text(
                json.dumps({"status": "passed", **remaining}, indent=2)
            )
        except Exception:
            (logs / "cleanup.json").write_text(json.dumps({"status": "failed"}))
            raise

    with cleanup_on_termination(cleanup):
        images.append(env["NATIVE_RAY_IMAGE"])
        run(
            [
                "docker",
                "build",
                "--build-context",
                "hadoop-native-source=" + str(hadoop_source),
                "--pull=false",
                "-t",
                images[-1],
                "-f",
                "tests/native_integration/ray.Dockerfile",
                ".",
            ]
        )
        for version in args.versions:
            env["NATIVE_DORIS_VERSION"] = version
            for role in ["fe", "be"]:
                name = f"ray-doris-it-{role}:{tag}-{version}"
                images.append(name)
                env[f"NATIVE_{role.upper()}_IMAGE"] = name
                run(
                    [
                        "docker",
                        "build",
                        "--pull=false",
                        "-t",
                        name,
                        "--build-arg",
                        f"DORIS_BASE_IMAGE=apache/doris:{role}-{version}",
                        "--build-arg",
                        f"DORIS_ROLE={role}",
                        "-f",
                        "tests/native_integration/doris.Dockerfile",
                        ".",
                    ]
                )
            compose("up", "-d", "--no-build", "fe", "be", "hms", "minio", "ray-head")
            container_ids = run(
                [
                    "docker",
                    "ps",
                    "-aq",
                    "--filter",
                    "label=com.docker.compose.project=ray-doris-it",
                ],
                capture=True,
            ).split()
            mounted = json.loads(run(["docker", "inspect", *container_ids], capture=True))
            for container in mounted:
                owned_volumes.update(
                    mount["Name"] for mount in container["Mounts"] if mount["Type"] == "volume"
                )
            (logs / "owned-volumes.json").write_text(json.dumps(sorted(owned_volumes)))
            for service in ["hms", "fe", "be", "ray-head", "minio"]:
                running = run(
                    [
                        "docker",
                        "inspect",
                        "--format",
                        "{{.State.Running}}",
                        f"ray-doris-it-{service}-1",
                    ],
                    capture=True,
                )
                if running.strip() != "true":
                    raise RuntimeError(f"{service} exited during startup; inspect service logs")
            run(["docker", "image", "inspect", "--format", "{{.RepoTags}} {{.Id}}", *images])
            compose(
                "exec",
                "-T",
                "ray-head",
                "bash",
                "tests/native_integration/ray-entrypoint.sh",
                "python",
                "tests/native_integration/wait_services.py",
            )
            if version == args.versions[0]:
                compose("run", "--rm", "--no-deps", "spark-producer")
                compose("up", "-d", "--no-build", "ray-worker-a", "ray-worker-b")
            started = time.time()
            result = compose(
                "exec",
                "-T",
                "ray-head",
                "bash",
                "tests/native_integration/ray-entrypoint.sh",
                "python",
                "-m",
                "pytest",
                "tests/native_integration/test_native_cluster.py",
                "-v",
                "--maxfail=1",
                "--tb=short",
                f"--junitxml=/evidence/native-{version}.xml",
                check=False,
            )
            results.append(
                {"version": version, "exit_code": result, "seconds": time.time() - started}
            )
            (logs / "results.json").write_text(json.dumps(results, indent=2))
            compose("logs", "--no-color", "fe", "be")
            if result:
                raise RuntimeError(
                    f"native {version} integration failed; preserve and diagnose logs"
                )
            if version == "4.0.6" and not args.skip_query_regression:
                regression = run(
                    [
                        str(ROOT / ".venv/bin/python"),
                        "-m",
                        "pytest",
                        "tests/integration",
                        "-v",
                        f"--junitxml={logs}/query-regression.xml",
                    ],
                    check=False,
                )
                results.append(
                    {"version": version, "suite": "query-regression", "exit_code": regression}
                )
                (logs / "results.json").write_text(json.dumps(results, indent=2))
                if regression:
                    raise RuntimeError("existing integration regression failed")
            compose("stop", "fe", "be")
            compose("rm", "-f", "fe", "be")
            run(["docker", "volume", "rm", "ray-doris-it_fe-data", "ray-doris-it_be-data"])
    print("Native matrix passed; evidence:", logs)


if __name__ == "__main__":
    main()
