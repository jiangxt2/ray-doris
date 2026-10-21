"""Fetch pinned Apache Hadoop JNI sources, verifying their Git blob identities."""

import argparse
import hashlib
import json
from pathlib import Path
from urllib.request import Request, build_opener

COMMIT = "aa96f1871bfd858f9bac59cf2a81ec470da649af"  # Hadoop rel/release-3.3.0
PREFIX = "hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--destination", required=True)
    destination = Path(parser.parse_args().destination)
    destination.mkdir(parents=True, exist_ok=False)
    # Use standard HTTPS_PROXY/HTTP_PROXY configuration, without embedding a
    # developer-specific proxy in the portable test helper.
    opener = build_opener()
    manifest = {"commit": COMMIT, "files": {}}

    def visit(relative):
        path = PREFIX + ("/" + relative if relative else "")
        request = Request(
            f"https://api.github.com/repos/apache/hadoop/contents/{path}?ref={COMMIT}",
            headers={"User-Agent": "ray-doris-native-integration"},
        )
        with opener.open(request, timeout=30) as response:
            entries = json.load(response)
        for entry in entries:
            name = "/".join(part for part in [relative, entry["name"]] if part)
            if entry["type"] == "dir" and name in ("include", "include/hdfs", "os", "os/posix"):
                visit(name)
            elif entry["type"] == "file" and entry["name"].endswith((".c", ".h")):
                with opener.open(entry["download_url"], timeout=30) as response:
                    data = response.read()
                actual = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
                if actual != entry["sha"]:
                    raise RuntimeError("Apache Hadoop source blob checksum mismatch")
                target = destination / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                manifest["files"][name] = actual

    visit("")
    (destination / "SOURCE.json").write_text(json.dumps(manifest, indent=2))
    print("Verified Hadoop JNI sources:", COMMIT, len(manifest["files"]))


if __name__ == "__main__":
    main()
