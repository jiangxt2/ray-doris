"""Check Kerberos fixture cleanup when startup fails before Docker resources exist."""

import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("signal_parent", [False, True])
def test_early_kerberos_fixture_failure_cleans_temporary_directory(tmp_path, signal_parent):
    source = tmp_path / "source"
    evidence = tmp_path / "evidence"
    scratch = tmp_path / "scratch"
    binaries = tmp_path / "bin"
    for directory in (source, evidence, scratch, binaries):
        directory.mkdir()
    (source / "SOURCE.json").write_text("{}")
    docker = binaries / "docker"
    signal_command = 'kill -TERM "$PPID"\n' if signal_parent else ""
    docker.write_text("#!/bin/sh\n" + signal_command + "exit 1\n")
    docker.chmod(0o755)
    trash = binaries / "trash"
    trash.write_text('#!/bin/sh\nmv "$1" "$1.recycled"\n')
    trash.chmod(0o755)
    script = Path(__file__).resolve().parents[1] / "native_integration/kerberos_cluster_run.py"
    env = dict(
        os.environ,
        RD_HADOOP_SOURCE=str(source),
        RD_KERBEROS_EVIDENCE=str(evidence),
        TMPDIR=str(scratch),
        PATH=f"{binaries}{os.pathsep}{os.environ.get('PATH', '')}",
        PYTHONDONTWRITEBYTECODE="1",
    )
    bootstrap = """\
import pyarrow.parquet
import runpy
import socket
import ssl
import sys
class SocketStub:
    def bind(self, address): pass
    def getsockname(self): return ('127.0.0.1', 1)
    def close(self): pass
socket.socket = lambda *args, **kwargs: SocketStub()
runpy.run_path(sys.argv[1], run_name='__main__')
"""
    result = subprocess.run(
        [sys.executable, "-c", bootstrap, str(script)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    expected = "timeout" if signal_parent else "integration command failed"
    assert expected in result.stderr
    roots = list(scratch.glob("rd-krb-cluster-*"))
    assert len(roots) == 1 and roots[0].name.endswith(".recycled")
