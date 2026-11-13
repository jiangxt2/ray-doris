"""Exercise process-local HDFS credential isolation and cleanup."""

import datetime
import gc
import os
import pathlib
import pickle
import signal
import subprocess
import sys
import threading
import time
import weakref
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow.fs as fs
import pytest
import ray.cloudpickle

from ray_doris._errors import DorisConfigurationError, DorisPlanningError
from ray_doris._hdfs_auth import HdfsKerberosConfig
from ray_doris._hdfs_proxy import HdfsKerberosHandler, _check_keytab, _RetainedInput, _TicketLease


def ticket_listing():
    expiry = (datetime.datetime.now() + datetime.timedelta(minutes=10)).strftime(
        "%m/%d/%y %H:%M:%S"
    )
    return f"11/06/26 00:00:00 {expiry} krbtgt/TEST@TEST\n"


def successful_command(args):
    if args[:2] == ["kinit", "-k"]:
        pathlib.Path(args[-2]).write_text("synthetic")
    return SimpleNamespace(returncode=0, stdout=ticket_listing())


def test_keytab_must_be_in_authorized_root(tmp_path, monkeypatch):
    root = tmp_path / "trusted"
    root.mkdir()
    keytab = root / "reader.keytab"
    keytab.write_bytes(b"synthetic")
    other = tmp_path / "other.keytab"
    other.write_bytes(b"synthetic")
    monkeypatch.setenv("RAY_DORIS_HDFS_KEYTAB_ROOT", str(root))
    _check_keytab(str(keytab))
    with pytest.raises(DorisConfigurationError):
        _check_keytab(str(other))
    (root / "outside").symlink_to(other)
    with pytest.raises(DorisConfigurationError):
        _check_keytab(str(root / "outside"))
    monkeypatch.setenv("RAY_DORIS_HDFS_KEYTAB_ROOT", "/")
    with pytest.raises(DorisConfigurationError):
        _check_keytab(str(keytab))


def test_keytab_failure_includes_doris_retry_hint(tmp_path, monkeypatch):
    root = tmp_path / "trusted"
    root.mkdir()
    monkeypatch.setenv("RAY_DORIS_HDFS_KEYTAB_ROOT", str(root))
    handler = HdfsKerberosHandler(
        "namenode", 8020, HdfsKerberosConfig("reader@EXAMPLE.COM", str(root / "missing")), {}
    )
    with pytest.raises(DorisConfigurationError, match="auto_native_read=False") as error:
        handler.normalize_path("/table")
    assert str(root) not in str(error.value)


@pytest.mark.parametrize("serializer", [pickle, ray.cloudpickle])
def test_arrow_proxy_serialization_does_not_serialize_live_state(serializer):
    config = HdfsKerberosConfig("reader@EXAMPLE.COM", "/secure/reader.keytab")
    handler = HdfsKerberosHandler("hdfs", 8020, config, {})
    handler._lease = SimpleNamespace(secret="SYNTHETIC-DO-NOT-SERIALIZE")
    payload = serializer.dumps(fs.PyFileSystem(handler))
    assert b"SYNTHETIC-DO-NOT-SERIALIZE" not in payload
    restored = serializer.loads(payload)
    assert restored.handler._lease is None
    assert restored.handler == handler


def test_proxy_reopens_backend_after_ticket_refresh(tmp_path, monkeypatch):
    class Lease:
        version = 1

        def __init__(self, config):
            self.config = config
            self._lock = threading.RLock()

        def snapshot(self):
            return "/private/cache", self.version

    calls = []

    def new_filesystem(host, port, **kwargs):
        calls.append((host, port, kwargs))
        return fs.LocalFileSystem()

    monkeypatch.setattr("ray_doris._hdfs_proxy._TicketLease", Lease)
    monkeypatch.setattr("ray_doris._hdfs_proxy.fs.HadoopFileSystem", new_filesystem)
    config = HdfsKerberosConfig("reader@EXAMPLE.COM", "/trusted/reader.keytab")
    handler = HdfsKerberosHandler("namenode", 8020, config, {})
    proxy = fs.PyFileSystem(handler)
    assert proxy.get_file_info(str(tmp_path)).type == fs.FileType.Directory
    assert handler._lease is not None
    handler._lease.version += 1
    assert proxy.get_file_info(str(tmp_path)).type == fs.FileType.Directory
    assert len(calls) == 2
    assert all(call[2]["user"] == config.principal for call in calls)
    assert all(call[2]["kerb_ticket"] == "/private/cache" for call in calls)


def test_hdfs_connection_error_does_not_expose_ticket_path(monkeypatch):
    class Lease:
        def __init__(self, config):
            self._lock = threading.RLock()

        def snapshot(self):
            return "/private/secret-cache", 1

    def failed_connection(*args, **kwargs):
        raise OSError("secret-cache")

    monkeypatch.setattr("ray_doris._hdfs_proxy._TicketLease", Lease)
    monkeypatch.setattr("ray_doris._hdfs_proxy.fs.HadoopFileSystem", failed_connection)
    config = HdfsKerberosConfig("reader@EXAMPLE.COM", "/trusted/reader.keytab")
    handler = HdfsKerberosHandler("namenode", 8020, config, {})
    with pytest.raises(DorisPlanningError) as error:
        handler.normalize_path("/table")
    assert "secret-cache" not in str(error.value)
    assert "auto_native_read=False" in str(error.value)
    assert error.value.__cause__ is None


def test_hdfs_file_info_error_keeps_original_and_logs_retry_hint(monkeypatch, caplog):
    failure = OSError("sensitive-source")
    backend = Mock()
    backend.get_file_info.side_effect = failure
    handler = HdfsKerberosHandler(
        "namenode", 8020, HdfsKerberosConfig("reader@EXAMPLE.COM", "/trusted/keytab"), {}
    )
    monkeypatch.setattr(handler, "_filesystem", Mock(return_value=backend))
    with pytest.raises(OSError) as captured:
        handler.get_file_info(["/data"])
    assert captured.value is failure
    assert "auto_native_read=False" in caplog.text
    assert "sensitive-source" not in caplog.text


def test_hdfs_stream_error_keeps_original_and_logs_retry_hint(caplog):
    failure = OSError("sensitive-source")
    stream = Mock()
    stream.read.side_effect = failure
    retained = _RetainedInput(stream, Mock(), Mock())
    try:
        with pytest.raises(OSError) as captured:
            retained.read(4)
        assert captured.value is failure
        assert "auto_native_read=False" in caplog.text
        assert "sensitive-source" not in caplog.text
    finally:
        retained.close()


def test_backend_connect_holds_ticket_lock(monkeypatch):
    class Lease:
        def __init__(self, config):
            self._lock = threading.RLock()

        def snapshot(self):
            return "/private/cache", 1

    connecting = threading.Event()
    release = threading.Event()
    renewed = threading.Event()

    def connect(*args, **kwargs):
        connecting.set()
        assert release.wait(5)
        return fs.LocalFileSystem()

    monkeypatch.setattr("ray_doris._hdfs_proxy._TicketLease", Lease)
    monkeypatch.setattr("ray_doris._hdfs_proxy.fs.HadoopFileSystem", connect)
    handler = HdfsKerberosHandler(
        "namenode", 8020, HdfsKerberosConfig("reader@EXAMPLE.COM", "/trusted/keytab"), {}
    )
    connector = threading.Thread(target=handler._filesystem)

    def renew():
        assert handler._lease is not None
        with handler._lease._lock:
            renewed.set()

    renewer = threading.Thread(target=renew)
    try:
        connector.start()
        assert connecting.wait(5)
        renewer.start()
        assert not renewed.wait(0.1)
    finally:
        release.set()
        connector.join(timeout=5)
        renewer.join(timeout=5)
    assert renewed.is_set()
    assert not connector.is_alive() and not renewer.is_alive()


def test_open_stream_retains_old_backend_until_stream_closes(tmp_path, monkeypatch):
    class Lease:
        version = 1

        def __init__(self, config):
            self._lock = threading.RLock()

        def snapshot(self):
            return "/private/cache", self.version

    class Backend:
        def __init__(self, *args, **kwargs):
            self.local = fs.LocalFileSystem()

        def open_input_file(self, path):
            return self.local.open_input_file(path)

        def get_file_info(self, paths):
            return self.local.get_file_info(paths)

    monkeypatch.setattr("ray_doris._hdfs_proxy._TicketLease", Lease)
    monkeypatch.setattr("ray_doris._hdfs_proxy.fs.HadoopFileSystem", Backend)
    config = HdfsKerberosConfig("reader@EXAMPLE.COM", "/trusted/reader.keytab")
    handler = HdfsKerberosHandler("namenode", 8020, config, {})
    path = tmp_path / "data"
    path.write_bytes(b"first second")
    proxy = fs.PyFileSystem(handler)
    stream = proxy.open_input_file(str(path))
    first_backend = weakref.ref(handler._backend)
    assert stream.read(5) == b"first"
    handler._lease.version += 1
    proxy.get_file_info(str(path))
    gc.collect()
    assert first_backend() is not None
    assert stream.read() == b" second"
    stream.close()
    del stream
    gc.collect()
    assert first_backend() is None


def test_lease_close_removes_private_cache(tmp_path, monkeypatch):
    root = tmp_path / "trusted"
    root.mkdir()
    keytab = root / "reader.keytab"
    keytab.write_bytes(b"synthetic")
    monkeypatch.setenv("RAY_DORIS_HDFS_KEYTAB_ROOT", str(root))
    monkeypatch.setattr(
        "ray_doris._hdfs_proxy._command",
        successful_command,
    )
    lease = _TicketLease(HdfsKerberosConfig("reader@EXAMPLE.COM", str(keytab)))
    directory = os.path.dirname(lease.path)
    assert os.stat(directory).st_mode & 0o777 == 0o700
    assert os.stat(lease.path).st_mode & 0o777 == 0o600
    with open(lease.path, "w") as cache:
        cache.write("synthetic")
    lease.close()
    assert not os.path.exists(directory)
    with pytest.raises(DorisPlanningError, match="closed"):
        lease.snapshot()


def test_guard_start_failure_removes_cache_directory(tmp_path, monkeypatch):
    keytab = tmp_path / "reader.keytab"
    keytab.write_bytes(b"synthetic")
    directory = tmp_path / "cache"
    directory.mkdir()
    monkeypatch.setenv("RAY_DORIS_HDFS_KEYTAB_ROOT", str(tmp_path))
    monkeypatch.setattr("ray_doris._hdfs_proxy.tempfile.mkdtemp", lambda **kwargs: str(directory))

    def fail_start(*args, **kwargs):
        raise OSError("synthetic-secret")

    monkeypatch.setattr("ray_doris._hdfs_proxy.subprocess.Popen", fail_start)
    with pytest.raises(DorisPlanningError) as error:
        _TicketLease(HdfsKerberosConfig("reader@EXAMPLE.COM", str(keytab)))
    assert "synthetic-secret" not in str(error.value)
    assert not directory.exists()


def test_renewal_reauthenticates_if_ticket_is_not_renewable(tmp_path, monkeypatch):
    root = tmp_path / "trusted"
    root.mkdir()
    keytab = root / "reader.keytab"
    keytab.write_bytes(b"synthetic")
    monkeypatch.setenv("RAY_DORIS_HDFS_KEYTAB_ROOT", str(root))
    calls = []

    def command(args):
        calls.append(args[0:2])
        if args[0:2] == ["kinit", "-R"]:
            return SimpleNamespace(returncode=1, stdout="")
        return successful_command(args)

    monkeypatch.setattr("ray_doris._hdfs_proxy._command", command)
    lease = _TicketLease(HdfsKerberosConfig("reader@EXAMPLE.COM", str(keytab)))
    try:
        initial = lease.version
        lease.renew()
        assert lease.version == initial + 1
        assert calls.count(["kinit", "-k"]) == 2
        assert ["kinit", "-R"] in calls
    finally:
        lease.close()


def test_failed_reauthentication_does_not_report_expired_ticket_as_valid(tmp_path, monkeypatch):
    root = tmp_path / "trusted"
    root.mkdir()
    keytab = root / "reader.keytab"
    keytab.write_bytes(b"synthetic")
    monkeypatch.setenv("RAY_DORIS_HDFS_KEYTAB_ROOT", str(root))
    monkeypatch.setattr(
        "ray_doris._hdfs_proxy._command",
        successful_command,
    )
    lease = _TicketLease(HdfsKerberosConfig("reader@EXAMPLE.COM", str(keytab)))
    try:
        lease.expiry = time.time() - 1
        monkeypatch.setattr(
            "ray_doris._hdfs_proxy._command", lambda args: SimpleNamespace(returncode=1, stdout="")
        )
        with pytest.raises(DorisPlanningError, match="login failed"):
            lease.snapshot()
    finally:
        lease.close()


def test_close_waits_for_inflight_renewal_before_removing_cache(tmp_path, monkeypatch):
    root = tmp_path / "trusted"
    root.mkdir()
    keytab = root / "reader.keytab"
    keytab.write_bytes(b"synthetic")
    monkeypatch.setenv("RAY_DORIS_HDFS_KEYTAB_ROOT", str(root))
    started = threading.Event()
    release = threading.Event()

    def command(args):
        if args[:2] == ["kinit", "-R"]:
            started.set()
            assert release.wait(5)
            return SimpleNamespace(returncode=1, stdout="")
        return successful_command(args)

    monkeypatch.setattr("ray_doris._hdfs_proxy._command", command)
    lease = _TicketLease(HdfsKerberosConfig("reader@EXAMPLE.COM", str(keytab)))
    directory = pathlib.Path(lease.path).parent
    renewing = threading.Thread(target=lease.renew)
    closing = threading.Thread(target=lease.close)
    try:
        renewing.start()
        assert started.wait(5)
        closing.start()
        assert directory.is_dir()
    finally:
        release.set()
        renewing.join(timeout=5)
        closing.join(timeout=5)
        lease.close()
    assert not renewing.is_alive() and not closing.is_alive()
    assert not directory.exists()


def test_guard_removes_cache_after_owner_sigkill(tmp_path):
    script = r"""
import os
import sys
import time
from types import SimpleNamespace
from ray_doris._hdfs_auth import HdfsKerberosConfig
import ray_doris._hdfs_proxy as p
p._check_keytab = lambda path: None
def command(args):
    if args[:2] == ["kinit", "-k"]:
        with open(args[-2], "w") as cache: cache.write("synthetic")
    return SimpleNamespace(returncode=0, stdout=sys.argv[1])
p._command = command
lease = p._TicketLease(HdfsKerberosConfig("reader@EXAMPLE.COM", "/tmp/test.keytab"))
with open(lease.path, "w") as f: f.write("synthetic")
print(lease.path, flush=True)
while True: time.sleep(1)
"""
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", script, ticket_listing()],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        assert process.stdout is not None
        cache = process.stdout.readline().strip()
        assert cache and os.path.isfile(cache)
        directory = os.path.dirname(cache)
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        for _ in range(50):
            if not os.path.exists(directory):
                break
            time.sleep(0.1)
        assert not os.path.exists(directory)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()
        gc.collect()
