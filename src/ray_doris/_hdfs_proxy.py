"""Process-local Kerberos authentication for Arrow's HDFS reader."""

from __future__ import annotations

import datetime
import io
import logging
import os
import select
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import weakref
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union

import pyarrow as pa
import pyarrow.fs as fs

from ray_doris._errors import DorisConfigurationError, DorisPlanningError
from ray_doris._hdfs_auth import KEYTAB_ROOT_ENV, HdfsKerberosConfig

_GUARD = (
    "import os,shutil,sys; "
    "os.write(int(sys.argv[3]),b'R'); os.close(int(sys.argv[3])); "
    "f=os.fdopen(int(sys.argv[1]),'rb'); f.read(); f.close(); "
    "shutil.rmtree(sys.argv[2])"
)
logger = logging.getLogger(__name__)
_T = TypeVar("_T")


def _hdfs_io_with_hint(operation: Callable[[], _T]) -> _T:
    try:
        return operation()
    except OSError:
        logger.error(
            "Native HDFS I/O failed; retry with auto_native_read=False to read through Doris"
        )
        raise


def _check_keytab(path: str) -> None:
    root = os.environ.get(KEYTAB_ROOT_ENV)
    if not root or not os.path.isabs(root) or os.path.realpath(root) == os.path.sep:
        raise DorisConfigurationError("HDFS Kerberos requires an authorized keytab root")
    root = os.path.realpath(root)
    target = os.path.realpath(path)
    try:
        inside = os.path.commonpath((root, target)) == root and target != root
    except ValueError:
        inside = False
    if not inside or not os.path.isfile(target) or not os.access(target, os.R_OK):
        raise DorisConfigurationError("HDFS Kerberos keytab is not an authorized readable file")


def _command(args: List[str]) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ, LC_ALL="C")
    try:
        return subprocess.run(
            args,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise DorisPlanningError("HDFS Kerberos ticket command failed") from None


def _ticket_expiry(output: str) -> float:
    for line in output.splitlines():
        columns = line.split()
        if len(columns) >= 5 and columns[4].startswith("krbtgt/"):
            for pattern in ("%m/%d/%y %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
                try:
                    return datetime.datetime.strptime(
                        columns[2] + " " + columns[3], pattern
                    ).timestamp()
                except ValueError:
                    continue
    raise DorisPlanningError("HDFS Kerberos ticket expiry is unavailable")


def _cleanup_lease(stop: threading.Event, fd: int, guard: subprocess.Popen[bytes]) -> None:
    stop.set()
    try:
        os.close(fd)
    except OSError:
        pass
    try:
        if guard.wait(timeout=5) != 0:
            logger.warning("HDFS Kerberos cache cleanup failed")
    except (OSError, subprocess.TimeoutExpired):
        logger.warning("HDFS Kerberos cache cleanup could not be verified")


def _renew_loop(reference: "weakref.ReferenceType[_TicketLease]", stop: threading.Event) -> None:
    while True:
        lease = reference()
        if lease is None:
            return
        delay = max(5.0, min(3600.0, (lease.expiry - time.time()) / 2))
        del lease
        if stop.wait(delay):
            return
        lease = reference()
        if lease is None:
            return
        lease.renew()
        del lease


class _TicketLease:
    """Own a private cache and a guard that removes it if this process dies."""

    def __init__(self, config: HdfsKerberosConfig) -> None:
        _check_keytab(config.keytab)
        self.config = config
        self._lock = threading.RLock()
        self._stop = threading.Event()
        try:
            self._directory = tempfile.mkdtemp(prefix="ray-doris-hdfs-krb-")
        except OSError:
            raise DorisPlanningError("HDFS Kerberos cache directory could not be created") from None
        self.path = os.path.join(self._directory, "ticket")
        self.expiry = 0.0
        self.version = 0
        try:
            read_fd, write_fd = os.pipe()
            try:
                ready_fd, ready_write_fd = os.pipe()
            except OSError:
                os.close(read_fd)
                os.close(write_fd)
                raise
        except OSError:
            shutil.rmtree(self._directory, ignore_errors=True)
            raise DorisPlanningError("HDFS Kerberos cleanup guard could not start") from None
        try:
            guard = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    "-c",
                    _GUARD,
                    str(read_fd),
                    self._directory,
                    str(ready_write_fd),
                ],
                pass_fds=(read_fd, ready_write_fd),
                start_new_session=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            os.close(ready_write_fd)
            ready_write_fd = -1
            if not select.select([ready_fd], [], [], 5)[0] or os.read(ready_fd, 1) != b"R":
                raise DorisPlanningError("HDFS Kerberos cleanup guard failed to start")
        except OSError:
            os.close(write_fd)
            shutil.rmtree(self._directory, ignore_errors=True)
            raise DorisPlanningError("HDFS Kerberos cleanup guard could not start") from None
        except BaseException:
            os.close(write_fd)
            shutil.rmtree(self._directory, ignore_errors=True)
            raise
        finally:
            os.close(read_fd)
            os.close(ready_fd)
            if ready_write_fd >= 0:
                os.close(ready_write_fd)
        self._finalizer = weakref.finalize(self, _cleanup_lease, self._stop, write_fd, guard)
        try:
            self._login()
        except BaseException:
            self.close()
            raise
        self._thread = threading.Thread(
            target=_renew_loop, args=(weakref.ref(self), self._stop), daemon=True
        )
        try:
            self._thread.start()
        except BaseException:
            self.close()
            raise

    def _get_expiry(self) -> float:
        result = _command(["klist", "-c", self.path])
        if result.returncode:
            raise DorisPlanningError("HDFS Kerberos ticket cache is unavailable")
        expiry = _ticket_expiry(result.stdout)
        if expiry <= time.time():
            raise DorisPlanningError("HDFS Kerberos ticket has expired")
        return expiry

    def _login(self) -> None:
        _check_keytab(self.config.keytab)
        result = _command(
            ["kinit", "-k", "-t", self.config.keytab, "-c", self.path, self.config.principal]
        )
        if result.returncode:
            raise DorisPlanningError("HDFS Kerberos login failed")
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            raise DorisPlanningError("HDFS Kerberos ticket cache could not be secured") from None
        self.expiry = self._get_expiry()
        self.version += 1

    def renew(self) -> None:
        with self._lock:
            if self._stop.is_set():
                return
            try:
                previous = self.expiry
                result = _command(["kinit", "-R", "-c", self.path])
                if result.returncode:
                    self._login()
                    return
                current = self._get_expiry()
                if current <= previous:
                    self._login()
                    return
                self.expiry = current
                self.version += 1
            except (DorisConfigurationError, DorisPlanningError):
                # An existing ticket remains usable until it expires. Retry
                # on the next interval and never treat expiry as success.
                pass

    def snapshot(self) -> Tuple[str, int]:
        with self._lock:
            if self._stop.is_set():
                raise DorisPlanningError("HDFS Kerberos ticket lease is closed")
            if self.expiry <= time.time():
                self._login()
            return self.path, self.version

    def close(self) -> None:
        with self._lock:
            self._finalizer()


class _RetainedInput(io.RawIOBase):
    """Keep the HDFS client and ticket lease alive until Arrow closes the stream."""

    def __init__(self, stream: pa.NativeFile, backend: fs.HadoopFileSystem, lease: _TicketLease):
        super().__init__()
        self._stream = stream
        self._backend: Optional[fs.HadoopFileSystem] = backend
        self._lease: Optional[_TicketLease] = lease

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return self._stream.seekable()

    def read(self, size: int = -1) -> bytes:
        return _hdfs_io_with_hint(lambda: self._stream.read(size))

    def readinto(self, buffer: Any) -> int:
        return _hdfs_io_with_hint(lambda: self._stream.readinto(buffer))

    def seek(self, offset: int, whence: int = 0) -> int:
        return _hdfs_io_with_hint(lambda: self._stream.seek(offset, whence))

    def tell(self) -> int:
        return _hdfs_io_with_hint(self._stream.tell)

    def close(self) -> None:
        if not self.closed:
            try:
                _hdfs_io_with_hint(self._stream.close)
            finally:
                self._backend = None
                self._lease = None
                super().close()


class HdfsKerberosHandler(fs.FileSystemHandler):
    """Lazily construct a local HDFS client; serialize no credential contents."""

    def __init__(
        self, host: str, port: int, config: HdfsKerberosConfig, extra_conf: Dict[str, str]
    ) -> None:
        self._host = host
        self._port = port
        self._config = config
        self._extra_conf = dict(extra_conf)
        self._lock = threading.RLock()
        self._lease: Optional[_TicketLease] = None
        self._backend: Optional[fs.HadoopFileSystem] = None
        self._version = -1

    def __reduce__(self) -> Tuple[Any, Tuple[Any, ...]]:
        return type(self), (self._host, self._port, self._config, self._extra_conf)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, HdfsKerberosHandler) and (
            self._host,
            self._port,
            self._config,
            self._extra_conf,
        ) == (other._host, other._port, other._config, other._extra_conf)

    def _filesystem(self) -> fs.HadoopFileSystem:
        try:
            with self._lock:
                if self._lease is None:
                    self._lease = _TicketLease(self._config)
                with self._lease._lock:
                    path, version = self._lease.snapshot()
                    if self._backend is None or self._version != version:
                        try:
                            self._backend = fs.HadoopFileSystem(
                                self._host,
                                self._port,
                                user=self._config.principal,
                                kerb_ticket=path,
                                extra_conf=self._extra_conf,
                            )
                        except Exception:
                            raise DorisPlanningError(
                                "HDFS Kerberos filesystem connection failed"
                            ) from None
                        self._version = version
                    return self._backend
        except (DorisPlanningError, DorisConfigurationError) as error:
            raise type(error)(
                f"{error}; retry with auto_native_read=False to read through Doris"
            ) from None

    def get_type_name(self) -> str:
        return "hdfs"

    def normalize_path(self, path: str) -> str:
        return _hdfs_io_with_hint(lambda: self._filesystem().normalize_path(path))

    def get_file_info(self, paths: Union[str, List[str]]) -> Union[fs.FileInfo, List[fs.FileInfo]]:
        if isinstance(paths, str):
            return _hdfs_io_with_hint(lambda: self._filesystem().get_file_info(paths))
        return _hdfs_io_with_hint(lambda: self._filesystem().get_file_info(paths))

    def get_file_info_selector(self, selector: fs.FileSelector) -> List[fs.FileInfo]:
        return _hdfs_io_with_hint(lambda: self._filesystem().get_file_info(selector))

    def open_input_stream(self, path: str) -> pa.NativeFile:
        with self._lock:
            backend = self._filesystem()
            lease = self._lease
            assert lease is not None
            return _hdfs_io_with_hint(
                lambda: pa.PythonFile(
                    _RetainedInput(backend.open_input_stream(path), backend, lease), mode="r"
                )
            )

    def open_input_file(self, path: str) -> pa.NativeFile:
        with self._lock:
            backend = self._filesystem()
            lease = self._lease
            assert lease is not None
            return _hdfs_io_with_hint(
                lambda: pa.PythonFile(
                    _RetainedInput(backend.open_input_file(path), backend, lease), mode="r"
                )
            )

    def create_dir(self, path: str, recursive: bool) -> None:
        raise NotImplementedError("HDFS Kerberos reader is read-only")

    def delete_dir(self, path: str) -> None:
        raise NotImplementedError("HDFS Kerberos reader is read-only")

    def delete_dir_contents(self, path: str, missing_dir_ok: bool = False) -> None:
        raise NotImplementedError("HDFS Kerberos reader is read-only")

    def delete_root_dir_contents(self) -> None:
        raise NotImplementedError("HDFS Kerberos reader is read-only")

    def delete_file(self, path: str) -> None:
        raise NotImplementedError("HDFS Kerberos reader is read-only")

    def move(self, src: str, dest: str) -> None:
        raise NotImplementedError("HDFS Kerberos reader is read-only")

    def copy_file(self, src: str, dest: str) -> None:
        raise NotImplementedError("HDFS Kerberos reader is read-only")

    def open_output_stream(self, path: str, metadata: Any) -> pa.NativeFile:
        raise NotImplementedError("HDFS Kerberos reader is read-only")

    def open_append_stream(self, path: str, metadata: Any) -> pa.NativeFile:
        raise NotImplementedError("HDFS Kerberos reader is read-only")
