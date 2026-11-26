"""Small, driver-only adapter for the optional generated Hive Metastore SDK."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from importlib import import_module
from typing import Any, Iterator, Tuple
from urllib.parse import urlparse

from ray_doris._errors import (
    DorisConfigurationError,
    DorisError,
    DorisPlanningError,
    _diagnostic_suffix,
    _NativeReadNotApplicableError,
    _with_native_diagnostic,
)


@dataclass(frozen=True)
class HiveStorage:
    """Storage metadata; never a filesystem reader or a transaction snapshot."""

    location: str = field(repr=False)
    input_format: str
    serde: str
    columns: Tuple[Tuple[str, str], ...]


@dataclass(frozen=True)
class HiveTable:
    storage: HiveStorage
    partition_keys: Tuple[Tuple[str, str], ...]
    parameters: Tuple[Tuple[str, str], ...] = field(repr=False)
    table_type: str


@dataclass(frozen=True)
class HivePartition:
    name: str = field(repr=False)
    values: Tuple[str, ...] = field(repr=False)
    storage: HiveStorage
    parameters: Tuple[Tuple[str, str], ...] = field(repr=False)


def storage_descriptor(sd: Any) -> HiveStorage:
    if sd is None or sd.serdeInfo is None or not isinstance(sd.location, str):
        raise DorisPlanningError("HMS returned an incomplete storage descriptor")
    return HiveStorage(
        sd.location,
        sd.inputFormat or "",
        sd.serdeInfo.serializationLib or "",
        tuple((column.name, column.type) for column in sd.cols or ()),
    )


def _hms_diagnostic_suffix(error: BaseException, transport_error: Any) -> str:
    if transport_error is not None and type(error) is transport_error:
        if getattr(error, "type", None) == transport_error.TIMED_OUT:
            return " (timeout)"
        inner = getattr(error, "inner", None)
        if isinstance(inner, BaseException):
            error = inner
    return _diagnostic_suffix(error)


class HiveMetadataClient:
    """Use generated SDK methods without serializing its connection to Ray."""

    def __init__(self, client: Any, transport_error: Any = None) -> None:
        self._client = client
        self._transport_error = transport_error

    def _call(self, method: str, *args: Any) -> Any:
        try:
            return getattr(self._client, method)(*args)
        except DorisError:
            raise
        except Exception as error:
            suffix = _hms_diagnostic_suffix(error, self._transport_error)
            raise _with_native_diagnostic(
                DorisPlanningError(
                    f"HMS metadata request failed during {method}{suffix}; "
                    "verify access and service state"
                ),
                "HMS metadata",
                suffix,
            ) from None

    def table(self, database: str, table: str) -> HiveTable:
        raw = self._call("get_table", database, table)
        if raw.viewOriginalText or raw.viewExpandedText:
            raise _NativeReadNotApplicableError("native reads do not support Hive views")
        return HiveTable(
            storage_descriptor(raw.sd),
            tuple((column.name, column.type) for column in raw.partitionKeys or ()),
            tuple((raw.parameters or {}).items()),
            raw.tableType or "",
        )

    def partitions(self, database: str, table: str, maximum: int) -> Tuple[HivePartition, ...]:
        names = self._call("get_partition_names", database, table, maximum + 1)
        if len(names) > maximum:
            raise _NativeReadNotApplicableError("registered partitions exceed max_partitions")
        if len(set(names)) != len(names):
            raise DorisPlanningError("HMS returned duplicate partition identifiers")
        result = []
        for offset in range(0, len(names), 256):
            batch = names[offset : offset + 256]
            raw = self._call("get_partitions_by_names", database, table, batch)
            # Get the original names through HMS's own encoding, not a local Hive parser.
            by_values = {}
            for name in batch:
                values = tuple(self._call("partition_name_to_vals", name))
                if values in by_values:
                    raise DorisPlanningError("HMS returned ambiguous partition identifiers")
                by_values[values] = name
            if len(raw) != len(batch):
                raise DorisPlanningError("HMS partition enumeration changed during planning")
            seen = set()
            for partition in raw:
                values = tuple(partition.values or ())
                if values not in by_values or values in seen:
                    raise DorisPlanningError("HMS returned mismatched partition descriptors")
                seen.add(values)
                result.append(
                    HivePartition(
                        by_values[values],
                        values,
                        storage_descriptor(partition.sd),
                        tuple((partition.parameters or {}).items()),
                    )
                )
        return tuple(result)


@contextmanager
def open_hms(uri: str, timeout: float) -> Iterator[HiveMetadataClient]:
    parsed = urlparse(uri)
    if (
        parsed.scheme != "thrift"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise DorisConfigurationError("HMS requires a plain Thrift host and port URI")
    try:
        port = parsed.port if parsed.port is not None else 9083
    except ValueError:
        raise DorisConfigurationError("HMS URI has an invalid port") from None
    if port == 0:
        raise DorisConfigurationError("HMS URI has an invalid port")
    try:
        sdk = import_module("pymetastore.hive_metastore.ThriftHiveMetastore")
        sockets = import_module("thrift.transport.TSocket")
        transports = import_module("thrift.transport.TTransport")
        protocols = import_module("thrift.protocol.TBinaryProtocol")
    except ImportError:
        raise DorisConfigurationError("native reads require ray-doris[native]") from None
    socket = sockets.TSocket(parsed.hostname, port)
    socket.setTimeout(max(1, int(timeout * 1000)))
    transport = transports.TBufferedTransport(socket)
    try:
        transport.open()
        client = HiveMetadataClient(
            sdk.Client(protocols.TBinaryProtocol(transport)), transports.TTransportException
        )
    except Exception as error:
        transport.close()
        suffix = _hms_diagnostic_suffix(error, transports.TTransportException)
        raise _with_native_diagnostic(
            DorisPlanningError(f"HMS connection failed{suffix}; verify access and service state"),
            "HMS connection",
            suffix,
        ) from None
    try:
        yield client
    finally:
        transport.close()
