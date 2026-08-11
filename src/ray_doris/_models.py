"""Immutable data passed between the driver and Ray workers."""

from __future__ import annotations

import math
from collections.abc import Mapping as MappingABC
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Optional, Tuple

import pyarrow as pa

from ray_doris._errors import DorisConfigurationError

Transport = Literal["auto", "mysql", "flight"]
QueryPlanPolicy = Literal["single_task", "error"]
HttpScheme = Literal["http", "https"]
FlightScheme = Literal["grpc", "grpc+tls"]

_ADBC_CONNECT_TIMEOUT_OPTION = "adbc.flight.sql.rpc.timeout_seconds.connect"
_MAX_CONNECT_TIMEOUT_SECONDS = 31_536_000

_RESERVED_MYSQL_OPTIONS = {
    "charset",
    "connect_timeout",
    "cursorclass",
    "database",
    "db",
    "host",
    "password",
    "passwd",
    "port",
    "user",
}
_RESERVED_FLIGHT_OPTIONS = {
    _ADBC_CONNECT_TIMEOUT_OPTION,
    "password",
    "uri",
    "username",
}


@dataclass(frozen=True)
class QualifiedTable:
    """A validated two-part Doris table name."""

    database: str
    table: str


@dataclass(frozen=True)
class DorisInputSplit:
    """A group of tablets, or an unpartitioned fallback query."""

    tablet_ids: Optional[Tuple[int, ...]]


@dataclass(frozen=True)
class DorisPlan:
    """The canonical schema and worker splits produced on the Ray driver."""

    schema: pa.Schema
    splits: Tuple[DorisInputSplit, ...]


@dataclass(frozen=True)
class DorisPlanningSnapshot:
    """Schema and tablet discovery shared across Ray planning calls."""

    schema: pa.Schema
    tablet_ids: Optional[Tuple[int, ...]]


@dataclass(frozen=True)
class DorisReadConfig:
    """Serializable configuration shared by planning and worker reads."""

    host: str
    table: QualifiedTable
    user: str
    password: str = field(default="", repr=False)
    mysql_port: int = 9030
    http_port: int = 8030
    flight_port: int = 8070
    http_scheme: HttpScheme = "http"
    flight_scheme: FlightScheme = "grpc"
    columns: Optional[Tuple[str, ...]] = None
    filter: Optional[str] = None
    transport: Transport = "mysql"
    tablet_size: int = 1
    batch_size: int = 10_000
    on_query_plan_error: QueryPlanPolicy = "single_task"
    connect_timeout: float = 10.0
    client_options: Tuple[Tuple[str, Any], ...] = field(default_factory=tuple, repr=False)
    flight_options: Tuple[Tuple[str, Any], ...] = field(default_factory=tuple, repr=False)

    def __post_init__(self) -> None:
        """Validate scalar settings without performing network I/O."""
        if not isinstance(self.host, str) or not self.host.strip():
            raise DorisConfigurationError("host must not be empty")
        if not isinstance(self.user, str) or not self.user:
            raise DorisConfigurationError("user must not be empty")
        if not isinstance(self.password, str):
            raise DorisConfigurationError("password must be a string")
        for name, port in (
            ("mysql_port", self.mysql_port),
            ("http_port", self.http_port),
            ("flight_port", self.flight_port),
        ):
            if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
                raise DorisConfigurationError(f"{name} must be between 1 and 65535")
        if (
            isinstance(self.tablet_size, bool)
            or not isinstance(self.tablet_size, int)
            or self.tablet_size <= 0
        ):
            raise DorisConfigurationError("tablet_size must be a positive integer")
        if (
            isinstance(self.batch_size, bool)
            or not isinstance(self.batch_size, int)
            or self.batch_size <= 0
        ):
            raise DorisConfigurationError("batch_size must be a positive integer")
        if (
            isinstance(self.connect_timeout, bool)
            or not isinstance(self.connect_timeout, (int, float))
            or self.connect_timeout <= 0
            or self.connect_timeout > _MAX_CONNECT_TIMEOUT_SECONDS
            or not math.isfinite(self.connect_timeout)
        ):
            raise DorisConfigurationError(
                "connect_timeout must be finite, positive, and at most 31536000 seconds"
            )
        if self.transport not in ("auto", "mysql", "flight"):
            raise DorisConfigurationError(f"unsupported transport: {self.transport!r}")
        if self.http_scheme not in ("http", "https"):
            raise DorisConfigurationError(f"unsupported HTTP scheme: {self.http_scheme!r}")
        if self.flight_scheme not in ("grpc", "grpc+tls"):
            raise DorisConfigurationError(f"unsupported Flight SQL scheme: {self.flight_scheme!r}")
        if self.on_query_plan_error not in ("single_task", "error"):
            raise DorisConfigurationError(
                f"unsupported on_query_plan_error: {self.on_query_plan_error!r}"
            )
        self._validate_options("client_options", self.client_options)
        self._validate_options("flight_options", self.flight_options)
        self._reject_reserved_options(
            "client_options", self.client_options, _RESERVED_MYSQL_OPTIONS
        )
        self._reject_reserved_options(
            "flight_options", self.flight_options, _RESERVED_FLIGHT_OPTIONS
        )
        self._validate_mysql_timeouts()
        if any(not isinstance(value, str) for _, value in self.flight_options):
            raise DorisConfigurationError("flight_options values must be strings")

    def _validate_mysql_timeouts(self) -> None:
        options = dict(self.client_options)
        for name in ("read_timeout", "write_timeout"):
            if name not in options:
                continue
            value = options[name]
            try:
                finite = (
                    not isinstance(value, bool)
                    and isinstance(value, (int, float))
                    and math.isfinite(value)
                )
            except (TypeError, OverflowError):
                finite = False
            if not finite or value <= 0:
                raise DorisConfigurationError(
                    f"client_options {name} must be a finite positive number"
                )

    @staticmethod
    def _validate_options(name: str, options: Tuple[Tuple[str, Any], ...]) -> None:
        seen = set()
        for key, _ in options:
            if not isinstance(key, str) or not key:
                raise DorisConfigurationError(f"{name} keys must be non-empty strings")
            if key in seen:
                raise DorisConfigurationError(f"duplicate {name} key: {key!r}")
            seen.add(key)

    @staticmethod
    def _reject_reserved_options(
        name: str, options: Tuple[Tuple[str, Any], ...], reserved: set[str]
    ) -> None:
        overridden = sorted(key for key, _ in options if key in reserved)
        if overridden:
            raise DorisConfigurationError(
                f"{name} must not override managed connection options: " + ", ".join(overridden)
            )

    @classmethod
    def from_options(
        cls,
        *,
        client_options: Optional[Mapping[str, Any]] = None,
        flight_options: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> "DorisReadConfig":
        """Construct a config while snapshotting caller-owned option mappings."""
        return cls(
            client_options=cls._copy_options("client_options", client_options),
            flight_options=cls._copy_options("flight_options", flight_options),
            **kwargs,
        )

    @staticmethod
    def _copy_options(
        name: str, options: Optional[Mapping[str, Any]]
    ) -> Tuple[Tuple[str, Any], ...]:
        if options is None:
            return ()
        if not isinstance(options, MappingABC):
            raise DorisConfigurationError(f"{name} must be a mapping")
        try:
            copied = deepcopy(dict(options))
        except Exception:
            raise DorisConfigurationError(f"{name} values must be copyable") from None
        return tuple(copied.items())

    def mysql_options(self) -> Mapping[str, Any]:
        """Return a fresh mapping for a PyMySQL connection."""
        return deepcopy(dict(self.client_options))

    def adbc_options(self) -> Mapping[str, Any]:
        """Return a fresh mapping for an ADBC Flight SQL connection."""
        return deepcopy(dict(self.flight_options))

    def __repr__(self) -> str:
        """Return a representation that never exposes credentials or option values."""
        rendered_filter = "None" if self.filter is None else "<redacted>"
        return (
            "DorisReadConfig("
            f"host={self.host!r}, table={self.table!r}, user={self.user!r}, "
            "password=<redacted>, "
            f"mysql_port={self.mysql_port}, http_port={self.http_port}, "
            f"flight_port={self.flight_port}, http_scheme={self.http_scheme!r}, "
            f"flight_scheme={self.flight_scheme!r}, columns={self.columns!r}, "
            f"filter={rendered_filter}, transport={self.transport!r}, "
            f"tablet_size={self.tablet_size}, batch_size={self.batch_size}, "
            f"on_query_plan_error={self.on_query_plan_error!r}, "
            f"connect_timeout={self.connect_timeout}, "
            f"client_options={tuple(key for key, _ in self.client_options)!r}, "
            f"flight_options={tuple(key for key, _ in self.flight_options)!r})"
        )
