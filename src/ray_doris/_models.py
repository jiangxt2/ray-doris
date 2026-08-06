"""Immutable data passed between the driver and Ray workers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Optional, Tuple

import pyarrow as pa

from ray_doris._errors import DorisConfigurationError

Transport = Literal["auto", "mysql", "flight"]
QueryPlanPolicy = Literal["single_task", "error"]
HttpScheme = Literal["http", "https"]
FlightScheme = Literal["grpc", "grpc+tls"]

_RESERVED_MYSQL_OPTIONS = {
    "charset",
    "connect_timeout",
    "cursorclass",
    "database",
    "db",
    "host",
    "password",
    "port",
    "user",
}
_RESERVED_FLIGHT_OPTIONS = {"password", "uri", "username"}


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
        ):
            raise DorisConfigurationError("connect_timeout must be positive")
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
        if any(not isinstance(value, str) for _, value in self.flight_options):
            raise DorisConfigurationError("flight_options values must be strings")

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
        """Construct a config while freezing caller-owned option mappings."""
        return cls(
            client_options=tuple((client_options or {}).items()),
            flight_options=tuple((flight_options or {}).items()),
            **kwargs,
        )

    def mysql_options(self) -> Mapping[str, Any]:
        """Return a fresh mapping for a PyMySQL connection."""
        return dict(self.client_options)

    def adbc_options(self) -> Mapping[str, Any]:
        """Return a fresh mapping for an ADBC Flight SQL connection."""
        return dict(self.flight_options)

    def __repr__(self) -> str:
        """Return a representation that never exposes credentials or option values."""
        return (
            "DorisReadConfig("
            f"host={self.host!r}, table={self.table!r}, user={self.user!r}, "
            "password=<redacted>, "
            f"mysql_port={self.mysql_port}, http_port={self.http_port}, "
            f"flight_port={self.flight_port}, http_scheme={self.http_scheme!r}, "
            f"flight_scheme={self.flight_scheme!r}, columns={self.columns!r}, "
            f"filter={self.filter!r}, transport={self.transport!r}, "
            f"tablet_size={self.tablet_size}, batch_size={self.batch_size}, "
            f"on_query_plan_error={self.on_query_plan_error!r}, "
            f"connect_timeout={self.connect_timeout}, "
            f"client_options={tuple(key for key, _ in self.client_options)!r}, "
            f"flight_options={tuple(key for key, _ in self.flight_options)!r})"
        )
