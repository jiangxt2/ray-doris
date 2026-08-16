"""Serializable endpoint and table settings for Doris Stream Load."""

from __future__ import annotations

import ipaddress
import os
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote

from ray_doris._errors import DorisConfigurationError

_ENVIRONMENT_VARIABLE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_host(host: str) -> str:
    if not isinstance(host, str) or not host:
        raise DorisConfigurationError("Doris host must be a non-empty string")
    if any(character.isspace() for character in host) or any(
        character in "/?#@" for character in host
    ):
        raise DorisConfigurationError("Doris host must be a bare hostname or IP address")
    if ":" in host:
        try:
            ipaddress.IPv6Address(host)
        except ipaddress.AddressValueError:
            raise DorisConfigurationError(
                "Doris IPv6 hosts must be unbracketed address literals"
            ) from None
    return host


def _validate_port(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise DorisConfigurationError(f"{name} must be between 1 and 65535")
    return value


def _validate_timeout(name: str, value: object) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value <= 0
        or value > 31_536_000
    ):
        raise DorisConfigurationError(
            f"{name} must be finite, positive, and at most 31536000 seconds"
        )
    return float(value)


def _authority(host: str, port: int) -> str:
    rendered_host = f"[{host}]" if ":" in host else host
    return f"{rendered_host}:{port}"


@dataclass(frozen=True)
class DorisTable:
    """A validated Doris database and physical table identifier."""

    database: str
    name: str

    def __post_init__(self) -> None:
        for kind, value in (("database", self.database), ("table", self.name)):
            if not isinstance(value, str) or not value or "\x00" in value:
                raise DorisConfigurationError(f"{kind} must be a non-empty identifier")

    def sql(self) -> str:
        """Render the table with escaped Doris identifiers."""
        return f"`{self.database.replace('`', '``')}`.`{self.name.replace('`', '``')}`"


@dataclass(frozen=True)
class DorisConnection:
    """Immutable HTTP and metadata connection settings."""

    host: str
    username: str = "root"
    password: str = field(default="", repr=False, compare=False)
    password_env: Optional[str] = field(default=None, repr=False, compare=False)
    http_port: int = 8030
    mysql_port: int = 9030
    http_secure: bool = False
    http_ca_file: Optional[str] = field(default=None, repr=False)
    mysql_ca_file: Optional[str] = field(default=None, repr=False)
    connect_timeout_seconds: float = 10.0
    request_timeout_seconds: float = 300.0
    redirect_hosts: tuple[str, ...] = ()
    redirect_ports: tuple[int, ...] = ()
    redirect_policy: str = ""
    verify_tls: bool = field(default=True, repr=False)

    def __post_init__(self) -> None:
        _validate_host(self.host)
        if not isinstance(self.username, str) or not self.username:
            raise DorisConfigurationError("Doris username must be a non-empty string")
        if not isinstance(self.password, str):
            raise DorisConfigurationError("Doris password must be a string")
        if self.password_env is not None and (
            not isinstance(self.password_env, str)
            or _ENVIRONMENT_VARIABLE_PATTERN.fullmatch(self.password_env) is None
        ):
            raise DorisConfigurationError("password_env must be a portable environment name")
        if self.password and self.password_env is not None:
            raise DorisConfigurationError("password and password_env are mutually exclusive")
        _validate_port("http_port", self.http_port)
        _validate_port("mysql_port", self.mysql_port)
        _validate_timeout("connect_timeout_seconds", self.connect_timeout_seconds)
        _validate_timeout("request_timeout_seconds", self.request_timeout_seconds)
        if not isinstance(self.http_secure, bool):
            raise DorisConfigurationError("http_secure must be a boolean")
        for name, value in (
            ("http_ca_file", self.http_ca_file),
            ("mysql_ca_file", self.mysql_ca_file),
        ):
            if value is not None and (not isinstance(value, str) or not value):
                raise DorisConfigurationError(f"{name} must be a non-empty string")
        if self.http_ca_file is not None and not self.http_secure:
            raise DorisConfigurationError("http_ca_file requires http_secure=True")
        if self.verify_tls is not True:
            raise DorisConfigurationError(
                "Doris TLS certificate and hostname verification is required"
            )
        hosts = tuple(self.redirect_hosts)
        ports = tuple(self.redirect_ports)
        object.__setattr__(self, "redirect_hosts", hosts)
        object.__setattr__(self, "redirect_ports", ports)
        for host in hosts:
            _validate_host(host)
        for port in ports:
            _validate_port("redirect_port", port)
        if self.redirect_policy not in ("", "direct", "public", "private"):
            raise DorisConfigurationError(
                "redirect_policy must be empty, direct, public, or private"
            )

    @property
    def scheme(self) -> str:
        return "https" if self.http_secure else "http"

    def endpoint(self, table: DorisTable) -> str:
        """Return the FE Stream Load endpoint with safely quoted path segments."""
        return (
            f"{self.scheme}://{_authority(self.host, self.http_port)}/api/"
            f"{quote(table.database, safe='')}/{quote(table.name, safe='')}/_stream_load"
        )

    def allowed_redirect_hosts(self) -> tuple[str, ...]:
        """Return the FE host plus the explicit redirect host allowlist."""
        return tuple(dict.fromkeys((self.host, *self.redirect_hosts)))

    def allowed_redirect_ports(self) -> tuple[int, ...]:
        return self.redirect_ports

    def resolve_password(self) -> str:
        """Resolve credentials in the current process without caching the value."""
        if self.password_env is None:
            return self.password
        try:
            return os.environ[self.password_env]
        except KeyError:
            raise DorisConfigurationError(
                "configured password environment variable is unavailable"
            ) from None

    def mysql_kwargs(self, table: DorisTable) -> dict[str, object]:
        """Build fresh metadata connection arguments."""
        return {
            "host": self.host,
            "port": self.mysql_port,
            "user": self.username,
            "password": self.resolve_password(),
            "database": table.database,
            "charset": "utf8mb4",
            "autocommit": True,
            "connect_timeout": self.connect_timeout_seconds,
            "read_timeout": self.request_timeout_seconds,
            "write_timeout": self.request_timeout_seconds,
            **(
                {"ssl": {"ca": self.mysql_ca_file, "check_hostname": True}}
                if self.mysql_ca_file is not None
                else {}
            ),
        }

    def __repr__(self) -> str:
        return (
            "DorisConnection("
            f"host={self.host!r}, username={self.username!r}, password=<redacted>, "
            f"password_env={'<configured>' if self.password_env else 'None'}, "
            f"http_port={self.http_port}, mysql_port={self.mysql_port}, "
            f"http_secure={self.http_secure}, "
            f"http_ca_file={'<configured>' if self.http_ca_file else 'None'}, "
            f"mysql_ca_file={'<configured>' if self.mysql_ca_file else 'None'}, "
            f"connect_timeout_seconds={self.connect_timeout_seconds!r}, "
            f"request_timeout_seconds={self.request_timeout_seconds!r}, "
            f"redirect_hosts={self.redirect_hosts!r}, redirect_ports={self.redirect_ports!r}, "
            f"redirect_policy={self.redirect_policy!r})"
        )
