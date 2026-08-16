"""Immutable and typed Doris Stream Load options."""

from __future__ import annotations

import math
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from typing import Literal, Mapping, Optional
from uuid import uuid4

from ray_doris._errors import DorisConfigurationError

WriteOperation = Literal["load", "upsert", "partial_update"]
WriteFormat = Literal["parquet", "json"]
_LABEL_PREFIX = "ray_doris_"
_MAX_LABEL_LENGTH = 128
_MANAGED_PROPERTIES = {
    "columns",
    "format",
    "label",
    "max_filter_ratio",
    "partial_columns",
    "read_json_by_line",
    "strict_mode",
    "two_phase_commit",
    "txn_operation",
}
_ALLOWED_PROPERTIES = {"load_to_single_tablet", "partial_update_new_key_behavior", "timezone"}


@dataclass(frozen=True)
class DorisWriteOptions:
    """Serializable Stream Load policy with no automatic replay."""

    operation: WriteOperation = "load"
    format: Optional[WriteFormat] = None
    batch_rows: int = 65_536
    batch_bytes: int = 64 * 1024 * 1024
    max_filter_ratio: float = 0.0
    strict_mode: bool = True
    request_timeout_seconds: Optional[float] = None
    load_properties: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        expected = "json" if self.operation == "partial_update" else "parquet"
        if self.operation not in ("load", "upsert", "partial_update"):
            raise DorisConfigurationError("operation must be load, upsert, or partial_update")
        if self.format is None:
            object.__setattr__(self, "format", expected)
        if self.format not in ("parquet", "json"):
            raise DorisConfigurationError("format must be parquet or json")
        if self.format != expected:
            raise DorisConfigurationError(f"{self.operation} requires format={expected!r}")
        if (
            isinstance(self.batch_rows, bool)
            or not isinstance(self.batch_rows, int)
            or not 0 < self.batch_rows <= 1_000_000
        ):
            raise DorisConfigurationError("batch_rows must be between 1 and 1,000,000")
        if (
            isinstance(self.batch_bytes, bool)
            or not isinstance(self.batch_bytes, int)
            or not 0 < self.batch_bytes <= 1 << 30
        ):
            raise DorisConfigurationError("batch_bytes must be between 1 and 1 GiB")
        if (
            isinstance(self.max_filter_ratio, bool)
            or not isinstance(self.max_filter_ratio, (int, float))
            or not math.isfinite(float(self.max_filter_ratio))
            or not 0 <= float(self.max_filter_ratio) <= 1
        ):
            raise DorisConfigurationError(
                "max_filter_ratio must be a finite number between 0 and 1"
            )
        if not isinstance(self.strict_mode, bool):
            raise DorisConfigurationError("strict_mode must be a boolean")
        if self.request_timeout_seconds is not None and (
            isinstance(self.request_timeout_seconds, bool)
            or not isinstance(self.request_timeout_seconds, (int, float))
            or self.request_timeout_seconds <= 0
            or self.request_timeout_seconds > 31_536_000
            or not math.isfinite(float(self.request_timeout_seconds))
        ):
            raise DorisConfigurationError(
                "request_timeout_seconds must be finite, positive, and at most 31536000 seconds"
            )
        if self.load_properties is None:
            normalized = ()
        elif isinstance(self.load_properties, MappingABC):
            normalized = tuple(self.load_properties.items())
        else:
            try:
                normalized = tuple(self.load_properties)
            except (TypeError, ValueError):
                raise DorisConfigurationError("load_properties must be a mapping") from None
        seen: set[str] = set()
        frozen: list[tuple[str, str]] = []
        for pair in normalized:
            if not isinstance(pair, (tuple, list)) or len(pair) != 2:
                raise DorisConfigurationError("load_properties must contain key/value pairs")
            key, value = pair
            if (
                not isinstance(key, str)
                or not key
                or key in seen
                or key in _MANAGED_PROPERTIES
                or key not in _ALLOWED_PROPERTIES
                or not isinstance(value, str)
            ):
                raise DorisConfigurationError("load_properties contains a managed or invalid key")
            seen.add(key)
            frozen.append((key, value))
        object.__setattr__(self, "load_properties", tuple(frozen))

    @classmethod
    def from_mapping(
        cls,
        *,
        operation: WriteOperation = "load",
        format: Optional[WriteFormat] = None,
        batch_rows: int = 65_536,
        batch_bytes: int = 64 * 1024 * 1024,
        max_filter_ratio: float = 0.0,
        strict_mode: bool = True,
        request_timeout_seconds: Optional[float] = None,
        load_properties: Optional[Mapping[str, str]] = None,
    ) -> "DorisWriteOptions":
        """Freeze caller-owned load properties before serializing the sink."""
        properties = tuple(sorted((load_properties or {}).items()))
        return cls(
            operation=operation,
            format=format,
            batch_rows=batch_rows,
            batch_bytes=batch_bytes,
            max_filter_ratio=max_filter_ratio,
            strict_mode=strict_mode,
            request_timeout_seconds=request_timeout_seconds,
            load_properties=properties,
        )

    def label(self) -> str:
        """Generate the fixed RFC label format for one physical request."""
        label = _LABEL_PREFIX + uuid4().hex
        if len(label) > _MAX_LABEL_LENGTH:
            raise DorisConfigurationError(
                "generated Stream Load label exceeds Doris's length limit"
            )
        return label
