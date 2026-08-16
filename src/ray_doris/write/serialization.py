"""Bounded Arrow serialization for Doris Stream Load."""

from __future__ import annotations

import base64
import json
import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Iterator, NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq

from ray_doris._errors import DorisTableCompatibilityError


class SerializedBatch(NamedTuple):
    """One bounded payload ready for a request-scoped Stream Load."""

    payload: bytes
    rows: int
    columns: tuple[str, ...]


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise DorisTableCompatibilityError("non-finite JSON numbers are unsupported")
        return value
    if isinstance(value, (datetime, time)) and value.tzinfo is not None:
        raise DorisTableCompatibilityError("timezone-aware JSON temporal values are unsupported")
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise DorisTableCompatibilityError("non-finite JSON decimals are unsupported")
        return format(value, "f")
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    raise DorisTableCompatibilityError(f"unsupported JSON value type {type(value).__name__}")


def serialize_parquet(table: pa.Table) -> bytes:
    """Serialize one Arrow table to an in-memory compressed Parquet payload."""
    for field, column in zip(table.schema, table.columns):
        if pa.types.is_timestamp(field.type) and field.type.tz is not None:
            raise DorisTableCompatibilityError(
                "timezone-aware Parquet temporal values are unsupported"
            )
        if pa.types.is_floating(field.type) and any(
            value is not None and not math.isfinite(value) for value in column.to_pylist()
        ):
            raise DorisTableCompatibilityError("non-finite Parquet numbers are unsupported")
        if pa.types.is_decimal(field.type) and any(
            value is not None and not value.is_finite() for value in column.to_pylist()
        ):
            raise DorisTableCompatibilityError("non-finite Parquet decimals are unsupported")
    output = pa.BufferOutputStream()
    pq.write_table(table, output, compression="zstd")
    return output.getvalue().to_pybytes()


def serialize_json(table: pa.Table) -> bytes:
    """Serialize one Arrow table as line-delimited JSON."""
    lines = [
        json.dumps(
            {name: _json_value(value) for name, value in row.items()},
            separators=(",", ":"),
            ensure_ascii=False,
        )
        for row in table.to_pylist()
    ]
    return ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")


def _serialize(table: pa.Table, format: str) -> bytes:
    if format == "parquet":
        return serialize_parquet(table)
    if format == "json":
        return serialize_json(table)
    raise DorisTableCompatibilityError(f"unsupported Stream Load format {format!r}")


def iter_serialized_batches(
    table: pa.Table,
    *,
    format: str,
    max_rows: int,
    max_bytes: int,
) -> Iterator[SerializedBatch]:
    """Yield batches bounded by row and serialized-byte targets."""
    if max_rows <= 0 or max_bytes <= 0:
        raise DorisTableCompatibilityError("batch limits must be positive")
    columns = tuple(table.column_names)
    start = 0
    while start < table.num_rows:
        length = min(max_rows, table.num_rows - start)
        while True:
            candidate = table.slice(start, length)
            payload = _serialize(candidate, format)
            if len(payload) <= max_bytes or length == 1:
                break
            length = max(1, length // 2)
        yield SerializedBatch(payload, length, columns)
        start += length
