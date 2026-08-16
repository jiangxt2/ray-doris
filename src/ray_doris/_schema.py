"""Doris DESCRIBE parsing and fail-closed Arrow schema mapping."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Optional, Sequence, Tuple

import pyarrow as pa

from ray_doris._errors import DorisSchemaError

_TYPE_PATTERN = re.compile(r"^([A-Z][A-Z0-9_]*)\s*(?:\(([^)]*)\))?$")
_UNSUPPORTED_PREFIXES = ("ARRAY<", "MAP<", "STRUCT<")
_UNSUPPORTED_TYPES = {
    "AGG_STATE",
    "BITMAP",
    "HLL",
    "QUANTILE_STATE",
    "TIME",
    "TIMEV2",
    "VARIANT",
}


@dataclass(frozen=True)
class DorisColumn:
    """The schema fields used from one DESCRIBE row."""

    name: str
    doris_type: str
    nullable: bool


def parse_describe_rows(rows: Iterable[Sequence[Any]]) -> Tuple[DorisColumn, ...]:
    """Convert PyMySQL DESCRIBE tuples into stable column records."""
    columns = []
    for row in rows:
        if len(row) < 3:
            raise DorisSchemaError(f"invalid DESCRIBE row: {row!r}")
        name, doris_type, nullable = row[0], row[1], row[2]
        if not isinstance(name, str) or not isinstance(doris_type, str):
            raise DorisSchemaError(f"invalid DESCRIBE row: {row!r}")
        nullable_text = str(nullable).upper()
        if nullable_text not in {"YES", "NO"}:
            raise DorisSchemaError(
                f"invalid DESCRIBE nullability for column {name!r}: {nullable!r}"
            )
        columns.append(
            DorisColumn(name=name, doris_type=doris_type, nullable=nullable_text == "YES")
        )
    if not columns:
        raise DorisSchemaError("DESCRIBE returned no columns")
    return tuple(columns)


def doris_type_to_arrow(doris_type: str, *, column_name: str) -> pa.DataType:
    """Map a concrete Doris scalar type to Arrow without lossy inference."""
    normalized = doris_type.strip().upper()
    if normalized.startswith(_UNSUPPORTED_PREFIXES):
        raise DorisSchemaError(f"unsupported Doris type for column {column_name!r}: {doris_type!r}")
    match = _TYPE_PATTERN.fullmatch(normalized)
    if match is None:
        raise DorisSchemaError(f"unsupported Doris type for column {column_name!r}: {doris_type!r}")
    base, arguments = match.groups()
    if base in _UNSUPPORTED_TYPES:
        raise DorisSchemaError(f"unsupported Doris type for column {column_name!r}: {doris_type!r}")
    simple: dict[str, pa.DataType] = {
        "BOOLEAN": pa.bool_(),
        "TINYINT": pa.int8(),
        "SMALLINT": pa.int16(),
        "INT": pa.int32(),
        "INTEGER": pa.int32(),
        "BIGINT": pa.int64(),
        "LARGEINT": pa.decimal256(39, 0),
        "FLOAT": pa.float32(),
        "DOUBLE": pa.float64(),
        "CHAR": pa.string(),
        "VARCHAR": pa.string(),
        "STRING": pa.string(),
        "JSON": pa.string(),
        "JSONB": pa.string(),
        "DATE": pa.date32(),
        "DATEV2": pa.date32(),
        "DATETIME": pa.timestamp("us"),
        "DATETIMEV2": pa.timestamp("us"),
    }
    if base in simple:
        return simple[base]
    if base in {
        "DECIMAL",
        "DECIMALV2",
        "DECIMALV3",
        "DECIMAL32",
        "DECIMAL64",
        "DECIMAL128",
        "DECIMAL256",
    }:
        if arguments is None:
            raise DorisSchemaError(
                f"decimal type for column {column_name!r} must include precision and scale"
            )
        parts = [part.strip() for part in arguments.split(",")]
        if len(parts) != 2:
            raise DorisSchemaError(
                f"invalid decimal type for column {column_name!r}: {doris_type!r}"
            )
        try:
            precision, scale = (int(part) for part in parts)
        except ValueError as exc:
            raise DorisSchemaError(
                f"invalid decimal type for column {column_name!r}: {doris_type!r}"
            ) from exc
        if precision <= 0 or precision > 76 or scale < 0 or scale > precision:
            raise DorisSchemaError(
                f"invalid decimal bounds for column {column_name!r}: {doris_type!r}"
            )
        if precision <= 38:
            return pa.decimal128(precision, scale)
        return pa.decimal256(precision, scale)
    raise DorisSchemaError(f"unsupported Doris type for column {column_name!r}: {doris_type!r}")


def build_arrow_schema(
    columns: Sequence[DorisColumn], projection: Optional[Sequence[str]] = None
) -> pa.Schema:
    """Build a canonical schema, preserving projection order and nullability."""
    by_name = {column.name: column for column in columns}
    if len(by_name) != len(columns):
        raise DorisSchemaError("DESCRIBE returned duplicate column names")
    selected = tuple(by_name) if projection is None else tuple(projection)
    missing = [name for name in selected if name not in by_name]
    if missing:
        raise DorisSchemaError(f"unknown projected columns: {', '.join(missing)}")
    return pa.schema(
        [
            pa.field(
                name,
                doris_type_to_arrow(by_name[name].doris_type, column_name=name),
                nullable=by_name[name].nullable,
            )
            for name in selected
        ]
    )


def coerce_decimal(value: Any) -> Optional[Decimal]:
    """Normalize decimal-compatible values without a float round trip."""
    if value is None or isinstance(value, Decimal):
        return value
    return Decimal(str(value))
