"""Doris metadata discovery and fail-closed write validation."""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Optional, cast

import pyarrow as pa
import pyarrow.compute as pc
import pymysql

from ray_doris._errors import (
    DorisAuthenticationError,
    DorisMetadataError,
    DorisPermissionError,
    DorisSchemaError,
    DorisTableCompatibilityError,
)
from ray_doris._schema import DorisColumn, doris_type_to_arrow, parse_describe_rows
from ray_doris.write.connection import DorisConnection, DorisTable

_MODEL = re.compile(r"\b(DUPLICATE|UNIQUE|AGGREGATE)\s+KEY\b", re.IGNORECASE)
_PROPERTIES = re.compile(r"\bPROPERTIES\b", re.IGNORECASE)
_MOW_PROPERTY = "enable_unique_key_merge_on_write"


@dataclass(frozen=True)
class DorisTableMetadata:
    """Normalized table model, key information, and column declarations."""

    model: str
    key_columns: tuple[str, ...]
    merge_on_write: bool
    columns: tuple[str, ...] = ()
    column_specs: tuple[DorisColumn, ...] = ()


def _quoted_text_end(text: str, start: int) -> int:
    index = start + 1
    while index < len(text):
        if text[index] != "`":
            index += 1
            continue
        if index + 1 < len(text) and text[index + 1] == "`":
            index += 2
            continue
        return index + 1
    raise DorisMetadataError("Doris CREATE TABLE metadata has an unterminated key identifier")


def _quoted_literal_end(text: str, start: int) -> int:
    quote = text[start]
    index = start + 1
    while index < len(text):
        character = text[index]
        if character == "\\":
            index += 2
            continue
        if character == quote:
            if index + 1 < len(text) and text[index + 1] == quote:
                index += 2
                continue
            return index + 1
        index += 1
    raise DorisMetadataError("Doris CREATE TABLE metadata has an unterminated string literal")


def _quoted_end(text: str, start: int) -> int:
    if text[start] == "`":
        return _quoted_text_end(text, start)
    if text[start] in ("'", '"'):
        return _quoted_literal_end(text, start)
    raise DorisMetadataError("Doris CREATE TABLE metadata has an invalid quoted token")


def _find_parenthesized_clause(text: str, start: int) -> str:
    opening = start
    while opening < len(text) and text[opening].isspace():
        opening += 1
    if opening >= len(text) or text[opening] != "(":
        raise DorisMetadataError("Doris CREATE TABLE metadata has an invalid key clause")
    depth = 0
    index = opening
    content_start = opening + 1
    while index < len(text):
        character = text[index]
        if character in ("`", "'", '"'):
            index = _quoted_end(text, index)
            continue
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return text[content_start:index]
            if depth < 0:
                break
        index += 1
    raise DorisMetadataError("Doris CREATE TABLE metadata has an unterminated key clause")


def _split_key_columns(raw: str) -> tuple[str, ...]:
    if not raw.strip():
        return ()
    parts: list[str] = []
    start = 0
    index = 0
    while index < len(raw):
        if raw[index] == "`":
            index = _quoted_text_end(raw, index)
            continue
        if raw[index] == ",":
            parts.append(raw[start:index])
            start = index + 1
        index += 1
    parts.append(raw[start:])
    if any(not part.strip() for part in parts):
        raise DorisMetadataError("Doris CREATE TABLE metadata has an empty key column")
    return tuple(_parse_identifier(part) for part in parts)


def _parse_identifier(raw: str) -> str:
    identifier = raw.strip()
    if not identifier:
        raise DorisMetadataError("Doris CREATE TABLE metadata has an empty key column")
    if identifier.startswith("`"):
        end = _quoted_text_end(identifier, 0)
        if end != len(identifier):
            raise DorisMetadataError("Doris CREATE TABLE metadata has an invalid key column")
        decoded = identifier[1:-1].replace("``", "`")
        if not decoded:
            raise DorisMetadataError("Doris CREATE TABLE metadata has an empty key column")
        return decoded
    if any(quote in identifier for quote in ("`", "'", '"')):
        raise DorisMetadataError("Doris CREATE TABLE metadata has an invalid key column")
    return identifier


def _find_model_clause(text: str) -> Optional[re.Match[str]]:
    index = 0
    while index < len(text):
        if text[index] in ("`", "'", '"'):
            index = _quoted_end(text, index)
            continue
        match = _MODEL.match(text, index)
        if match is not None:
            return match
        index += 1
    return None


def _find_properties_clause(text: str, start: int) -> Optional[str]:
    index = start
    while index < len(text):
        if text[index] in ("`", "'", '"'):
            index = _quoted_end(text, index)
            continue
        match = _PROPERTIES.match(text, index)
        if match is not None:
            return _find_parenthesized_clause(text, match.end())
        index += 1
    return None


def _property_token(text: str, start: int) -> tuple[str, int]:
    index = start
    while index < len(text) and text[index].isspace():
        index += 1
    if index >= len(text):
        raise DorisMetadataError("Doris CREATE TABLE metadata has an invalid property clause")
    if text[index] in ("'", '"', "`"):
        end = _quoted_end(text, index)
        value = text[index + 1 : end - 1]
        if text[index] in ("'", '"'):
            value = value.replace(text[index] * 2, text[index])
        return value, end
    end = index
    while end < len(text) and not text[end].isspace() and text[end] not in "=,":
        end += 1
    if end == index:
        raise DorisMetadataError("Doris CREATE TABLE metadata has an invalid property clause")
    return text[index:end], end


def _has_true_mow_property(properties: Optional[str]) -> bool:
    if properties is None:
        return False
    index = 0
    found = False
    while True:
        while index < len(properties) and (properties[index].isspace() or properties[index] == ","):
            index += 1
        if index >= len(properties):
            return found
        key, index = _property_token(properties, index)
        while index < len(properties) and properties[index].isspace():
            index += 1
        if index >= len(properties) or properties[index] != "=":
            raise DorisMetadataError("Doris CREATE TABLE metadata has an invalid property clause")
        value, index = _property_token(properties, index + 1)
        if key.casefold() == _MOW_PROPERTY and value.casefold() == "true":
            found = True
        while index < len(properties) and properties[index].isspace():
            index += 1
        if index < len(properties) and properties[index] != ",":
            raise DorisMetadataError("Doris CREATE TABLE metadata has an invalid property clause")


def parse_create_table(create_sql: str) -> DorisTableMetadata:
    """Parse only model, key, and Merge-on-Write properties from SHOW CREATE output."""
    match = _find_model_clause(create_sql)
    if match is None:
        raise DorisMetadataError("Doris CREATE TABLE metadata has no supported data model")
    model = match.group(1).upper()
    key_columns = _split_key_columns(_find_parenthesized_clause(create_sql, match.end()))
    if not key_columns:
        raise DorisMetadataError("Doris table metadata has no key columns")
    return DorisTableMetadata(
        model,
        key_columns,
        _has_true_mow_property(_find_properties_clause(create_sql, match.end())),
    )


def _metadata_error(exc: BaseException) -> Optional[BaseException]:
    code = exc.args[0] if getattr(exc, "args", ()) and isinstance(exc.args[0], int) else None
    text = str(exc).casefold()
    if code in {1045} or "access denied" in text and "using password" in text:
        return DorisAuthenticationError("Doris rejected credentials during metadata discovery")
    if code in {1044, 1142, 1143, 1227} or any(
        marker in text for marker in ("command denied", "permission", "privilege", "access denied")
    ):
        return DorisPermissionError("Doris denied metadata discovery")
    if "unknown table" in text or "does not exist" in text or "unknown database" in text:
        return DorisMetadataError("Doris target table was not found")
    return None


def discover_table_metadata(connection: DorisConnection, table: DorisTable) -> DorisTableMetadata:
    """Read SHOW CREATE TABLE and DESCRIBE through a short-lived MySQL connection."""
    client: Any = None
    try:
        client = pymysql.connect(**cast(Any, connection.mysql_kwargs(table)))
        with client.cursor() as cursor:
            cursor.execute(f"SHOW CREATE TABLE {table.sql()}")
            row = cursor.fetchone()
        if not row or len(row) < 2 or not isinstance(row[1], str):
            raise DorisMetadataError("Doris returned no CREATE TABLE definition")
        parsed = parse_create_table(row[1])
        with client.cursor() as cursor:
            cursor.execute(f"DESCRIBE {table.sql()}")
            describe_rows = cursor.fetchall()
        try:
            column_specs = parse_describe_rows(describe_rows)
        except DorisSchemaError:
            raise DorisMetadataError("Doris table metadata is malformed") from None
        for column in column_specs:
            doris_type_to_arrow(column.doris_type, column_name=column.name)
        return DorisTableMetadata(
            parsed.model,
            parsed.key_columns,
            parsed.merge_on_write,
            tuple(column.name for column in column_specs),
            column_specs,
        )
    except (DorisMetadataError, DorisAuthenticationError, DorisPermissionError):
        raise
    except (pymysql.MySQLError, OSError) as exc:
        translated = _metadata_error(exc)
        if translated is not None:
            raise translated from None
        raise DorisMetadataError("Doris table metadata discovery failed") from None
    finally:
        if client is not None:
            with suppress(Exception):
                client.close()


def validate_write_table(
    metadata: DorisTableMetadata,
    *,
    operation: str,
    arrow_schema: Optional[pa.Schema] = None,
    arrow_table: Optional[pa.Table] = None,
) -> Optional[pa.Table]:
    """Validate an input schema and safely cast values to the Doris declaration."""
    if arrow_table is not None:
        if arrow_schema is not None and not arrow_schema.equals(
            arrow_table.schema, check_metadata=False
        ):
            raise DorisTableCompatibilityError("arrow_schema and arrow_table do not match")
        arrow_schema = arrow_table.schema
    if arrow_schema is None:
        raise DorisTableCompatibilityError("write data schema is required")
    if len(set(arrow_schema.names)) != len(arrow_schema.names):
        raise DorisTableCompatibilityError("write data must not contain duplicate columns")
    for field in arrow_schema:
        if pa.types.is_timestamp(field.type) and field.type.tz is not None:
            raise DorisTableCompatibilityError(
                "timezone-aware datetime columns are unsupported for Doris writes"
            )
    if operation not in ("load", "upsert", "partial_update"):
        raise DorisTableCompatibilityError("unsupported write operation")
    names = set(arrow_schema.names)
    missing = tuple(column for column in metadata.key_columns if column not in names)
    if missing:
        raise DorisTableCompatibilityError("write data is missing Doris key columns")
    if operation == "upsert" and metadata.model != "UNIQUE":
        raise DorisTableCompatibilityError("upsert requires a Unique Key table")
    if operation == "partial_update" and (
        metadata.model != "UNIQUE" or not metadata.merge_on_write
    ):
        raise DorisTableCompatibilityError(
            "partial_update requires a Merge-on-Write Unique Key table"
        )
    if metadata.columns:
        data_columns = tuple(arrow_schema.names)
        if operation in ("load", "upsert") and data_columns != metadata.columns:
            raise DorisTableCompatibilityError(
                "full-row writes must match the Doris table columns in order"
            )
        if operation == "partial_update" and any(
            column not in metadata.columns for column in data_columns
        ):
            raise DorisTableCompatibilityError("partial update contains an unknown Doris column")
    if not metadata.column_specs or arrow_table is None:
        return arrow_table

    specifications = {column.name: column for column in metadata.column_specs}
    if tuple(specifications) != metadata.columns:
        raise DorisMetadataError("Doris metadata columns are internally inconsistent")
    prepared_arrays: list[Any] = []
    for field, array in zip(arrow_schema, arrow_table.columns):
        specification = specifications.get(field.name)
        if specification is None:
            raise DorisTableCompatibilityError("write data contains an unknown Doris column")
        if not specification.nullable and array.null_count:
            raise DorisTableCompatibilityError(
                f"Doris column {field.name!r} is not nullable but write data contains NULL"
            )
        try:
            target_type = doris_type_to_arrow(
                specification.doris_type, column_name=specification.name
            )
        except DorisSchemaError:
            raise DorisTableCompatibilityError(
                f"Doris column {field.name!r} has an unsupported write type"
            ) from None
        normalized_type = specification.doris_type.strip().upper()
        if operation == "partial_update" and normalized_type.startswith(("JSON", "JSONB")):
            prepared_arrays.append(array)
            continue
        try:
            prepared_arrays.append(pc.cast(array, target_type, safe=True))
        except (
            pa.ArrowInvalid,
            pa.ArrowNotImplementedError,
            pa.ArrowTypeError,
            TypeError,
            ValueError,
        ):
            raise DorisTableCompatibilityError(
                f"write column {field.name!r} cannot be safely converted to Doris"
            ) from None
    return pa.Table.from_arrays(prepared_arrays, names=arrow_schema.names)
