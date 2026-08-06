"""SQL validation and rendering helpers."""

from __future__ import annotations

import re
from typing import Iterable, Optional, Sequence, Tuple

from ray_doris._errors import DorisConfigurationError
from ray_doris._models import QualifiedTable

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_DISALLOWED_FILTER_TOKENS = (";", "--", "#", "/*", "*/")


def _has_disallowed_filter_token(predicate: str) -> bool:
    quote: Optional[str] = None
    index = 0
    while index < len(predicate):
        character = predicate[index]
        if quote is None:
            if character in ("'", '"', "`"):
                quote = character
            elif any(predicate.startswith(token, index) for token in _DISALLOWED_FILTER_TOKENS):
                return True
        elif character == "\\" and quote != "`":
            index += 1
        elif character == quote:
            if index + 1 < len(predicate) and predicate[index + 1] == quote:
                index += 1
            else:
                quote = None
        index += 1
    return quote is not None


def validate_identifier(value: str, *, kind: str = "identifier") -> str:
    """Validate a Doris identifier accepted by the public API."""
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise DorisConfigurationError(f"invalid {kind}: {value!r}")
    return value


def parse_table(value: str) -> QualifiedTable:
    """Parse an exact ``database.table`` reference for the internal catalog."""
    if not isinstance(value, str):
        raise DorisConfigurationError("table must be a string in database.table form")
    parts = value.split(".")
    if len(parts) != 2:
        raise DorisConfigurationError("table must use exact database.table form")
    return QualifiedTable(
        database=validate_identifier(parts[0], kind="database name"),
        table=validate_identifier(parts[1], kind="table name"),
    )


def normalize_columns(columns: Optional[Iterable[str]]) -> Optional[Tuple[str, ...]]:
    """Validate and freeze an optional projection."""
    if columns is None:
        return None
    if isinstance(columns, (str, bytes)):
        raise DorisConfigurationError("columns must be an iterable of column names, not a string")
    normalized = tuple(validate_identifier(column, kind="column name") for column in columns)
    if not normalized:
        raise DorisConfigurationError("columns must be None or a non-empty iterable")
    if len(set(normalized)) != len(normalized):
        raise DorisConfigurationError("columns must not contain duplicates")
    return normalized


def normalize_filter(predicate: Optional[str]) -> Optional[str]:
    """Validate the trusted scalar SQL expression supplied as a filter."""
    if predicate is None:
        return None
    if not isinstance(predicate, str) or not predicate.strip():
        raise DorisConfigurationError("filter must be None or a non-empty SQL expression")
    if _has_disallowed_filter_token(predicate):
        raise DorisConfigurationError("filter must be a single SQL expression without comments")
    return predicate.strip()


def quote_identifier(value: str) -> str:
    """Quote a previously validated identifier."""
    return f"`{validate_identifier(value)}`"


def render_table(table: QualifiedTable) -> str:
    """Render a qualified table name."""
    return f"{quote_identifier(table.database)}.{quote_identifier(table.table)}"


def build_select_sql(
    table: QualifiedTable,
    columns: Optional[Sequence[str]],
    predicate: Optional[str],
    tablet_ids: Optional[Sequence[int]],
) -> str:
    """Build a Doris SELECT with TABLET placed before WHERE."""
    projection = "*" if columns is None else ", ".join(quote_identifier(c) for c in columns)
    sql = f"SELECT {projection} FROM {render_table(table)}"
    if tablet_ids is not None:
        if not tablet_ids:
            raise DorisConfigurationError("tablet_ids must be None or non-empty")
        validated = []
        for tablet_id in tablet_ids:
            if isinstance(tablet_id, bool) or not isinstance(tablet_id, int) or tablet_id <= 0:
                raise DorisConfigurationError(f"invalid tablet id: {tablet_id!r}")
            validated.append(str(tablet_id))
        sql += f" TABLET({', '.join(validated)})"
    if predicate is not None:
        sql += f" WHERE {normalize_filter(predicate)}"
    return sql


def build_describe_sql(table: QualifiedTable) -> str:
    """Build the schema discovery statement."""
    return f"DESCRIBE {render_table(table)}"
