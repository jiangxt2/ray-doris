"""Structured external metadata with narrowly classified capability fallback."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

import pymysql

from ray_doris._errors import DorisPlanningError, _NativeReadNotApplicableError
from ray_doris._models import QualifiedTable
from ray_doris._schema import DorisColumn, parse_information_schema_rows
from ray_doris._sql import build_describe_sql, build_external_columns_sql, quote_identifier

MetadataQuery = Callable[[Any, str, Any], List[Dict[str, Any]]]


def capability_missing(error: pymysql.MySQLError, source: str) -> bool:
    """Recognize absence only in a fixed metadata request, never general SQL failure."""
    code = error.args[0] if error.args else None
    message = " ".join(arg for arg in error.args[1:] if isinstance(arg, str)).lower()
    if code in (1044, 1045, 1142, 1143, 1221, 1227, 5087) or any(
        word in message for word in ("denied", "permission", "authentication", "not authorized")
    ):
        return False
    if source == "session":
        return code == 1193 or (code == 1105 and "unknown" in message and "variable" in message)
    if source == "catalog":
        return code in (1054, 1305) or (
            code in (1064, 1105)
            and "catalogs" in message
            and any(word in message for word in ("unknown", "not found", "no matching", "syntax"))
        )
    if source == "columns":
        return code == 1054 or (
            code in (1105, 1109, 1146)
            and ("information_schema" in message or "columns" in message)
            and any(word in message for word in ("unknown", "not exist", "not found"))
        )
    if source == "partitions":
        return (
            code in (1054, 1105, 1109, 1146, 1305)
            and ("$partitions" in message or "partition_values" in message)
            and any(
                word in message for word in ("unknown", "not exist", "not found", "unsupported")
            )
        )
    if source in ("show-catalog", "show-partitions"):
        statement = "catalog" if source == "show-catalog" else "partition"
        return (
            code in (1064, 1105)
            and statement in message
            and any(word in message for word in ("syntax", "unsupported", "not supported"))
        )
    return False


def metadata_rows(cursor: Any, sql: str, parameters: Any = None) -> List[Dict[str, Any]]:
    cursor.execute(sql, parameters) if parameters is not None else cursor.execute(sql)
    rows: List[Dict[str, Any]] = []
    while True:
        batch = cursor.fetchmany(256)
        if not batch:
            return rows
        for row in batch:
            if not isinstance(row, Mapping):
                raise DorisPlanningError("Doris returned malformed structured metadata")
            rows.append(dict(row))


def named_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize result labels, retaining ambiguity as an error."""
    if not isinstance(row, Mapping) or any(not isinstance(key, str) for key in row):
        raise DorisPlanningError("Doris returned malformed metadata labels")
    result = {key.lower(): value for key, value in row.items()}
    if len(result) != len(row):
        raise DorisPlanningError("Doris returned duplicate metadata labels")
    return result


class ExternalMetadataReader:
    """Cache facts and their origins within one driver-owned metadata connection."""

    def __init__(self, cursor: Any, query: Optional[MetadataQuery] = None) -> None:
        self.cursor = cursor
        self.query = query or metadata_rows
        self.origins: Dict[str, str] = {}
        self._columns: Dict[Tuple[str, QualifiedTable], Tuple[DorisColumn, ...]] = {}
        self._catalogs: Dict[str, Dict[str, str]] = {}

    def catalog(self, catalog: str) -> Dict[str, str]:
        if catalog in self._catalogs:
            return dict(self._catalogs[catalog])
        try:
            rows = self.query(
                self.cursor,
                "SELECT CatalogType, Property, Value FROM catalogs() WHERE CatalogName = %s",
                (catalog,),
            )
        except pymysql.MySQLError as error:
            if not capability_missing(error, "catalog"):
                raise
            rows = []
        if rows:
            normalized = [named_row(row) for row in rows]
            kinds = [row.get("catalogtype") for row in normalized]
            if any(not isinstance(value, str) for value in kinds):
                raise DorisPlanningError("Doris returned invalid catalog identities")
            if len(set(kinds)) != 1:
                raise DorisPlanningError("Doris returned invalid catalog identities")
            kind = kinds[0]
            properties = [(row.get("property"), row.get("value")) for row in normalized]
            origin = "catalogs()"
        else:
            # Native SHOW checks visibility even when a relational view is empty.
            try:
                shown = self.query(self.cursor, f"SHOW CATALOG {quote_identifier(catalog)}", None)
                catalogs = [
                    named_row(row) for row in self.query(self.cursor, "SHOW CATALOGS", None)
                ]
            except pymysql.MySQLError as error:
                if not capability_missing(error, "show-catalog"):
                    raise
                raise _NativeReadNotApplicableError(
                    "Doris lacks structured Hive catalog metadata"
                ) from None
            properties = [(row.get("key"), row.get("value")) for row in map(named_row, shown)]
            matches = [row for row in catalogs if row.get("catalogname") == catalog]
            if len(matches) != 1 or not isinstance(matches[0].get("type"), str):
                raise DorisPlanningError("the requested catalog identity is absent or invisible")
            kind = matches[0]["type"]
            origin = "show-catalog"
        if not isinstance(kind, str) or kind.lower() != "hms":
            raise _NativeReadNotApplicableError("native reads require a visible Hive catalog")
        props: Dict[str, str] = {}
        for key, value in properties:
            if not isinstance(key, str) or not isinstance(value, str) or key in props:
                raise DorisPlanningError("Doris returned malformed catalog properties")
            props[key] = value
        if not props:
            raise DorisPlanningError("Doris catalog properties are empty or inaccessible")
        if "type" in props and props["type"].lower() != kind.lower():
            raise DorisPlanningError("Doris catalog type and properties disagree")
        self.origins["catalog"] = origin
        self._catalogs[catalog] = props
        return dict(props)

    def columns(self, table: QualifiedTable, catalog: str) -> Tuple[DorisColumn, ...]:
        key = (catalog, table)
        if key in self._columns:
            return self._columns[key]
        try:
            rows = self.query(
                self.cursor, build_external_columns_sql(catalog), (table.database, table.table)
            )
        except pymysql.MySQLError as error:
            if not capability_missing(error, "columns"):
                raise
            rows = []
        if rows:
            columns = parse_information_schema_rows(rows)
            origin = "information_schema.columns"
        else:
            rows = self.query(self.cursor, build_describe_sql(table, catalog), None)
            if not rows:
                raise DorisPlanningError(
                    "Doris external DESCRIBE returned empty or invisible columns"
                )
            declarations = []
            for item in rows:
                row = named_row(item)
                value = row.get("type")
                # TEXT is the external STRING display alias, not a global type extension.
                value = "STRING" if isinstance(value, str) and value.upper() == "TEXT" else value
                nullable = row.get("null")
                declarations.append(
                    {
                        "COLUMN_NAME": row.get("field"),
                        "COLUMN_TYPE": value,
                        "IS_NULLABLE": nullable.upper() if isinstance(nullable, str) else nullable,
                    }
                )
            columns = parse_information_schema_rows(declarations)
            origin = "describe"
        self.origins["columns"] = origin
        self._columns[key] = columns
        return columns

    def native_settings(self, *, timestamp: bool) -> Tuple[Any, Any]:
        try:
            rows = self.query(
                self.cursor,
                "SELECT @@hive_parquet_use_column_names AS use_names, @@time_zone AS timezone",
                None,
            )
        except pymysql.MySQLError as error:
            if not capability_missing(error, "session"):
                raise
            values: List[Any] = []
            for name in ("hive_parquet_use_column_names", "time_zone"):
                if name == "time_zone" and not timestamp:
                    values.append(None)
                    continue
                rows = self.query(self.cursor, f"SHOW VARIABLES LIKE '{name}'", None)
                if not rows:
                    raise _NativeReadNotApplicableError(
                        "required native session setting is unavailable"
                    ) from None
                row = named_row(rows[0])
                if len(rows) != 1 or row.get("variable_name") != name or "value" not in row:
                    raise DorisPlanningError(
                        "Doris returned malformed session variable metadata"
                    ) from None
                values.append(row["value"])
            self.origins["session"] = "show-variables"
            return values[0], values[1]
        if len(rows) != 1:
            raise DorisPlanningError("Doris returned incomplete native session settings")
        row = named_row(rows[0])
        if "use_names" not in row or (timestamp and "timezone" not in row):
            raise DorisPlanningError("Doris returned incomplete native session settings")
        self.origins["session"] = "session-select"
        return row["use_names"], row.get("timezone")
