"""Doris structured metadata and explicit Hive Parquet eligibility checks."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Tuple

import pyarrow as pa
import pymysql

from ray_doris._errors import DorisError, DorisPermissionError, DorisPlanningError, DorisSchemaError
from ray_doris._hms import HivePartition, HiveStorage, HiveTable, open_hms
from ray_doris._models import DorisReadConfig, NativeReadOptions
from ray_doris._planner import _mysql_access_error, _mysql_connection_kwargs, _mysql_error_code
from ray_doris._schema import DorisColumn, build_arrow_schema, doris_type_to_arrow
from ray_doris._sql import quote_identifier

_PARQUET_INPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
_PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"


@dataclass(frozen=True)
class ExternalMetadata:
    schema: pa.Schema
    storage: HiveStorage
    partitions: Tuple[HivePartition, ...] = field(repr=False)
    partition_keys: Tuple[Tuple[str, str], ...]
    version: Tuple[int, ...]


def doris_build_version(comment: str) -> Tuple[int, ...]:
    match = re.search(
        r"\bdoris\s+version\s+(?:doris-)?(\d+\.\d+\.\d+(?:\.\d+)?)(?![\d.])", comment, re.I
    )
    if match is None:
        raise DorisPlanningError("cannot identify the Doris build from version_comment")
    version = tuple(int(part) for part in match.group(1).split("."))
    if version not in ((3, 0, 6, 2), (4, 0, 6)):
        raise DorisPlanningError("native reads currently target Doris 3.0.6.2 and 4.0.6")
    return version


def _hive_type(value: str, name: str) -> pa.DataType:
    if (
        re.fullmatch(
            r"boolean|tinyint|smallint|int|bigint|float|double|string|date|timestamp|"
            r"decimal\(\s*\d+\s*,\s*\d+\s*\)",
            value.lower(),
        )
        is None
    ):
        raise DorisSchemaError("unsupported native Hive scalar type")
    aliases = {"timestamp": "DATETIME(6)", "binary": "UNSUPPORTED_BINARY"}
    return doris_type_to_arrow(aliases.get(value.lower(), value), column_name=name)


def information_schema_type(value: str) -> str:
    """Normalize the two verified Doris branches' MySQL display representation.

    Their columns scanner renders BOOLEAN as tinyint(1), and real TINYINT as
    tinyint(4). Keep the default DESCRIBE mapper unchanged.
    """
    return "BOOLEAN" if value.strip().upper() == "TINYINT(1)" else value


def validate_storage(storage: HiveStorage, parameters: Mapping[str, str]) -> None:
    if storage.input_format != _PARQUET_INPUT or storage.serde != _PARQUET_SERDE:
        raise DorisPlanningError("native reads require ordinary Hive Parquet storage")
    props = {key.lower(): value for key, value in parameters.items()}
    if (
        props.get("transactional", "false").lower() != "false"
        or props.get("spark.sql.sources.provider", "hive").lower() not in ("hive", "parquet")
        or props.get("table_type", "").upper() in ("ICEBERG", "HUDI", "DELTA")
        or "storage_handler" in props
        or "meta_table_storage" in props
        or any(key.startswith(("hoodie.", "delta.")) for key in props)
    ):
        raise DorisPlanningError(
            "native reads do not support transactional or table-format storage"
        )


def validate_table(table: HiveTable, schema: pa.Schema) -> None:
    if table.table_type not in ("MANAGED_TABLE", "EXTERNAL_TABLE"):
        raise DorisPlanningError("native reads require an ordinary managed or external Hive table")
    validate_storage(table.storage, dict(table.parameters))
    expected = table.storage.columns + table.partition_keys
    if len(expected) != len(schema) or [name for name, _ in expected] != schema.names:
        raise DorisSchemaError("Doris and HMS column identities or order differ")
    for name, value in expected:
        source_type = _hive_type(value, name)
        if source_type != schema.field(name).type:
            raise DorisSchemaError(
                f"Doris and HMS column types differ for {name!r}: "
                f"Doris={schema.field(name).type}, HMS={source_type}"
            )
    for _name, value in table.partition_keys:
        if value.lower() not in ("string", "bigint"):
            raise DorisSchemaError("native partition keys currently require STRING or BIGINT")


def partition_value(value: str, type_name: str) -> Any:
    if value is None or value == "__HIVE_DEFAULT_PARTITION__":
        raise DorisPlanningError("native reads do not support NULL/default partitions")
    if type_name.lower() == "string":
        return value
    try:
        number = int(value)
    except (ValueError, TypeError):
        raise DorisPlanningError("invalid BIGINT partition value") from None
    if not -(2**63) <= number < 2**63:
        raise DorisPlanningError("BIGINT partition value is out of range")
    return number


def _rows(cursor: Any, sql: str, parameters: Any = None) -> List[Dict[str, Any]]:
    cursor.execute(sql, parameters)
    return list(cursor.fetchall())


def discover_external(
    config: DorisReadConfig, catalog: str, options: NativeReadOptions
) -> ExternalMetadata:
    kwargs = _mysql_connection_kwargs(config, streaming=False)
    kwargs.pop("database", None)
    kwargs.update(cursorclass=pymysql.cursors.DictCursor)
    kwargs.setdefault("read_timeout", options.metadata_timeout)
    kwargs.setdefault("write_timeout", options.metadata_timeout)
    qualified = ".".join(
        quote_identifier(part)
        for part in (catalog, config.table.database, config.table.table + "$partitions")
    )
    stage = "connection"
    try:
        with pymysql.connect(**kwargs) as connection, connection.cursor() as cursor:
            stage = "version"
            version = doris_build_version(
                str(_rows(cursor, "SELECT @@version_comment AS build")[0]["build"])
            )
            stage = "catalog"
            rows = _rows(
                cursor,
                "SELECT CatalogType, Property, Value FROM catalogs() WHERE CatalogName = %s",
                (catalog,),
            )
            if not rows:
                # The relational view filters invisible catalogs instead of raising.
                # A direct metadata statement distinguishes denial from an empty view.
                _rows(cursor, f"SHOW CATALOG {quote_identifier(catalog)}")
                raise DorisPlanningError("the requested Hive catalog is not visible")
            if any(str(row["CatalogType"]).lower() != "hms" for row in rows):
                raise DorisPlanningError("native reads require a visible Hive catalog")
            props = {str(row["Property"]): str(row["Value"]) for row in rows}
            if (
                props.get("hive.metastore.type", "hms").lower() != "hms"
                or props.get("hive.metastore.sasl.enabled", "false").lower() != "false"
                or props.get("lower_case_meta_names", "false").lower() != "false"
                or props.get("meta_names_mapping", "")
            ):
                raise DorisPlanningError("unsupported HMS authentication or remote-name mapping")
            uris = tuple(
                uri.strip()
                for uri in props.get("hive.metastore.uris", "").split(",")
                if uri.strip()
            )
            uri = options.hms_uri or (uris[0] if uris else "")
            if not uri or uri not in uris:
                raise DorisPlanningError("HMS URI must match the Doris catalog")
            stage = "columns"
            rows = _rows(
                cursor,
                f"SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE FROM "
                f"{quote_identifier(catalog)}.information_schema.columns "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
                (config.table.database, config.table.table),
            )
            if any(row["IS_NULLABLE"] not in ("YES", "NO") for row in rows):
                raise DorisPlanningError("Doris returned invalid column nullability")
            schema = build_arrow_schema(
                tuple(
                    DorisColumn(
                        row["COLUMN_NAME"],
                        information_schema_type(row["COLUMN_TYPE"]),
                        row["IS_NULLABLE"] == "YES",
                    )
                    for row in rows
                )
            )
            if not rows:
                raise DorisPlanningError("Doris column metadata is empty or inaccessible")
            with open_hms(uri, options.metadata_timeout) as hms:
                table = hms.table(config.table.database, config.table.table)
                validate_table(table, schema)
                partitions = (
                    hms.partitions(
                        config.table.database, config.table.table, options.max_partitions
                    )
                    if table.partition_keys
                    else ()
                )
                if table.partition_keys:
                    stage = "partition-values"
                    values = _rows(
                        cursor, f"SELECT * FROM {qualified} LIMIT {options.max_partitions + 1}"
                    )
                    declared = Counter(
                        tuple(
                            partition_value(value, type_name)
                            for value, (_, type_name) in zip(part.values, table.partition_keys)
                        )
                        for part in partitions
                    )
                    observed = Counter(
                        tuple(row[name] for name, _ in table.partition_keys) for row in values
                    )
                    if observed != declared:
                        raise DorisPlanningError("Doris and HMS registered partition values differ")
                for partition in partitions:
                    validate_storage(partition.storage, dict(partition.parameters))
                    if (
                        len(partition.values) != len(table.partition_keys)
                        or partition.storage.columns != table.storage.columns
                    ):
                        raise DorisSchemaError(
                            "partition schema or value count differs from the table"
                        )
            return ExternalMetadata(
                schema, table.storage, partitions, table.partition_keys, version
            )
    except pymysql.MySQLError as exc:
        access_error = _mysql_access_error(
            exc, operation="metadata discovery", context="external table"
        )
        # Both target Doris branches define catalog-denial codes in addition
        # to the standard table/database MySQL permission codes.
        wrapped_catalog_denial = (
            stage == "catalog"
            and _mysql_error_code(exc) == 1105
            and any(
                isinstance(arg, str) and "Access denied for user" in arg and "to catalog" in arg
                for arg in exc.args
            )
        )
        if access_error is None and (
            _mysql_error_code(exc) in (1221, 5087) or wrapped_catalog_denial
        ):
            access_error = DorisPermissionError("Doris denied external catalog metadata access")
        if access_error is not None:
            raise access_error from None
        raise DorisPlanningError(
            f"Doris external metadata request failed during {stage} "
            f"(MySQL error {_mysql_error_code(exc)})"
        ) from None
    except DorisError:
        raise
    except (KeyError, IndexError, TypeError, ValueError):
        raise DorisPlanningError(
            "Doris returned incomplete or incompatible external metadata"
        ) from None
