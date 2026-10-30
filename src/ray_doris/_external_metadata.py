"""Doris structured metadata and explicit Hive Parquet eligibility checks."""

from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Tuple

import pyarrow as pa
import pymysql

from ray_doris._errors import (
    DorisError,
    DorisPlanningError,
    DorisSchemaError,
    _NativeReadNotApplicableError,
)
from ray_doris._hms import HivePartition, HiveStorage, HiveTable, open_hms
from ray_doris._metadata import ExternalMetadataReader, capability_missing, named_row
from ray_doris._models import DorisReadConfig, NativeReadOptions
from ray_doris._planner import _mysql_access_error, _mysql_connection_kwargs, _mysql_error_code
from ray_doris._schema import build_arrow_schema, doris_type_to_arrow
from ray_doris._sql import quote_identifier

logger = logging.getLogger(__name__)

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
    # Diagnostic only: the actual metadata contracts determine eligibility.
    return tuple(int(part) for part in match.group(1).split(".")) if match else ()


def _hive_type(value: str, name: str) -> pa.DataType:
    if (
        re.fullmatch(
            r"boolean|tinyint|smallint|int|bigint|float|double|string|date|timestamp|"
            r"decimal\(\s*\d+\s*,\s*\d+\s*\)",
            value.lower(),
        )
        is None
    ):
        raise _NativeReadNotApplicableError("unsupported native Hive scalar type")
    aliases = {"timestamp": "DATETIME(6)", "binary": "UNSUPPORTED_BINARY"}
    return doris_type_to_arrow(aliases.get(value.lower(), value), column_name=name)


def validate_storage(storage: HiveStorage, parameters: Mapping[str, str]) -> None:
    if storage.input_format != _PARQUET_INPUT or storage.serde != _PARQUET_SERDE:
        raise _NativeReadNotApplicableError("native reads require ordinary Hive Parquet storage")
    props = {key.lower(): value for key, value in parameters.items()}
    if (
        props.get("transactional", "false").lower() != "false"
        or props.get("spark.sql.sources.provider", "hive").lower() not in ("hive", "parquet")
        or props.get("table_type", "").upper() in ("ICEBERG", "HUDI", "DELTA")
        or "storage_handler" in props
        or "meta_table_storage" in props
        or any(key.startswith(("hoodie.", "delta.")) for key in props)
    ):
        raise _NativeReadNotApplicableError(
            "native reads do not support transactional or table-format storage"
        )


def validate_table(table: HiveTable, schema: pa.Schema) -> None:
    if table.table_type not in ("MANAGED_TABLE", "EXTERNAL_TABLE"):
        raise _NativeReadNotApplicableError(
            "native reads require an ordinary managed or external Hive table"
        )
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
            raise _NativeReadNotApplicableError(
                "native partition keys currently require STRING or BIGINT"
            )


def partition_value(value: str, type_name: str) -> Any:
    if value is None or value == "__HIVE_DEFAULT_PARTITION__":
        raise _NativeReadNotApplicableError("native reads do not support NULL/default partitions")
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


def validate_native_session(
    cursor: Any, schema: pa.Schema, reader: ExternalMetadataReader | None = None
) -> None:
    """Keep the file reader within the verified Doris name/UTC semantics."""
    reader = reader or ExternalMetadataReader(cursor, _rows)
    timestamp = any(pa.types.is_timestamp(column.type) for column in schema)
    use_names, timezone = reader.native_settings(timestamp=timestamp)
    if timestamp and (not isinstance(timezone, str) or not timezone):
        raise DorisPlanningError("Doris returned incomplete native session settings")
    by_name = str(use_names).lower()
    if by_name in ("false", "0"):
        raise _NativeReadNotApplicableError("positional Parquet mapping requires Doris execution")
    if by_name not in ("true", "1"):
        raise DorisPlanningError("Doris returned invalid Parquet name mapping")
    if timestamp and timezone not in (
        "UTC",
        "GMT",
        "Etc/UTC",
        "Etc/GMT",
        "+00:00",
        "-00:00",
        "GMT+00:00",
        "GMT-00:00",
    ):
        raise _NativeReadNotApplicableError("native timestamps require a UTC Doris session")


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
            try:
                version = doris_build_version(
                    str(_rows(cursor, "SELECT @@version_comment AS build")[0]["build"])
                )
            except pymysql.MySQLError as error:
                if not capability_missing(error, "session"):
                    raise
                version = ()
            reader = ExternalMetadataReader(cursor, _rows)
            stage = "catalog"
            props = reader.catalog(catalog)
            if props.get("access_controller.class", "") or any(
                key.startswith("access_controller.properties.") for key in props
            ):
                raise _NativeReadNotApplicableError(
                    "catalog access controller requires Doris execution"
                )
            if (
                props.get("hive.metastore.type", "hms").lower() != "hms"
                or props.get("hive.metastore.sasl.enabled", "false").lower() != "false"
                or props.get("lower_case_meta_names", "false").lower() != "false"
                or props.get("meta_names_mapping", "")
            ):
                raise _NativeReadNotApplicableError(
                    "unsupported HMS authentication or remote-name mapping"
                )
            uris = tuple(
                uri.strip()
                for uri in props.get("hive.metastore.uris", "").split(",")
                if uri.strip()
            )
            uri = options.hms_uri or (uris[0] if uris else "")
            if not uris:
                raise _NativeReadNotApplicableError("the catalog does not advertise an HMS URI")
            if uri not in uris:
                raise DorisPlanningError("HMS URI must match the Doris catalog")
            stage = "columns"
            columns = reader.columns(config.table, catalog)
            if any(
                column.doris_type.strip().upper().startswith(("ARRAY<", "MAP<", "STRUCT<"))
                for column in columns
            ):
                raise _NativeReadNotApplicableError("nested types require Doris execution")
            schema = build_arrow_schema(columns)
            validate_native_session(cursor, schema, reader)
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
                    try:
                        values = _rows(
                            cursor, f"SELECT * FROM {qualified} LIMIT {options.max_partitions + 1}"
                        )
                    except pymysql.MySQLError as error:
                        if not capability_missing(error, "partitions"):
                            raise
                        table_name = ".".join(
                            quote_identifier(part)
                            for part in (catalog, config.table.database, config.table.table)
                        )
                        try:
                            shown = _rows(cursor, f"SHOW PARTITIONS FROM {table_name}")
                        except pymysql.MySQLError as secondary:
                            if not capability_missing(secondary, "show-partitions"):
                                raise
                            raise _NativeReadNotApplicableError(
                                "Doris lacks registered Hive partition metadata"
                            ) from None
                        names = []
                        for item in shown:
                            row = named_row(item)
                            if "partition" not in row:
                                raise _NativeReadNotApplicableError(
                                    "Hive partition identity metadata is unavailable"
                                ) from None
                            if not isinstance(row["partition"], str):
                                raise DorisPlanningError(
                                    "Doris returned invalid partition identities"
                                ) from None
                            names.append(row["partition"])
                        if Counter(names) != Counter(part.name for part in partitions):
                            raise DorisPlanningError(
                                "Doris and HMS registered partition identities differ"
                            ) from None
                        reader.origins["partitions"] = "show-partitions"
                        values = None
                    else:
                        reader.origins["partitions"] = "$partitions"
                    declared = Counter(
                        tuple(
                            partition_value(value, type_name)
                            for value, (_, type_name) in zip(part.values, table.partition_keys)
                        )
                        for part in partitions
                    )
                    observed = (
                        Counter(
                            tuple(row[name] for name, _ in table.partition_keys) for row in values
                        )
                        if values is not None
                        else declared
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
            logger.debug(
                "Doris native metadata origin_count=%d origins=%s",
                len(reader.origins),
                ",".join(sorted(set(reader.origins.values()))),
            )
            return ExternalMetadata(
                schema, table.storage, partitions, table.partition_keys, version
            )
    except pymysql.MySQLError as exc:
        access_error = _mysql_access_error(
            exc, operation="metadata discovery", context="external table"
        )
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
