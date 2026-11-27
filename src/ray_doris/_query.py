"""Read trusted Doris SQL directly through one streaming MySQL task."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from functools import partial
from math import isfinite, isnan
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence, cast

import pyarrow as pa
import pymysql
from pymysql.constants import FIELD_TYPE
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask

from ray_doris._compat import ensure_supported_ray_version, make_read_task
from ray_doris._errors import DorisConfigurationError, DorisReadError, DorisSchemaError
from ray_doris._models import DorisReadConfig, QualifiedTable
from ray_doris._planner import _mysql_access_error, _mysql_connection_kwargs, _mysql_error_code
from ray_doris._readers import _cleanup_mysql_reader, _rows_to_table, _validate_result_columns
from ray_doris.datasource import _validate_config_serialization

_TEXT_TYPES = {
    FIELD_TYPE.VARCHAR,
    FIELD_TYPE.VAR_STRING,
    FIELD_TYPE.STRING,
    FIELD_TYPE.TINY_BLOB,
    FIELD_TYPE.MEDIUM_BLOB,
    FIELD_TYPE.LONG_BLOB,
    FIELD_TYPE.BLOB,
    FIELD_TYPE.JSON,
}


def _protocol_schema(description: Sequence[Sequence[Any]]) -> pa.Schema:
    """Build a stable Arrow schema from the MySQL result metadata."""
    fields = []
    for column in description:
        if len(column) < 7 or not isinstance(column[0], str) or not column[0]:
            raise DorisSchemaError("Doris query returned invalid column metadata")
        name, type_code, _, width, _, scale, _ = column[:7]
        data_type: pa.DataType
        if type_code == FIELD_TYPE.TINY:
            data_type = pa.bool_() if width == 1 else pa.int8()
        elif type_code == FIELD_TYPE.SHORT:
            data_type = pa.int16()
        elif type_code in (FIELD_TYPE.LONG, FIELD_TYPE.INT24):
            data_type = pa.int32()
        elif type_code == FIELD_TYPE.LONGLONG:
            data_type = pa.int64()
        elif type_code == FIELD_TYPE.FLOAT:
            data_type = pa.float32()
        elif type_code == FIELD_TYPE.DOUBLE:
            data_type = pa.float64()
        elif type_code in (FIELD_TYPE.DATE, FIELD_TYPE.NEWDATE):
            data_type = pa.date32()
        elif type_code in (FIELD_TYPE.DATETIME, FIELD_TYPE.TIMESTAMP):
            data_type = pa.timestamp("us")
        elif type_code in (FIELD_TYPE.DECIMAL, FIELD_TYPE.NEWDECIMAL):
            if (
                isinstance(width, bool)
                or not isinstance(width, int)
                or isinstance(scale, bool)
                or not isinstance(scale, int)
            ):
                raise DorisSchemaError(f"Doris decimal metadata is incomplete for {name!r}")
            precision = width - (2 if scale else 1)
            if not 1 <= precision <= 76 or not 0 <= scale <= precision:
                raise DorisSchemaError(f"Doris decimal metadata is invalid for {name!r}")
            data_type = (
                pa.decimal128(precision, scale)
                if precision <= 38
                else pa.decimal256(precision, scale)
            )
        elif type_code == FIELD_TYPE.NULL:
            data_type = pa.null()
        elif type_code in _TEXT_TYPES:
            # Doris may report VARCHAR, LARGEINT and complex types identically.
            # The default retains the textual value supplied by PyMySQL.
            data_type = pa.string()
        else:
            raise DorisSchemaError(f"unsupported MySQL result type {type_code!r} for {name!r}")
        # MySQL result nullability flags are not reliable for Doris expressions.
        fields.append(pa.field(name, data_type, nullable=True))
    if not fields:
        raise DorisSchemaError("Doris query returned no columns")
    if len({field.name for field in fields}) != len(fields):
        raise DorisSchemaError("Doris query returned duplicate column names")
    return pa.schema(fields)


def _validate_schema(schema: Optional[pa.Schema]) -> None:
    if schema is None:
        return
    if not isinstance(schema, pa.Schema) or not len(schema):
        raise DorisConfigurationError("schema must be a non-empty Arrow schema")
    if len(set(schema.names)) != len(schema.names):
        raise DorisConfigurationError("schema must have unique column names")


@dataclass(frozen=True)
class QueryReadConfig:
    """Immutable query and existing MySQL connection settings."""

    connection: DorisReadConfig
    query: str = field(repr=False)
    schema: Optional[pa.Schema] = None


def _query_connection(config: DorisReadConfig) -> Any:
    kwargs = _mysql_connection_kwargs(config, streaming=True)
    # Queries name their own tables; the synthetic config table is never selected.
    kwargs.pop("database", None)
    return pymysql.connect(**kwargs)


def _query_error(exc: pymysql.MySQLError) -> Exception:
    access_error = _mysql_access_error(exc, operation="query read", context="query")
    if access_error is not None:
        return access_error
    code = _mysql_error_code(exc)
    if type(code) is int:
        return DorisReadError(f"Doris query read failed (MySQL error {code})")
    return DorisReadError("Doris query read failed")


def _validate_numeric_values(
    rows: Sequence[Sequence[object]],
    table: pa.Table,
    schema: pa.Schema,
    description: Sequence[Sequence[Any]],
) -> None:
    for index, arrow_field in enumerate(schema):
        for row, converted in zip(rows, table.column(index).to_pylist()):
            original = row[index]
            if isinstance(original, (int, float, Decimal)) and isinstance(
                converted, (int, float, Decimal)
            ):
                if (
                    description[index][1] == FIELD_TYPE.FLOAT
                    and pa.types.is_float32(arrow_field.type)
                    and isinstance(original, float)
                    and isinstance(converted, float)
                    and isfinite(original)
                    and isfinite(converted)
                    and (original == 0 or converted != 0)
                ):
                    continue
                both_nan = (
                    isinstance(original, (float, Decimal))
                    and isinstance(converted, (float, Decimal))
                    and isnan(original)
                    and isnan(converted)
                )
                if not both_nan and original != converted:
                    raise DorisReadError(
                        f"lossy conversion of Doris column {arrow_field.name!r} "
                        f"to {arrow_field.type}"
                    )


def _read_query(config: QueryReadConfig) -> Iterator[pa.Table]:
    """Execute the caller's SQL once per task attempt and stream its result."""
    connection: Any = None
    cursor: Any = None
    reached_eof = False
    emitted = False
    try:
        connection = _query_connection(config.connection)
        cursor = connection.cursor()
        cursor.execute(config.query)
        if cursor.description is None:
            raise DorisReadError("Doris executed the SQL but returned no result set")
        names = tuple(description[0] for description in cursor.description)
        schema = config.schema or _protocol_schema(cursor.description)
        _validate_result_columns(names, schema)
        while True:
            rows = cursor.fetchmany(config.connection.batch_size)
            if not rows:
                reached_eof = True
                break
            if config.schema is None:
                for index, field in enumerate(schema):
                    if pa.types.is_string(field.type) and any(
                        value is not None and not isinstance(value, str)
                        for value in (row[index] for row in rows)
                    ):
                        raise DorisReadError(
                            f"MySQL returned non-text values for text column {field.name!r}"
                        )
            table = _rows_to_table(rows, names, schema)
            if config.schema is not None:
                _validate_numeric_values(rows, table, schema, cursor.description)
            del rows
            emitted = True
            yield table
        if not emitted:
            yield pa.Table.from_batches([], schema=schema)
    except (DorisConfigurationError, DorisSchemaError, DorisReadError):
        raise
    except pymysql.MySQLError as exc:
        raise _query_error(exc) from None
    except Exception:
        raise DorisReadError("Doris query read failed") from None
    finally:
        _cleanup_mysql_reader(connection, cursor, reached_eof=reached_eof, context="query")


class DorisQueryDatasource(Datasource):
    """Execute trusted SQL in one streaming Ray read task."""

    def __init__(
        self,
        *,
        query: str,
        host: str,
        mysql_port: int,
        user: str,
        password: str,
        password_env: Optional[str],
        batch_size: int,
        connect_timeout: float,
        client_kwargs: Optional[Mapping[str, Any]],
        schema: Optional[pa.Schema] = None,
    ) -> None:
        ensure_supported_ray_version()
        initialize = cast(Callable[[], None], super().__init__)
        initialize()
        if not isinstance(query, str) or not query.strip():
            raise DorisConfigurationError("query must be a non-empty SQL string")
        _validate_schema(schema)
        connection = DorisReadConfig.from_options(
            table=QualifiedTable("_query", "_result"),
            host=host,
            mysql_port=mysql_port,
            user=user,
            password=password,
            password_env=password_env,
            batch_size=batch_size,
            connect_timeout=connect_timeout,
            client_options=client_kwargs,
        )
        _validate_config_serialization(connection)
        self._config = QueryReadConfig(connection, query, schema)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[Any] = None,
    ) -> list[ReadTask]:
        del parallelism, data_context
        metadata = BlockMetadata(num_rows=None, size_bytes=None, exec_stats=None, input_files=None)
        return [
            make_read_task(
                partial(_read_query, self._config),
                metadata,
                self._config.schema,
                per_task_row_limit,
            )
        ]
