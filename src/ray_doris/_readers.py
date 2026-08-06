"""Worker-side streaming readers for Doris protocols."""

from __future__ import annotations

import importlib.util
import logging
import sys
from typing import Any, Iterable, Iterator, Optional, Sequence

import pyarrow as pa
import pymysql

from ray_doris._errors import DorisReadError
from ray_doris._models import DorisInputSplit, DorisReadConfig
from ray_doris._planner import _mysql_connection_kwargs
from ray_doris._schema import coerce_decimal
from ray_doris._sql import build_select_sql

logger = logging.getLogger(__name__)


class _FlightUnavailableError(DorisReadError):
    """A Flight setup failure that auto transport may safely fall back from."""


def _flight_import_error() -> ImportError:
    if sys.version_info < (3, 10):
        return ImportError(
            "Flight SQL requires Python 3.10 or newer; use the MySQL transport on Python 3.9"
        )
    return ImportError('Flight SQL support is not installed; run pip install "ray-doris[flight]"')


def _query(config: DorisReadConfig, split: DorisInputSplit) -> str:
    return build_select_sql(config.table, config.columns, config.filter, split.tablet_ids)


def _split_context(config: DorisReadConfig, split: DorisInputSplit) -> str:
    tablets = "all" if split.tablet_ids is None else ",".join(map(str, split.tablet_ids))
    return f"{config.table.database}.{config.table.table} tablets={tablets}"


def _normalize_values(values: Sequence[object], data_type: pa.DataType) -> Sequence[object]:
    if pa.types.is_decimal(data_type):
        return [coerce_decimal(value) for value in values]
    if pa.types.is_boolean(data_type):
        normalized = []
        for value in values:
            if value is None or isinstance(value, bool):
                normalized.append(value)
            elif isinstance(value, int) and value in (0, 1):
                normalized.append(bool(value))
            else:
                raise DorisReadError(f"invalid Doris BOOLEAN value: {value!r}")
        return normalized
    return values


def _rows_to_table(
    rows: Sequence[Sequence[object]], column_names: Sequence[str], schema: pa.Schema
) -> pa.Table:
    if tuple(column_names) != tuple(schema.names):
        raise DorisReadError(
            f"Doris result columns {tuple(column_names)!r} do not match planned schema "
            f"{tuple(schema.names)!r}"
        )
    arrays = []
    for index, field in enumerate(schema):
        values = _normalize_values([row[index] for row in rows], field.type)
        try:
            arrays.append(pa.array(values, type=field.type))
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError, ValueError) as exc:
            raise DorisReadError(
                f"failed to convert Doris column {field.name!r} to {field.type}"
            ) from exc
    return pa.Table.from_arrays(arrays, schema=schema)


def read_mysql(
    config: DorisReadConfig, split: DorisInputSplit, schema: pa.Schema
) -> Iterator[pa.Table]:
    """Stream PyMySQL rows in bounded batches and convert to canonical Arrow."""
    try:
        connection = pymysql.connect(**_mysql_connection_kwargs(config, streaming=True))
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(_query(config, split))
                if cursor.description is None:
                    raise DorisReadError("Doris SELECT returned no column metadata")
                names = tuple(description[0] for description in cursor.description)
                while True:
                    rows = cursor.fetchmany(config.batch_size)
                    if not rows:
                        break
                    table = _rows_to_table(rows, names, schema)
                    del rows
                    yield table
            finally:
                cursor.close()
        finally:
            connection.close()
    except DorisReadError as exc:
        raise DorisReadError(
            f"failed to read Doris {_split_context(config, split)}: {exc}"
        ) from exc
    except Exception as exc:
        raise DorisReadError(
            f"failed to read Doris {_split_context(config, split)} through the MySQL protocol"
        ) from exc


def _flight_is_installed() -> bool:
    try:
        return (
            importlib.util.find_spec("adbc_driver_flightsql.dbapi") is not None
            and importlib.util.find_spec("adbc_driver_manager.dbapi") is not None
        )
    except (ModuleNotFoundError, ValueError):
        return False


def _ensure_flight_available() -> None:
    if not _flight_is_installed():
        raise _flight_import_error()


def _flight_connection(config: DorisReadConfig) -> Any:
    try:
        import adbc_driver_flightsql.dbapi as flight_sql
        from adbc_driver_manager import DatabaseOptions
    except ImportError as exc:
        raise _flight_import_error() from exc
    db_kwargs = {
        DatabaseOptions.USERNAME.value: config.user,
        DatabaseOptions.PASSWORD.value: config.password,
    }
    db_kwargs.update(config.adbc_options())
    return flight_sql.connect(
        uri=f"{config.flight_scheme}://{config.host}:{config.flight_port}",
        db_kwargs=db_kwargs,
    )


def _cast_flight_batch(batch: pa.RecordBatch, schema: pa.Schema) -> pa.Table:
    if tuple(batch.schema.names) != tuple(schema.names):
        raise DorisReadError(
            f"Doris Flight result columns {tuple(batch.schema.names)!r} do not match planned "
            f"schema {tuple(schema.names)!r}"
        )
    try:
        return pa.Table.from_batches([batch]).cast(schema, safe=True)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError, ValueError) as exc:
        raise DorisReadError("Doris Flight result does not match the planned schema") from exc


def _slice_flight_table(table: pa.Table, batch_size: int) -> Iterator[pa.Table]:
    """Bound Ray output blocks without claiming control of the server RecordBatch size."""
    for offset in range(0, table.num_rows, batch_size):
        yield table.slice(offset, batch_size)


def read_flight(
    config: DorisReadConfig, split: DorisInputSplit, schema: pa.Schema
) -> Iterator[pa.Table]:
    """Stream Arrow RecordBatches from ADBC Flight SQL."""
    try:
        try:
            connection = _flight_connection(config)
        except ImportError as exc:
            raise _FlightUnavailableError(
                f"Flight SQL setup is unavailable for {_split_context(config, split)}"
            ) from exc
        except Exception as exc:
            if _is_flight_setup_error(exc):
                raise _FlightUnavailableError(
                    f"Flight SQL setup is unavailable for {_split_context(config, split)}"
                ) from exc
            raise
        try:
            try:
                cursor = connection.cursor()
            except Exception as exc:
                if _is_flight_setup_error(exc):
                    raise _FlightUnavailableError(
                        f"Flight SQL setup is unavailable for {_split_context(config, split)}"
                    ) from exc
                raise
            try:
                try:
                    cursor.execute(_query(config, split))
                    reader = cursor.fetch_record_batch()
                except Exception as exc:
                    if _is_flight_setup_error(exc):
                        raise _FlightUnavailableError(
                            f"Flight SQL setup is unavailable for {_split_context(config, split)}"
                        ) from exc
                    raise
                try:
                    for batch in reader:
                        table = _cast_flight_batch(batch, schema)
                        yield from _slice_flight_table(table, config.batch_size)
                finally:
                    reader.close()
            finally:
                cursor.close()
        finally:
            connection.close()
    except _FlightUnavailableError:
        raise
    except DorisReadError as exc:
        raise DorisReadError(
            f"failed to read Doris {_split_context(config, split)}: {exc}"
        ) from exc
    except Exception as exc:
        raise DorisReadError(
            f"failed to read Doris {_split_context(config, split)} through Flight SQL"
        ) from exc


def _is_flight_setup_error(exc: BaseException) -> bool:
    error_type: Any = ()
    fallback_statuses: set[Any] = set()
    try:
        from adbc_driver_manager import AdbcStatusCode
        from adbc_driver_manager.dbapi import Error
    except ImportError:
        pass
    else:
        error_type = Error
        fallback_statuses = {
            AdbcStatusCode.IO,
            AdbcStatusCode.NOT_IMPLEMENTED,
            AdbcStatusCode.TIMEOUT,
        }
    candidate: Optional[BaseException] = exc
    seen = set()
    while candidate is not None and id(candidate) not in seen:
        seen.add(id(candidate))
        if isinstance(candidate, (ImportError, OSError)):
            return True
        if (
            isinstance(candidate, error_type)
            and getattr(candidate, "status_code", None) in fallback_statuses
        ):
            return True
        candidate = candidate.__cause__
    return False


def read_split(
    config: DorisReadConfig, split: DorisInputSplit, schema: pa.Schema
) -> Iterable[pa.Table]:
    """Select a transport and limit auto fallback to Flight setup failures."""
    if config.transport == "mysql":
        return read_mysql(config, split, schema)
    if config.transport == "flight":
        _ensure_flight_available()
        return read_flight(config, split, schema)
    if not _flight_is_installed():
        logger.info("Flight SQL extra is unavailable; using the MySQL protocol")
        return read_mysql(config, split, schema)

    def auto_reader() -> Iterator[pa.Table]:
        try:
            yield from read_flight(config, split, schema)
        except _FlightUnavailableError:
            logger.warning("Flight SQL setup is unavailable; using MySQL")
            yield from read_mysql(config, split, schema)

    return auto_reader()
