"""Worker-side streaming readers for Doris protocols."""

from __future__ import annotations

import importlib.util
import logging
import sys
import time
from typing import Any, Iterable, Iterator, Optional, Sequence

import pyarrow as pa
import pymysql

from ray_doris._errors import DorisConfigurationError, DorisReadError
from ray_doris._logging import (
    DiagnosticSnapshot,
    EventContext,
    ProgressBudget,
    bind,
    current_context,
    emit,
    error_type,
    new_id,
    stage,
)
from ray_doris._models import (
    _ADBC_CONNECT_TIMEOUT_OPTION,
    DorisInputSplit,
    DorisReadConfig,
)
from ray_doris._planner import (
    _contains_mysql_tls_error,
    _mysql_access_error,
    _mysql_connection_kwargs,
    _mysql_error_code,
    _mysql_tls_is_configured,
)
from ray_doris._schema import coerce_decimal
from ray_doris._sql import build_read_sql

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
    return build_read_sql(config, split.tablet_ids, row_limit=split.row_limit)[0]


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
                raise DorisReadError("invalid Doris BOOLEAN value")
        return normalized
    return values


def _validate_result_columns(column_names: Sequence[str], schema: pa.Schema) -> None:
    if tuple(column_names) != tuple(schema.names):
        raise DorisReadError(
            f"Doris result columns {tuple(column_names)!r} do not match planned schema "
            f"{tuple(schema.names)!r}"
        )


def _validate_nullability(array: pa.Array[Any], field: pa.Field[Any]) -> None:
    if not field.nullable and array.null_count:
        raise DorisReadError(
            f"Doris column {field.name!r} contains NULL but the planned schema is non-nullable"
        )


def _rows_to_table(
    rows: Sequence[Sequence[object]], column_names: Sequence[str], schema: pa.Schema
) -> pa.Table:
    _validate_result_columns(column_names, schema)
    arrays = []
    for index, field in enumerate(schema):
        values = _normalize_values([row[index] for row in rows], field.type)
        try:
            array = pa.array(values, type=field.type)
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError, ValueError):
            raise DorisReadError(
                f"failed to convert Doris column {field.name!r} to {field.type}"
            ) from None
        _validate_nullability(array, field)
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=schema)


def _close_reader_resource(resource: Any, *, label: str, context: str) -> None:
    if resource is None:
        return
    try:
        resource.close()
    except Exception as error:
        active = current_context()
        if active is not None:
            active.cleanup_error = True
        emit(
            logger,
            logging.WARNING,
            "doris.cleanup.failed",
            phase="cleanup",
            status="failed",
            resource_kind="mysql_cursor" if label == "MySQL cursor" else "mysql_connection",
            error_type=error_type(error),
        )


def _detach_mysql_result(connection: Any, cursor: Any, *, context: str) -> None:
    """Detach PyMySQL's active result after closing its socket without draining.

    PyMySQL 1.x has no public non-draining ``SSCursor`` cancellation API. The
    private references used here are isolated in this helper and covered by the
    required real-Doris integration test.
    """
    if connection is None and cursor is None:
        return
    try:
        result = getattr(cursor, "_result", None)
        if result is None:
            result = getattr(connection, "_result", None)
        if result is not None:
            result.unbuffered_active = False
            result.connection = None
        if connection is not None:
            connection._result = None
        if cursor is not None:
            cursor._result = None
            cursor.connection = None
    except Exception as error:
        active = current_context()
        if active is not None:
            active.cleanup_error = True
        emit(
            logger,
            logging.WARNING,
            "doris.cleanup.failed",
            phase="detach",
            status="failed",
            resource_kind="mysql_result",
            error_type=error_type(error),
        )


def _cleanup_mysql_reader(
    connection: Any,
    cursor: Any,
    *,
    reached_eof: bool,
    context: str,
) -> None:
    if reached_eof:
        _close_reader_resource(cursor, label="MySQL cursor", context=context)
        _close_reader_resource(connection, label="MySQL connection", context=context)
        return
    _close_reader_resource(connection, label="MySQL connection", context=context)
    _detach_mysql_result(connection, cursor, context=context)


def _close_flight_resource(resource: Any, kind: str) -> None:
    """Report close failures while preserving Flight's existing exception semantics."""
    try:
        resource.close()
    except Exception as error:
        active = current_context()
        if active is not None:
            active.cleanup_error = True
        emit(
            logger,
            logging.WARNING,
            "doris.cleanup.failed",
            phase="cleanup",
            status="failed",
            resource_kind=kind,
            error_type=error_type(error),
        )
        raise


def read_mysql(
    config: DorisReadConfig, split: DorisInputSplit, schema: pa.Schema
) -> Iterator[pa.Table]:
    """Stream PyMySQL rows in bounded batches and convert to canonical Arrow."""
    active = current_context()
    if active is not None:
        active.transport = "mysql"
    connection: Any = None
    cursor: Any = None
    reached_eof = False
    emitted = False
    context = _split_context(config, split)
    try:
        try:
            with stage(logger, "doris.read.setup", "connection"):
                connection = pymysql.connect(**_mysql_connection_kwargs(config, streaming=True))
                cursor = connection.cursor()
            sql, parameters = build_read_sql(
                config, split.tablet_ids, bind=True, row_limit=split.row_limit
            )
            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)
            if cursor.description is None:
                raise DorisReadError("Doris SELECT returned no column metadata")
            names = tuple(description[0] for description in cursor.description)
            while True:
                rows = cursor.fetchmany(config.batch_size)
                if not rows:
                    reached_eof = True
                    break
                table = _rows_to_table(rows, names, schema)
                del rows
                emitted = True
                yield table
            if config.catalog is not None and not emitted:
                yield pa.Table.from_batches([], schema=schema)
        finally:
            _cleanup_mysql_reader(
                connection,
                cursor,
                reached_eof=reached_eof,
                context=context,
            )
    except DorisConfigurationError:
        raise
    except DorisReadError as exc:
        raise DorisReadError(f"failed to read Doris {context}: {exc}") from exc
    except pymysql.MySQLError as exc:
        if _contains_mysql_tls_error(exc):
            raise DorisReadError(f"Doris MySQL TLS validation failed for {context}") from None
        access_error = _mysql_access_error(
            exc,
            operation="split read",
            context=context,
        )
        if access_error is not None:
            raise access_error from None
        raise DorisReadError(
            f"failed to read Doris {context} through the "
            f"MySQL protocol (MySQL error {_mysql_error_code(exc)!r})"
        ) from None
    except OSError:
        if _mysql_tls_is_configured(config):
            raise DorisReadError(
                f"Doris MySQL TLS configuration is invalid for {context}"
            ) from None
        raise DorisReadError(f"failed to read Doris {context} through the MySQL protocol") from None
    except Exception:
        raise DorisReadError(f"failed to read Doris {context} through the MySQL protocol") from None


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
    except ImportError:
        raise _flight_import_error() from None
    db_kwargs = {
        DatabaseOptions.USERNAME.value: config.user,
        DatabaseOptions.PASSWORD.value: config.resolve_password(),
        _ADBC_CONNECT_TIMEOUT_OPTION: str(config.connect_timeout),
    }
    db_kwargs.update(config.adbc_options())
    return flight_sql.connect(
        uri=f"{config.flight_scheme}://{config.host}:{config.flight_port}",
        db_kwargs=db_kwargs,
    )


def _cast_flight_batch(batch: pa.RecordBatch, schema: pa.Schema) -> pa.Table:
    _validate_result_columns(batch.schema.names, schema)
    arrays = []
    for index, field in enumerate(schema):
        source_field = batch.schema.field(index)
        try:
            array = batch.column(index).cast(field.type, safe=True)
        except (
            pa.ArrowInvalid,
            pa.ArrowTypeError,
            OverflowError,
            TypeError,
            ValueError,
        ):
            raise DorisReadError(
                f"failed to convert Doris Flight column {field.name!r} "
                f"from {source_field.type} to {field.type}"
            ) from None
        _validate_nullability(array, field)
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=schema)


def _slice_flight_table(table: pa.Table, batch_size: int) -> Iterator[pa.Table]:
    """Bound Ray output blocks without claiming control of the server RecordBatch size."""
    for offset in range(0, table.num_rows, batch_size):
        yield table.slice(offset, batch_size)


def read_flight(
    config: DorisReadConfig, split: DorisInputSplit, schema: pa.Schema
) -> Iterator[pa.Table]:
    """Stream Arrow RecordBatches from ADBC Flight SQL."""
    active = current_context()
    if active is not None:
        active.transport = "flight"
    try:
        try:
            with stage(logger, "doris.read.setup", "connection"):
                connection = _flight_connection(config)
        except ImportError:
            raise _FlightUnavailableError(
                f"Flight SQL setup is unavailable for {_split_context(config, split)}"
            ) from None
        except Exception as exc:
            if _is_flight_setup_error(exc, allow_timeout=True):
                raise _FlightUnavailableError(
                    f"Flight SQL setup is unavailable for {_split_context(config, split)}"
                ) from None
            raise
        try:
            try:
                cursor = connection.cursor()
            except Exception as exc:
                if _is_flight_setup_error(exc, allow_timeout=True):
                    raise _FlightUnavailableError(
                        f"Flight SQL setup is unavailable for {_split_context(config, split)}"
                    ) from None
                raise
            try:
                try:
                    cursor.execute(_query(config, split))
                    reader = cursor.fetch_record_batch()
                except Exception as exc:
                    if _is_flight_setup_error(exc, allow_timeout=False):
                        raise _FlightUnavailableError(
                            f"Flight SQL setup is unavailable for {_split_context(config, split)}"
                        ) from None
                    raise
                try:
                    emitted = False
                    for batch in reader:
                        table = _cast_flight_batch(batch, schema)
                        emitted = emitted or table.num_rows > 0
                        yield from _slice_flight_table(table, config.batch_size)
                    if config.catalog is not None and not emitted:
                        yield pa.Table.from_batches([], schema=schema)
                finally:
                    _close_flight_resource(reader, "flight_reader")
            finally:
                _close_flight_resource(cursor, "flight_cursor")
        finally:
            _close_flight_resource(connection, "flight_connection")
    except (_FlightUnavailableError, DorisConfigurationError):
        raise
    except DorisReadError as exc:
        raise DorisReadError(
            f"failed to read Doris {_split_context(config, split)}: {exc}"
        ) from exc
    except Exception:
        raise DorisReadError(
            f"failed to read Doris {_split_context(config, split)} through Flight SQL"
        ) from None


def _is_flight_setup_error(exc: BaseException, *, allow_timeout: bool = True) -> bool:
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
        if isinstance(candidate, TimeoutError):
            return allow_timeout
        if isinstance(candidate, (ImportError, OSError)):
            return True
        if (
            isinstance(candidate, error_type)
            and getattr(candidate, "status_code", None) in fallback_statuses
        ):
            if (
                getattr(candidate, "status_code", None) == AdbcStatusCode.TIMEOUT
                and not allow_timeout
            ):
                return False
            return True
        candidate = candidate.__cause__
    return False


def _select_reader(
    config: DorisReadConfig, split: DorisInputSplit, schema: pa.Schema
) -> Iterable[pa.Table]:
    """Select a transport and limit auto fallback to Flight setup failures."""
    if config.transport == "mysql":
        return read_mysql(config, split, schema)
    if config.transport == "flight":
        _ensure_flight_available()
        return read_flight(config, split, schema)
    if not _flight_is_installed():
        if config.flight_scheme == "grpc+tls":
            raise _flight_import_error()
        active = current_context()
        if active is not None:
            active.transport = "mysql"
        emit(
            logger,
            logging.DEBUG,
            "doris.read.selection",
            phase="setup",
            status="selected",
            reason_code="flight_extra_unavailable",
            transport="mysql",
        )
        return read_mysql(config, split, schema)

    def auto_reader() -> Iterator[pa.Table]:
        try:
            yield from read_flight(config, split, schema)
        except _FlightUnavailableError:
            if config.flight_scheme == "grpc+tls":
                raise DorisReadError(
                    "Flight SQL TLS setup failed; refusing automatic MySQL fallback"
                ) from None
            active = current_context()
            if active is not None:
                active.transport = "mysql"
            emit(
                logger,
                logging.WARNING,
                "doris.transport.fallback",
                phase="setup",
                status="fallback",
                from_transport="flight",
                to_transport="mysql",
                reason_code="flight_setup_unavailable",
            )
            yield from read_mysql(config, split, schema)

    return auto_reader()


def read_split(
    config: DorisReadConfig,
    split: DorisInputSplit,
    schema: pa.Schema,
    diagnostics: Optional[DiagnosticSnapshot] = None,
) -> Iterator[pa.Table]:
    """Observe an existing stream without retaining context across consumer yields."""
    context = EventContext(
        diagnostics or DiagnosticSnapshot(),
        role="worker",
        transport="mysql" if config.transport == "mysql" else "flight",
    )
    # Preserve the existing eager dependency checks, without starting a database stream.
    with bind(context):
        try:
            selected = _select_reader(config, split, schema)
        except BaseException as error:
            emit(
                logger,
                logging.ERROR,
                "doris.read.finished",
                phase="selection",
                status="failed",
                error_type=error_type(error),
            )
            raise

    def observed() -> Iterator[pa.Table]:
        context.execution_id = new_id()
        started = time.monotonic()
        rows = blocks = 0
        first = False
        status = "succeeded"
        failure: Optional[str] = None
        budget = ProgressBudget(context.snapshot.log_batches)
        iterator = iter(selected)
        emit(
            logger,
            logging.DEBUG,
            "doris.read.started",
            context=context,
            phase="read",
            status="started",
            batch_size=config.batch_size,
        )
        try:
            while True:
                try:
                    with bind(context):
                        table = next(iterator)
                except StopIteration:
                    break
                blocks += 1
                rows += table.num_rows
                if table.num_rows and not first:
                    first = True
                    emit(
                        logger,
                        logging.DEBUG,
                        "doris.read.first_block",
                        context=context,
                        phase="read",
                        status="produced",
                        duration_ms=(time.monotonic() - started) * 1000,
                    )
                if budget.allow(logger):
                    emit(
                        logger,
                        logging.DEBUG,
                        "doris.read.progress",
                        context=context,
                        phase="read",
                        status="produced",
                        batch_index=blocks,
                        emitted_rows=rows,
                        arrow_bytes=table.nbytes,
                    )
                yield table
        except GeneratorExit:
            status = "consumer_closed"
            raise
        except BaseException as error:
            status = "cancelled" if isinstance(error, KeyboardInterrupt) else "failed"
            failure = error_type(error)
            raise
        finally:
            try:
                with bind(context):
                    close = getattr(iterator, "close", None)
                    if close is not None:
                        close()
            except BaseException as error:
                status = "failed"
                failure = error_type(error)
                raise
            finally:
                emit(
                    logger,
                    logging.ERROR if status == "failed" else logging.DEBUG,
                    "doris.read.finished",
                    context=context,
                    phase="read",
                    status=status,
                    emitted_rows=rows,
                    emitted_blocks=blocks,
                    cleanup_error=context.cleanup_error,
                    duration_ms=(time.monotonic() - started) * 1000,
                    error_type=failure,
                    suppressed_progress=budget.seen - budget.emitted,
                )

    return observed()
