import sys
import traceback
from datetime import datetime
from decimal import Decimal
from types import ModuleType
from unittest.mock import Mock

import pyarrow as pa
import pytest

from ray_doris import _readers
from ray_doris._errors import (
    DorisAuthenticationError,
    DorisConfigurationError,
    DorisPermissionError,
    DorisReadError,
)
from ray_doris._models import DorisInputSplit, DorisReadConfig
from ray_doris._sql import parse_table


def make_config(**kwargs: object) -> DorisReadConfig:
    values = {
        "table": parse_table("db.table"),
        "host": "fe",
        "user": "reader",
        "batch_size": 2,
    }
    values.update(kwargs)
    return DorisReadConfig.from_options(**values)


def _assert_redacted_exception(exception: BaseException, caplog, sentinel: str) -> None:
    rendered_traceback = "".join(
        traceback.format_exception(type(exception), exception, exception.__traceback__)
    )
    assert sentinel not in str(exception)
    assert sentinel not in rendered_traceback
    assert sentinel not in caplog.text
    assert exception.__cause__ is None


class FakeResult:
    def __init__(self) -> None:
        self.connection = None
        self.unbuffered_active = True


class FakeCursor:
    def __init__(self, events=None) -> None:
        self.events = events if events is not None else []
        self.description = (("id",), ("amount",), ("created_at",))
        self._batches = [
            [(1, Decimal("12.34"), datetime(2025, 1, 1, 1, 2, 3, 456789))],
            [(2, Decimal("56.78"), datetime(2025, 1, 2, 1, 2, 3, 1))],
            [],
        ]
        self.fetchmany_calls = []
        self.closed = False
        self.close_calls = 0
        self.query = None
        self.connection = None
        self._result = FakeResult()

    def execute(self, query: str) -> None:
        self.query = query

    def fetchmany(self, size: int):
        self.fetchmany_calls.append(size)
        return self._batches.pop(0)

    def close(self) -> None:
        self.events.append("cursor-close")
        self.closed = True
        self.close_calls += 1


class FakeConnection:
    def __init__(self, cursor: FakeCursor, events=None) -> None:
        self.events = events if events is not None else cursor.events
        self._cursor = cursor
        cursor.connection = self
        cursor._result.connection = self
        self._result = cursor._result
        self.closed = False
        self.close_calls = 0

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.events.append("connection-close")
        self.closed = True
        self.close_calls += 1


def test_mysql_reader_streams_fetchmany_batches_and_closes_resources(monkeypatch) -> None:
    events = []
    cursor = FakeCursor(events)
    connection = FakeConnection(cursor, events)
    monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("amount", pa.decimal128(10, 2)),
            pa.field("created_at", pa.timestamp("us")),
        ]
    )
    tables = list(_readers.read_mysql(make_config(), DorisInputSplit((7,)), schema))
    assert [table.num_rows for table in tables] == [1, 1]
    assert tables[0]["created_at"][0].as_py().microsecond == 456789
    assert cursor.fetchmany_calls == [2, 2, 2]
    assert "TABLET(7)" in str(cursor.query)
    assert cursor.closed and connection.closed
    assert events == ["cursor-close", "connection-close"]
    assert cursor.close_calls == connection.close_calls == 1


def test_mysql_reader_resolves_environment_password_for_each_attempt(monkeypatch) -> None:
    schema = pa.schema(
        [("id", pa.int64()), ("amount", pa.decimal128(10, 2)), ("created_at", pa.timestamp("us"))]
    )
    connect = Mock(
        side_effect=[
            FakeConnection(FakeCursor()),
            FakeConnection(FakeCursor()),
        ]
    )
    monkeypatch.setattr(_readers.pymysql, "connect", connect)
    config = make_config(password_env="RAY_DORIS_WORKER_PASSWORD")
    monkeypatch.setenv("RAY_DORIS_WORKER_PASSWORD", "first-password")
    list(_readers.read_mysql(config, DorisInputSplit(None), schema))
    monkeypatch.setenv("RAY_DORIS_WORKER_PASSWORD", "second-password")
    list(_readers.read_mysql(config, DorisInputSplit(None), schema))
    assert connect.call_args_list[0].kwargs["password"] == "first-password"
    assert connect.call_args_list[1].kwargs["password"] == "second-password"


def test_mysql_reader_missing_environment_password_fails_before_network(
    monkeypatch, caplog
) -> None:
    sentinel = "RAY_DORIS_READER_MISSING_PASSWORD_SENTINEL"
    connect = Mock()
    monkeypatch.setattr(_readers.pymysql, "connect", connect)
    monkeypatch.delenv(sentinel, raising=False)
    with pytest.raises(
        DorisConfigurationError, match="environment variable is unavailable"
    ) as captured:
        list(
            _readers.read_mysql(
                make_config(password_env=sentinel),
                DorisInputSplit(None),
                pa.schema([("id", pa.int64())]),
            )
        )
    _assert_redacted_exception(captured.value, caplog, sentinel)
    connect.assert_not_called()


def test_mysql_reader_abort_closes_connection_without_draining_cursor(monkeypatch) -> None:
    events = []
    cursor = FakeCursor(events)
    connection = FakeConnection(cursor, events)
    result = cursor._result
    monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("amount", pa.decimal128(10, 2)),
            pa.field("created_at", pa.timestamp("us")),
        ]
    )
    reader = _readers.read_mysql(make_config(), DorisInputSplit(None), schema)
    assert next(reader).num_rows == 1
    reader.close()
    reader.close()

    assert events == ["connection-close"]
    assert connection.close_calls == 1
    assert cursor.close_calls == 0
    assert cursor.connection is None
    assert cursor._result is None
    assert connection._result is None
    assert not result.unbuffered_active
    assert result.connection is None


def test_mysql_reader_detects_schema_drift(monkeypatch) -> None:
    cursor = FakeCursor()
    cursor.description = (("wrong",), ("amount",), ("created_at",))
    connection = FakeConnection(cursor)
    monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))
    schema = pa.schema([("id", pa.int64()), ("amount", pa.string()), ("created_at", pa.string())])
    with pytest.raises(DorisReadError, match=r"db\.table tablets=all.*do not match"):
        list(_readers.read_mysql(make_config(), DorisInputSplit(None), schema))
    assert not cursor.closed and connection.closed
    assert cursor.connection is None


def test_mysql_reader_rejects_non_nullable_null_and_aborts_without_drain(monkeypatch) -> None:
    cursor = FakeCursor()
    cursor.description = (("id",),)
    cursor._batches = [[(None,)], []]
    connection = FakeConnection(cursor)
    monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))

    with pytest.raises(DorisReadError, match="non-nullable"):
        list(
            _readers.read_mysql(
                make_config(),
                DorisInputSplit(None),
                pa.schema([pa.field("id", pa.int64(), nullable=False)]),
            )
        )

    assert connection.close_calls == 1
    assert cursor.close_calls == 0
    assert cursor.connection is None


def test_mysql_empty_result_closes_resources(monkeypatch) -> None:
    cursor = FakeCursor()
    cursor._batches = [[]]
    connection = FakeConnection(cursor)
    monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))
    schema = pa.schema(
        [("id", pa.int64()), ("amount", pa.decimal128(10, 2)), ("created_at", pa.timestamp("us"))]
    )
    assert list(_readers.read_mysql(make_config(), DorisInputSplit(None), schema)) == []
    assert cursor.closed and connection.closed


@pytest.mark.parametrize(
    ("code", "error_type"),
    [(1045, DorisAuthenticationError), (1142, DorisPermissionError)],
)
def test_mysql_reader_preserves_access_error(monkeypatch, code, error_type) -> None:
    monkeypatch.setattr(
        _readers.pymysql,
        "connect",
        Mock(side_effect=_readers.pymysql.err.OperationalError(code, "denied")),
    )
    with pytest.raises(error_type, match="split read") as captured:
        list(
            _readers.read_mysql(
                make_config(),
                DorisInputSplit(None),
                pa.schema([("id", pa.int64())]),
            )
        )
    assert captured.value.__cause__ is None


def test_mysql_reader_classifies_tls_error_without_exposing_driver_text(
    monkeypatch, caplog
) -> None:
    sentinel = "worker-tls-secret-sentinel"
    monkeypatch.setattr(
        _readers.pymysql,
        "connect",
        Mock(
            side_effect=_readers.pymysql.err.OperationalError(
                2003,
                f"[SSL: CERTIFICATE_VERIFY_FAILED] {sentinel}",
            )
        ),
    )
    with pytest.raises(DorisReadError, match="MySQL TLS validation failed") as captured:
        list(
            _readers.read_mysql(
                make_config(),
                DorisInputSplit(None),
                pa.schema([("id", pa.int64())]),
            )
        )
    _assert_redacted_exception(captured.value, caplog, sentinel)


def test_mysql_reader_redacts_ca_setup_error(monkeypatch, caplog) -> None:
    sentinel = "worker-ca-path-secret-sentinel"
    monkeypatch.setattr(
        _readers.pymysql,
        "connect",
        Mock(side_effect=FileNotFoundError(sentinel)),
    )
    with pytest.raises(DorisReadError, match="TLS configuration is invalid") as captured:
        list(
            _readers.read_mysql(
                make_config(client_options={"ssl": {"ca": "private-ca.pem"}}),
                DorisInputSplit(None),
                pa.schema([("id", pa.int64())]),
            )
        )
    _assert_redacted_exception(captured.value, caplog, sentinel)


def test_boolean_normalization_accepts_only_doris_zero_and_one() -> None:
    assert _readers._normalize_values([0, 1, None, True], pa.bool_()) == [
        False,
        True,
        None,
        True,
    ]
    with pytest.raises(DorisReadError, match="BOOLEAN") as captured:
        _readers._normalize_values([2], pa.bool_())
    assert "2" not in str(captured.value)


def test_mysql_cleanup_error_does_not_mask_conversion_error(monkeypatch, caplog) -> None:
    class FailingCloseConnection(FakeConnection):
        def close(self) -> None:
            super().close()
            raise RuntimeError("cleanup-secret-sentinel")

    cursor = FakeCursor()
    cursor.description = (("active",),)
    cursor._batches = [[(2,)], []]
    connection = FailingCloseConnection(cursor)
    monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))

    with pytest.raises(DorisReadError, match="BOOLEAN") as captured:
        list(
            _readers.read_mysql(
                make_config(),
                DorisInputSplit(None),
                pa.schema([pa.field("active", pa.bool_(), nullable=False)]),
            )
        )

    assert connection.close_calls == 1
    assert cursor.close_calls == 0
    assert "cleanup-secret-sentinel" not in str(captured.value)
    assert "cleanup-secret-sentinel" not in caplog.text


def test_explicit_flight_missing_extra_fails_with_install_hint(monkeypatch) -> None:
    monkeypatch.setattr(_readers, "_flight_is_installed", Mock(return_value=False))
    expected = "Python 3.10" if sys.version_info < (3, 10) else r"ray-doris\[flight\]"
    with pytest.raises(ImportError, match=expected):
        _readers.read_split(make_config(transport="flight"), DorisInputSplit(None), pa.schema([]))


def test_auto_without_flight_selects_mysql_without_attempt(monkeypatch) -> None:
    mysql_reader = Mock(return_value=iter([pa.table({"id": [1]})]))
    monkeypatch.setattr(_readers, "_flight_is_installed", Mock(return_value=False))
    monkeypatch.setattr(_readers, "read_mysql", mysql_reader)
    result = list(
        _readers.read_split(make_config(transport="auto"), DorisInputSplit(None), pa.schema([]))
    )
    assert len(result) == 1
    mysql_reader.assert_called_once()


def test_auto_never_falls_back_after_first_flight_batch(monkeypatch) -> None:
    first = pa.table({"id": [1]})

    def failing_flight(*args):
        yield first
        raise OSError("lost")

    mysql_reader = Mock(return_value=iter(()))
    monkeypatch.setattr(_readers, "_flight_is_installed", Mock(return_value=True))
    monkeypatch.setattr(_readers, "read_flight", failing_flight)
    monkeypatch.setattr(_readers, "read_mysql", mysql_reader)
    reader = iter(
        _readers.read_split(make_config(transport="auto"), DorisInputSplit(None), pa.schema([]))
    )
    assert next(reader).equals(first)
    with pytest.raises(OSError, match="lost"):
        next(reader)
    mysql_reader.assert_not_called()


def test_flight_import_and_network_errors_are_fallback_eligible_during_setup() -> None:
    assert _readers._is_flight_setup_error(OSError("connection refused"))
    assert _readers._is_flight_setup_error(ImportError("lazy setup dependency failed"))
    wrapped = DorisReadError("context")
    wrapped.__cause__ = OSError("connection refused")
    assert _readers._is_flight_setup_error(wrapped)
    assert not _readers._is_flight_setup_error(TimeoutError("query deadline"), allow_timeout=False)


class FakeBatchReader:
    def __init__(self, batches) -> None:
        self._batches = batches
        self.closed = False

    def __iter__(self):
        return iter(self._batches)

    def close(self) -> None:
        self.closed = True


class FakeFlightCursor:
    def __init__(self, reader: FakeBatchReader) -> None:
        self.reader = reader
        self.query = None
        self.closed = False

    def execute(self, query: str) -> None:
        self.query = query

    def fetch_record_batch(self) -> FakeBatchReader:
        return self.reader

    def close(self) -> None:
        self.closed = True


class FakeFlightConnection:
    def __init__(self, cursor: FakeFlightCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> FakeFlightCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


class QueryTimeoutFlightCursor(FakeFlightCursor):
    def execute(self, query: str) -> None:
        raise TimeoutError("query deadline")


def test_flight_reader_streams_record_batches_and_closes_resources(monkeypatch) -> None:
    schema = pa.schema([("id", pa.int64())])
    reader = FakeBatchReader(
        [
            pa.record_batch([pa.array([], type=pa.int64())], schema=schema),
            pa.record_batch([[1, 2, 3, 4, 5]], schema=schema),
        ]
    )
    cursor = FakeFlightCursor(reader)
    connection = FakeFlightConnection(cursor)
    monkeypatch.setattr(_readers, "_flight_connection", Mock(return_value=connection))
    tables = list(_readers.read_flight(make_config(), DorisInputSplit((5,)), schema))
    assert [table.num_rows for table in tables] == [2, 2, 1]
    assert [row for table in tables for row in table.to_pylist()] == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
        {"id": 4},
        {"id": 5},
    ]
    assert "TABLET(5)" in cursor.query
    assert reader.closed and cursor.closed and connection.closed


def test_flight_reader_rejects_null_in_non_nullable_field_and_closes_resources(
    monkeypatch,
) -> None:
    planned_schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    batch_schema = pa.schema([pa.field("id", pa.int64(), nullable=True)])
    reader = FakeBatchReader(
        [
            pa.record_batch(
                [pa.array([None], type=pa.int64())],
                schema=batch_schema,
            )
        ]
    )
    cursor = FakeFlightCursor(reader)
    connection = FakeFlightConnection(cursor)
    monkeypatch.setattr(_readers, "_flight_connection", Mock(return_value=connection))

    with pytest.raises(DorisReadError, match="non-nullable"):
        list(
            _readers.read_flight(
                make_config(),
                DorisInputSplit(None),
                planned_schema,
            )
        )

    assert reader.closed and cursor.closed and connection.closed


def test_flight_cast_error_identifies_column_without_exposing_value() -> None:
    batch = pa.record_batch(
        [pa.array(["0000-00-00 00:00:00"])],
        names=["created_at"],
    )
    with pytest.raises(
        DorisReadError,
        match=r"column 'created_at'.*string.*timestamp",
    ) as captured:
        _readers._cast_flight_batch(
            batch,
            pa.schema([("created_at", pa.timestamp("us"))]),
        )
    assert "0000-00-00" not in str(captured.value)


def test_auto_falls_back_for_eligible_setup_error(monkeypatch) -> None:
    def failing_flight(*args):
        if False:
            yield pa.table({})
        raise _readers._FlightUnavailableError("connection refused")

    mysql_table = pa.table({"id": [1]})
    monkeypatch.setattr(_readers, "_flight_is_installed", Mock(return_value=True))
    monkeypatch.setattr(_readers, "read_flight", failing_flight)
    monkeypatch.setattr(_readers, "read_mysql", Mock(return_value=iter([mysql_table])))
    result = list(
        _readers.read_split(make_config(transport="auto"), DorisInputSplit(None), pa.schema([]))
    )
    assert result == [mysql_table]


def test_auto_tls_setup_failure_refuses_mysql_fallback(monkeypatch) -> None:
    def failing_flight(*args):
        if False:
            yield pa.table({})
        raise _readers._FlightUnavailableError("certificate failed")

    mysql_reader = Mock(return_value=iter(()))
    monkeypatch.setattr(_readers, "_flight_is_installed", Mock(return_value=True))
    monkeypatch.setattr(_readers, "read_flight", failing_flight)
    monkeypatch.setattr(_readers, "read_mysql", mysql_reader)
    with pytest.raises(DorisReadError, match="refusing automatic MySQL fallback"):
        list(
            _readers.read_split(
                make_config(transport="auto", flight_scheme="grpc+tls"),
                DorisInputSplit(None),
                pa.schema([]),
            )
        )
    mysql_reader.assert_not_called()


def test_auto_query_timeout_does_not_fallback(monkeypatch) -> None:
    reader = FakeBatchReader([])
    cursor = QueryTimeoutFlightCursor(reader)
    connection = FakeFlightConnection(cursor)
    mysql_reader = Mock(return_value=iter(()))
    monkeypatch.setattr(_readers, "_flight_is_installed", Mock(return_value=True))
    monkeypatch.setattr(_readers, "_flight_connection", Mock(return_value=connection))
    monkeypatch.setattr(_readers, "read_mysql", mysql_reader)

    with pytest.raises(DorisReadError, match="through Flight SQL"):
        list(
            _readers.read_split(
                make_config(transport="auto"),
                DorisInputSplit(None),
                pa.schema([]),
            )
        )

    mysql_reader.assert_not_called()
    assert cursor.closed and connection.closed


def test_auto_does_not_fallback_for_non_setup_error_before_first_batch(monkeypatch) -> None:
    def failing_flight(*args):
        if False:
            yield pa.table({})
        raise DorisReadError("invalid SQL or schema")

    mysql_reader = Mock(return_value=iter(()))
    monkeypatch.setattr(_readers, "_flight_is_installed", Mock(return_value=True))
    monkeypatch.setattr(_readers, "read_flight", failing_flight)
    monkeypatch.setattr(_readers, "read_mysql", mysql_reader)
    with pytest.raises(DorisReadError, match="invalid SQL or schema"):
        list(
            _readers.read_split(make_config(transport="auto"), DorisInputSplit(None), pa.schema([]))
        )
    mysql_reader.assert_not_called()


class FailingBatchReader(FakeBatchReader):
    def __iter__(self):
        raise OSError("stream failed")


def test_flight_stream_failure_closes_resources_and_is_not_setup_fallback(monkeypatch) -> None:
    schema = pa.schema([("id", pa.int64())])
    reader = FailingBatchReader([])
    cursor = FakeFlightCursor(reader)
    connection = FakeFlightConnection(cursor)
    monkeypatch.setattr(_readers, "_flight_connection", Mock(return_value=connection))
    with pytest.raises(DorisReadError, match="through Flight SQL") as captured:
        list(_readers.read_flight(make_config(), DorisInputSplit(None), schema))
    assert "stream failed" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert reader.closed and cursor.closed and connection.closed


class ImportFailingBatchReader(FakeBatchReader):
    def __iter__(self):
        raise ImportError("lazy stream dependency failed")


def test_auto_does_not_fallback_for_import_error_after_reader_setup(monkeypatch) -> None:
    schema = pa.schema([("id", pa.int64())])
    reader = ImportFailingBatchReader([])
    cursor = FakeFlightCursor(reader)
    connection = FakeFlightConnection(cursor)
    mysql_reader = Mock(return_value=iter(()))
    monkeypatch.setattr(_readers, "_flight_is_installed", Mock(return_value=True))
    monkeypatch.setattr(_readers, "_flight_connection", Mock(return_value=connection))
    monkeypatch.setattr(_readers, "read_mysql", mysql_reader)

    with pytest.raises(DorisReadError, match="through Flight SQL"):
        list(_readers.read_split(make_config(transport="auto"), DorisInputSplit(None), schema))

    mysql_reader.assert_not_called()
    assert reader.closed and cursor.closed and connection.closed


def test_flight_connection_uses_configured_scheme_and_options(monkeypatch) -> None:
    driver_package = ModuleType("adbc_driver_flightsql")
    driver_dbapi = ModuleType("adbc_driver_flightsql.dbapi")
    connect = Mock(return_value=object())
    driver_dbapi.connect = connect
    driver_package.dbapi = driver_dbapi

    class Option:
        def __init__(self, value: str) -> None:
            self.value = value

    class DatabaseOptions:
        USERNAME = Option("username")
        PASSWORD = Option("password")

    manager = ModuleType("adbc_driver_manager")
    manager.DatabaseOptions = DatabaseOptions
    monkeypatch.setitem(sys.modules, "adbc_driver_flightsql", driver_package)
    monkeypatch.setitem(sys.modules, "adbc_driver_flightsql.dbapi", driver_dbapi)
    monkeypatch.setitem(sys.modules, "adbc_driver_manager", manager)

    timeout_key = "adbc.flight.sql.rpc.timeout_seconds.query"
    monkeypatch.setenv("RAY_DORIS_FLIGHT_PASSWORD", "resolved-flight-password")
    _readers._flight_connection(
        make_config(
            flight_scheme="grpc+tls",
            password_env="RAY_DORIS_FLIGHT_PASSWORD",
            flight_options={timeout_key: "30"},
        )
    )
    assert connect.call_args.kwargs["uri"] == "grpc+tls://fe:8070"
    assert (
        connect.call_args.kwargs["db_kwargs"]["adbc.flight.sql.rpc.timeout_seconds.connect"]
        == "10.0"
    )
    assert connect.call_args.kwargs["db_kwargs"][timeout_key] == "30"
    assert connect.call_args.kwargs["db_kwargs"]["password"] == "resolved-flight-password"


def test_adbc_status_classification_uses_structured_status_code(monkeypatch) -> None:
    class StatusCode:
        IO = object()
        NOT_IMPLEMENTED = object()
        TIMEOUT = object()
        UNAUTHORIZED = object()

    class Error(Exception):
        def __init__(self, status_code) -> None:
            self.status_code = status_code

    manager = ModuleType("adbc_driver_manager")
    manager.AdbcStatusCode = StatusCode
    dbapi = ModuleType("adbc_driver_manager.dbapi")
    dbapi.Error = Error
    monkeypatch.setitem(sys.modules, "adbc_driver_manager", manager)
    monkeypatch.setitem(sys.modules, "adbc_driver_manager.dbapi", dbapi)

    assert _readers._is_flight_setup_error(Error(StatusCode.IO))
    assert _readers._is_flight_setup_error(Error(StatusCode.NOT_IMPLEMENTED), allow_timeout=False)
    assert not _readers._is_flight_setup_error(Error(StatusCode.TIMEOUT), allow_timeout=False)
    assert not _readers._is_flight_setup_error(Error(StatusCode.UNAUTHORIZED))
