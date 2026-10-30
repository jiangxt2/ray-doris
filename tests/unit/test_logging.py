"""Standard logs describe existing operations without changing their effects."""

import json
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pytest
from ray.data.datasource import WriteResult
from tests.unit.test_readers import (
    FakeBatchReader,
    FakeConnection,
    FakeCursor,
    FakeFlightConnection,
    FakeFlightCursor,
    make_config,
)
from tests.unit.test_write import _connection

from ray_doris import DorisDatasink, DorisTable, _readers
from ray_doris._errors import DorisAmbiguousWriteError, DorisReadError, DorisWriteError
from ray_doris._models import DorisInputSplit, DorisPlanningSnapshot
from ray_doris.datasource import DorisDatasource
from ray_doris.write.options import DorisWriteOptions
from ray_doris.write.stream_load import StreamLoadClient


@pytest.fixture(autouse=True)
def debug_logs(caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")


def test_planning_logs_cache_and_actual_tasks_without_discovering_twice(monkeypatch, caplog):
    discover = Mock(return_value=DorisPlanningSnapshot(pa.schema([("id", pa.int64())]), (1, 2, 3)))
    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.discover", discover)
    source = DorisDatasource(
        table="db.table",
        host="fe",
        password_env="CREDENTIAL_SENTINEL",
        filter="id > 123456789",
        tablet_size=1,
    )
    first, second = source.get_read_tasks(2), source.get_read_tasks(1)
    discover.assert_called_once()
    assert f"cache_hit=False tablet_count=3 tasks={len(first)} parallelism=2" in caplog.text
    assert f"cache_hit=True tablet_count=3 tasks={len(second)} parallelism=1" in caplog.text
    assert "CREDENTIAL_SENTINEL" not in caplog.text and "123456789" not in caplog.text


@pytest.mark.parametrize("transport", ["mysql", "flight"])
@pytest.mark.parametrize("mode", ["eof", "close", "empty", "error"])
def test_reader_logs_observed_rows_and_closes_once(monkeypatch, caplog, transport, mode):
    schema = pa.schema([("id", pa.int64())])
    if transport == "mysql":
        cursor = FakeCursor()
        cursor.description = (("id",),)
        cursor._batches = [] if mode == "empty" else [[(1,), (2,)], [(3,)]]
        cursor._batches.append([])
        if mode == "error":
            cursor.fetchmany = Mock(side_effect=[[(1,), (2,)], OSError("backend-secret")])
        connection = FakeConnection(cursor)
        monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))
        read = _readers.read_mysql
    else:
        batches = pa.table({"id": [1, 2, 3]}).to_batches()
        if mode == "error":

            def failing():
                yield pa.table({"id": [1, 2]}).to_batches()[0]
                raise OSError("backend-secret")

            batches = failing()
        reader = FakeBatchReader([] if mode == "empty" else batches)
        cursor = FakeFlightCursor(reader)
        connection = FakeFlightConnection(cursor)
        monkeypatch.setattr(_readers, "_flight_connection", Mock(return_value=connection))
        read = _readers.read_flight
    connection.close = Mock(wraps=connection.close)
    cursor.close = Mock(wraps=cursor.close)
    stream = read(make_config(password="password-secret"), DorisInputSplit(None), schema)
    if mode == "close":
        next(stream)
        stream.close()
    elif mode == "error":
        with pytest.raises(DorisReadError):
            list(stream)
    else:
        list(stream)
    status = {"eof": "eof", "close": "consumer_closed", "empty": "eof", "error": "failed"}[mode]
    rows, blocks = (0, 0) if mode == "empty" else (3, 2) if mode == "eof" else (2, 1)
    assert f"finished status={status} rows={rows} blocks={blocks}" in caplog.text
    assert caplog.text.count("first block elapsed_ms=") == (0 if mode == "empty" else 1)
    connection.close.assert_called_once()
    assert cursor.close.call_count == (
        0 if transport == "mysql" and mode in ("close", "error") else 1
    )
    assert "password-secret" not in caplog.text and "backend-secret" not in caplog.text


@pytest.mark.parametrize("close_error", [False, True])
def test_flight_status_ignores_callers_exception_but_keeps_close_failure(
    monkeypatch, caplog, close_error
):
    table = pa.table({"id": [1, 2]})
    reader = FakeBatchReader(table.to_batches())
    reader.close = Mock(side_effect=OSError("cleanup-secret") if close_error else None)
    cursor = FakeFlightCursor(reader)
    connection = FakeFlightConnection(cursor)
    monkeypatch.setattr(_readers, "_flight_connection", Mock(return_value=connection))
    stream = _readers.read_flight(make_config(), DorisInputSplit(None), table.schema)
    try:
        raise RuntimeError("caller-secret")
    except RuntimeError:
        if close_error:
            with pytest.raises(DorisReadError):
                list(stream)
        else:
            assert sum(block.num_rows for block in stream) == 2
    assert f"finished status={'failed' if close_error else 'eof'} rows=2 blocks=1" in caplog.text
    reader.close.assert_called_once()
    assert cursor.closed and connection.closed
    assert "caller-secret" not in caplog.text and "cleanup-secret" not in caplog.text


@pytest.mark.parametrize("status", ["Success", "Publish Timeout"])
def test_stream_load_logs_existing_label_and_counts(monkeypatch, caplog, status):
    response = SimpleNamespace(
        read=lambda limit: json.dumps(
            {
                "Status": status,
                "NumberLoadedRows": 1,
                "NumberFilteredRows": 0,
                "NumberTotalRows": 1,
            }
        ).encode(),
        close=Mock(),
    )
    requests = []

    def opened(request, **kwargs):
        requests.append(request)
        return response

    monkeypatch.setattr(
        "ray_doris.write.stream_load.urllib.request.build_opener",
        lambda *handlers: SimpleNamespace(open=opened),
    )
    result = StreamLoadClient(
        _connection(password="password-secret"), DorisTable("db", "table"), DorisWriteOptions()
    ).load(b"payload-secret", rows=1, columns=("id",))
    assert result.status == status and len(requests) == 1
    label = requests[0].get_header("Label")
    assert label and f"label={label} status={status}" in caplog.text
    assert "loaded_rows=1 filtered_rows=0" in caplog.text
    assert "password-secret" not in caplog.text and "payload-secret" not in caplog.text
    response.close.assert_called_once()


@pytest.mark.parametrize("error_type", [DorisWriteError, DorisAmbiguousWriteError])
def test_failed_load_logs_payload_size_and_keeps_original_error(monkeypatch, caplog, error_type):
    error = error_type("backend-secret")
    opened = Mock(side_effect=error)
    monkeypatch.setattr(
        "ray_doris.write.stream_load.urllib.request.build_opener",
        lambda *handlers: SimpleNamespace(open=opened),
    )
    with pytest.raises(error_type) as caught:
        StreamLoadClient(_connection(), DorisTable("db", "table"), DorisWriteOptions()).load(
            b"payload-secret", rows=1, columns=("id",)
        )
    assert caught.value is error
    opened.assert_called_once()
    label = opened.call_args.args[0].get_header("Label")
    assert f"failed label={label} error_type={error_type.__name__}" in caplog.text
    assert "payload_bytes=14" in caplog.text
    assert "uploaded_bytes=" not in caplog.text
    assert "loaded_rows=" not in caplog.text
    assert "backend-secret" not in caplog.text and "payload-secret" not in caplog.text


def test_driver_logs_validated_summary_and_independent_failures(caplog):
    caplog.set_level(logging.INFO, logger="ray_doris")
    sink = DorisDatasink(_connection(), DorisTable("db", "table"))
    sink.on_write_complete(WriteResult(0, 0, []))
    sink.on_write_failed(RuntimeError("upstream-secret"))
    sink.on_write_failed(RuntimeError("upstream-secret"))
    assert "completed status=success batches=0" in caplog.text
    failures = [r.getMessage() for r in caplog.records if "write failed" in r.getMessage()]
    assert len(failures) == 2 and all("error_type=RuntimeError" in text for text in failures)
    assert all("loaded_rows=" not in text for text in failures)
    assert "upstream-secret" not in caplog.text
