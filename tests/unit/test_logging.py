"""Diagnostics must describe existing effects without adding new ones."""

import logging
import weakref
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pytest
from ray import cloudpickle
from ray.data.datasource import WriteResult
from tests.unit.test_readers import (
    FakeBatchReader,
    FakeConnection,
    FakeCursor,
    FakeFlightConnection,
    FakeFlightCursor,
    make_config,
)
from tests.unit.test_write import _connection, _metadata

from ray_doris import _logging, _readers, read_doris
from ray_doris._errors import (
    DorisAmbiguousWriteError,
    DorisConfigurationError,
    DorisLabelExistsError,
    DorisReadError,
)
from ray_doris._logging import DiagnosticSnapshot, EventContext, ProgressBudget, bind, emit
from ray_doris._models import DorisInputSplit, DorisPlanningSnapshot
from ray_doris._planner import DorisPlanner
from ray_doris.datasource import DorisDatasource
from ray_doris.write.datasink import DorisDatasink
from ray_doris.write.options import DorisWriteOptions
from ray_doris.write.serialization import SerializedBatch
from ray_doris.write.stream_load import DorisLoadResult, StreamLoadClient


def events(caplog, name):
    return [record for record in caplog.records if getattr(record, "event", None) == name]


@pytest.mark.parametrize("value", ["", "true", "2", "secret-environment-value"])
def test_invalid_detail_option_fails_before_any_network(monkeypatch, value):
    monkeypatch.setenv("RAY_DORIS_LOG_BATCHES", value)
    network = Mock(side_effect=AssertionError("network must not run"))
    monkeypatch.setattr("ray_doris._planner.pymysql.connect", network)
    for construct in (
        lambda: DorisDatasource(table="db.table", host="fe"),
        lambda: DorisDatasink(_connection(), SimpleNamespace()),
    ):
        with pytest.raises(DorisConfigurationError) as failure:
            construct()
        assert value not in str(failure.value) or value in ("", "2")
    network.assert_not_called()


def test_snapshot_freezes_option_and_preserves_branch_identity(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    monkeypatch.setenv("RAY_DORIS_LOG_BATCHES", "1")
    schema = pa.schema([("id", pa.int64())])
    discover = Mock(return_value=DorisPlanningSnapshot(schema, (1, 2)))
    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.discover", discover)
    source = DorisDatasource(table="db.table", host="fe", password_env="CREDENTIAL_NAME")
    monkeypatch.setenv("RAY_DORIS_LOG_BATCHES", "0")
    first = source.get_read_tasks(2)
    second = source.get_read_tasks(1)
    snapshot = cloudpickle.loads(cloudpickle.dumps(first[0].read_fn)).args[3]
    assert snapshot.log_batches
    assert snapshot.operation_id == source._diagnostics.operation_id
    assert snapshot.plan_id != second[0].read_fn.args[3].plan_id
    assert first[0].read_fn.args[3].split_id != first[1].read_fn.args[3].split_id
    assert source.apply_projection({})._diagnostics.operation_id != snapshot.operation_id
    assert source.apply_projection(None)._diagnostics.operation_id == snapshot.operation_id
    assert [r.cache_hit for r in events(caplog, "doris.plan.finished")] == [False, True]
    discover.assert_called_once()


def test_progress_has_fixed_cap_and_does_not_read_clock_when_disabled(monkeypatch, caplog):
    logger = logging.getLogger("ray_doris.test")
    clock = Mock(side_effect=AssertionError("disabled clock"))
    monkeypatch.setattr(_logging, "time", SimpleNamespace(monotonic=clock))
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    disabled = ProgressBudget(False)
    assert not any(disabled.allow(logger) for _ in range(500))
    clock.assert_not_called()
    clock = Mock(side_effect=iter(range(1000)))
    monkeypatch.setattr(_logging, "time", SimpleNamespace(monotonic=clock))
    budget = ProgressBudget(True)
    selected = [i for i in range(1, 50_001) if budget.allow(logger)]
    assert selected[:3] == [1, 100, 200]
    assert len(selected) == 100
    assert budget.seen == 50_000
    assert clock.call_count == 100


def test_progress_requires_both_debug_and_one_second(monkeypatch, caplog):
    logger = logging.getLogger("ray_doris.test")
    caplog.set_level(logging.INFO, logger="ray_doris")
    clock = Mock(side_effect=[10.0, 10.5, 11.0])
    monkeypatch.setattr(_logging, "time", SimpleNamespace(monotonic=clock))
    budget = ProgressBudget(True)
    assert not budget.allow(logger)
    clock.assert_not_called()
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    assert [i for i in range(1, 201) if budget.allow(logger)] == [1, 200]


def test_field_allowlist_and_handler_failure_do_not_change_business_result(caplog):
    logger = logging.getLogger("ray_doris.test")
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    snapshot = DiagnosticSnapshot()
    with bind(EventContext(snapshot)):
        emit(
            logger,
            logging.WARNING,
            "doris.test",
            phase="test",
            status="failed",
            password="secret",
            filter="secret",
            client_options="secret",
            label="x" * 129,
            duration_ms=float("inf"),
        )
    record = events(caplog, "doris.test")[0]
    assert record.operation_id == snapshot.operation_id
    assert record.fields_truncated
    assert not any(
        hasattr(record, name)
        for name in ("password", "filter", "client_options", "label", "duration_ms")
    )
    assert record.exc_info is None

    class BrokenHandler(logging.Handler):
        def emit(self, record):
            raise RuntimeError("handler-secret")

    handler = BrokenHandler()
    logger.addHandler(handler)
    try:
        emit(logger, logging.ERROR, "doris.test", phase="test", status="failed")
    finally:
        logger.removeHandler(handler)


@pytest.mark.parametrize("close_early", [False, True])
def test_reader_lifecycle_closes_once_and_does_not_leak_context(monkeypatch, caplog, close_early):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    closed = []

    def underlying(*args):
        try:
            yield pa.table({"id": [1, 2]})
            yield pa.table({"id": [3]})
        finally:
            closed.append(True)

    monkeypatch.setattr(_readers, "read_mysql", underlying)
    snapshot = DiagnosticSnapshot(log_batches=True)
    reader = _readers.read_split(make_config(), DorisInputSplit(None), pa.schema([]), snapshot)
    assert not events(caplog, "doris.read.started")
    next(reader)
    assert _logging.current_context() is None
    if close_early:
        reader.close()
    else:
        assert sum(table.num_rows for table in reader) == 1
    finished = events(caplog, "doris.read.finished")
    assert len(finished) == 1 and closed == [True]
    assert finished[0].status == ("consumer_closed" if close_early else "succeeded")
    assert finished[0].emitted_rows == (2 if close_early else 3)
    assert finished[0].operation_id == snapshot.operation_id
    assert len(events(caplog, "doris.read.first_block")) == 1


def test_empty_and_failing_streams_have_no_false_first_block(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")

    def empty(*args):
        yield pa.table({"id": pa.array([], type=pa.int64())})

    monkeypatch.setattr(_readers, "read_mysql", empty)
    list(_readers.read_split(make_config(), DorisInputSplit(None), pa.schema([])))
    assert not events(caplog, "doris.read.first_block")
    assert events(caplog, "doris.read.finished")[-1].emitted_rows == 0
    error = RuntimeError("row-secret")

    def failing(*args):
        yield pa.table({"id": [1]})
        raise error

    monkeypatch.setattr(_readers, "read_mysql", failing)
    with pytest.raises(RuntimeError) as captured:
        list(_readers.read_split(make_config(), DorisInputSplit(None), pa.schema([])))
    assert captured.value is error
    assert events(caplog, "doris.read.finished")[-1].status == "failed"
    assert "row-secret" not in caplog.text


@pytest.mark.parametrize("status", ["Success", "Publish Timeout", "label", "ambiguous"])
def test_load_events_preserve_labels_outcome_and_first_request(monkeypatch, caplog, status):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    labels = []

    def load(self, payload, *, rows, columns, label):
        labels.append(label)
        self._active_transmission.body_started = True
        if status == "label":
            raise DorisLabelExistsError("label-secret")
        if status == "ambiguous":
            raise DorisAmbiguousWriteError("payload-secret")
        return DorisLoadResult(status, 1, 0, 1)

    monkeypatch.setattr(StreamLoadClient, "_load", load)
    context = EventContext(DiagnosticSnapshot(), role="worker", execution_id="execution")
    with bind(context):
        for _ in range(2):
            client = StreamLoadClient(_connection(), SimpleNamespace(), DorisWriteOptions())
            if status in ("label", "ambiguous"):
                with pytest.raises((DorisLabelExistsError, DorisAmbiguousWriteError)):
                    client.load(b"secret", rows=1, columns=("id",))
            else:
                client.load(b"secret", rows=1, columns=("id",))
    terminal = events(caplog, "doris.load.finished")
    assert len(events(caplog, "doris.load.started")) == 1
    assert len(terminal) == (1 if status == "Success" else 2)
    assert [r.label for r in terminal] == labels[: len(terminal)]
    assert (
        terminal[0].status
        == {
            "Success": "success",
            "Publish Timeout": "publish_timeout",
            "label": "label_exists",
            "ambiguous": "ambiguous",
        }[status]
    )
    assert not events(caplog, "doris.write.progress")
    assert "secret" not in caplog.text


def test_write_partial_success_and_driver_failure_keep_known_counts(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    sink = DorisDatasink(_connection(), SimpleNamespace(), DorisWriteOptions(batch_rows=1))
    sink._metadata = _metadata()
    error = DorisAmbiguousWriteError("secret-error")
    load = Mock(side_effect=[DorisLoadResult("Success", 1, 0, 1), error])
    monkeypatch.setattr(StreamLoadClient, "load", load)
    with pytest.raises(DorisAmbiguousWriteError) as captured:
        sink.write([pa.table({"id": [1, 2], "value": ["a", "b"]})], None)
    assert captured.value is error
    sink.on_write_failed(error)
    terminal = events(caplog, "doris.write.finished")
    assert len(terminal) == 2
    worker, driver = terminal
    assert worker.role == "worker" and worker.loaded_rows == 1 and worker.batches == 1
    assert worker.counts_scope == "confirmed_batches"
    assert driver.role == "driver" and driver.status == "failed" and driver.outcome == "ambiguous"
    assert not any(hasattr(driver, name) for name in ("loaded_rows", "batches", "uploaded_bytes"))
    assert "secret-error" not in caplog.text


@pytest.mark.parametrize("detail", ["0", "1"])
def test_write_releases_sent_payloads_while_serializing_later_batches(monkeypatch, caplog, detail):
    monkeypatch.setenv("RAY_DORIS_LOG_BATCHES", detail)
    caplog.set_level(logging.DEBUG if detail == "1" else logging.INFO, logger="ray_doris")
    references = []
    sent = []

    class Payload(bytearray):
        def __init__(self, value):
            super().__init__(value)
            references.append(weakref.ref(self))

    def serialize(*args, **kwargs):
        for index in range(5):
            yield SerializedBatch(Payload(str(index).encode()), 1, ("id", "value"))

    def load(self, payload, *, rows, columns):
        sent.append(bytes(payload))
        if len(sent) >= 3:
            assert references[0]() is None, "the first uploaded payload is still retained"
        return DorisLoadResult("Success", rows, 0, rows)

    monkeypatch.setattr("ray_doris.write.datasink.iter_serialized_batches", serialize)
    monkeypatch.setattr(StreamLoadClient, "load", load)
    sink = DorisDatasink(_connection(), SimpleNamespace())
    sink._metadata = _metadata()
    result = sink.write([pa.table({"id": list(range(5)), "value": ["a"] * 5})], None)
    assert sent == [str(index).encode() for index in range(5)]
    assert result["batches"] == result["loaded_rows"] == 5
    assert all(reference() is None for reference in references)


def test_reused_sink_reports_each_pre_start_failure_with_a_frozen_option(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    monkeypatch.setenv("RAY_DORIS_LOG_BATCHES", "1")
    sink = DorisDatasink(_connection(), SimpleNamespace())
    operation = sink._diagnostics.operation_id
    sink.on_write_complete(WriteResult(0, 0, []))
    monkeypatch.setenv("RAY_DORIS_LOG_BATCHES", "invalid-after-construction")
    for _ in range(2):
        sink.on_write_failed(RuntimeError("upstream-secret"))
    terminal = events(caplog, "doris.write.finished")
    assert [record.status for record in terminal] == ["success", "failed", "failed"]
    assert terminal[0].operation_id == operation
    assert len({record.operation_id for record in terminal}) == 3
    assert sink._diagnostics.log_batches
    for record in terminal[1:]:
        assert not any(hasattr(record, key) for key in ("loaded_rows", "uploaded_bytes", "batches"))
    assert "upstream-secret" not in str([record.__dict__ for record in terminal])


def test_first_serialization_failure_keeps_original_error_and_never_sends(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    error = ValueError("serialization-secret")

    def serialize(*args, **kwargs):
        assert events(caplog, "doris.write.prepare.started")[-1].phase == "serialization"
        raise error
        yield

    load = Mock()
    monkeypatch.setattr("ray_doris.write.datasink.iter_serialized_batches", serialize)
    monkeypatch.setattr(StreamLoadClient, "load", load)
    sink = DorisDatasink(_connection(), SimpleNamespace())
    sink._metadata = _metadata()
    with pytest.raises(ValueError) as failure:
        sink.write([pa.table({"id": [1], "value": ["a"]})], None)
    assert failure.value is error
    load.assert_not_called()
    prepare = events(caplog, "doris.write.prepare.finished")[-1]
    assert prepare.phase == "serialization" and prepare.status == "failed"
    assert events(caplog, "doris.write.finished")[-1].status == "failed"
    assert "serialization-secret" not in str([record.__dict__ for record in caplog.records])


def test_first_ambiguous_write_omits_unknown_worker_counters(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    sink = DorisDatasink(_connection(), SimpleNamespace())
    sink._metadata = _metadata()
    monkeypatch.setattr(StreamLoadClient, "load", Mock(side_effect=DorisAmbiguousWriteError()))
    with pytest.raises(DorisAmbiguousWriteError):
        sink.write([pa.table({"id": [1], "value": ["a"]})], None)
    terminal = events(caplog, "doris.write.finished")[0]
    assert terminal.outcome == "ambiguous"
    assert not any(hasattr(terminal, key) for key in ("loaded_rows", "uploaded_bytes", "batches"))


def test_invalid_aggregation_never_emits_success(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    sink = DorisDatasink(_connection(), SimpleNamespace())
    try:
        sink.on_write_complete(WriteResult(1, 1, [{}]))
    except Exception as error:
        sink.on_write_failed(error)
    terminal = events(caplog, "doris.write.finished")
    assert len(terminal) == 1 and terminal[0].status == "failed"


def test_api_records_independent_parallelism_controls(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="ray_doris")
    read = Mock(return_value="dataset")
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", read)
    assert (
        read_doris(
            table="db.table", host="unused", concurrency=2, override_num_blocks=7, tablet_size=3
        )
        == "dataset"
    )
    selected = events(caplog, "doris.read.path_selected")[0]
    assert (selected.concurrency, selected.override_num_blocks, selected.tablet_size) == (2, 7, 3)
    assert read.call_args.kwargs["concurrency"] == 2
    assert read.call_args.kwargs["override_num_blocks"] == 7


def test_discovery_start_precedes_io_and_original_failure_is_preserved(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    error = RuntimeError("schema-secret")

    def fail(self):
        assert events(caplog, "doris.discovery.started")[-1].phase == "schema"
        raise error

    monkeypatch.setattr(DorisPlanner, "_describe_schema", fail)
    source = DorisDatasource(table="db.table", host="unused")
    with pytest.raises(RuntimeError) as captured:
        source.get_read_tasks(1)
    assert captured.value is error
    assert events(caplog, "doris.discovery.finished")[-1].status == "failed"
    assert events(caplog, "doris.plan.finished")[-1].status == "failed"
    assert "schema-secret" not in str([r.__dict__ for r in caplog.records])


def test_cleanup_warning_is_attached_to_success_without_repeating_close(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    cursor = FakeCursor()
    cursor.description = (("id",),)
    cursor._batches = [[(1,)], []]
    connection = FakeConnection(cursor)
    original_close = connection.close

    def fail_close():
        original_close()
        raise RuntimeError("cleanup-secret")

    connection.close = fail_close
    monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))
    list(_readers.read_split(make_config(), DorisInputSplit(None), pa.schema([("id", pa.int64())])))
    finished = events(caplog, "doris.read.finished")
    assert len(finished) == 1 and finished[0].status == "succeeded"
    assert finished[0].cleanup_error and connection.close_calls == 1
    assert len(events(caplog, "doris.cleanup.failed")) == 1
    assert "cleanup-secret" not in str([r.__dict__ for r in caplog.records])


def test_write_progress_cap_does_not_hide_later_uncertainty(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    ticks = iter(range(100_000))
    monkeypatch.setattr(_logging, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    index = 0

    def load(self, payload, *, rows, columns, label):
        nonlocal index
        index += 1
        return DorisLoadResult("Publish Timeout" if index == 10_001 else "Success", 1, 0, 1)

    monkeypatch.setattr(StreamLoadClient, "_load", load)
    context = EventContext(
        DiagnosticSnapshot(log_batches=True), role="worker", execution_id="physical"
    )
    with bind(context):
        for _ in range(10_001):
            StreamLoadClient(_connection(), SimpleNamespace(), DorisWriteOptions()).load(
                b"row", rows=1, columns=("id",)
            )
    assert len(events(caplog, "doris.write.progress")) == 100
    terminal = events(caplog, "doris.load.finished")
    assert len(terminal) == 2 and terminal[-1].status == "publish_timeout"


def test_flight_cleanup_failure_keeps_existing_failure_semantics(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ray_doris")
    table = pa.table({"id": [1]})
    reader = FakeBatchReader(table.to_batches())
    cursor = FakeFlightCursor(reader)
    connection = FakeFlightConnection(cursor)
    reader.close = Mock(side_effect=OSError("flight-cleanup-secret"))
    monkeypatch.setattr(_readers, "_flight_connection", Mock(return_value=connection))
    monkeypatch.setattr(_readers, "_ensure_flight_available", Mock())
    with pytest.raises(DorisReadError):
        list(
            _readers.read_split(
                make_config(transport="flight"), DorisInputSplit(None), table.schema
            )
        )
    finished = events(caplog, "doris.read.finished")
    assert len(finished) == 1 and finished[0].status == "failed" and finished[0].cleanup_error
    assert events(caplog, "doris.cleanup.failed")[0].resource_kind == "flight_reader"
    assert cursor.closed and connection.closed and reader.close.call_count == 1
    assert "flight-cleanup-secret" not in str([r.__dict__ for r in caplog.records])
