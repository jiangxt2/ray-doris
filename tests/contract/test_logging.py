"""Actual workers must retain frozen diagnostics and use independent budgets."""

import logging
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest
import ray
from ray.exceptions import RayTaskError

from ray_doris import DorisConnection, DorisDatasink, DorisTable, DorisWriteError, _logging
from ray_doris._logging import DiagnosticSnapshot, EventContext, emit
from ray_doris._models import DorisPlanningSnapshot
from ray_doris.datasource import DorisDatasource
from tests.logging_capture import Capture, install, records
from tests.unit.test_write import _metadata

pytestmark = pytest.mark.contract


@ray.remote
def observe_read(read_fn, directory):
    os.environ["RAY_DORIS_LOG_BATCHES"] = "0"
    install(directory)
    ticks = iter(range(1000))
    table = pa.table({"id": [1]})

    def read(*args):
        yield from (table for _ in range(301))

    with (
        patch("ray_doris._readers.read_mysql", read),
        patch.object(_logging, "time", SimpleNamespace(monotonic=lambda: next(ticks))),
    ):
        return sum(block.num_rows for block in read_fn())


def test_actual_workers_preserve_ids_frozen_details_and_execution_budgets(monkeypatch, tmp_path):
    monkeypatch.setenv("RAY_DORIS_LOG_BATCHES", "1")
    schema = pa.schema([("id", pa.int64())])
    monkeypatch.setattr(
        "ray_doris.datasource.DorisPlanner.discover",
        Mock(return_value=DorisPlanningSnapshot(schema, (1,))),
    )
    source = DorisDatasource(table="db.table", host="unused", password_env="UNRESOLVED_CREDENTIAL")
    task = source.get_read_tasks(1)[0]
    owned = not ray.is_initialized()
    if owned:
        ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        assert ray.get([observe_read.remote(task.read_fn, str(tmp_path)) for _ in range(2)]) == [
            301,
            301,
        ]
    finally:
        if owned:
            ray.shutdown()
    captured = records(tmp_path)
    finished = [r for r in captured if r.get("event") == "doris.read.finished"]
    assert len(finished) == 2
    assert len({r["execution_id"] for r in finished}) == 2
    assert {r["operation_id"] for r in finished} == {source._diagnostics.operation_id}
    assert len({r["plan_id"] for r in finished}) == 1
    assert len({r["split_id"] for r in finished}) == 1
    for terminal in finished:
        assert terminal["status"] == "succeeded" and terminal["emitted_rows"] == 301
        progress = [
            r
            for r in captured
            if r.get("event") == "doris.read.progress"
            and r["execution_id"] == terminal["execution_id"]
        ]
        assert [r["batch_index"] for r in progress] == [1, 100, 200, 300]
    assert "UNRESOLVED_CREDENTIAL" not in str(captured)


class InvalidSummarySink(DorisDatasink):
    def on_write_start(self, schema=None):
        pass

    def write(self, blocks, ctx):
        list(blocks)
        return {}


def _fail_upstream(row):
    raise RuntimeError("upstream-sensitive-message")


@pytest.mark.parametrize("boundary", ["upstream", "metadata", "aggregation"])
def test_ray_notifies_driver_failure_without_fabricated_totals(monkeypatch, caplog, boundary):
    caplog.set_level("DEBUG", logger="ray_doris")
    if boundary == "aggregation":
        sink = InvalidSummarySink(DorisConnection(host="unused"), DorisTable("db", "table"))
    else:
        sink = DorisDatasink(DorisConnection(host="unused"), DorisTable("db", "table"))
    discover = Mock(side_effect=RuntimeError("metadata-sensitive-message"))
    monkeypatch.setattr("ray_doris.write.datasink.discover_table_metadata", discover)
    owned = not ray.is_initialized()
    if owned:
        ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        dataset = ray.data.from_arrow(pa.table({"id": [1]}))
        if boundary == "upstream":
            dataset = dataset.map(_fail_upstream)
        with pytest.raises((RuntimeError, DorisWriteError, RayTaskError)):
            dataset.write_datasink(sink, ray_remote_args={"max_retries": 0})
    finally:
        if owned:
            ray.shutdown()
    driver = [
        r
        for r in caplog.records
        if getattr(r, "event", None) == "doris.write.finished"
        and getattr(r, "role", None) == "driver"
    ]
    assert len(driver) == 1 and driver[0].status == "failed"
    assert not any(hasattr(driver[0], key) for key in ("loaded_rows", "uploaded_bytes", "batches"))
    assert "sensitive-message" not in str(driver[0].__dict__)
    if boundary == "metadata":
        discover.assert_called_once()
        prepares = [
            r for r in caplog.records if getattr(r, "event", None) == "doris.write.prepare.finished"
        ]
        assert len(prepares) == 1 and prepares[0].status == "failed"
    else:
        discover.assert_not_called()


class SuccessfulSummarySink(DorisDatasink):
    def write(self, blocks, ctx):
        rows = sum(block.num_rows for block in blocks)
        return {
            "status": "success",
            "request_statuses": ("Success",),
            "batches": 1,
            "attempted_rows": rows,
            "loaded_rows": rows,
            "filtered_rows": 0,
            "uploaded_bytes": rows,
            "diagnostic_operation_id": self._diagnostics.operation_id,
        }

    def on_write_complete(self, write_result):
        assert all(
            summary["diagnostic_operation_id"] == self._diagnostics.operation_id
            for summary in write_result.write_returns
        )
        super().on_write_complete(write_result)


@pytest.mark.parametrize("empty_first", [False, True])
def test_reused_sink_reports_upstream_failures_before_start(monkeypatch, caplog, empty_first):
    caplog.set_level("DEBUG", logger="ray_doris")
    sink = SuccessfulSummarySink(DorisConnection(host="unused"), DorisTable("db", "table"))
    discover = Mock(return_value=_metadata())
    monkeypatch.setattr("ray_doris.write.datasink.discover_table_metadata", discover)
    owned = not ray.is_initialized()
    if owned:
        ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        dataset = ray.data.from_arrow(pa.table({"id": [1], "value": ["a"]}))
        first = ray.data.range(0) if empty_first else dataset
        first.write_datasink(sink, ray_remote_args={"max_retries": 0})
        starts = discover.call_count
        for _ in range(2):
            with pytest.raises(RayTaskError):
                dataset.map(_fail_upstream).write_datasink(sink, ray_remote_args={"max_retries": 0})
            assert discover.call_count == starts
        dataset.write_datasink(sink, ray_remote_args={"max_retries": 0})
        assert discover.call_count == starts + 1
    finally:
        if owned:
            ray.shutdown()
    terminal = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "doris.write.finished"
        and getattr(record, "role", None) == "driver"
    ]
    assert [record.status for record in terminal] == ["success", "failed", "failed", "success"]
    assert len({record.operation_id for record in terminal}) == 4
    for record in terminal[1:3]:
        assert not any(hasattr(record, key) for key in ("loaded_rows", "uploaded_bytes", "batches"))
    assert "upstream-sensitive-message" not in str([record.__dict__ for record in terminal])


@ray.remote(max_retries=0)
def hook_event():
    logger = logging.getLogger("ray_doris.hook_test")
    assert any(isinstance(h, Capture) for h in logging.getLogger("ray_doris").handlers)
    snapshot = DiagnosticSnapshot()
    emit(
        logger,
        logging.DEBUG,
        "doris.test.hook",
        context=EventContext(snapshot, role="worker"),
        phase="test",
        status="succeeded",
    )
    return snapshot.operation_id


def test_fully_qualified_worker_hook_runs_before_task(tmp_path):
    assert not ray.is_initialized()
    ray.init(
        address="local",
        include_dashboard=False,
        num_cpus=1,
        runtime_env={
            "env_vars": {
                "RAY_DORIS_TEST_LOG_DIR": str(tmp_path),
                "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
            },
            "worker_process_setup_hook": "tests.logging_capture.setup",
        },
    )
    try:
        operation = ray.get(hook_event.remote(), timeout=30)
    finally:
        ray.shutdown()
    matched = [r for r in records(tmp_path) if r.get("event") == "doris.test.hook"]
    assert len(matched) == 1 and matched[0]["operation_id"] == operation
