"""Verify native Dataset limits at the real Doris execution boundary."""

from __future__ import annotations

import pytest
import ray
from ray.data.expressions import col
from tests.projection_helpers import RecordingDorisDatasource, SQLRecorder

pytestmark = pytest.mark.integration


@pytest.fixture
def recorder():
    actor = SQLRecorder.remote()
    try:
        yield actor
    finally:
        ray.kill(actor)


@pytest.mark.parametrize("transport", ["mysql", "flight", "auto"])
@pytest.mark.parametrize("operation", ["map", "rename", "predicate_map"])
def test_native_budget_limits_real_worker_sql(doris_config, recorder, transport, operation):
    source = RecordingDorisDatasource(
        recorder=recorder,
        **doris_config.reader_kwargs(transport=transport, columns=["id"], batch_size=1),
    )
    source.get_read_tasks(1)
    dataset = ray.data.read_datasource(source, override_num_blocks=4, concurrency=2)
    if operation == "rename":
        dataset = dataset.rename_columns({"id": "key"})
        key = "key"
    else:
        if operation == "predicate_map":
            dataset = dataset.filter(expr=col("id") >= 2)
        dataset = dataset.map(lambda row: {"id": row["id"] + 100})
        key = "id"
    rows = dataset.limit(2).take_all()
    assert len(rows) == 2
    expected = {1, 2, 3, 4} if key == "key" else {101, 102, 103, 104}
    if operation == "predicate_map":
        expected.remove(101)
    assert {row[key] for row in rows} <= expected
    records = ray.get(recorder.read.remote())
    scans = [sql for stage, sql, _ in records if stage == "worker"]
    plans = [sql for stage, sql, _ in records if stage == "planner"]
    assert scans and all(sql.endswith(" LIMIT 2") for sql in scans)
    assert plans and all(" LIMIT " not in sql for sql in plans)
    # A later unbounded read reuses discovery without inheriting this task's budget.
    assert sorted(row["id"] for row in ray.data.read_datasource(source).take_all()) == [1, 2, 3, 4]


@pytest.mark.parametrize("transport", ["mysql", "flight", "auto"])
def test_residual_and_direct_limits_leave_real_sql_unbounded(doris_config, recorder, transport):
    source = RecordingDorisDatasource(
        recorder=recorder,
        **doris_config.reader_kwargs(transport=transport, columns=["id"], batch_size=1),
    )
    root = ray.data.read_datasource(source, override_num_blocks=4)
    assert len(root.limit(2).take_all()) == 2
    assert len(root.select_columns(["id"]).limit(2).take_all()) == 2
    residual = root.filter(lambda row: row["id"] == 4).map(lambda row: row)
    assert residual.limit(1).take_all() == [{"id": 4}]
    scans = [sql for stage, sql, _ in ray.get(recorder.read.remote()) if stage == "worker"]
    assert scans and all(" LIMIT " not in sql for sql in scans)


@pytest.mark.parametrize("transport", ["mysql", "flight", "auto"])
def test_zero_empty_projection_and_single_query_budget(doris_config, recorder, transport):
    source = RecordingDorisDatasource(
        recorder=recorder,
        **doris_config.reader_kwargs(transport=transport, columns=["id"], batch_size=1),
    )
    root = ray.data.read_datasource(source, override_num_blocks=4)
    assert root.map(lambda row: row).limit(0).take_all() == []
    assert not [sql for stage, sql, _ in ray.get(recorder.read.remote()) if stage == "worker"]
    projected = root.select_columns([]).map(lambda row: {"marker": 1}).limit(2)
    assert projected.take_all() == [{"marker": 1}, {"marker": 1}]

    empty = RecordingDorisDatasource(
        recorder=recorder,
        **doris_config.reader_kwargs(transport=transport, columns=["id"], filter="1 = 0"),
    )
    assert ray.data.read_datasource(empty).map(lambda row: row).limit(2).take_all() == []
    fallback = RecordingDorisDatasource(
        recorder=recorder,
        **doris_config.reader_kwargs(
            transport=transport,
            columns=["id"],
            filter=f"id IN (SELECT id FROM `{doris_config.database}`.`{doris_config.table}`)",
        ),
    )
    rows = ray.data.read_datasource(fallback).map(lambda row: row).limit(10).take_all()
    assert sorted(row["id"] for row in rows) == [1, 2, 3, 4]
    scans = [sql for stage, sql, _ in ray.get(recorder.read.remote()) if stage == "worker"]
    assert scans and all(sql.endswith((" LIMIT 2", " LIMIT 10")) for sql in scans)
    fallback_sql = [sql for sql in scans if " IN (SELECT " in sql]
    assert len(fallback_sql) == 1 and " TABLET(" not in fallback_sql[0]
