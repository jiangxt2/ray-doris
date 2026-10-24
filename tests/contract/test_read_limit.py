"""Observe real Dataset row budgets and SQL execution before ReadTask slicing."""

from __future__ import annotations

import inspect
import sqlite3
import sys
from functools import partial
from unittest.mock import patch

import pyarrow as pa
import pytest
import ray
from ray import cloudpickle
from ray.data.datasource import ReadTask
from ray.data.expressions import col

from ray_doris import DorisDatasource
from ray_doris._compat import make_read_task, supports_native_predicates
from ray_doris._models import DorisPlanningSnapshot
from ray_doris._sql import build_read_sql

pytestmark = pytest.mark.contract
SCHEMA = pa.schema([pa.field("id", pa.int64(), nullable=False), ("text", pa.string())])
ROWS = [(number, f"value-{number}") for number in range(30)]


@ray.remote(num_cpus=0)
class LimitRecorder:
    def __init__(self):
        self.records = []

    def record(self, kind, value):
        self.records.append((kind, value))

    def read(self):
        return self.records


def _fixture_read(config, split, schema, diagnostics=None, *, recorder):
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("ATTACH ':memory:' AS db")
        connection.execute("CREATE TABLE db.records (id BIGINT NOT NULL, text TEXT)")
        rows = [row for row in ROWS if row[0] % 3 + 1 in split.tablet_ids]
        connection.executemany("INSERT INTO db.records VALUES (?, ?)", rows)
        sql = build_read_sql(config, None, row_limit=split.row_limit)[0]
        cursor = connection.execute(sql)
        names = [item[0] for item in cursor.description]
        result = [dict(zip(names, values)) for values in cursor.fetchall()]
        ray.get(recorder.record.remote("query", (sql, len(result), split.row_limit)))
        table = pa.Table.from_pylist(result, schema=schema)
        if not result:
            yield table
        for offset in range(0, len(result), 4):
            yield table.slice(offset, 4)
    finally:
        connection.close()


def _run_fixture(read_fn, recorder):
    with patch("ray_doris.datasource.read_split", partial(_fixture_read, recorder=recorder)):
        yield from read_fn()


class LimitFixtureDatasource(DorisDatasource):
    def __init__(self, recorder, **kwargs):
        self.recorder = recorder
        super().__init__(table="db.records", host="fixture", **kwargs)

    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        ray.get(self.recorder.record.remote("budget", per_task_row_limit))
        tasks = super().get_read_tasks(parallelism, per_task_row_limit, data_context)
        return [
            make_read_task(
                partial(_run_fixture, task.read_fn, self.recorder),
                task.metadata,
                task.schema,
                per_task_row_limit,
            )
            for task in tasks
        ]


@pytest.fixture(scope="module", autouse=True)
def ray_runtime():
    module = sys.modules[__name__]
    cloudpickle.register_pickle_by_value(module)
    started = not ray.is_initialized()
    if started:
        ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        yield
    finally:
        if started:
            ray.shutdown()
        cloudpickle.unregister_pickle_by_value(module)


@pytest.fixture
def source(monkeypatch):
    def describe(planner):
        columns = planner._config.columns
        return SCHEMA if columns is None else pa.schema([SCHEMA.field(name) for name in columns])

    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.describe_schema", describe)
    monkeypatch.setattr(
        "ray_doris.datasource.DorisPlanner.discover",
        lambda planner: DorisPlanningSnapshot(describe(planner), (1, 2, 3)),
    )
    recorder = LimitRecorder.remote()
    monkeypatch.setattr(
        "ray_doris.datasource.read_split", partial(_fixture_read, recorder=recorder)
    )
    try:
        yield LimitFixtureDatasource(recorder), recorder
    finally:
        ray.kill(recorder)


@pytest.mark.parametrize(
    "operation",
    ["map", "rename", "predicate_map", "direct", "projection", "residual", "sort", "flat_map"],
)
def test_native_dataset_budget_and_sql_boundaries(source, operation):
    datasource, recorder = source
    dataset = ray.data.read_datasource(datasource, override_num_blocks=3, concurrency=2)
    eligible = operation in ("map", "rename", "predicate_map")
    if operation == "map":
        dataset = dataset.map(lambda row: {"id": row["id"] + 100})
    elif operation == "rename":
        dataset = dataset.rename_columns({"id": "key"})
    elif operation == "predicate_map":
        if supports_native_predicates():
            dataset = dataset.filter(expr=col("id") >= 10)
        else:
            dataset = dataset.filter(lambda row: row["id"] >= 10)
            eligible = False
        dataset = dataset.map(lambda row: {"id": row["id"] + 100})
    elif operation == "projection":
        dataset = dataset.select_columns(["id"])
    elif operation == "residual":
        dataset = dataset.filter(lambda row: row["id"] >= 28).map(lambda row: row)
    elif operation == "sort":
        dataset = dataset.sort("id")
    elif operation == "flat_map":
        dataset = dataset.flat_map(lambda row: [row])
    rows = dataset.limit(2).take_all()
    assert len(rows) == 2
    if operation == "residual":
        assert {row["id"] for row in rows} == {28, 29}
    if operation == "sort":
        assert [row["id"] for row in rows] == [0, 1]
    records = ray.get(recorder.read.remote())
    queries = [value for kind, value in records if kind == "query"]
    budgets = [value for kind, value in records if kind == "budget"]
    assert queries
    modern = "per_task_row_limit" in inspect.signature(ReadTask).parameters
    # Projection hooks on older releases can leave a Project above Read.
    if operation == "projection" and not datasource.supports_projection_pushdown():
        eligible = modern
    if eligible and modern:
        assert 2 in budgets
        assert all(
            sql.endswith(" LIMIT 2") and count <= 2 and budget == 2
            for sql, count, budget in queries
        )
    else:
        assert all(budget is None for budget in budgets)
        assert all(" LIMIT " not in sql and budget is None for sql, _, budget in queries)


@pytest.mark.parametrize("budget", [0, 100, 1 << 63])
def test_native_zero_and_insufficient_rows(source, budget):
    datasource, recorder = source
    dataset = ray.data.read_datasource(datasource, override_num_blocks=3)
    rows = dataset.map(lambda row: row).limit(budget).take_all()
    assert len(rows) == min(budget, len(ROWS))
    if budget:
        assert {row["id"] for row in rows} == set(range(30))
    else:
        queries = [value for kind, value in ray.get(recorder.read.remote()) if kind == "query"]
        if "per_task_row_limit" in inspect.signature(ReadTask).parameters:
            assert not queries
        else:
            # Legacy Ray can start a read for limit(0), without supplying a budget.
            assert all(budget is None and " LIMIT " not in sql for sql, _, budget in queries)
