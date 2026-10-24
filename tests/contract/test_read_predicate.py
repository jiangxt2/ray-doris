"""Exercise the native Dataset optimizer with a deterministic SQL-backed fixture."""

from __future__ import annotations

import sqlite3
from functools import partial
from typing import Iterable
from unittest.mock import patch

import pyarrow as pa
import pytest
import ray

from ray_doris import DorisDatasource
from ray_doris._compat import make_read_task, supports_native_predicates
from ray_doris._models import DorisPlanningSnapshot
from ray_doris._sql import build_read_sql

if supports_native_predicates():
    from ray.data.expressions import col

pytestmark = pytest.mark.contract
SCHEMA = pa.schema(
    [("id", pa.int64()), ("n", pa.int64()), ("active", pa.bool_()), ("text", pa.string())]
)
ROWS = [(1, 1, True, "a%"), (2, None, False, "b"), (3, 3, None, "c"), (4, 4, True, "d")]


def _read_fixture(config, split, schema, diagnostics=None):
    del split
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("ATTACH ':memory:' AS db")
        connection.execute(
            "CREATE TABLE db.records (id BIGINT, n BIGINT, active BOOLEAN, text TEXT)"
        )
        connection.executemany("INSERT INTO db.records VALUES (?, ?, ?, ?)", ROWS)
        cursor = connection.execute(build_read_sql(config, None)[0])
        names = [item[0] for item in cursor.description]
        rows = [dict(zip(names, values)) for values in cursor.fetchall()]
        for row in rows:
            if "active" in row and row["active"] is not None:
                row["active"] = bool(row["active"])
        yield pa.Table.from_pylist(rows, schema=schema)
    finally:
        connection.close()


def _run_fixture_reader(read_fn):
    with patch("ray_doris.datasource.read_split", _read_fixture):
        yield from read_fn()


class SQLFixtureDatasource(DorisDatasource):
    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        tasks = super().get_read_tasks(parallelism, per_task_row_limit, data_context)
        return [
            make_read_task(
                partial(_run_fixture_reader, task.read_fn),
                task.metadata,
                task.schema,
                per_task_row_limit,
            )
            for task in tasks
        ]


@pytest.fixture(scope="module", autouse=True)
def ray_runtime() -> Iterable[None]:
    started = not ray.is_initialized()
    if started:
        ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        yield
    finally:
        if started:
            ray.shutdown()


@pytest.fixture
def source_factory(monkeypatch):
    def describe(planner):
        columns = planner._config.columns
        return SCHEMA if columns is None else pa.schema([SCHEMA.field(name) for name in columns])

    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.describe_schema", describe)
    monkeypatch.setattr(
        "ray_doris.datasource.DorisPlanner.discover",
        lambda planner: DorisPlanningSnapshot(describe(planner), (1,)),
    )
    monkeypatch.setattr("ray_doris.datasource.read_split", _read_fixture)
    return lambda **kwargs: SQLFixtureDatasource(table="db.records", host="fixture", **kwargs)


def test_native_acceptance_and_complete_residual_match_ray(source_factory, monkeypatch) -> None:
    source = source_factory()
    if not supports_native_predicates():
        assert not source.supports_predicate_pushdown()
        assert (
            len(ray.data.read_datasource(source).filter(lambda row: row["id"] > 1).take_all()) == 3
        )
        return

    calls = []
    apply = DorisDatasource.apply_predicate

    def record(instance, expression):
        result = apply(instance, expression)
        calls.append(result is not instance)
        return result

    monkeypatch.setattr(DorisDatasource, "apply_predicate", record)
    baseline = ray.data.from_arrow(
        pa.Table.from_pylist([dict(zip(SCHEMA.names, row)) for row in ROWS], schema=SCHEMA)
    ).materialize()
    cases = [
        ((col("id") >= 2) & ~(col("n").is_null()), True),
        ((col("n") > 2) | (col("active") == False), True),  # noqa: E712
        (~((col("n") > 1) & (col("active") == False)), True),  # noqa: E712
        ((col("id") > 1) & (col("text") == "b"), False),
        ((col("id") > 1) | (col("text") == "a%"), False),
    ]
    for expression, accepted in cases:
        calls.clear()
        actual = ray.data.read_datasource(source_factory()).filter(expr=expression).take_all()
        expected = baseline.filter(expr=expression).take_all()
        assert sorted(actual, key=lambda r: r["id"]) == sorted(expected, key=lambda r: r["id"])
        assert calls and all(value is accepted for value in calls)


def test_branches_repeated_filters_projection_and_residual_limit(source_factory) -> None:
    if not supports_native_predicates():
        assert not source_factory().supports_predicate_pushdown()
        return

    root = ray.data.read_datasource(source_factory())
    first = root.filter(expr=col("id") < 3)
    second = root.filter(expr=col("id") >= 3)
    assert sorted(row["id"] for row in first.take_all()) == [1, 2]
    assert sorted(row["id"] for row in second.take_all()) == [3, 4]
    assert root.count() == 4
    chained = root.filter(expr=col("id") > 1).filter(expr=col("id") < 4)
    assert sorted(row["id"] for row in chained.take_all()) == [2, 3]
    projected = root.select_columns(["id", "text"]).filter(expr=col("id") > 3)
    assert projected.take_all() == [{"id": 4, "text": "d"}]
    aliased = root.select_columns(["id"]).rename_columns({"id": "key"})
    assert aliased.filter(expr=col("key") > 3).take_all() == [{"key": 4}]
    sparse = root.filter(expr=(col("id") > 1) & (col("text") == "d"))
    assert sparse.limit(1).take_all() == [dict(zip(SCHEMA.names, ROWS[-1]))]
    assert sparse.limit(0).take_all() == []
    assert root.filter(expr=col("id") > 100).take_all() == []


def test_trusted_filter_and_native_filter_compose(source_factory) -> None:
    if not supports_native_predicates():
        assert not source_factory().supports_predicate_pushdown()
        return

    dataset = ray.data.read_datasource(source_factory(filter="text LIKE 'a%' OR id % 2 = 0"))
    assert sorted(row["id"] for row in dataset.filter(expr=col("id") > 1).take_all()) == [2, 4]


@pytest.mark.parametrize("predicate_first", [False, True])
@pytest.mark.parametrize("empty_projection", [False, True])
def test_native_predicate_survives_projection_of_its_dependency(
    source_factory, predicate_first, empty_projection
) -> None:
    if not supports_native_predicates():
        assert not source_factory().supports_predicate_pushdown()
        return

    root = ray.data.read_datasource(source_factory())
    dataset = root if predicate_first else root.select_columns(["text", "n", "id"])
    filtered = dataset.filter(expr=col("n") > 1)
    selected = filtered.select_columns([] if empty_projection else ["text", "id"]).materialize()
    if empty_projection:
        native = ray.data.from_items([{"id": 3}, {"id": 4}]).select_columns([]).materialize()
        assert selected.count() == native.count() == 2
        assert selected.take_all() == native.take_all()
    else:
        assert sorted(selected.take_all(), key=lambda row: row["id"]) == [
            {"text": "c", "id": 3},
            {"text": "d", "id": 4},
        ]
        assert selected.schema().names == ["text", "id"]
    assert root.count() == 4
