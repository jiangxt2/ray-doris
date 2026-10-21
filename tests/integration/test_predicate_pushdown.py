"""Compare native Dataset predicates with Ray evaluation on real Doris data."""

from __future__ import annotations

import sys
from functools import partial
from importlib import import_module
from unittest.mock import patch

import pyarrow as pa
import pymysql
import pytest
import ray
from ray import cloudpickle
from ray.data.expressions import col

from ray_doris import DorisDatasource, read_doris
from ray_doris._compat import make_read_task
from ray_doris._planner import QueryPlanClient

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def serialize_probe_by_value():
    """Ship the test probe without requiring pytest's import path on workers."""
    module = sys.modules[__name__]
    cloudpickle.register_pickle_by_value(module)
    try:
        yield
    finally:
        cloudpickle.unregister_pickle_by_value(module)


@ray.remote(num_cpus=0)
class QueryRecorder:
    def __init__(self):
        self.queries = []

    def record(self, transport, sql, parameters):
        self.queries.append((transport, sql, parameters))

    def read(self):
        return self.queries


def _record_read(read_fn, recorder):
    cursor_type = import_module("adbc_driver_manager.dbapi").Cursor

    mysql_execute = pymysql.cursors.SSCursor.execute
    flight_execute = cursor_type.execute

    def mysql(cursor, query, args=None):
        ray.get(recorder.record.remote("mysql", query, args))
        return mysql_execute(cursor, query, args)

    def flight(cursor, query, parameters=None, **kwargs):
        ray.get(recorder.record.remote("flight", query, parameters))
        return flight_execute(cursor, query, parameters, **kwargs)

    with (
        patch.object(pymysql.cursors.SSCursor, "execute", mysql),
        patch.object(cursor_type, "execute", flight),
    ):
        yield from read_fn()


class RecordingDatasource(DorisDatasource):
    def __init__(self, recorder, **kwargs):
        super().__init__(**kwargs)
        self.recorder = recorder

    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        tasks = super().get_read_tasks(parallelism, per_task_row_limit, data_context)
        return [
            make_read_task(
                partial(_record_read, task.read_fn, self.recorder),
                task.metadata,
                task.schema,
                per_task_row_limit,
            )
            for task in tasks
        ]


@pytest.fixture(scope="module")
def predicate_table(doris_config):
    connection = pymysql.connect(
        host=doris_config.host,
        port=doris_config.mysql_port,
        user=doris_config.user,
        password=doris_config.password,
        autocommit=True,
    )
    table = f"{doris_config.database}.predicate_records"
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE {table} (id BIGINT NOT NULL, n TINYINT NULL, "
                "active BOOLEAN NULL, text VARCHAR(32) NULL) DUPLICATE KEY(id) "
                "DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ('replication_num'='1')"
            )
            cursor.execute(
                f"INSERT INTO {table} VALUES (-2,-128,FALSE,'a%'), "
                "(-1,NULL,TRUE,'b'),(0,0,NULL,'c'),(1,127,TRUE,NULL),"
                "(2,NULL,FALSE,'e'),(3,3,TRUE,'f')"
            )
        yield table
    finally:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        connection.close()


@pytest.mark.parametrize("transport", ["mysql", "flight", "auto"])
def test_native_predicates_match_ray_and_reach_both_protocols(
    doris_config,
    predicate_table,
    transport,
    monkeypatch,
) -> None:
    kwargs = doris_config.reader_kwargs(
        table=predicate_table, transport=transport, on_query_plan_error="error"
    )
    rows = read_doris(**kwargs).take_all()
    baseline = ray.data.from_arrow(pa.Table.from_pylist(rows)).materialize()
    plans = []
    fetch = QueryPlanClient.fetch_tablet_ids

    def record_plan(client, sql):
        plans.append(sql)
        return fetch(client, sql)

    monkeypatch.setattr(QueryPlanClient, "fetch_tablet_ids", record_plan)
    expressions = [
        col("id") == 2,
        col("id") != 2,
        col("id") < 0,
        col("id") <= 0,
        col("id") > 0,
        col("id") >= 0,
        col("n") == -128,
        col("n") == 127,
        col("n").is_null(),
        col("n").is_not_null(),
        col("active") == True,  # noqa: E712
        ~((col("n") > 0) & (col("active") == False)),  # noqa: E712
        (col("n") > 0) | (col("active") == False),  # noqa: E712
    ]
    recorder = QueryRecorder.remote()
    try:
        for expression in expressions:
            source = RecordingDatasource(recorder, **kwargs)
            actual = ray.data.read_datasource(source).filter(expr=expression).take_all()
            expected = baseline.filter(expr=expression).take_all()
            assert sorted(actual, key=lambda row: row["id"]) == sorted(
                expected, key=lambda row: row["id"]
            )
        queries = ray.get(recorder.read.remote())
        assert queries and any("WHERE" in sql for _, sql, _ in queries)
        assert any("`id` = 2" in sql for sql in plans)
        if transport == "mysql":
            assert any("%s" in sql and parameters == (2,) for _, sql, parameters in queries)
        else:
            assert any(kind == "flight" and "`id` = 2" in sql for kind, sql, _ in queries)
    finally:
        ray.kill(recorder)


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_residual_limit_trusted_filter_and_branches(
    doris_config, predicate_table, transport
) -> None:
    kwargs = doris_config.reader_kwargs(table=predicate_table, transport=transport)
    root = read_doris(**kwargs)
    left = root.filter(expr=col("id") < 0)
    right = root.filter(expr=col("id") >= 0)
    assert sorted(row["id"] for row in left.take_all()) == [-2, -1]
    assert sorted(row["id"] for row in right.take_all()) == [0, 1, 2, 3]
    assert root.count() == 6
    assert right.filter(expr=col("id") < 2).count() == 2
    assert root.filter(expr=col("id") > 100).take_all() == []
    residual = root.filter(expr=(col("id") > 0) & (col("text") == "f"))
    assert [row["id"] for row in residual.limit(1).take_all()] == [3]
    assert residual.limit(0).take_all() == []
    projected = root.select_columns(["id", "text"]).filter(expr=col("id") == 3)
    assert projected.take_all() == [{"id": 3, "text": "f"}]
    trusted = read_doris(**dict(kwargs, filter="text LIKE 'a%' OR id % 2 = 0"))
    assert sorted(row["id"] for row in trusted.filter(expr=col("id") >= 0).take_all()) == [0, 2]
