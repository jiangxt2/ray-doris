"""Test-only recorders at the public Ray and database execution boundaries."""

from __future__ import annotations

from contextlib import ExitStack
from functools import partial
from unittest.mock import patch

import pymysql.cursors
import ray

from ray_doris import DorisDatasource
from ray_doris._compat import make_read_task
from ray_doris._models import DorisPlanningSnapshot
from ray_doris._planner import DorisPlanner, QueryPlanClient
from ray_doris._sql import build_select_sql


@ray.remote(num_cpus=0)
class SQLRecorder:
    def __init__(self):
        self.records = []

    def record(self, stage, sql, node_id):
        self.records.append((stage, sql, node_id))

    def read(self):
        return self.records


def _record_reader(read_fn, recorder, transport):
    def record_execute(original):
        def execute(cursor, sql, *args, **kwargs):
            if isinstance(sql, str) and sql.startswith("SELECT "):
                ray.get(
                    recorder.record.remote("worker", sql, ray.get_runtime_context().get_node_id())
                )
            return original(cursor, sql, *args, **kwargs)

        return execute

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(
                pymysql.cursors.Cursor,
                "execute",
                record_execute(pymysql.cursors.Cursor.execute),
            )
        )
        if transport in ("flight", "auto"):
            try:
                from adbc_driver_manager.dbapi import Cursor
            except ImportError as exc:
                raise ImportError(
                    "Flight SQL recording requires the optional ADBC dependency"
                ) from exc

            stack.enter_context(patch.object(Cursor, "execute", record_execute(Cursor.execute)))
        # yield from propagates consumer close to the real reader while the
        # recording contexts are still installed. Real SQL execution is retained.
        yield from read_fn()


def _fake_read(config, split, schema, diagnostics=None, *, table, recorder):
    del schema
    sql = build_select_sql(config.table, config.columns, config.filter, split.tablet_ids)
    ray.get(recorder.record.remote("synthetic", sql, ray.get_runtime_context().get_node_id()))
    selected = table if config.columns is None else table.select(config.columns)
    if not selected.num_rows:
        yield selected
    for offset in range(0, selected.num_rows, 2):
        yield selected.slice(offset, 2)


def _read_fixture(read_fn, table, recorder):
    with patch(
        "ray_doris.datasource.read_split", partial(_fake_read, table=table, recorder=recorder)
    ):
        yield from read_fn()


class RecordingDorisDatasource(DorisDatasource):
    """Record actual SQL, or supply Arrow fixtures for database-free Ray contracts."""

    def __init__(self, *, recorder, fixture_table=None, projection_enabled=True, **kwargs):
        self.recorder = recorder
        self.fixture_table = fixture_table
        self.projection_enabled = projection_enabled
        super().__init__(**kwargs)

    def supports_projection_pushdown(self):
        return self.projection_enabled and super().supports_projection_pushdown()

    def supports_predicate_pushdown(self):
        # Arrow fixtures model projection only; keep their filters in Ray.
        # Real database reads retain both native datasource capabilities.
        return self.fixture_table is None and super().supports_predicate_pushdown()

    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        if self.fixture_table is not None:
            table = self.fixture_table

            def discover(planner):
                columns = planner._config.columns
                selected = table if columns is None else table.select(columns)
                return DorisPlanningSnapshot(schema=selected.schema, tablet_ids=(7,))

            with (
                patch.object(DorisPlanner, "discover", discover),
                patch(
                    "ray_doris.datasource.read_split",
                    partial(_fake_read, recorder=self.recorder, table=table),
                ),
            ):
                tasks = super().get_read_tasks(parallelism, per_task_row_limit, data_context)
            return [
                make_read_task(
                    partial(_read_fixture, task.read_fn, table, self.recorder),
                    task.metadata,
                    task.schema,
                    per_task_row_limit,
                )
                for task in tasks
            ]

        fetch = QueryPlanClient.fetch_tablet_ids

        def record_plan(client, sql):
            ray.get(
                self.recorder.record.remote("planner", sql, ray.get_runtime_context().get_node_id())
            )
            return fetch(client, sql)

        with patch.object(QueryPlanClient, "fetch_tablet_ids", record_plan):
            tasks = super().get_read_tasks(parallelism, per_task_row_limit, data_context)
        return [
            make_read_task(
                partial(_record_reader, task.read_fn, self.recorder, self.config.transport),
                task.metadata,
                task.schema,
                per_task_row_limit,
            )
            for task in tasks
        ]
