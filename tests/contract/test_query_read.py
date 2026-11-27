"""Ray contract for query reads whose schema is discovered by the worker."""

from __future__ import annotations

import sys

import pyarrow as pa
import pytest
import ray
from ray import cloudpickle

from ray_doris import read_doris
from ray_doris._query import DorisQueryDatasource

pytestmark = pytest.mark.contract


def _query_rows(config):
    del config
    yield pa.table({"id": pa.array([1, 2], type=pa.int64())})


def _query_empty(config):
    del config
    yield pa.table({"id": pa.array([], type=pa.int64())})


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


@pytest.mark.parametrize(
    "reader,expected", [(_query_rows, [{"id": 1}, {"id": 2}]), (_query_empty, [])]
)
@pytest.mark.parametrize("blocks", [None, 1, 2])
def test_ray_infers_worker_schema_when_planning_schema_is_unknown(
    monkeypatch, reader, expected, blocks
):
    monkeypatch.setattr("ray_doris._query._read_query", reader)
    source = DorisQueryDatasource(
        query="SELECT id FROM db.table",
        host="unused",
        mysql_port=9030,
        user="reader",
        password="",
        password_env=None,
        batch_size=1,
        connect_timeout=10,
        client_kwargs=None,
    )
    tasks = source.get_read_tasks(1)
    assert len(tasks) == 1 and tasks[0].schema is None
    dataset = read_doris(
        host="unused", query="SELECT id FROM db.table", override_num_blocks=blocks
    ).materialize()
    assert dataset.take_all() == expected
    assert dataset.schema().base_schema == pa.schema([("id", pa.int64())])


@pytest.mark.parametrize("blocks", [1, 2])
def test_ray_preserves_explicit_schema_for_empty_query(monkeypatch, blocks):
    monkeypatch.setattr("ray_doris._query._read_query", _query_empty)
    schema = pa.schema([("id", pa.int64())])
    dataset = read_doris(
        host="unused", query="SELECT id FROM db.table", schema=schema, override_num_blocks=blocks
    ).materialize()
    assert dataset.take_all() == []
    assert dataset.schema().base_schema == schema
