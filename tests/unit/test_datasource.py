from unittest.mock import Mock

import pyarrow as pa
import pytest
from ray import cloudpickle

from ray_doris._models import DorisInputSplit, DorisPlan
from ray_doris.datasource import DorisDatasource


def test_estimate_size_returns_none_without_planning() -> None:
    datasource = DorisDatasource(table="db.table", host="fe")
    assert datasource.estimate_inmemory_data_size() is None
    assert not datasource.should_create_reader
    assert cloudpickle.loads(cloudpickle.dumps(datasource)).config == datasource.config


def test_explicit_flight_dependency_is_checked_on_driver(monkeypatch) -> None:
    monkeypatch.setattr(
        "ray_doris._readers._flight_is_installed",
        Mock(return_value=False),
    )
    with pytest.raises(ImportError, match="Flight SQL"):
        DorisDatasource(table="db.table", host="fe", transport="flight")


def test_get_read_tasks_has_current_compatible_signature_and_schema(monkeypatch) -> None:
    schema = pa.schema([("id", pa.int64())])
    plan = DorisPlan(schema=schema, splits=(DorisInputSplit((7,)),))
    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.plan", Mock(return_value=plan))
    datasource = DorisDatasource(table="db.table", host="fe")
    tasks = datasource.get_read_tasks(4, per_task_row_limit=None, data_context=object())
    assert len(tasks) == 1
    assert tasks[0].schema == schema
    assert tasks[0].metadata.num_rows is None
    assert tasks[0].metadata.size_bytes is None
    restored = cloudpickle.loads(cloudpickle.dumps(tasks[0]))
    assert restored.schema == schema
    assert restored.metadata == tasks[0].metadata


def test_get_read_tasks_preserves_valid_empty_plan(monkeypatch) -> None:
    plan = DorisPlan(schema=pa.schema([("id", pa.int64())]), splits=())
    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.plan", Mock(return_value=plan))
    datasource = DorisDatasource(table="db.table", host="fe")
    assert datasource.get_read_tasks(4) == []
