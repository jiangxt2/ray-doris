from unittest.mock import Mock

import pyarrow as pa
import pytest
from ray import cloudpickle

from ray_doris._models import DorisPlanningSnapshot
from ray_doris.datasource import DorisDatasource


def test_estimate_size_returns_none_without_planning() -> None:
    datasource = DorisDatasource(table="db.table", host="fe")
    assert datasource.estimate_inmemory_data_size() is None
    assert not datasource.should_create_reader
    assert cloudpickle.loads(cloudpickle.dumps(datasource)).config == datasource.config


def test_datasource_rejects_unserializable_options_without_exposing_value() -> None:
    class NotSerializable:
        def __deepcopy__(self, memo):
            return self

        def __reduce__(self):
            raise RuntimeError("serialization-secret-sentinel")

    with pytest.raises(
        ValueError,
        match="copyable and serializable values",
    ) as captured:
        DorisDatasource(
            table="db.table",
            host="fe",
            client_kwargs={"program_name": NotSerializable()},
        )
    assert "serialization-secret-sentinel" not in str(captured.value)
    assert captured.value.__cause__ is None


def test_explicit_flight_dependency_is_checked_on_driver(monkeypatch) -> None:
    monkeypatch.setattr(
        "ray_doris._readers._flight_is_installed",
        Mock(return_value=False),
    )
    with pytest.raises(ImportError, match="Flight SQL"):
        DorisDatasource(table="db.table", host="fe", transport="flight")


def test_auto_tls_dependency_is_checked_on_driver(monkeypatch) -> None:
    monkeypatch.setattr(
        "ray_doris._readers._flight_is_installed",
        Mock(return_value=False),
    )
    with pytest.raises(ImportError, match="Flight SQL"):
        DorisDatasource(
            table="db.table",
            host="fe",
            transport="auto",
            flight_scheme="grpc+tls",
        )


def test_get_read_tasks_has_current_compatible_signature_and_schema(monkeypatch) -> None:
    schema = pa.schema([("id", pa.int64())])
    discover = Mock(return_value=DorisPlanningSnapshot(schema=schema, tablet_ids=(7,)))
    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.discover", discover)
    datasource = DorisDatasource(table="db.table", host="fe")
    tasks = datasource.get_read_tasks(4, per_task_row_limit=None, data_context=object())
    assert len(tasks) == 1
    assert tasks[0].schema == schema
    assert tasks[0].metadata.num_rows is None
    assert tasks[0].metadata.size_bytes is None
    restored = cloudpickle.loads(cloudpickle.dumps(tasks[0]))
    assert restored.schema == schema
    assert restored.metadata == tasks[0].metadata
    datasource.get_read_tasks(2)
    discover.assert_called_once()


def test_environment_password_never_enters_datasource_or_read_task_pickle(monkeypatch) -> None:
    secret = "datasource-password-secret-sentinel"
    monkeypatch.setenv("RAY_DORIS_TASK_PASSWORD", secret)
    schema = pa.schema([("id", pa.int64())])
    discover = Mock(return_value=DorisPlanningSnapshot(schema=schema, tablet_ids=(7,)))
    monkeypatch.setattr(
        "ray_doris.datasource.DorisPlanner.discover",
        discover,
    )
    datasource = DorisDatasource(
        table="db.table",
        host="fe",
        password_env="RAY_DORIS_TASK_PASSWORD",
    )
    task = datasource.get_read_tasks(1)[0]
    assert secret.encode() not in cloudpickle.dumps(datasource)
    assert secret.encode() not in cloudpickle.dumps(task)
    restored_datasource = cloudpickle.loads(cloudpickle.dumps(datasource))
    assert restored_datasource.get_read_tasks(2)[0].schema == schema
    discover.assert_called_once()


def test_get_read_tasks_preserves_schema_for_valid_empty_plan(monkeypatch) -> None:
    schema = pa.schema([("id", pa.int64())])
    snapshot = DorisPlanningSnapshot(schema=schema, tablet_ids=())
    monkeypatch.setattr(
        "ray_doris.datasource.DorisPlanner.discover",
        Mock(return_value=snapshot),
    )
    datasource = DorisDatasource(table="db.table", host="fe")
    tasks = datasource.get_read_tasks(4)
    assert len(tasks) == 1
    assert tasks[0].schema == schema
    assert tasks[0].metadata.num_rows == 0
    assert tasks[0].metadata.size_bytes == 0
    blocks = list(tasks[0]())
    assert len(blocks) == 1
    assert blocks[0].schema == schema
    assert blocks[0].num_rows == 0


def test_get_read_tasks_rejects_parallelism_before_discovery(monkeypatch) -> None:
    discover = Mock()
    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.discover", discover)
    datasource = DorisDatasource(table="db.table", host="fe")
    with pytest.raises(ValueError, match="parallelism"):
        datasource.get_read_tasks(0)
    discover.assert_not_called()
