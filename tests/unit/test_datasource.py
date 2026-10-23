from dataclasses import FrozenInstanceError
from unittest.mock import Mock

import pyarrow as pa
import pytest
from ray import cloudpickle
from ray.data.block import BlockAccessor

from ray_doris._models import DorisPlanningSnapshot
from ray_doris.datasource import DorisDatasource, _read_zero_columns


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


@pytest.mark.parametrize("budget", [-1, True, 1.5, "2"])
def test_row_budget_is_rejected_before_discovery(monkeypatch, budget) -> None:
    discover = Mock()
    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.discover", discover)
    with pytest.raises(ValueError, match="per_task_row_limit"):
        DorisDatasource(table="db.records", host="fe").get_read_tasks(2, budget)
    discover.assert_not_called()


def test_row_budgets_are_task_local_serializable_and_preserve_discovery(monkeypatch) -> None:
    schema = pa.schema([("id", pa.int64())])
    snapshot = DorisPlanningSnapshot(schema=schema, tablet_ids=(1, 2, 3))
    discover = Mock(return_value=snapshot)
    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.discover", discover)
    monkeypatch.setattr("ray_doris.datasource.make_read_task", lambda *args: args)
    source = DorisDatasource(table="db.records", host="fe")
    retained = []
    for parallelism, budget in ((3, 2), (1, None), (2, 5), (3, None)):
        tasks = source.get_read_tasks(parallelism, budget)
        for task in tasks:
            read_fn, _, _, task_budget = task
            split = read_fn.args[1]
            assert split.row_limit == task_budget == budget
            assert read_fn.args[0] is source.config
            restored = cloudpickle.loads(cloudpickle.dumps(split))
            assert restored == split
            with pytest.raises(FrozenInstanceError):
                split.row_limit = 100
            retained.append((split, budget))
    assert all(split.row_limit == budget for split, budget in retained)
    assert source._planning_snapshot is snapshot
    discover.assert_called_once()


@pytest.mark.parametrize("zero_columns", [False, True])
def test_zero_budget_returns_schema_without_worker_scan(monkeypatch, zero_columns) -> None:
    schema = pa.schema([("id", pa.int64())])
    monkeypatch.setattr(
        "ray_doris.datasource.DorisPlanner.discover",
        Mock(return_value=DorisPlanningSnapshot(schema, (7,))),
    )
    monkeypatch.setattr("ray_doris.datasource.make_read_task", lambda *args: args)
    scan = Mock(side_effect=AssertionError("zero budget must not scan"))
    monkeypatch.setattr("ray_doris.datasource.read_split", scan)
    source = DorisDatasource(table="db.records", host="fe")
    if zero_columns:
        source = source.apply_projection({})
    tasks = source.get_read_tasks(4, 0)
    assert len(tasks) == 1
    read_fn, metadata, output_schema, budget = tasks[0]
    blocks = read_fn()
    assert budget == 0 and metadata.num_rows == 0
    assert len(blocks) == 1 and blocks[0].num_rows == 0
    assert blocks[0].schema == output_schema
    expected = schema
    if zero_columns:
        native = BlockAccessor.for_block(pa.Table.from_batches([], schema=schema)).select([])
        expected = BlockAccessor.for_block(native).to_arrow().schema
    assert output_schema == expected
    scan.assert_not_called()


def test_projection_is_ordered_immutable_and_preserves_subclass_state() -> None:
    class TaggedSource(DorisDatasource):
        tag = "retained"

    source = TaggedSource(table="db.table", host="fe", columns=["id", "value", "other"])
    requested = {"value": "alias-is-not-applied", "id": "id"}
    clone = source.apply_projection(requested)
    requested.clear()
    assert isinstance(clone, TaggedSource)
    assert clone.tag == "retained"
    assert clone is not source
    assert clone.config.columns == ("value", "id")
    assert clone.get_projection_map() == {"value": "value", "id": "id"}
    returned = clone.get_projection_map()
    returned.clear()
    assert clone.get_projection_map() == {"value": "value", "id": "id"}
    assert source.config.columns == ("id", "value", "other")
    second = clone.apply_projection({"id": "id"})
    assert second.config.columns == ("id",)
    assert clone.config.columns == ("value", "id")
    assert second.apply_projection(None).get_projection_map() == {"id": "id"}


def test_projection_cannot_restore_explicit_or_previously_removed_columns() -> None:
    source = DorisDatasource(table="db.table", host="fe", columns=["id", "value"])
    for projected in (source, source.apply_projection({"id": "id"})):
        with pytest.raises(ValueError, match="cannot restore"):
            projected.apply_projection({"other": "other"})
    empty = source.apply_projection({})
    assert empty.get_projection_map() == {}
    assert empty.config.columns == ("id", "value")
    with pytest.raises(ValueError, match="cannot restore"):
        empty.apply_projection({"id": "id"})


@pytest.mark.parametrize("name", ["bad-name", "x;select", "", 1])
def test_projection_reuses_identifier_validation(name) -> None:
    source = DorisDatasource(table="db.table", host="fe")
    with pytest.raises(ValueError):
        source.apply_projection({name: name})


def test_projection_invalidates_only_changed_clone_cache(monkeypatch) -> None:
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False), ("value", pa.string())])
    discovered = []

    def discover(planner):
        columns = planner._config.columns or schema.names
        selected = pa.schema([schema.field(name) for name in columns])
        discovered.append(tuple(columns))
        return DorisPlanningSnapshot(schema=selected, tablet_ids=(7, 8))

    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.discover", discover)
    source = DorisDatasource(table="db.table", host="fe")
    source.get_read_tasks(2)
    same = source.apply_projection(None)
    same.get_read_tasks(1)
    clone = source.apply_projection({"value": "value", "id": "id"})
    assert clone.get_read_tasks(2)[0].schema.names == ["value", "id"]
    assert not clone.get_read_tasks(1)[0].schema.field("id").nullable
    assert source.get_read_tasks(1)[0].schema == schema
    assert discovered == [("id", "value"), ("value", "id")]
    restored = cloudpickle.loads(cloudpickle.dumps(clone))
    assert restored.get_read_tasks(2)[0].schema.names == ["value", "id"]
    assert discovered == [("id", "value"), ("value", "id")]


@pytest.mark.parametrize("abort", [False, True])
def test_empty_projection_preserves_batches_and_closes_reader(monkeypatch, abort) -> None:
    table = pa.table({"id": [1, 2, 3]})
    closed = []
    monkeypatch.setattr(
        "ray_doris.datasource.DorisPlanner.discover",
        lambda self: DorisPlanningSnapshot(schema=table.schema, tablet_ids=(7,)),
    )

    def read(*args):
        try:
            yield table.slice(0, 2)
            yield table.slice(2, 1)
        finally:
            closed.append(True)

    monkeypatch.setattr("ray_doris.datasource.read_split", read)
    source = DorisDatasource(table="db.table", host="fe").apply_projection({})
    task = source.get_read_tasks(1)[0]
    reader = iter(task.read_fn())
    first = next(reader)
    assert first.equals(BlockAccessor.for_block(table.slice(0, 2)).select([]))
    assert first.schema == task.schema
    if abort:
        reader.close()
    else:
        second = next(reader)
        assert second.equals(BlockAccessor.for_block(table.slice(2, 1)).select([]))
        assert list(reader) == []
    assert closed == [True]


def test_empty_projection_closes_reader_if_projection_fails(monkeypatch) -> None:
    closed = []

    def read(*args):
        try:
            yield pa.table({"id": [1]})
        finally:
            closed.append(True)

    def fail(block):
        raise RuntimeError("projection failed")

    monkeypatch.setattr("ray_doris.datasource.read_split", read)
    monkeypatch.setattr("ray_doris.datasource.project_empty_block", fail)
    with pytest.raises(RuntimeError, match="projection failed"):
        list(_read_zero_columns(None, None, None))
    assert closed == [True]


def test_empty_projection_keeps_empty_plan_schema_without_querying(monkeypatch) -> None:
    monkeypatch.setattr(
        "ray_doris.datasource.DorisPlanner.discover",
        lambda self: DorisPlanningSnapshot(schema=pa.schema([("id", pa.int64())]), tablet_ids=()),
    )
    reader = Mock(side_effect=AssertionError("empty plan queried Doris"))
    monkeypatch.setattr("ray_doris.datasource.read_split", reader)
    source = DorisDatasource(table="db.table", host="fe").apply_projection({})
    task = source.get_read_tasks(1)[0]
    assert list(task())[0].num_rows == 0
    assert task.metadata.num_rows == 0
    assert list(task())[0].schema == task.schema
    reader.assert_not_called()
