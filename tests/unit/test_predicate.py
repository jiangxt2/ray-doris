from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from unittest.mock import Mock

import pyarrow as pa
import pytest
from pymysql.connections import Connection
from ray import cloudpickle
from test_readers import FakeConnection, FakeCursor, make_config

from ray_doris import _readers
from ray_doris._compat import project_empty_block, supports_native_predicates
from ray_doris._models import DorisInputSplit, DorisPlanningSnapshot, DorisReadConfig
from ray_doris._planner import DorisPlanner
from ray_doris._predicate import compile_predicate
from ray_doris._sql import build_read_sql, parse_table
from ray_doris.datasource import DorisDatasource

if supports_native_predicates():
    from ray.data.expressions import col, lit

SCHEMA = pa.schema(
    [("id", pa.int64()), ("small", pa.int8()), ("active", pa.bool_()), ("text", pa.string())]
)


def test_native_predicates_on_supported_runtime(monkeypatch) -> None:
    if not supports_native_predicates():
        source = DorisDatasource(table="db.table", host="fe")
        assert source.apply_predicate(object()) is source
        return

    cases = [
        (col("id") >= 2, "`id` >= %s", (2,), "`id` >= 2"),
        (lit(2) < col("id"), "`id` > %s", (2,), "`id` > 2"),
        (col("active") == True, "`active` = %s", (True,), "`active` = TRUE"),  # noqa: E712
        (col("active") != False, "`active` != %s", (False,), "`active` != FALSE"),  # noqa: E712
        (col("small").is_null(), "`small` IS NULL", (), "`small` IS NULL"),
        (col("active").is_not_null(), "`active` IS NOT NULL", (), "`active` IS NOT NULL"),
    ]
    for expr, mysql, parameters, literal in cases:
        compiled = compile_predicate(expr, SCHEMA)
        assert compiled is not None
        assert (compiled.mysql_sql, compiled.parameters, compiled.literal_sql) == (
            mysql,
            parameters,
            literal,
        )
    for operator in ("eq", "ne", "lt", "le", "gt", "ge"):
        assert compile_predicate(getattr(col("small"), f"__{operator}__")(-128), SCHEMA)
        assert compile_predicate(getattr(col("small"), f"__{operator}__")(127), SCHEMA)
    for boundary in (-(2**63), 2**63 - 1):
        assert compile_predicate(col("id") == boundary, SCHEMA)
    expr = ~((col("id") > 1) & ((col("active") == True) | col("active").is_null()))  # noqa: E712
    compiled = compile_predicate(expr, SCHEMA)
    assert compiled is not None and compiled.parameters == (1, True)
    assert compiled.literal_sql == "NOT ((`id` > 1) AND ((`active` = TRUE) OR (`active` IS NULL)))"


def test_unsupported_complete_predicates_are_rejected() -> None:
    if not supports_native_predicates():
        assert not DorisDatasource(table="db.table", host="fe").supports_predicate_pushdown()
        return

    unsupported = [
        col("id") == None,  # noqa: E711
        col("id") == True,  # noqa: E712
        col("id") > 1.0,
        col("id") == Decimal(1),
        col("small") > 128,
        col("small") < -129,
        col("id") > 2**63,
        col("active") > True,
        col("active") == 1,
        col("id") == col("small"),
        col("missing") > 1,
        col("id").is_in([1, 2]),
        col("text").is_null(),
        col("text") == "secret-sentinel",
        (col("id") + 1) > 2,
    ]
    for expr in unsupported:
        assert compile_predicate(expr, SCHEMA) is None
        assert compile_predicate((col("id") > 1) & expr, SCHEMA) is None
        assert compile_predicate((col("id") > 1) | expr, SCHEMA) is None
        assert compile_predicate(~expr, SCHEMA) is None
    for dtype in (pa.float64(), pa.decimal128(10, 2), pa.date32(), pa.timestamp("us")):
        assert compile_predicate(col("id") == 1, pa.schema([("id", dtype)])) is None
    assert compile_predicate(col("bad-name") == 1, pa.schema([("bad-name", pa.int64())])) is None
    deep = col("id") > 1
    for _ in range(66):
        deep = ~deep
    assert compile_predicate(deep, SCHEMA) is None


def test_source_clone_preserves_branches_and_replans(monkeypatch) -> None:
    if not supports_native_predicates():
        assert not DorisDatasource(table="db.table", host="fe").supports_predicate_pushdown()
        return

    original_snapshot = DorisPlanningSnapshot(SCHEMA, (1, 2))
    discover = Mock(return_value=DorisPlanningSnapshot(SCHEMA, (2,)))
    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.discover", discover)
    monkeypatch.setattr(
        "ray_doris.datasource.DorisPlanner.describe_schema", Mock(return_value=SCHEMA)
    )
    source = DorisDatasource(table="db.table", host="fe", filter="id > 0")
    source._planning_snapshot = original_snapshot
    expression = col("id") > 1
    clone = source.apply_predicate(expression)
    sibling = source.apply_predicate(col("id") < 0)
    assert clone is not source and sibling is not source
    assert source._planning_snapshot is original_snapshot and source.config.predicate is None
    assert clone._planning_snapshot is None and sibling._planning_snapshot is None
    assert clone.get_current_predicate() is expression
    assert clone.config.predicate.parameters == (1,)
    assert sibling.config.predicate.parameters == (0,)
    assert clone.apply_predicate(col("text") == "x") is clone
    clone.get_read_tasks(2)
    clone.get_read_tasks(1)
    discover.assert_called_once()
    second = clone.apply_predicate(col("id") < 4)
    assert second.config.predicate.parameters == (1, 4)
    assert clone.config.predicate.parameters == (1,)
    assert second._planning_snapshot is None
    restored = cloudpickle.loads(cloudpickle.dumps(second))
    assert restored.config.predicate == second.config.predicate
    assert "1" not in repr(second.config.predicate)
    with pytest.raises(FrozenInstanceError):
        second.config.predicate.literal_sql = "changed"


def test_schema_is_validated_before_acceptance_and_errors_are_not_swallowed(monkeypatch) -> None:
    if not supports_native_predicates():
        assert not DorisDatasource(table="db.table", host="fe").supports_predicate_pushdown()
        return

    describe = Mock(return_value=SCHEMA)
    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.describe_schema", describe)
    source = DorisDatasource(table="db.table", host="fe")
    assert source.supports_predicate_pushdown()
    describe.assert_not_called()
    assert source.apply_predicate(col("id") == 1) is not source
    describe.assert_called_once()
    describe.side_effect = RuntimeError("schema discovery failed")
    with pytest.raises(RuntimeError, match="schema discovery failed"):
        source.apply_predicate(col("id") == 1)


@pytest.mark.parametrize("predicate_first", [False, True])
@pytest.mark.parametrize("empty_projection", [False, True])
def test_predicate_and_projection_preserve_sql_cache_and_branches(
    monkeypatch, predicate_first, empty_projection
) -> None:
    source = DorisDatasource(table="db.table", host="fe")
    if not supports_native_predicates():
        assert not source.supports_predicate_pushdown()
        return

    def describe(planner):
        columns = planner._config.columns
        return SCHEMA if columns is None else pa.schema([SCHEMA.field(name) for name in columns])

    monkeypatch.setattr("ray_doris.datasource.DorisPlanner.describe_schema", describe)
    monkeypatch.setattr(
        "ray_doris.datasource.DorisPlanner.discover",
        lambda planner: DorisPlanningSnapshot(describe(planner), (7,)),
    )
    source.get_read_tasks(1)
    snapshot = source._planning_snapshot
    if predicate_first:
        filtered = source.apply_predicate(col("id") > 1)
    else:
        filtered = source.apply_projection({"text": "text", "id": "id"})
        filtered.get_read_tasks(1)
        filtered = filtered.apply_predicate(col("id") > 1)
    clone = filtered.apply_projection({} if empty_projection else {"text": "text"})
    assert clone._planning_snapshot is None
    assert clone.config.predicate.parameters == (1,)
    assert clone.get_current_predicate() is filtered.get_current_predicate()
    query, parameters = build_read_sql(clone.config, (7,), bind=True)
    assert "WHERE `id` > %s" in query and parameters == (1,)
    if not empty_projection:
        assert query.startswith("SELECT `text` FROM")
        assert clone.apply_predicate(col("id") > 2) is clone
    task = clone.get_read_tasks(1)[0]
    if empty_projection:
        expected = project_empty_block(
            pa.Table.from_batches([], schema=describe(DorisPlanner(clone.config)))
        )
        assert task.schema == expected.schema
    else:
        assert task.schema.names == ["text"]
    assert source._planning_snapshot is snapshot and source.config.predicate is None
    assert source.config.columns is None and source.get_projection_map() is None
    restored = cloudpickle.loads(cloudpickle.dumps(clone))
    assert restored.config == clone.config
    assert restored.get_projection_map() == clone.get_projection_map()


def test_query_forms_preserve_trusted_filter_and_bind_only_native_values() -> None:
    if not supports_native_predicates():
        assert not DorisDatasource(table="db.table", host="fe").supports_predicate_pushdown()
        return

    compiled = compile_predicate(col("id") >= 2, SCHEMA)
    config = DorisReadConfig(
        host="fe",
        table=parse_table("db.table"),
        user="reader",
        filter="text LIKE 'a%' OR id % 2 = 0",
        predicate=compiled,
    )
    mysql, parameters = build_read_sql(config, (7,), bind=True)
    literal, no_parameters = build_read_sql(config, (7,))
    assert parameters == (2,) and no_parameters == ()
    assert "WHERE (text LIKE 'a%%' OR id %% 2 = 0) AND (`id` >= %s)" in mysql
    assert "WHERE (text LIKE 'a%' OR id % 2 = 0) AND (`id` >= 2)" in literal
    assert "TABLET(7)" in mysql and "LIMIT" not in mysql
    null_config = replace(config, predicate=compile_predicate(col("id").is_null(), SCHEMA))
    assert "a%" in build_read_sql(null_config, None, bind=True)[0]
    assert build_read_sql(null_config, None, bind=True)[1] == ()

    cursor = Connection(defer_connect=True).cursor()
    assert cursor.mogrify(mysql, parameters) == literal
    assert "2" not in repr(compiled)


def test_mysql_reader_uses_bound_parameters_and_cleans_up(monkeypatch) -> None:
    if not supports_native_predicates():
        assert not DorisDatasource(table="db.table", host="fe").supports_predicate_pushdown()
        return

    cursor = FakeCursor()
    cursor._batches = []
    cursor.fetchmany = Mock(return_value=[])
    cursor.execute = Mock()
    connection = FakeConnection(cursor)
    monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))
    config = make_config(predicate=compile_predicate(col("id") > 1, SCHEMA))
    assert list(_readers.read_mysql(config, DorisInputSplit((7,)), pa.schema([]))) == []
    assert cursor.execute.call_args.args[1] == (1,)
    assert "`id` > %s" in cursor.execute.call_args.args[0]
    assert cursor.closed and connection.closed


def test_native_predicate_survives_auto_transport_fallback(monkeypatch) -> None:
    if not supports_native_predicates():
        assert not DorisDatasource(table="db.table", host="fe").supports_predicate_pushdown()
        return

    config = make_config(transport="auto", predicate=compile_predicate(col("id") > 1, SCHEMA))
    monkeypatch.setattr(_readers, "_flight_is_installed", lambda: True)
    flight = Mock(side_effect=_readers._FlightUnavailableError("setup unavailable"))
    mysql = Mock(return_value=[pa.table({"id": [2]})])
    monkeypatch.setattr(_readers, "read_flight", flight)
    monkeypatch.setattr(_readers, "read_mysql", mysql)
    assert len(list(_readers.read_split(config, DorisInputSplit((7,)), SCHEMA))) == 1
    assert mysql.call_args.args[0].predicate is config.predicate
    assert "`id` > 1" in _readers._query(config, DorisInputSplit((7,)))
