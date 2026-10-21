"""External-table Doris reads reuse the canonical planner and streaming readers."""

from importlib import import_module
from unittest.mock import Mock

import pyarrow as pa
import pymysql
import pytest

from ray_doris import (
    DorisConfigurationError,
    DorisDatasource,
    DorisPermissionError,
    DorisPlanningError,
    DorisSchemaError,
    _planner,
    _readers,
)
from ray_doris._models import DorisInputSplit, DorisPredicate, DorisReadConfig
from ray_doris._planner import DorisPlanner, QueryPlanClient, _mysql_connection_kwargs
from ray_doris._schema import build_arrow_schema, parse_information_schema_rows
from ray_doris._sql import (
    build_describe_sql,
    build_external_columns_sql,
    build_read_sql,
    build_select_sql,
    parse_table,
)


def config(**extra):
    return DorisReadConfig.from_options(
        host="fe", table=parse_table("db.t"), user="reader", catalog="hive", **extra
    )


def test_external_sql_is_fully_qualified_and_never_uses_tablets():
    table = parse_table("db.t")
    assert build_select_sql(table, ["id"], "id > 1", None, "hive") == (
        "SELECT `id` FROM `hive`.`db`.`t` WHERE id > 1"
    )
    assert build_describe_sql(table, "hive") == "DESCRIBE `hive`.`db`.`t`"
    with pytest.raises(DorisConfigurationError, match="tablet"):
        build_select_sql(table, None, None, (1,), "hive")
    with pytest.raises(DorisConfigurationError, match="catalog"):
        DorisReadConfig(host="fe", table=table, user="u", catalog="hive;bad")


def test_external_connections_have_no_internal_default_database():
    assert "database" not in _mysql_connection_kwargs(config(), streaming=False)
    assert _mysql_connection_kwargs(config(), streaming=True)["cursorclass"] is (
        pymysql.cursors.SSCursor
    )


@pytest.mark.parametrize("predicate", [None, DorisPredicate("`id` > %s", (3,), "`id` > 3")])
def test_external_planning_uses_one_full_query_without_http_tablet_discovery(
    monkeypatch, predicate
):
    schema = pa.schema([("id", pa.int32())])
    describe = Mock(return_value=schema)
    tablets = Mock(side_effect=AssertionError("tablet query-plan used"))
    monkeypatch.setattr(DorisPlanner, "_describe_schema", describe)
    monkeypatch.setattr(QueryPlanClient, "fetch_tablet_ids", tablets)
    planner = DorisPlanner(config(predicate=predicate))
    snapshot = planner.discover()
    assert snapshot.schema == schema and snapshot.tablet_ids is None
    assert planner.plan_from_snapshot(snapshot, 8).splits == (DorisInputSplit(None),)
    tablets.assert_not_called()


@pytest.mark.parametrize("catalog", [None, "hive"])
@pytest.mark.parametrize("bind", [False, True])
def test_combined_read_sql_preserves_catalog_projection_and_bindings(catalog, bind):
    options = DorisReadConfig.from_options(
        host="fe",
        table=parse_table("db.t"),
        user="reader",
        catalog=catalog,
        columns=("payload",),
        filter="payload LIKE 'a%' AND id % 2 = 0",
        predicate=DorisPredicate("`id` > %s", (3,), "`id` > 3"),
    )
    sql, parameters = build_read_sql(options, None, bind=bind)
    table = "`db`.`t`" if catalog is None else "`hive`.`db`.`t`"
    trusted = "payload LIKE 'a%%' AND id %% 2 = 0" if bind else options.filter
    predicate = "`id` > %s" if bind else "`id` > 3"
    assert sql == f"SELECT `payload` FROM {table} WHERE ({trusted}) AND ({predicate})"
    assert parameters == ((3,) if bind else ())
    if catalog is not None:
        with pytest.raises(DorisConfigurationError, match="tablet"):
            build_read_sql(options, (1,), bind=bind)


def test_external_mysql_predicate_is_bound_on_the_qualified_query(monkeypatch):
    cursor = Mock()
    cursor.description = (("payload",),)
    cursor.fetchmany.side_effect = [[("a%",)], []]
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))
    options = config(columns=("payload",), predicate=DorisPredicate("`id` > %s", (3,), "`id` > 3"))
    schema = pa.schema([("payload", pa.string())])
    blocks = list(_readers.read_mysql(options, DorisInputSplit(None), schema))
    assert blocks[0].to_pylist() == [{"payload": "a%"}]
    cursor.execute.assert_called_once_with(
        "SELECT `payload` FROM `hive`.`db`.`t` WHERE `id` > %s", (3,)
    )
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


def test_external_flight_predicate_uses_the_qualified_literal_query(monkeypatch):
    reader = Mock()
    reader.__iter__ = Mock(return_value=iter([pa.record_batch({"payload": ["a%"]})]))
    cursor = Mock()
    cursor.fetch_record_batch.return_value = reader
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr(_readers, "_ensure_flight_available", Mock())
    monkeypatch.setattr(_readers, "_flight_connection", Mock(return_value=connection))
    options = config(
        columns=("payload",),
        predicate=DorisPredicate("`id` > %s", (3,), "`id` > 3"),
        transport="flight",
    )
    blocks = list(
        _readers.read_flight(options, DorisInputSplit(None), pa.schema([("payload", pa.string())]))
    )
    assert blocks[0].to_pylist() == [{"payload": "a%"}]
    cursor.execute.assert_called_once_with("SELECT `payload` FROM `hive`.`db`.`t` WHERE `id` > 3")
    reader.close.assert_called_once()
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


def test_external_predicate_and_projection_clones_preserve_source_identity(monkeypatch):
    source = DorisDatasource(host="fe", table="db.t", catalog="hive")
    if not source.supports_predicate_pushdown():
        assert source.config.catalog == "hive" and source.config.predicate is None
        return
    monkeypatch.setattr(
        DorisPlanner,
        "describe_schema",
        Mock(return_value=pa.schema([("id", pa.int32()), ("payload", pa.string())])),
    )
    expression = import_module("ray.data.expressions").col("id") > 3
    filtered = source.apply_predicate(expression)
    projected = filtered.apply_projection({"payload": "payload"})
    assert source.config.predicate is None and source.config.columns is None
    assert filtered.config.catalog == projected.config.catalog == "hive"
    assert projected.config.predicate is filtered.config.predicate
    assert projected.config.columns == ("payload",)
    sql, parameters = build_read_sql(projected.config, None, bind=True)
    assert sql.startswith("SELECT `payload` FROM `hive`.`db`.`t` WHERE ")
    assert "`id`" in sql and parameters == (3,)
    assert projected.apply_projection({}).config == projected.config


def test_external_metadata_reuses_structured_doris_types_and_closes(monkeypatch):
    cursor = Mock()
    cursor.fetchmany.side_effect = [
        [
            {"COLUMN_NAME": "id", "COLUMN_TYPE": "INT", "IS_NULLABLE": "YES"},
            {"COLUMN_NAME": "flag", "COLUMN_TYPE": "tinyint(1)", "IS_NULLABLE": "YES"},
            {"COLUMN_NAME": "payload", "COLUMN_TYPE": "STRING", "IS_NULLABLE": "YES"},
        ],
        [],
    ]
    connection = Mock()
    connection.cursor.return_value = cursor
    connect = Mock(return_value=connection)
    monkeypatch.setattr(_planner.pymysql, "connect", connect)
    assert DorisPlanner(config()).discover().schema == pa.schema(
        [
            ("id", pa.int32()),
            ("flag", pa.bool_()),
            ("payload", pa.string()),
        ]
    )
    cursor.execute.assert_called_once_with(build_external_columns_sql("hive"), ("db", "t"))
    assert "database" not in connect.call_args.kwargs
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.parametrize("denied", [False, True])
def test_empty_external_columns_check_visibility_and_close(monkeypatch, denied):
    cursor = Mock()
    cursor.fetchmany.return_value = []
    cursor.execute.side_effect = [
        None,
        pymysql.err.OperationalError(1142, "secret") if denied else None,
    ]
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr(_planner.pymysql, "connect", Mock(return_value=connection))
    with pytest.raises(DorisPermissionError if denied else DorisPlanningError) as error:
        DorisPlanner(config()).discover()
    assert "secret" not in str(error.value)
    assert cursor.execute.call_args.args == ("DESCRIBE `hive`.`db`.`t`",)
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [{"COLUMN_NAME": "id", "COLUMN_TYPE": "INT", "IS_NULLABLE": "unknown"}],
        [{"COLUMN_NAME": 1, "COLUMN_TYPE": "INT", "IS_NULLABLE": "YES"}],
        [None],
    ],
)
def test_external_structured_columns_reject_malformed_metadata(rows):
    with pytest.raises(DorisSchemaError):
        parse_information_schema_rows(rows)


def test_external_projection_can_exclude_an_unsupported_nested_column():
    columns = parse_information_schema_rows(
        [
            {"COLUMN_NAME": "id", "COLUMN_TYPE": "INT", "IS_NULLABLE": "YES"},
            {"COLUMN_NAME": "items", "COLUMN_TYPE": "ARRAY<INT>", "IS_NULLABLE": "YES"},
        ]
    )
    assert build_arrow_schema(columns, ("id",)) == pa.schema([("id", pa.int32())])
    with pytest.raises(DorisSchemaError, match="unsupported"):
        build_arrow_schema(columns)


@pytest.mark.parametrize(
    "code,message",
    [
        (1221, "secret"),
        (5087, "secret"),
        (1105, "Access denied for user 'u' to catalog 'hive'; secret"),
    ],
)
def test_external_catalog_denial_is_not_hidden(monkeypatch, code, message):
    monkeypatch.setattr(
        _planner.pymysql, "connect", Mock(side_effect=pymysql.err.OperationalError(code, message))
    )
    with pytest.raises(DorisPermissionError) as error:
        DorisPlanner(config()).discover()
    assert "secret" not in str(error.value)


def test_external_mysql_empty_read_emits_typed_schema_and_closes(monkeypatch):
    cursor = Mock()
    cursor.description = (("id",),)
    cursor.fetchmany.return_value = []
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr(_readers.pymysql, "connect", Mock(return_value=connection))
    schema = pa.schema([("id", pa.int32())])
    blocks = list(_readers.read_mysql(config(), DorisInputSplit(None), schema))
    assert len(blocks) == 1 and blocks[0].num_rows == 0 and blocks[0].schema == schema
    cursor.execute.assert_called_once_with("SELECT * FROM `hive`.`db`.`t`")
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


def test_external_flight_empty_read_emits_typed_schema_and_closes(monkeypatch):
    reader = Mock()
    reader.__iter__ = Mock(return_value=iter(()))
    cursor = Mock()
    cursor.fetch_record_batch.return_value = reader
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr(_readers, "_ensure_flight_available", Mock())
    monkeypatch.setattr(_readers, "_flight_connection", Mock(return_value=connection))
    schema = pa.schema([("id", pa.int32())])
    blocks = list(_readers.read_flight(config(transport="flight"), DorisInputSplit(None), schema))
    assert len(blocks) == 1 and blocks[0].schema == schema and blocks[0].num_rows == 0
    cursor.execute.assert_called_once_with("SELECT * FROM `hive`.`db`.`t`")
    reader.close.assert_called_once()
    cursor.close.assert_called_once()
    connection.close.assert_called_once()
