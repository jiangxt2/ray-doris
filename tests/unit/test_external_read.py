"""External-table Doris reads reuse the canonical planner and streaming readers."""

from unittest.mock import Mock

import pyarrow as pa
import pymysql
import pytest

from ray_doris import (
    DorisConfigurationError,
    DorisPermissionError,
    DorisPlanningError,
    DorisSchemaError,
    _planner,
    _readers,
)
from ray_doris._models import DorisInputSplit, DorisReadConfig
from ray_doris._planner import DorisPlanner, QueryPlanClient, _mysql_connection_kwargs
from ray_doris._schema import build_arrow_schema, parse_information_schema_rows
from ray_doris._sql import (
    build_describe_sql,
    build_external_columns_sql,
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


def test_external_planning_uses_one_full_query_without_http_tablet_discovery(monkeypatch):
    schema = pa.schema([("id", pa.int32())])
    describe = Mock(return_value=schema)
    tablets = Mock(side_effect=AssertionError("tablet query-plan used"))
    monkeypatch.setattr(DorisPlanner, "_describe_schema", describe)
    monkeypatch.setattr(QueryPlanClient, "fetch_tablet_ids", tablets)
    planner = DorisPlanner(config())
    snapshot = planner.discover()
    assert snapshot.schema == schema and snapshot.tablet_ids is None
    assert planner.plan_from_snapshot(snapshot, 8).splits == (DorisInputSplit(None),)
    tablets.assert_not_called()


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
