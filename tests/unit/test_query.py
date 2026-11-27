"""Tests for direct trusted SQL reads."""

from decimal import Decimal
from unittest.mock import Mock

import pyarrow as pa
import pymysql
import pytest
from pymysql.constants import FIELD_TYPE

from ray_doris import (
    DorisAuthenticationError,
    DorisConfigurationError,
    DorisPermissionError,
    DorisReadError,
    DorisSchemaError,
    read_doris,
)
from ray_doris._query import DorisQueryDatasource, _protocol_schema, _query_error, _read_query


def _description(name: str, code: int, width: int = 255, scale: int = 0) -> tuple:
    return (name, code, None, width, width, scale, True)


def _query_source(
    query: str = "SELECT id FROM db.table",
    schema: pa.Schema | None = None,
) -> DorisQueryDatasource:
    return DorisQueryDatasource(
        query=query,
        host="fe",
        mysql_port=9030,
        user="reader",
        password="",
        password_env=None,
        batch_size=1,
        connect_timeout=10,
        client_kwargs=None,
        schema=schema,
    )


@pytest.mark.parametrize(
    "description,expected",
    [
        (_description("v", FIELD_TYPE.TINY, 1), pa.bool_()),
        (_description("v", FIELD_TYPE.TINY, 4), pa.int8()),
        (_description("v", FIELD_TYPE.SHORT), pa.int16()),
        (_description("v", FIELD_TYPE.LONG), pa.int32()),
        (_description("v", FIELD_TYPE.LONGLONG), pa.int64()),
        (_description("v", FIELD_TYPE.FLOAT), pa.float32()),
        (_description("v", FIELD_TYPE.DOUBLE), pa.float64()),
        (_description("v", FIELD_TYPE.DATE), pa.date32()),
        (_description("v", FIELD_TYPE.DATETIME), pa.timestamp("us")),
        (_description("v", FIELD_TYPE.NEWDECIMAL, 22, 6), pa.decimal128(20, 6)),
        (_description("v", FIELD_TYPE.NEWDECIMAL, 40, 6), pa.decimal128(38, 6)),
        (_description("v", FIELD_TYPE.NEWDECIMAL, 40, 0), pa.decimal256(39, 0)),
        (_description("v", FIELD_TYPE.NULL), pa.null()),
        (_description("v", FIELD_TYPE.STRING), pa.string()),
        (_description("v", FIELD_TYPE.BLOB), pa.string()),
        (_description("v", FIELD_TYPE.JSON), pa.string()),
    ],
)
def test_protocol_schema_uses_mysql_result_metadata(description: tuple, expected) -> None:
    schema = _protocol_schema([description])
    assert schema == pa.schema([pa.field("v", expected, nullable=True)])


@pytest.mark.parametrize(
    "description",
    [
        [],
        [("missing metadata",)],
        [_description("v", FIELD_TYPE.GEOMETRY)],
        [_description("v", FIELD_TYPE.NEWDECIMAL, 0, 6)],
        [_description("same", FIELD_TYPE.LONG), _description("same", FIELD_TYPE.LONG)],
    ],
)
def test_protocol_schema_rejects_unusable_metadata(description: list[tuple]) -> None:
    with pytest.raises(DorisSchemaError):
        _protocol_schema(description)


def test_query_public_entrypoint_ignores_block_hint(monkeypatch, caplog) -> None:
    read = Mock(return_value="dataset")
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", read)
    schema = pa.schema([("id", pa.int64())])
    assert (
        read_doris(
            host="fe",
            query="SELECT id FROM db.table",
            schema=schema,
            concurrency=2,
            override_num_blocks=4,
            ray_remote_args={"num_cpus": 0.5},
        )
        == "dataset"
    )
    source = read.call_args.args[0]
    assert isinstance(source, DorisQueryDatasource)
    assert source.get_read_tasks(8)[0].schema == schema
    assert read.call_args.kwargs == {
        "concurrency": 2,
        "override_num_blocks": 1,
        "ray_remote_args": {"num_cpus": 0.5},
    }
    assert "ignore override_num_blocks" in caplog.text


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"table": "db.table", "query": "SELECT 1"},
        {"query": "SELECT 1", "filter": "id=1"},
        {"query": "SELECT 1", "transport": "flight"},
        {"query": "SELECT 1", "auto_native_read": True},
        {"query": "SELECT 1", "schema": "not an Arrow schema"},
        {"query": "SELECT 1", "schema": pa.schema([])},
        {"table": "db.table", "schema": pa.schema([("id", pa.int64())])},
        {"query": "SELECT 1", "override_num_blocks": 0},
        {"query": "SELECT 1", "override_num_blocks": True},
    ],
)
def test_incompatible_source_options_fail_early(kwargs: dict) -> None:
    with pytest.raises(DorisConfigurationError):
        read_doris(host="fe", **kwargs)


@pytest.mark.parametrize("query", ["", "  ", None, 5])
def test_query_must_be_nonempty_string(query) -> None:
    with pytest.raises(DorisConfigurationError):
        _query_source(query)


def test_query_planning_creates_one_task_without_network(monkeypatch) -> None:
    connect = Mock(side_effect=AssertionError("planning must not connect"))
    monkeypatch.setattr("ray_doris._query._query_connection", connect)
    source = _query_source()
    tasks = source.get_read_tasks(8)
    assert len(tasks) == 1
    assert tasks[0].schema is None
    connect.assert_not_called()
    assert "SELECT id" not in repr(source._config)


def _mock_connection(monkeypatch, description, batches):
    cursor = Mock()
    cursor.description = description
    cursor.fetchmany.side_effect = batches
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr("ray_doris._query._query_connection", Mock(return_value=connection))
    cleanup = Mock()
    monkeypatch.setattr("ray_doris._query._cleanup_mysql_reader", cleanup)
    return connection, cursor, cleanup


@pytest.mark.parametrize(
    "query",
    [
        "  SELECT id FROM db.table; SELECT id FROM db.other;  ",
        "WITH t AS (SELECT 1) SELECT * FROM t",
        "SELECT 1 /* outer /* inner */ -- */; DELETE FROM db.table",
    ],
)
def test_query_is_executed_verbatim_once(monkeypatch, query: str) -> None:
    connection, cursor, cleanup = _mock_connection(
        monkeypatch, [_description("id", FIELD_TYPE.LONGLONG, 20)], [[(1,)], []]
    )
    blocks = list(_read_query(_query_source(query)._config))
    assert [block.to_pylist() for block in blocks] == [[{"id": 1}]]
    cursor.execute.assert_called_once_with(query)
    cleanup.assert_called_once_with(connection, cursor, reached_eof=True, context="query")


def test_default_schema_preserves_ambiguous_wire_values_as_text(monkeypatch) -> None:
    _, cursor, _ = _mock_connection(
        monkeypatch,
        [_description("name", FIELD_TYPE.STRING), _description("large", FIELD_TYPE.STRING)],
        [[("example", "170141183460469231731687303715884105727")], []],
    )
    blocks = list(_read_query(_query_source()._config))
    assert blocks[0].to_pylist() == [
        {"name": "example", "large": "170141183460469231731687303715884105727"}
    ]
    assert blocks[0].schema == pa.schema([("name", pa.string()), ("large", pa.string())])
    assert cursor.fetchmany.call_count == 2


def test_explicit_schema_converts_largeint_without_loss(monkeypatch) -> None:
    _mock_connection(
        monkeypatch,
        [_description("large", FIELD_TYPE.STRING)],
        [[("170141183460469231731687303715884105727",)], []],
    )
    schema = pa.schema([("large", pa.decimal256(39, 0))])
    block = list(_read_query(_query_source(schema=schema)._config))[0]
    assert block.schema == schema
    assert block.to_pylist() == [{"large": Decimal("170141183460469231731687303715884105727")}]


@pytest.mark.parametrize("explicit", [False, True])
def test_empty_result_preserves_schema(monkeypatch, explicit: bool) -> None:
    schema = pa.schema([("id", pa.int64())]) if explicit else None
    connection, cursor, cleanup = _mock_connection(
        monkeypatch, [_description("id", FIELD_TYPE.LONGLONG, 20)], [[]]
    )
    blocks = list(_read_query(_query_source(schema=schema)._config))
    assert blocks[0].schema == pa.schema([("id", pa.int64())])
    assert blocks[0].num_rows == 0
    cleanup.assert_called_once_with(connection, cursor, reached_eof=True, context="query")


def test_explicit_schema_column_mismatch_fails_before_fetch(monkeypatch) -> None:
    _, cursor, _ = _mock_connection(monkeypatch, [_description("other", FIELD_TYPE.LONG)], [])
    with pytest.raises(DorisReadError, match="do not match planned schema"):
        list(_read_query(_query_source(schema=pa.schema([("id", pa.int32())]))._config))
    cursor.fetchmany.assert_not_called()


def test_non_result_statement_fails_after_execution(monkeypatch) -> None:
    connection, cursor, cleanup = _mock_connection(monkeypatch, None, [])
    sql = "DELETE FROM db.table"
    with pytest.raises(DorisReadError, match="executed the SQL but returned no result set"):
        list(_read_query(_query_source(sql)._config))
    cursor.execute.assert_called_once_with(sql)
    cleanup.assert_called_once_with(connection, cursor, reached_eof=False, context="query")


def test_query_reader_cleans_up_on_early_close(monkeypatch) -> None:
    connection, cursor, cleanup = _mock_connection(
        monkeypatch, [_description("id", FIELD_TYPE.LONGLONG, 20)], [[(1,)]]
    )
    reader = _read_query(_query_source()._config)
    assert next(reader).to_pylist() == [{"id": 1}]
    reader.close()
    cleanup.assert_called_once_with(connection, cursor, reached_eof=False, context="query")


def test_query_reader_redacts_driver_error_and_cleans_up(monkeypatch) -> None:
    connection, cursor, cleanup = _mock_connection(monkeypatch, None, [])
    cursor.execute.side_effect = pymysql.err.OperationalError(1105, "credential sentinel")
    with pytest.raises(DorisReadError) as captured:
        list(_read_query(_query_source()._config))
    assert "credential sentinel" not in str(captured.value)
    assert captured.value.__cause__ is None
    cleanup.assert_called_once_with(connection, cursor, reached_eof=False, context="query")


@pytest.mark.parametrize(
    "code,error_type", [(1045, DorisAuthenticationError), (1142, DorisPermissionError)]
)
def test_query_access_errors_keep_categories(code, error_type) -> None:
    error = _query_error(pymysql.err.OperationalError(code, "private credential"))
    assert isinstance(error, error_type)
    assert "private credential" not in str(error)
