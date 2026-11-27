import json
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from ray_doris import read_doris
from ray_doris._query import DorisQueryDatasource

pytestmark = pytest.mark.integration


def test_query_join_aggregation_and_largeint(doris_config) -> None:
    table = f"`{doris_config.database}`.`{doris_config.table}`"
    dataset = read_doris(
        host=doris_config.host,
        mysql_port=doris_config.mysql_port,
        user=doris_config.user,
        password=doris_config.password,
        query=(
            f"SELECT a.category AS category, SUM(a.amount) AS total "
            f"FROM {table} a JOIN {table} b ON a.id = b.id "
            "WHERE a.id <= 3 AND a.category IS NOT NULL GROUP BY a.category"
        ),
        batch_size=1,
    )
    rows = sorted(dataset.take_all(), key=lambda row: row["category"])
    assert rows == [
        {"category": "alpha", "total": Decimal("1234.567890")},
        {"category": "beta", "total": Decimal("0.000001")},
    ]
    large = read_doris(
        host=doris_config.host,
        mysql_port=doris_config.mysql_port,
        user=doris_config.user,
        password=doris_config.password,
        query=f"SELECT large_value FROM {table} WHERE id = 1",
    )
    assert large.take_all() == [{"large_value": "170141183460469231731687303715884105727"}]
    typed = read_doris(
        host=doris_config.host,
        mysql_port=doris_config.mysql_port,
        user=doris_config.user,
        password=doris_config.password,
        query=f"SELECT large_value FROM {table} WHERE id = 1",
        schema=pa.schema([("large_value", pa.decimal256(39, 0))]),
    )
    assert typed.take_all() == [{"large_value": Decimal("170141183460469231731687303715884105727")}]
    assert typed.schema().base_schema.field("large_value").type == pa.decimal256(39, 0)


def test_query_empty_result_keeps_schema_and_one_task(doris_config) -> None:
    table = f"`{doris_config.database}`.`{doris_config.table}`"
    datasource = DorisQueryDatasource(
        query=f"WITH records AS (SELECT category, large_value FROM {table}) "
        "SELECT category, large_value FROM records WHERE 1 = 0",
        host=doris_config.host,
        mysql_port=doris_config.mysql_port,
        user=doris_config.user,
        password=doris_config.password,
        password_env=None,
        batch_size=1,
        connect_timeout=10,
        client_kwargs=None,
    )
    tasks = datasource.get_read_tasks(8)
    assert len(tasks) == 1
    blocks = list(tasks[0]())
    assert len(blocks) == 1 and blocks[0].num_rows == 0
    assert blocks[0].schema.field("category").type == pa.string()
    assert blocks[0].schema.field("large_value").type == pa.string()


def test_query_outer_join_preserves_nullable_output(doris_config) -> None:
    table = f"`{doris_config.database}`.`{doris_config.table}`"
    dataset = read_doris(
        host=doris_config.host,
        mysql_port=doris_config.mysql_port,
        user=doris_config.user,
        password=doris_config.password,
        query=(
            f"SELECT a.id AS left_id, b.id AS right_id FROM {table} a "
            f"LEFT JOIN {table} b ON a.id=b.id AND b.id < 0 WHERE a.id=1"
        ),
    )
    assert dataset.take_all() == [{"left_id": 1, "right_id": None}]
    assert dataset.schema().base_schema.field("right_id").nullable


def test_query_order_and_limit_are_applied_by_doris(doris_config) -> None:
    table = f"`{doris_config.database}`.`{doris_config.table}`"
    dataset = read_doris(
        host=doris_config.host,
        mysql_port=doris_config.mysql_port,
        user=doris_config.user,
        password=doris_config.password,
        query=f"SELECT id FROM {table} ORDER BY id DESC LIMIT 2",
        override_num_blocks=4,
        batch_size=1,
    )
    assert sorted(row["id"] for row in dataset.take_all()) == [3, 4]


def test_query_preserves_scalar_types(doris_config) -> None:
    table = f"`{doris_config.database}`.`{doris_config.table}`"
    dataset = read_doris(
        host=doris_config.host,
        mysql_port=doris_config.mysql_port,
        user=doris_config.user,
        password=doris_config.password,
        query=f"SELECT active, created_at, event_date, payload FROM {table} WHERE id = 1",
    )
    rows = dataset.take_all()
    assert len(rows) == 1
    assert json.loads(rows[0].pop("payload")) == {"source": "a"}
    assert rows == [
        {
            "active": True,
            "created_at": datetime(2025, 1, 1, 1, 2, 3, 123456),
            "event_date": date(2025, 1, 1),
        }
    ]


def test_query_preserves_nested_comments(doris_config) -> None:
    table = f"`{doris_config.database}`.`{doris_config.table}`"
    dataset = read_doris(
        host=doris_config.host,
        mysql_port=doris_config.mysql_port,
        user=doris_config.user,
        password=doris_config.password,
        query=f"SELECT id FROM {table} WHERE id = 1 /* outer /* inner */ end */",
    )
    assert dataset.take_all() == [{"id": 1}]
