from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pymysql
import pytest
import ray

from ray_doris import DorisPermissionError, DorisTableCompatibilityError, write_doris

pytestmark = pytest.mark.integration


def _query(config: Any, sql: str) -> list[tuple[Any, ...]]:
    connection = pymysql.connect(
        host=config.host,
        port=config.mysql_port,
        user=config.user,
        password=config.password,
        database=config.database,
        autocommit=True,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return list(cursor.fetchall())
    finally:
        connection.close()


def _from_pydict(values: dict[str, list[Any]]) -> ray.data.Dataset:
    return ray.data.from_arrow(pa.table(values))


def test_duplicate_stream_load_round_trips_rows_and_reports_doris_statistics(doris_config) -> None:
    result = write_doris(
        _from_pydict(
            {
                "id": [101, 102],
                "category": ["write-a", "write-b"],
                "score": [10, 20],
                "payload": ['{"source":"a"}', '{"source":"b"}'],
            }
        ),
        connection=doris_config.write_connection(),
        table=doris_config.write_table("write_events"),
        batch_rows=1,
    )
    assert result.status == "success"
    assert result.batches == 2
    assert result.attempted_rows == 2
    assert result.loaded_rows == 2
    assert result.filtered_rows == 0
    assert result.ray_num_rows == 2
    assert _query(
        doris_config,
        "SELECT id, category, score FROM write_events WHERE id IN (101, 102) ORDER BY id",
    ) == [(101, "write-a", 10), (102, "write-b", 20)]


def test_unique_key_upsert_replaces_the_existing_row(doris_config) -> None:
    connection = doris_config.write_connection()
    table = doris_config.write_table("unique_events")
    write_doris(
        _from_pydict({"id": [201], "category": ["old"], "score": [1], "payload": ['{"v":"old"}']}),
        connection=connection,
        table=table,
        operation="upsert",
    )
    result = write_doris(
        _from_pydict({"id": [201], "category": ["new"], "score": [2], "payload": ['{"v":"new"}']}),
        connection=connection,
        table=table,
        operation="upsert",
    )
    assert result.loaded_rows == 1
    assert _query(
        doris_config,
        "SELECT id, category, score FROM unique_events WHERE id = 201",
    ) == [(201, "new", 2)]


def test_merge_on_write_partial_update_preserves_omitted_columns(doris_config) -> None:
    connection = doris_config.write_connection()
    table = doris_config.write_table("partial_events")
    write_doris(
        _from_pydict({"id": [301], "category": ["old"], "score": [10], "payload": ['{"v":"old"}']}),
        connection=connection,
        table=table,
        operation="upsert",
    )
    result = write_doris(
        _from_pydict({"id": [301], "category": ["new"]}),
        connection=connection,
        table=table,
        operation="partial_update",
    )
    assert result.loaded_rows == 1
    assert _query(
        doris_config,
        "SELECT id, category, score, payload FROM partial_events WHERE id = 301",
    ) == [(301, "new", 10, '{"v":"old"}')]


def test_aggregate_key_stream_load_keeps_doris_aggregation_semantics(doris_config) -> None:
    result = write_doris(
        _from_pydict({"id": [401, 401], "score": [2, 3]}),
        connection=doris_config.write_connection(),
        table=doris_config.write_table("aggregate_events"),
    )
    assert result.loaded_rows == 2
    assert _query(doris_config, "SELECT id, score FROM aggregate_events WHERE id = 401") == [
        (401, 5)
    ]


def test_supported_scalar_types_round_trip_through_parquet_stream_load(doris_config) -> None:
    table = pa.table(
        {
            "id": pa.array([501], type=pa.int64()),
            "category": pa.array(["typed"], type=pa.string()),
            "amount": pa.array([Decimal("42.123456")], type=pa.decimal128(20, 6)),
            "created_at": pa.array(
                [datetime(2026, 2, 3, 4, 5, 6, 123456)], type=pa.timestamp("us")
            ),
            "event_date": pa.array([date(2026, 2, 3)], type=pa.date32()),
            "active": pa.array([True], type=pa.bool_()),
            "large_value": pa.array(
                [Decimal("170141183460469231731687303715884105727")],
                type=pa.decimal256(39, 0),
            ),
            "payload": pa.array(['{"source":"typed"}'], type=pa.string()),
        }
    )
    result = write_doris(
        ray.data.from_arrow(table),
        connection=doris_config.write_connection(),
        table=doris_config.write_table("records"),
    )
    assert result.loaded_rows == 1
    row = _query(
        doris_config,
        "SELECT category, amount, created_at, event_date, active, large_value "
        "FROM records WHERE id = 501",
    )[0]
    assert row[0] == "typed"
    assert row[1] == Decimal("42.123456")
    assert row[2] == datetime(2026, 2, 3, 4, 5, 6, 123456)
    assert row[3] == date(2026, 2, 3)
    assert row[4] in (1, True)
    assert str(row[5]) == "170141183460469231731687303715884105727"


def test_write_metadata_permission_is_rejected_before_upload(doris_config) -> None:
    with pytest.raises(DorisPermissionError):
        write_doris(
            _from_pydict({"id": [601], "category": ["denied"], "score": [1], "payload": ["{}"]}),
            connection=doris_config.write_connection(
                user=doris_config.reader_user,
                password=doris_config.reader_password,
            ),
            table=doris_config.write_table("write_events"),
        )
    assert _query(doris_config, "SELECT id FROM write_events WHERE id = 601") == []


def test_invalid_write_schema_fails_before_stream_load(doris_config) -> None:
    with pytest.raises(DorisTableCompatibilityError):
        write_doris(
            _from_pydict(
                {"id": [701], "category": ["bad"], "score": ["not-an-int"], "payload": ["{}"]}
            ),
            connection=doris_config.write_connection(),
            table=doris_config.write_table("write_events"),
        )
    assert _query(doris_config, "SELECT id FROM write_events WHERE id = 701") == []


def test_empty_dataset_has_zero_batches_and_no_metadata_requirement(doris_config) -> None:
    result = write_doris(
        ray.data.range(0),
        connection=doris_config.write_connection(),
        table=doris_config.write_table("write_events"),
    )
    assert result.status == "success"
    assert result.batches == 0
    assert result.attempted_rows == 0
