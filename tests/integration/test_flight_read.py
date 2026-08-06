import json
from datetime import datetime
from decimal import Decimal

import pytest

from ray_doris import DorisDatasource, read_doris

pytestmark = pytest.mark.integration


def test_flight_reads_multiple_record_batches_with_canonical_schema(doris_config) -> None:
    datasource = DorisDatasource(
        **doris_config.reader_kwargs(
            transport="flight",
            columns=["id", "amount", "created_at", "large_value"],
            batch_size=1,
            tablet_size=4,
        )
    )
    tasks = datasource.get_read_tasks(parallelism=1)
    assert len(tasks) == 1
    batches = list(tasks[0]())
    assert len(batches) == 4
    assert all(batch.num_rows <= 1 for batch in batches)
    rows = sorted(
        (row for batch in batches for row in batch.to_pylist()), key=lambda row: row["id"]
    )
    assert len(rows) == 4
    assert rows[0]["amount"] == Decimal("1234.567890")
    assert rows[0]["created_at"] == datetime(2025, 1, 1, 1, 2, 3, 123456)
    assert rows[0]["large_value"] == Decimal("170141183460469231731687303715884105727")


def test_mysql_and_flight_datetimev2_microseconds_match(doris_config) -> None:
    mysql_rows = read_doris(
        **doris_config.reader_kwargs(columns=["id", "created_at"], transport="mysql")
    ).take_all()
    flight_rows = read_doris(
        **doris_config.reader_kwargs(columns=["id", "created_at"], transport="flight")
    ).take_all()
    assert sorted(mysql_rows, key=lambda row: row["id"]) == sorted(
        flight_rows, key=lambda row: row["id"]
    )


def test_flight_filter_and_remaining_scalar_types(doris_config) -> None:
    rows = read_doris(
        **doris_config.reader_kwargs(
            transport="flight",
            columns=["id", "event_date", "active", "payload"],
            filter="id >= 2",
        )
    ).take_all()
    rows = sorted(rows, key=lambda row: row["id"])
    assert [row["id"] for row in rows] == [2, 3, 4]
    assert rows[0]["event_date"].isoformat() == "2025-01-02"
    assert rows[0]["active"] is False
    assert rows[0]["payload"] is None
    assert rows[1]["event_date"] is None
    assert rows[1]["active"] is None
    assert json.loads(rows[1]["payload"]) == {"source": "b"}


def test_minimum_select_privilege_reads_through_flight(doris_config) -> None:
    rows = read_doris(
        **doris_config.minimal_reader_kwargs(transport="flight", columns=["id"], filter="id >= 3")
    ).take_all()
    assert sorted(row["id"] for row in rows) == [3, 4]
