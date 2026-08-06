from datetime import datetime
from decimal import Decimal

import pytest

from ray_doris import DorisConfigurationError, DorisDatasource, DorisPlanningError, read_doris

pytestmark = pytest.mark.integration


def test_mysql_reads_projected_filtered_rows_in_bounded_batches(doris_config) -> None:
    dataset = read_doris(
        **doris_config.reader_kwargs(
            columns=["id", "category", "amount", "created_at", "large_value"],
            filter="id >= 2",
            batch_size=1,
            tablet_size=1,
        )
    )
    rows = sorted(dataset.take_all(), key=lambda row: row["id"])
    assert [row["id"] for row in rows] == [2, 3, 4]
    assert rows[0]["category"] is None
    assert rows[0]["amount"] is None
    assert rows[0]["created_at"] == datetime(2025, 1, 2, 1, 2, 3, 1)
    assert rows[0]["large_value"] == Decimal("-170141183460469231731687303715884105728")
    assert rows[1]["amount"] == Decimal("0.000001")


def test_mysql_parallel_plan_uses_multiple_tablet_tasks(doris_config) -> None:
    datasource = DorisDatasource(**doris_config.reader_kwargs(tablet_size=1))
    tasks = datasource.get_read_tasks(parallelism=8)
    assert 2 <= len(tasks) <= 4
    ids = sorted(row["id"] for task in tasks for table in task() for row in table.to_pylist())
    assert ids == [1, 2, 3, 4]


def test_mysql_task_emits_blocks_bounded_by_batch_size(doris_config) -> None:
    datasource = DorisDatasource(
        **doris_config.reader_kwargs(columns=["id"], batch_size=1, tablet_size=4)
    )
    tasks = datasource.get_read_tasks(parallelism=1)
    assert len(tasks) == 1
    blocks = list(tasks[0]())
    assert [block.num_rows for block in blocks] == [1, 1, 1, 1]
    assert sorted(row["id"] for block in blocks for row in block.to_pylist()) == [1, 2, 3, 4]


def test_empty_filter_is_valid_empty_result_not_full_table_fallback(doris_config) -> None:
    dataset = read_doris(**doris_config.reader_kwargs(filter="1 = 0"))
    assert dataset.take_all() == []


def test_complex_filter_falls_back_to_one_task_and_remains_correct(doris_config) -> None:
    predicate = (
        f"id IN (SELECT id FROM `{doris_config.database}`.`{doris_config.table}` WHERE id >= 3)"
    )
    datasource = DorisDatasource(
        **doris_config.reader_kwargs(filter=predicate, on_query_plan_error="single_task")
    )
    tasks = datasource.get_read_tasks(parallelism=8)
    assert len(tasks) == 1
    rows = [row for table in tasks[0]() for row in table.to_pylist()]
    assert sorted(row["id"] for row in rows) == [3, 4]


def test_complex_filter_error_policy_propagates_real_plan_rejection(doris_config) -> None:
    predicate = (
        f"id IN (SELECT id FROM `{doris_config.database}`.`{doris_config.table}` WHERE id >= 3)"
    )
    datasource = DorisDatasource(
        **doris_config.reader_kwargs(filter=predicate, on_query_plan_error="error")
    )
    with pytest.raises(DorisPlanningError, match=r"body status 400.*ray_doris_it\.records"):
        datasource.get_read_tasks(parallelism=8)


def test_external_catalog_reference_fails_before_network_access(doris_config) -> None:
    with pytest.raises(DorisConfigurationError, match="database.table"):
        read_doris(
            **doris_config.reader_kwargs(
                table=f"internal.{doris_config.database}.{doris_config.table}"
            )
        )


def test_actual_empty_table_returns_empty_dataset(doris_config) -> None:
    dataset = read_doris(
        **doris_config.reader_kwargs(table=f"{doris_config.database}.{doris_config.empty_table}")
    )
    assert dataset.take_all() == []


def test_minimum_select_privilege_reads_through_public_entrypoint(doris_config) -> None:
    rows = read_doris(
        **doris_config.minimal_reader_kwargs(columns=["id"], filter="id <= 2")
    ).take_all()
    assert sorted(row["id"] for row in rows) == [1, 2]
