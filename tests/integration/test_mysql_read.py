import gc
import traceback
from datetime import datetime
from decimal import Decimal

import pymysql
import pytest

from ray_doris import (
    DorisAuthenticationError,
    DorisConfigurationError,
    DorisDatasource,
    DorisPlanningError,
    read_doris,
)

pytestmark = pytest.mark.integration


def _assert_redacted_exception(exception: BaseException, caplog, sentinel: str) -> None:
    rendered_traceback = "".join(
        traceback.format_exception(type(exception), exception, exception.__traceback__)
    )
    assert sentinel not in str(exception)
    assert sentinel not in rendered_traceback
    assert sentinel not in caplog.text
    assert exception.__cause__ is None


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
    assert len(tasks) == 4
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
    assert dataset.schema() is not None
    assert dataset.schema().names == [
        "id",
        "category",
        "amount",
        "created_at",
        "event_date",
        "active",
        "large_value",
        "payload",
    ]


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


def test_environment_password_reads_on_driver_and_ray_workers(doris_config) -> None:
    rows = read_doris(
        **doris_config.minimal_env_reader_kwargs(
            columns=["id"],
            filter="id <= 2",
            on_query_plan_error="error",
        )
    ).take_all()
    assert sorted(row["id"] for row in rows) == [1, 2]


def test_missing_environment_password_fails_before_driver_network(
    doris_config, monkeypatch, caplog
) -> None:
    variable = "RAY_DORIS_IT_MISSING_PASSWORD_SENTINEL"
    monkeypatch.delenv(variable, raising=False)
    datasource = DorisDatasource(
        **doris_config.minimal_env_reader_kwargs(
            password_env=variable,
            on_query_plan_error="error",
        )
    )
    with pytest.raises(
        DorisConfigurationError, match="environment variable is unavailable"
    ) as captured:
        datasource.get_read_tasks(parallelism=1)
    _assert_redacted_exception(captured.value, caplog, variable)


def test_environment_password_is_resolved_again_when_split_starts(
    doris_config, monkeypatch, caplog
) -> None:
    variable = "RAY_DORIS_IT_RETRY_PASSWORD_SENTINEL"
    monkeypatch.setenv(variable, doris_config.reader_password)
    datasource = DorisDatasource(
        **doris_config.minimal_env_reader_kwargs(
            password_env=variable,
            columns=["id"],
            tablet_size=4,
            on_query_plan_error="error",
        )
    )
    task = datasource.get_read_tasks(parallelism=1)[0]
    monkeypatch.delenv(variable)
    with pytest.raises(
        DorisConfigurationError, match="environment variable is unavailable"
    ) as captured:
        list(task())
    _assert_redacted_exception(captured.value, caplog, variable)


def test_incorrect_environment_password_fails_closed(doris_config, monkeypatch, caplog) -> None:
    variable = "RAY_DORIS_IT_WRONG_PASSWORD"
    sentinel = "wrong-password-secret-sentinel"
    monkeypatch.setenv(variable, sentinel)
    datasource = DorisDatasource(
        **doris_config.minimal_env_reader_kwargs(
            password_env=variable,
            on_query_plan_error="error",
        )
    )
    with pytest.raises(DorisAuthenticationError, match="schema-discovery") as captured:
        datasource.get_read_tasks(parallelism=1)
    _assert_redacted_exception(captured.value, caplog, sentinel)


def test_mysql_consumer_close_does_not_drain_active_unbuffered_result(
    doris_config, monkeypatch
) -> None:
    finish_calls = 0
    original_finish = pymysql.connections.MySQLResult._finish_unbuffered_query

    def track_finish(result) -> None:
        nonlocal finish_calls
        finish_calls += 1
        original_finish(result)

    monkeypatch.setattr(
        pymysql.connections.MySQLResult,
        "_finish_unbuffered_query",
        track_finish,
    )
    datasource = DorisDatasource(
        **doris_config.reader_kwargs(columns=["id"], batch_size=1, tablet_size=4)
    )
    task = datasource.get_read_tasks(parallelism=1)[0]
    reader = iter(task())
    assert next(reader).num_rows == 1
    reader.close()
    del reader, task, datasource
    gc.collect()

    assert finish_calls == 0
