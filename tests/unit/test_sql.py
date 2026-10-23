from dataclasses import replace

import pytest

from ray_doris._errors import DorisConfigurationError
from ray_doris._models import DorisPredicate, DorisReadConfig
from ray_doris._sql import (
    build_describe_sql,
    build_read_sql,
    build_select_sql,
    normalize_columns,
    normalize_filter,
    parse_table,
)


def test_parse_table_requires_exact_valid_two_part_name() -> None:
    assert parse_table("analytics.events").database == "analytics"
    for value in ("events", "internal.analytics.events", "a.bad-name", "a.`events`"):
        with pytest.raises(DorisConfigurationError):
            parse_table(value)


def test_columns_are_non_empty_unique_and_validated() -> None:
    assert normalize_columns(name for name in ["id", "event_time"]) == (
        "id",
        "event_time",
    )
    for columns in ([], "id", ["id", "id"], ["id; DROP TABLE x"]):
        with pytest.raises(DorisConfigurationError):
            normalize_columns(columns)


def test_filter_rejects_statement_and_comment_tokens() -> None:
    assert normalize_filter(" score >= 10 ") == "score >= 10"
    for predicate in (
        "",
        "id = 1; SELECT 2",
        "id = 1 --",
        "id = 1--2",
        "id = 1 # comment",
        "id /* comment */ = 1",
    ):
        with pytest.raises(DorisConfigurationError):
            normalize_filter(predicate)


def test_filter_allows_comment_like_tokens_inside_sql_literals() -> None:
    predicate = "message = 'a;--/*b*/#c' AND note = 'it''s ok'"
    assert normalize_filter(predicate) == predicate
    with pytest.raises(DorisConfigurationError):
        normalize_filter("message = 'unterminated")


def test_build_select_places_tablet_hint_before_where() -> None:
    table = parse_table("analytics.events")
    assert build_select_sql(table, ["id", "score"], "score > 80", [11, 12]) == (
        "SELECT `id`, `score` FROM `analytics`.`events` TABLET(11, 12) WHERE score > 80"
    )
    assert build_select_sql(table, None, None, None) == ("SELECT * FROM `analytics`.`events`")
    assert build_describe_sql(table) == "DESCRIBE `analytics`.`events`"


def test_build_select_rejects_empty_or_invalid_tablets() -> None:
    table = parse_table("analytics.events")
    for tablets in ([], [0], [-1], [True]):
        with pytest.raises(DorisConfigurationError):
            build_select_sql(table, None, None, tablets)


@pytest.mark.parametrize("budget", [None, 0, 1, 10000])
@pytest.mark.parametrize("bind", [False, True])
def test_read_limit_follows_complete_bound_predicate(budget, bind) -> None:
    config = DorisReadConfig.from_options(
        table=parse_table("db.records"),
        host="fe",
        user="reader",
        columns=("id",),
        filter="id % 2 = 0",
    )
    config = replace(config, predicate=DorisPredicate("`id` >= %s", (2,), "`id` >= 2"))
    sql, parameters = build_read_sql(config, (7,), bind=bind, row_limit=budget)
    expected_filter = "id %% 2 = 0" if bind else "id % 2 = 0"
    expected_native = "`id` >= %s" if bind else "`id` >= 2"
    expected = (
        f"SELECT `id` FROM `db`.`records` TABLET(7) "
        f"WHERE ({expected_filter}) AND ({expected_native})"
    )
    assert sql == expected + ("" if budget is None else f" LIMIT {budget}")
    assert parameters == ((2,) if bind else ())


@pytest.mark.parametrize("predicate", [None, "id > 0"])
def test_read_limit_without_native_predicate_and_single_query_fallback(predicate) -> None:
    config = DorisReadConfig.from_options(
        table=parse_table("db.records"), host="fe", user="reader", filter=predicate
    )
    base = build_select_sql(config.table, None, predicate, None)
    assert build_read_sql(config, None) == (base, ())
    assert build_read_sql(config, None, row_limit=3) == (base + " LIMIT 3", ())


@pytest.mark.parametrize("budget", [-1, True, 1.5, "1; DROP TABLE records"])
def test_read_limit_rejects_invalid_budget(budget) -> None:
    config = DorisReadConfig.from_options(table=parse_table("db.records"), host="fe", user="reader")
    with pytest.raises(DorisConfigurationError, match="per_task_row_limit"):
        build_read_sql(config, None, row_limit=budget)


def test_row_budget_above_doris_long_range_preserves_unbounded_sql() -> None:
    config = DorisReadConfig.from_options(table=parse_table("db.records"), host="fe", user="reader")
    base = build_read_sql(config, None)
    assert build_read_sql(config, None, row_limit=1 << 63) == base
    assert build_read_sql(config, None, row_limit=(1 << 63) - 1)[0].endswith(
        " LIMIT 9223372036854775807"
    )
