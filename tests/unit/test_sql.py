import pytest

from ray_doris._errors import DorisConfigurationError
from ray_doris._sql import (
    build_describe_sql,
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
