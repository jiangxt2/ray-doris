import pytest

from ray_doris._errors import DorisAuthenticationError, DorisPermissionError
from ray_doris._models import DorisReadConfig
from ray_doris._planner import QueryPlanClient
from ray_doris._sql import build_select_sql, parse_table

pytestmark = pytest.mark.integration


def test_query_plan_access_denied_body_is_permission_error(doris_config) -> None:
    config = DorisReadConfig.from_options(
        table=parse_table(f"{doris_config.database}.{doris_config.table}"),
        host=doris_config.host,
        mysql_port=doris_config.mysql_port,
        http_port=doris_config.http_port,
        flight_port=doris_config.flight_port,
        user="ray_doris_no_select",
        password="no-select-password",
    )
    sql = build_select_sql(config.table, None, None, None)
    with pytest.raises(DorisPermissionError, match="Access denied"):
        QueryPlanClient(config).fetch_tablet_ids(sql)


def test_query_plan_bad_password_uses_outer_401_and_never_falls_back(doris_config) -> None:
    config = DorisReadConfig.from_options(
        table=parse_table(f"{doris_config.database}.{doris_config.table}"),
        host=doris_config.host,
        mysql_port=doris_config.mysql_port,
        http_port=doris_config.http_port,
        flight_port=doris_config.flight_port,
        user=doris_config.user,
        password="definitely-wrong",
        on_query_plan_error="single_task",
    )
    sql = build_select_sql(config.table, None, None, None)
    with pytest.raises(DorisAuthenticationError, match="401"):
        QueryPlanClient(config).fetch_tablet_ids(sql)


def test_public_planner_bad_password_fails_during_schema_discovery(doris_config) -> None:
    from ray_doris import DorisDatasource

    datasource = DorisDatasource(
        **doris_config.reader_kwargs(password="definitely-wrong", on_query_plan_error="single_task")
    )
    with pytest.raises(DorisAuthenticationError, match=r"schema-discovery.*ray_doris_it\.records"):
        datasource.get_read_tasks(parallelism=4)
