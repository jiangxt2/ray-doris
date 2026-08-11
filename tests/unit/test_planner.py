import http.client
import io
import json
import ssl
import urllib.error
from unittest.mock import Mock

import pyarrow as pa
import pymysql
import pytest

from ray_doris import _planner
from ray_doris._errors import (
    DorisAuthenticationError,
    DorisConfigurationError,
    DorisPermissionError,
    DorisPlanningError,
)
from ray_doris._models import DorisInputSplit, DorisReadConfig
from ray_doris._planner import DorisPlanner, QueryPlanClient, group_tablets
from ray_doris._sql import parse_table


def make_config(**kwargs: object) -> DorisReadConfig:
    values = {"table": parse_table("db.table"), "host": "fe", "user": "reader"}
    values.update(kwargs)
    return DorisReadConfig.from_options(**values)


def success_payload(partitions: object) -> object:
    return {
        "code": 0,
        "msg": "success",
        "data": {
            "status": 200,
            "partitions": partitions,
            "opaqued_query_plan": "opaque",
        },
        "count": 0,
    }


def test_query_plan_response_uses_body_envelope_and_only_tablet_ids() -> None:
    payload = success_payload({"12": {"routings": ["be:8060"]}, "5": {}})
    assert QueryPlanClient._parse_response(payload) == (5, 12)
    assert QueryPlanClient._parse_response(success_payload({5: {}, "5": {}})) == (5,)
    assert QueryPlanClient._parse_response(success_payload({})) == ()


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self) -> bytes:
        return self._payload


def test_query_plan_http_client_sends_post_auth_scheme_and_timeout(monkeypatch) -> None:
    response = FakeResponse(json.dumps(success_payload({"9": {}})).encode("utf-8"))
    open_request = Mock(return_value=response)
    monkeypatch.setattr(_planner, "_open_query_plan_request", open_request)
    config = make_config(
        password="secret",
        http_scheme="https",
        connect_timeout=3.5,
    )
    assert QueryPlanClient(config).fetch_tablet_ids("SELECT * FROM `db`.`table`") == (9,)
    request = open_request.call_args.args[0]
    assert request.full_url.startswith("https://fe:8030/")
    assert request.method == "POST"
    assert request.get_header("Authorization") == "Basic cmVhZGVyOnNlY3JldA=="
    assert request.data == b'{"sql": "SELECT * FROM `db`.`table`"}'
    assert open_request.call_args.kwargs["timeout"] == 3.5


def test_query_plan_http_client_classifies_transport_and_invalid_json(monkeypatch) -> None:
    config = make_config()
    monkeypatch.setattr(
        _planner,
        "_open_query_plan_request",
        Mock(side_effect=urllib.error.HTTPError("url", 502, "bad gateway", {}, io.BytesIO())),
    )
    with pytest.raises(DorisPlanningError, match="HTTP 502"):
        QueryPlanClient(config).fetch_tablet_ids("SELECT 1")
    monkeypatch.setattr(
        _planner,
        "_open_query_plan_request",
        Mock(return_value=FakeResponse(b"not-json")),
    )
    with pytest.raises(DorisPlanningError, match="invalid JSON"):
        QueryPlanClient(config).fetch_tablet_ids("SELECT 1")
    monkeypatch.setattr(
        _planner,
        "_open_query_plan_request",
        Mock(side_effect=http.client.IncompleteRead(b"{")),
    )
    with pytest.raises(DorisPlanningError, match="unavailable"):
        QueryPlanClient(config).fetch_tablet_ids("SELECT 1")


@pytest.mark.parametrize(
    ("status", "error_type"),
    [(401, DorisAuthenticationError), (403, DorisPermissionError)],
)
def test_query_plan_http_auth_errors_never_become_planning_fallback(
    monkeypatch, status, error_type
) -> None:
    config = make_config(on_query_plan_error="single_task")
    monkeypatch.setattr(
        _planner,
        "_open_query_plan_request",
        Mock(side_effect=urllib.error.HTTPError("url", status, "denied", {}, io.BytesIO())),
    )
    with pytest.raises(error_type, match=rf"db\.table.*HTTP {status}"):
        QueryPlanClient(config).fetch_tablet_ids("SELECT 1")


def test_query_plan_timeout_has_table_context(monkeypatch) -> None:
    monkeypatch.setattr(
        _planner,
        "_open_query_plan_request",
        Mock(side_effect=TimeoutError("timed out")),
    )
    with pytest.raises(DorisPlanningError, match=r"unavailable.*db\.table"):
        QueryPlanClient(make_config()).fetch_tablet_ids("SELECT 1")


def test_query_plan_redirect_and_tls_fail_closed(monkeypatch) -> None:
    redirect = urllib.error.HTTPError(
        "url", 302, "redirect", {"Location": "https://other.example"}, io.BytesIO()
    )
    monkeypatch.setattr(
        _planner,
        "_open_query_plan_request",
        Mock(side_effect=redirect),
    )
    with pytest.raises(DorisConfigurationError, match="redirected HTTP 302"):
        QueryPlanClient(make_config()).fetch_tablet_ids("SELECT 1")

    tls_error = urllib.error.URLError(ssl.SSLCertVerificationError("certificate verify failed"))
    monkeypatch.setattr(
        _planner,
        "_open_query_plan_request",
        Mock(side_effect=tls_error),
    )
    with pytest.raises(DorisConfigurationError, match="TLS validation failed"):
        QueryPlanClient(make_config(http_scheme="https")).fetch_tablet_ids("SELECT 1")


def test_query_plan_redirect_handler_refuses_post_rewrite() -> None:
    handler = _planner._NoRedirectHandler()
    request = urllib.request.Request("http://fe", data=b"{}", method="POST")
    assert handler.redirect_request(request, None, 302, "redirect", {}, "https://fe") is None


def test_query_plan_plan_shape_rejection_uses_integer_inner_status() -> None:
    payload = {"code": 0, "data": {"status": 400, "exception": "unsupported plan"}}
    with pytest.raises(DorisPlanningError, match="body status 400"):
        QueryPlanClient._parse_response(payload)


def test_query_plan_service_error_and_permission_use_string_inner_status() -> None:
    with pytest.raises(DorisPermissionError, match="denied") as captured:
        QueryPlanClient._parse_response(
            {
                "code": 0,
                "data": {
                    "status": "1",
                    "exception": "Access denied; sensitive-server-detail",
                },
            }
        )
    assert "sensitive-server-detail" not in str(captured.value)
    with pytest.raises(DorisPlanningError, match="body status '1'"):
        QueryPlanClient._parse_response(
            {"code": 0, "data": {"status": "1", "exception": "planner crashed"}}
        )


def test_query_plan_authentication_uses_outer_code_without_status() -> None:
    with pytest.raises(DorisAuthenticationError, match="401"):
        QueryPlanClient._parse_response({"code": 401, "msg": "Unauthorized"})


def test_query_plan_outer_bad_request_code_is_not_http_permission_error() -> None:
    with pytest.raises(DorisPlanningError, match="body code 403") as captured:
        QueryPlanClient._parse_response({"code": 403, "msg": "bad request"})
    assert not isinstance(captured.value, DorisPermissionError)


def test_query_plan_server_messages_are_redacted_from_public_errors() -> None:
    sentinel = "sensitive-server-detail"
    with pytest.raises(DorisPlanningError, match="body code 500") as captured:
        QueryPlanClient._parse_response({"code": 500, "msg": sentinel})
    assert sentinel not in str(captured.value)

    with pytest.raises(DorisPlanningError, match="invalid body code") as captured:
        QueryPlanClient._parse_response({"code": sentinel})
    assert sentinel not in str(captured.value)

    with pytest.raises(DorisPlanningError, match="invalid body status") as captured:
        QueryPlanClient._parse_response({"code": 0, "data": {"status": sentinel}})
    assert sentinel not in str(captured.value)
    assert captured.value.__cause__ is None

    with pytest.raises(DorisPlanningError, match="body status 500") as captured:
        QueryPlanClient._parse_response({"code": 0, "data": {"status": 500, "exception": sentinel}})
    assert sentinel not in str(captured.value)


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"code": False, "data": {"status": 200, "partitions": {}}},
        {"code": 1, "msg": "table not found", "data": "missing"},
        {"code": 0, "data": []},
        {"code": 0, "data": {"status": "200", "partitions": {}}},
        {"code": 0, "data": {"status": 200, "partitions": []}},
        success_payload({"not-an-id": {}}),
    ],
)
def test_query_plan_rejects_malformed_envelopes(payload: object) -> None:
    with pytest.raises(DorisPlanningError):
        QueryPlanClient._parse_response(payload)


def test_tablet_grouping_is_deterministic_balanced_and_respects_target_count() -> None:
    assert group_tablets(range(1, 11), tablet_size=2, parallelism=3) == (
        DorisInputSplit((1, 2, 3, 4)),
        DorisInputSplit((5, 6, 7)),
        DorisInputSplit((8, 9, 10)),
    )
    assert group_tablets(range(1, 11), tablet_size=1, parallelism=6) == (
        DorisInputSplit((1, 2)),
        DorisInputSplit((3, 4)),
        DorisInputSplit((5, 6)),
        DorisInputSplit((7, 8)),
        DorisInputSplit((9,)),
        DorisInputSplit((10,)),
    )
    assert group_tablets((), tablet_size=1, parallelism=2) == ()


@pytest.mark.parametrize(
    ("tablet_ids", "tablet_size", "parallelism", "expected_count"),
    [
        ((1,), 1, 8, 1),
        ((1, 2, 3), 10, 8, 1),
        (tuple(range(1, 11)), 3, 8, 4),
        (tuple(range(1, 11)), 1, 20, 10),
    ],
)
def test_tablet_grouping_covers_every_tablet_once_without_empty_splits(
    tablet_ids, tablet_size, parallelism, expected_count
) -> None:
    splits = group_tablets(
        tablet_ids,
        tablet_size=tablet_size,
        parallelism=parallelism,
    )
    groups = [split.tablet_ids for split in splits]
    assert len(groups) == expected_count
    assert all(group for group in groups)
    assert tuple(tablet for group in groups for tablet in group) == tablet_ids
    sizes = [len(group) for group in groups]
    assert max(sizes) - min(sizes) <= 1


def test_planner_returns_empty_plan_for_successfully_pruned_empty_table(monkeypatch) -> None:
    config = make_config()
    planner = DorisPlanner(config)
    monkeypatch.setattr(planner, "_describe_schema", Mock(return_value=pa.schema([])))
    monkeypatch.setattr(QueryPlanClient, "fetch_tablet_ids", Mock(return_value=()))
    assert planner.plan(4).splits == ()


def test_planner_falls_back_to_single_task_only_for_planning_error(monkeypatch, caplog) -> None:
    config = make_config(on_query_plan_error="single_task")
    planner = DorisPlanner(config)
    monkeypatch.setattr(planner, "_describe_schema", Mock(return_value=pa.schema([])))
    monkeypatch.setattr(
        QueryPlanClient,
        "fetch_tablet_ids",
        Mock(side_effect=DorisPlanningError("sensitive-planning-detail")),
    )
    assert planner.plan(4).splits == (DorisInputSplit(None),)
    assert "using one unpartitioned task" in caplog.text
    assert "sensitive-planning-detail" not in caplog.text


def test_planner_does_not_fallback_for_permission_error(monkeypatch) -> None:
    config = make_config(on_query_plan_error="single_task")
    planner = DorisPlanner(config)
    monkeypatch.setattr(planner, "_describe_schema", Mock(return_value=pa.schema([])))
    monkeypatch.setattr(
        QueryPlanClient,
        "fetch_tablet_ids",
        Mock(side_effect=DorisPermissionError("Access denied")),
    )
    with pytest.raises(DorisPermissionError):
        planner.plan(4)


def test_planner_error_policy_propagates_planning_error(monkeypatch) -> None:
    config = make_config(on_query_plan_error="error")
    planner = DorisPlanner(config)
    monkeypatch.setattr(planner, "_describe_schema", Mock(return_value=pa.schema([])))
    monkeypatch.setattr(
        QueryPlanClient, "fetch_tablet_ids", Mock(side_effect=DorisPlanningError("rejected"))
    )
    with pytest.raises(DorisPlanningError, match="rejected"):
        planner.plan(4)


class DescribeCursor:
    def __init__(self) -> None:
        self.batches = [[("id", "BIGINT", "NO")], []]
        self.executed = None
        self.closed = False

    def execute(self, sql: str) -> None:
        self.executed = sql

    def fetchmany(self, size: int):
        assert size == 256
        return self.batches.pop(0)

    def close(self) -> None:
        self.closed = True


class DescribeConnection:
    def __init__(self, cursor: DescribeCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> DescribeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


def test_describe_schema_uses_managed_connection_and_fetchmany(monkeypatch) -> None:
    cursor = DescribeCursor()
    connection = DescribeConnection(cursor)
    connect = Mock(return_value=connection)
    monkeypatch.setattr(_planner.pymysql, "connect", connect)
    schema = DorisPlanner(
        make_config(
            connect_timeout=2.5,
            client_options={"read_timeout": 30},
        )
    )._describe_schema()
    assert schema == pa.schema([pa.field("id", pa.int64(), nullable=False)])
    assert cursor.executed == "DESCRIBE `db`.`table`"
    assert cursor.closed and connection.closed
    assert connect.call_args.kwargs["database"] == "db"
    assert connect.call_args.kwargs["connect_timeout"] == 2.5
    assert connect.call_args.kwargs["read_timeout"] == 30


def test_group_tablets_rejects_invalid_parallelism() -> None:
    for parallelism in (0, True):
        with pytest.raises(DorisConfigurationError, match="parallelism"):
            group_tablets((1,), tablet_size=1, parallelism=parallelism)


@pytest.mark.parametrize("tablet_size", [0, True])
def test_group_tablets_rejects_invalid_tablet_size(tablet_size) -> None:
    with pytest.raises(DorisConfigurationError, match="tablet_size"):
        group_tablets((1,), tablet_size=tablet_size, parallelism=1)


def test_planner_rejects_invalid_parallelism_before_fallback_or_network() -> None:
    planner = DorisPlanner(make_config(on_query_plan_error="single_task"))
    for parallelism in (0, True):
        with pytest.raises(DorisConfigurationError, match="parallelism"):
            planner.plan(parallelism)


@pytest.mark.parametrize(
    ("code", "error_type"),
    [
        (1045, DorisAuthenticationError),
        (1142, DorisPermissionError),
        (2003, DorisPlanningError),
    ],
)
def test_describe_schema_classifies_mysql_errors_with_table_context(
    monkeypatch, code, error_type
) -> None:
    monkeypatch.setattr(
        _planner.pymysql,
        "connect",
        Mock(side_effect=pymysql.err.OperationalError(code, "sensitive server detail")),
    )
    with pytest.raises(error_type, match=r"db\.table") as captured:
        DorisPlanner(make_config())._describe_schema()
    assert "sensitive server detail" not in str(captured.value)
    assert captured.value.__cause__ is None
