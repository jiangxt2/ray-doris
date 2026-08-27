from __future__ import annotations

import base64
import json
import math
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, cast
from unittest.mock import Mock

import pandas as pd
import pyarrow as pa
import pytest
from ray.data.datasource import WriteResult

from ray_doris import _compat
from ray_doris._compat import datasink_compatibility, prepare_write_remote_args
from ray_doris._errors import (
    DorisAmbiguousWriteError,
    DorisConfigurationError,
    DorisLabelExistsError,
    DorisMetadataError,
    DorisTableCompatibilityError,
    DorisWriteError,
)
from ray_doris._schema import DorisColumn
from ray_doris.write.api import write_doris
from ray_doris.write.connection import DorisConnection, DorisTable
from ray_doris.write.datasink import DorisDatasink
from ray_doris.write.metadata import (
    DorisTableMetadata,
    parse_create_table,
    validate_write_table,
)
from ray_doris.write.options import DorisWriteOptions
from ray_doris.write.serialization import (
    iter_serialized_batches,
    serialize_json,
    serialize_parquet,
)
from ray_doris.write.stream_load import (
    DorisLoadResult,
    StreamLoadClient,
    _validate_redirect_target,
)


def _metadata(
    model: str = "DUPLICATE",
    *,
    merge_on_write: bool = False,
    columns: tuple[str, ...] = ("id", "value"),
    nullable: bool = True,
) -> DorisTableMetadata:
    return DorisTableMetadata(
        model,
        ("id",),
        merge_on_write,
        columns,
        (
            DorisColumn("id", "BIGINT", False),
            DorisColumn("value", "VARCHAR(32)", nullable),
        ),
    )


def _connection(**kwargs: object) -> DorisConnection:
    return DorisConnection(host="fe.example", **kwargs)


def test_connection_is_immutable_and_redacts_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DORIS_WRITE_PASSWORD", "password-secret")
    connection = _connection(password_env="DORIS_WRITE_PASSWORD")
    assert connection.resolve_password() == "password-secret"
    assert "password-secret" not in repr(connection)
    assert "DORIS_WRITE_PASSWORD" not in repr(connection)
    assert connection.endpoint(DorisTable("analytics", "events")) == (
        "http://fe.example:8030/api/analytics/events/_stream_load"
    )
    with pytest.raises((AttributeError, TypeError)):
        cast(Any, connection).redirect_hosts = connection.redirect_hosts + ("be.example",)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"host": ""},
        {"host": "fe.example/path"},
        {"host": "[::1]"},
        {"http_port": 0},
        {"redirect_ports": (8040, 65536)},
        {"password": "literal", "password_env": "DORIS_PASSWORD"},
        {"password_env": "not-a-name"},
        {"verify_tls": False},
    ],
)
def test_connection_rejects_unsafe_configuration(kwargs: dict[str, Any]) -> None:
    values: dict[str, Any] = {"host": "fe.example"}
    values.update(kwargs)
    with pytest.raises(DorisConfigurationError):
        DorisConnection(**values)


@pytest.mark.parametrize("name", ["connect_timeout_seconds", "request_timeout_seconds"])
@pytest.mark.parametrize(
    "value",
    [True, "30", 0, -1, math.nan, math.inf, -math.inf, 31_536_001],
)
def test_connection_rejects_invalid_timeout(name: str, value: object) -> None:
    with pytest.raises(
        DorisConfigurationError,
        match=rf"{name} must be finite, positive, and at most 31536000 seconds",
    ):
        _connection(**{name: value})


@pytest.mark.parametrize("name", ["connect_timeout_seconds", "request_timeout_seconds"])
def test_connection_accepts_timeout_upper_bound(name: str) -> None:
    assert getattr(_connection(**{name: 31_536_000}), name) == 31_536_000


def test_ipv6_endpoint_is_rendered_with_brackets() -> None:
    assert DorisConnection(host="::1").endpoint(DorisTable("db", "table")) == (
        "http://[::1]:8030/api/db/table/_stream_load"
    )


def test_connection_builds_verified_tls_metadata_options_without_repr_paths() -> None:
    connection = DorisConnection(
        host="fe.example",
        http_secure=True,
        http_ca_file="/secrets/http-ca.pem",
        mysql_ca_file="/secrets/mysql-ca.pem",
    )
    assert connection.mysql_kwargs(DorisTable("db", "table"))["ssl"] == {
        "ca": "/secrets/mysql-ca.pem",
        "check_hostname": True,
    }
    rendered = repr(connection)
    assert "/secrets/" not in rendered
    assert "<configured>" in rendered


def test_options_enforce_operation_format_and_fixed_label_contract() -> None:
    assert DorisWriteOptions(operation="partial_update").format == "json"
    label = DorisWriteOptions().label()
    assert label.startswith("ray_doris_")
    assert len(label) == 42
    assert label.isascii()
    with pytest.raises(DorisConfigurationError):
        DorisWriteOptions(operation="partial_update", format="parquet")
    with pytest.raises(DorisConfigurationError):
        DorisWriteOptions(operation="load", format="json")
    with pytest.raises(DorisConfigurationError):
        DorisWriteOptions(load_properties=(("label", "caller-label"),))
    with pytest.raises(DorisConfigurationError):
        DorisWriteOptions(load_properties=(("timezone", "UTC"), ("timezone", "PST")))


def test_ray_datasink_compatibility_matrix_is_explicit() -> None:
    old = datasink_compatibility("2.52.4")
    new = datasink_compatibility("2.53.0")
    assert not old.on_write_start_has_schema
    assert old.on_write_start_on_empty_dataset
    assert old.schema_source == "worker_blocks"
    assert new.on_write_start_has_schema
    assert not new.on_write_start_on_empty_dataset
    assert new.schema_source == "first_input_bundle"


def test_datasink_rejects_unsupported_ray_before_empty_write_can_skip_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_compat, "_RAY_VERSION", "2.59.0")
    with pytest.raises(DorisConfigurationError, match=r">=2\.49\.2,<2\.59"):
        DorisDatasink(_connection(), DorisTable("db", "table"))


def test_retry_policy_is_fail_closed_and_copies_mapping() -> None:
    original = {"max_retries": 0, "num_cpus": 1}
    prepared = prepare_write_remote_args(original)
    original["num_cpus"] = 99
    assert prepared == {"max_retries": 0, "num_cpus": 1}
    with pytest.raises(DorisConfigurationError):
        prepare_write_remote_args({"max_retries": 1})
    with pytest.raises(DorisConfigurationError):
        prepare_write_remote_args({"max_task_retries": 1})
    with pytest.raises(DorisConfigurationError):
        prepare_write_remote_args({"retry_exceptions": True})


def test_write_facade_fails_closed_without_driver_result() -> None:
    dataset = Mock()
    dataset.write_datasink.return_value = None
    with pytest.raises(DorisWriteError, match="without a driver write result"):
        write_doris(
            dataset,
            connection=_connection(),
            table=DorisTable("db", "table"),
        )
    assert dataset.write_datasink.call_args.kwargs["ray_remote_args"] == {"max_retries": 0}


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            "CREATE TABLE `events` (id BIGINT) UNIQUE KEY(`id`) "
            'PROPERTIES ("enable_unique_key_merge_on_write"="true")',
            ("UNIQUE", ("id",), True),
        ),
        ("CREATE TABLE x (id BIGINT) DUPLICATE KEY(`id`)", ("DUPLICATE", ("id",), False)),
        ("CREATE TABLE x (a INT) AGGREGATE KEY(`a`)", ("AGGREGATE", ("a",), False)),
    ],
)
def test_parse_create_table_models_and_merge_on_write(
    sql: str, expected: tuple[str, tuple[str, ...], bool]
) -> None:
    metadata = parse_create_table(sql)
    assert (metadata.model, metadata.key_columns, metadata.merge_on_write) == expected


def test_parse_create_table_handles_escaped_key_identifiers_and_rejects_malformed_sql() -> None:
    metadata = parse_create_table("CREATE TABLE x (x INT) UNIQUE KEY(`id``part`, `value,part`)")
    assert metadata.key_columns == ("id`part", "value,part")
    with pytest.raises(DorisMetadataError):
        parse_create_table("CREATE TABLE x (x INT) UNIQUE KEY(`id)")
    with pytest.raises(DorisMetadataError):
        parse_create_table("CREATE TABLE x (x INT)")


def test_validate_write_table_enforces_models_columns_nullability_and_safe_cast() -> None:
    table = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "value": pa.array(["ok"]),
        }
    )
    prepared = validate_write_table(_metadata(), operation="load", arrow_table=table)
    assert prepared is not None
    assert prepared.schema.field("id").type == pa.int64()
    with pytest.raises(DorisTableCompatibilityError, match="Unique Key"):
        validate_write_table(_metadata(), operation="upsert", arrow_table=table)
    with pytest.raises(DorisTableCompatibilityError, match="NULL"):
        validate_write_table(
            _metadata(nullable=False),
            operation="load",
            arrow_table=pa.table({"id": [1], "value": pa.array([None], type=pa.string())}),
        )
    with pytest.raises(DorisTableCompatibilityError, match="columns in order"):
        validate_write_table(
            _metadata(),
            operation="load",
            arrow_table=pa.table({"value": ["ok"], "id": [1]}),
        )


def test_validate_write_table_supports_only_mow_partial_update_and_rejects_timezone() -> None:
    metadata = _metadata("UNIQUE", merge_on_write=True)
    partial = validate_write_table(
        metadata,
        operation="partial_update",
        arrow_table=pa.table({"id": [1], "value": ["new"]}),
    )
    assert partial is not None
    with pytest.raises(DorisTableCompatibilityError, match="Merge-on-Write"):
        validate_write_table(_metadata("UNIQUE"), operation="partial_update", arrow_table=partial)
    with pytest.raises(DorisTableCompatibilityError, match="timezone-aware"):
        validate_write_table(
            metadata,
            operation="partial_update",
            arrow_table=pa.table(
                {
                    "id": [1],
                    "value": pa.array(
                        [datetime(2026, 1, 1, tzinfo=timezone.utc)],
                        type=pa.timestamp("us", tz="UTC"),
                    ),
                }
            ),
        )


def test_serialization_supports_json_temporals_and_batches_by_rows_and_bytes() -> None:
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "document": pa.array([{"ok": True}, None, {"value": 2}]),
            "amount": pa.array([Decimal("1.20"), Decimal("2.30"), Decimal("3.40")]),
        }
    )
    raw = serialize_json(table)
    assert len(raw.splitlines()) == 3
    assert json.loads(raw.splitlines()[0])["document"]["ok"] is True
    assert json.loads(serialize_json(pa.table({"value": [b"bytes"]}))) == {"value": "Ynl0ZXM="}
    batches = list(iter_serialized_batches(table, format="json", max_rows=2, max_bytes=1000))
    assert [batch.rows for batch in batches] == [2, 1]
    parquet = serialize_parquet(table.select(["id", "amount"]))
    assert parquet[:4] == b"PAR1"


def test_serialization_emits_oversized_single_row_and_rejects_nonfinite_values() -> None:
    table = pa.table({"id": [1], "text": ["x" * 100]})
    batches = list(iter_serialized_batches(table, format="json", max_rows=10, max_bytes=1))
    assert len(batches) == 1 and batches[0].rows == 1
    with pytest.raises(DorisTableCompatibilityError, match="non-finite"):
        serialize_parquet(pa.table({"value": pa.array([float("nan")])}))
    with pytest.raises(DorisTableCompatibilityError, match="timezone-aware"):
        serialize_json(
            pa.table(
                {
                    "value": pa.array(
                        [datetime(2026, 1, 1, tzinfo=timezone.utc)],
                        type=pa.timestamp("us", tz="UTC"),
                    )
                }
            )
        )


def test_redirect_validation_drops_userinfo_and_requires_allowlist() -> None:
    connection = _connection(redirect_hosts=("be.example",), redirect_ports=(8040,))
    assert (
        _validate_redirect_target(
            connection, "http://root:secret@be.example:8040/api/db/table/_stream_load"
        )
        == "http://be.example:8040/api/db/table/_stream_load"
    )
    with pytest.raises(DorisWriteError, match="allowlisted"):
        _validate_redirect_target(connection, "http://other.example:8040/load")
    with pytest.raises(DorisWriteError, match="allowlisted"):
        _validate_redirect_target(connection, "http://be.example:8050/load")
    with pytest.raises(DorisWriteError, match="HTTP"):
        _validate_redirect_target(
            _connection(http_secure=True, redirect_hosts=("be.example",), redirect_ports=(8040,)),
            "http://be.example:8040/load",
        )


def test_stream_load_headers_are_safe_and_partial_columns_are_quoted() -> None:
    client = StreamLoadClient(
        _connection(password="password-secret", redirect_policy="public"),
        DorisTable("analytics", "events"),
        DorisWriteOptions(operation="partial_update"),
    )
    headers = client._headers("ray_doris_label", ("id,part", "tick`column"))
    assert headers["format"] == "json"
    assert headers["partial_columns"] == "true"
    assert headers["columns"] == "`id,part`,`tick``column`"
    assert headers["redirect-policy"] == "public"
    assert base64.b64decode(headers["Authorization"].split()[1]).endswith(b":password-secret")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            b'{"Status":"Success","NumberLoadedRows":2,"NumberFilteredRows":0,"NumberTotalRows":2}',
            "Success",
        ),
        (
            b'{"status":"publish_timeout","NumberLoadedRows":"1","NumberFilteredRows":"0","NumberTotalRows":"1"}',
            "Publish Timeout",
        ),
    ],
)
def test_stream_load_response_parser_normalizes_successes(raw: bytes, expected: str) -> None:
    result = StreamLoadClient._parse_response(raw, rows=2 if expected == "Success" else 1)
    assert result.status == expected


def test_stream_load_response_parser_fails_closed_on_labels_counts_and_payloads() -> None:
    with pytest.raises(DorisLabelExistsError):
        StreamLoadClient._parse_response(b'{"Status":"Label Already Exists"}', rows=1)
    with pytest.raises(DorisWriteError, match="non-JSON"):
        StreamLoadClient._parse_response(b"payload-secret", rows=1)
    with pytest.raises(DorisWriteError, match="response field"):
        StreamLoadClient._parse_response(b'{"Status":"Success","NumberLoadedRows":1}', rows=1)
    with pytest.raises(DorisWriteError, match="failed"):
        StreamLoadClient._parse_response(b'{"Status":"Fail"}', rows=1)


def test_stream_load_pre_send_transport_failure_is_known(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingOpener:
        def open(self, request: object, **kwargs: object) -> object:
            del request, kwargs
            raise urllib.error.URLError("secret payload")

    monkeypatch.setattr(urllib.request, "build_opener", lambda *handlers: FailingOpener())
    with pytest.raises(DorisWriteError, match="before request body"):
        StreamLoadClient(_connection(), DorisTable("db", "table"), DorisWriteOptions()).load(
            b"payload", rows=1, columns=("id",)
        )


def test_stream_load_post_send_transport_failure_is_ambiguous_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingOpener:
        def open(self, request: Any, **kwargs: object) -> object:
            del kwargs
            request.transmission.body_started = True
            raise urllib.error.URLError("password-secret payload-secret")

    monkeypatch.setattr(urllib.request, "build_opener", lambda *handlers: FailingOpener())
    with pytest.raises(DorisAmbiguousWriteError, match="status is unknown") as captured:
        StreamLoadClient(
            _connection(password="password-secret"),
            DorisTable("db", "table"),
            DorisWriteOptions(),
        ).load(b"payload-secret", rows=1, columns=("id",))
    assert "password-secret" not in str(captured.value)
    assert "payload-secret" not in str(captured.value)


def test_stream_load_response_is_closed_and_cleanup_error_does_not_mask_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Response:
        closed = False

        def read(self, limit: int) -> bytes:
            assert limit > 0
            return (
                b'{"Status":"Success","NumberLoadedRows":1,'
                b'"NumberFilteredRows":0,"NumberTotalRows":1}'
            )

        def close(self) -> None:
            self.closed = True
            raise OSError("close-secret")

    response = Response()

    class Opener:
        def open(self, request: object, **kwargs: object) -> Response:
            del request, kwargs
            return response

    monkeypatch.setattr(urllib.request, "build_opener", lambda *handlers: Opener())
    result = StreamLoadClient(_connection(), DorisTable("db", "table"), DorisWriteOptions()).load(
        b"payload", rows=1, columns=("id",)
    )
    assert result.status == "Success"
    assert response.closed


def test_datasink_aggregates_write_returns_and_keeps_ray_accounting_distinct(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = DorisDatasink(_connection(), DorisTable("db", "table"))
    sink.on_write_complete(
        WriteResult(
            num_rows=99,
            size_bytes=1000,
            write_returns=[
                {
                    "status": "success",
                    "request_statuses": ("Success",),
                    "batches": 1,
                    "attempted_rows": 2,
                    "loaded_rows": 1,
                    "filtered_rows": 1,
                    "uploaded_bytes": 20,
                },
                {
                    "status": "publish_timeout",
                    "request_statuses": ("Publish Timeout",),
                    "batches": 1,
                    "attempted_rows": 3,
                    "loaded_rows": 3,
                    "filtered_rows": 0,
                    "uploaded_bytes": 30,
                },
            ],
        )
    )
    assert sink.result is not None
    assert sink.result.status == "mixed"
    assert sink.result.attempted_rows == 5
    assert sink.result.loaded_rows == 4
    assert sink.result.filtered_rows == 1
    assert sink.result.ray_num_rows == 99
    assert sink.result.ray_size_bytes == 1000
    assert sink.min_rows_per_write == DorisWriteOptions().batch_rows


def test_datasink_write_uses_one_summary_and_batches_by_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = DorisDatasink(
        _connection(),
        DorisTable("db", "table"),
        DorisWriteOptions(batch_rows=1, batch_bytes=1 << 20),
    )
    cast(Any, sink)._metadata = _metadata()
    calls: list[int] = []

    def fake_load(self: StreamLoadClient, payload: bytes, *, rows: int, columns: tuple[str, ...]):
        del self, payload, columns
        calls.append(rows)
        return DorisLoadResult("Success", rows, 0, rows)

    monkeypatch.setattr(StreamLoadClient, "load", fake_load)
    summary = sink.write(
        iter([pa.table({"id": [1, 2], "value": ["a", "b"]})]),
        object(),
    )
    assert calls == [1, 1]
    assert summary["batches"] == 2
    assert summary["attempted_rows"] == 2
    assert summary["loaded_rows"] == 2


def test_datasink_write_accepts_pandas_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    sink = DorisDatasink(_connection(), DorisTable("db", "table"))
    cast(Any, sink)._metadata = _metadata()

    def fake_load(self: StreamLoadClient, payload: bytes, *, rows: int, columns: tuple[str, ...]):
        del self, payload, columns
        return DorisLoadResult("Success", rows, 0, rows)

    monkeypatch.setattr(StreamLoadClient, "load", fake_load)
    summary = sink.write(
        iter([pd.DataFrame({"id": [1], "value": ["pandas"]})]),
        object(),
    )
    assert summary["attempted_rows"] == 1
    assert summary["loaded_rows"] == 1


def test_datasink_rejects_malformed_write_return() -> None:
    sink = DorisDatasink(_connection(), DorisTable("db", "table"))
    with pytest.raises(DorisWriteError, match="invalid task summary"):
        sink.on_write_complete(
            WriteResult(
                num_rows=1,
                size_bytes=1,
                write_returns=[
                    {
                        "request_statuses": "Success",
                        "batches": 1,
                        "attempted_rows": 1,
                        "loaded_rows": 1,
                        "filtered_rows": 0,
                        "uploaded_bytes": 1,
                    }
                ],
            )
        )


def test_datasink_on_write_start_validates_schema_before_worker_side_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = DorisDatasink(_connection(), DorisTable("db", "table"))
    monkeypatch.setattr(
        "ray_doris.write.datasink.discover_table_metadata", lambda connection, table: _metadata()
    )
    with pytest.raises(DorisTableCompatibilityError, match="key columns"):
        sink.on_write_start(pa.schema([("value", pa.string())]))
    assert cast(Any, sink)._metadata is None
