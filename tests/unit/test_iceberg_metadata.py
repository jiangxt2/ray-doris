from types import SimpleNamespace
from unittest.mock import Mock

import pymysql
import pytest

from ray_doris._errors import DorisPlanningError, _NativeReadNotApplicableError
from ray_doris._iceberg_metadata import (
    doris_main_snapshot,
    iceberg_catalog_defaults,
    rest_catalog_properties,
    validate_iceberg_catalog_defaults,
    verify_doris_policy,
    verify_legacy_snapshot_identity,
    verify_table_identity,
)
from ray_doris._sql import parse_table


@pytest.mark.parametrize("key", ["uri", "iceberg.rest.uri"])
def test_rest_catalog_properties_accepts_versioned_uri_names(key):
    assert rest_catalog_properties(
        {"iceberg.catalog.type": "REST", key: "https://catalog/iceberg", "warehouse": "prod"}
    ) == {"type": "rest", "uri": "https://catalog/iceberg", "warehouse": "prod"}


@pytest.mark.parametrize(
    "uri", ["https://user:secret@catalog", "file:///tmp", "http://x:bad", "https://x/?a=b"]
)
def test_rest_uri_rejects_malformed_or_secret_bearing_values(uri):
    with pytest.raises(DorisPlanningError):
        rest_catalog_properties({"iceberg.catalog.type": "rest", "uri": uri})


def test_rest_metadata_distinguishes_nonapplicability_and_contradiction():
    with pytest.raises(_NativeReadNotApplicableError):
        rest_catalog_properties({"iceberg.catalog.type": "hms", "uri": "https://a"})
    with pytest.raises(_NativeReadNotApplicableError):
        rest_catalog_properties({"iceberg.catalog.type": "rest"})
    with pytest.raises(DorisPlanningError):
        rest_catalog_properties(
            {"iceberg.catalog.type": "rest", "uri": "https://a", "iceberg.rest.uri": "https://b"}
        )


def test_nonsecret_defaults_use_aliases_without_copying_credentials():
    properties = {
        "AWS_ENDPOINT": "https://s3.example",
        "s3.endpoint": "https://s3.example",
        "AWS_REGION": "region-1",
        "iceberg.rest.oauth2.server-uri": "https://auth.example/token",
        "iceberg.rest.oauth2.scope": "catalog:read",
        "iceberg.rest.oauth2.credential": "doris-secret",
        "s3.secret_key": "storage-secret",
        "use_path_style": "true",
    }
    defaults = iceberg_catalog_defaults(properties, {"credential": "ray-secret"})
    assert defaults == {
        "s3.endpoint": "https://s3.example",
        "s3.region": "region-1",
        "oauth2-server-uri": "https://auth.example/token",
        "scope": "catalog:read",
    }
    assert all(
        secret not in str(defaults) for secret in ("doris-secret", "storage-secret", "ray-secret")
    )


def test_nonsecret_defaults_preserve_explicit_settings_and_nested_oauth():
    properties = {
        "s3.endpoint": "https://different.example",
        "AWS_ENDPOINT": "https://also-different.example",
        "iceberg.rest.oauth2.server-uri": "https://auth.example/token",
        "iceberg.rest.oauth2.scope": "catalog:read",
    }
    configured = {
        "s3.endpoint": "https://explicit.example",
        "auth": {
            "type": "oauth2",
            "oauth2": {"token_url": "https://explicit.example/token", "client_secret": "secret"},
        },
    }
    defaults = iceberg_catalog_defaults(properties, configured)
    assert defaults == {"auth": {"type": "oauth2", "oauth2": {"scope": "catalog:read"}}}
    assert "secret" not in str(defaults)


@pytest.mark.parametrize(
    "properties,configured",
    [
        ({"s3.endpoint": "https://a", "AWS_ENDPOINT": "https://b"}, {}),
        ({"s3.endpoint": "https://user:secret@s3.example"}, {}),
        ({"s3.endpoint": "https://s3.example/?token=secret"}, {}),
        ({"s3.region": "region\nsecret"}, {}),
        ({"iceberg.rest.oauth2.scope": "a\nsecret"}, {"credential": "x"}),
    ],
)
def test_invalid_or_conflicting_defaults_are_rejected_without_values(properties, configured):
    with pytest.raises(DorisPlanningError) as error:
        iceberg_catalog_defaults(properties, configured)
    assert "secret" not in str(error.value)


def test_missing_and_masked_defaults_are_not_forwarded():
    assert iceberg_catalog_defaults({"s3.endpoint": "*XXX", "s3.region": ""}, {}) == {}
    assert iceberg_catalog_defaults({"iceberg.rest.oauth2.scope": "x"}, {"token": "token"}) == {}
    assert (
        iceberg_catalog_defaults({"s3.region": "doris-region"}, {"client.region": "explicit"}) == {}
    )


def test_deferred_defaults_detect_conflicts_without_exposing_values():
    defaults = {
        "s3.endpoint": "https://s3.example",
        "auth": {"type": "oauth2", "oauth2": {"scope": "catalog:read"}},
    }
    validate_iceberg_catalog_defaults(
        defaults, {"auth": {"type": "oauth2", "oauth2": {"client_secret": "secret"}}}
    )
    validate_iceberg_catalog_defaults(defaults, {"s3.endpoint": "https://s3.example"})
    for configured in (
        {"s3.endpoint": "https://secret.example"},
        {"auth": {"type": "basic"}},
        {"auth": {"type": "oauth2", "oauth2": {"scope": "different-secret"}}},
    ):
        with pytest.raises(DorisPlanningError) as error:
            validate_iceberg_catalog_defaults(defaults, configured)
        assert "secret" not in str(error.value)
    with pytest.raises(DorisPlanningError, match="configuration changed"):
        validate_iceberg_catalog_defaults({"s3.region": "region-1"}, {"client.region": "region-2"})
    with pytest.raises(DorisPlanningError, match="configuration changed"):
        validate_iceberg_catalog_defaults({"scope": "read"}, {"auth": {"type": "basic"}})


@pytest.mark.parametrize("managed", [False, True])
def test_defaults_interoperate_with_pyiceberg_oauth_and_server_override(monkeypatch, managed):
    import json
    from urllib.parse import parse_qs

    import pyiceberg.catalog as catalogs
    import requests
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import SortOrder
    from pyiceberg.types import LongType, NestedField

    configured = (
        {"auth": {"type": "oauth2", "oauth2": {"client_id": "id", "client_secret": "secret"}}}
        if managed
        else {"credential": "id:secret"}
    )
    monkeypatch.setattr(
        catalogs, "_ENV_CONFIG", SimpleNamespace(get_catalog_config=lambda _name: configured)
    )
    calls = []
    metadata = new_table_metadata(
        Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=False)),
        PartitionSpec(),
        SortOrder(),
        "s3://bucket/table",
    ).model_dump(by_alias=True, mode="json")

    def send(_adapter, request, **_kwargs):
        method, url = request.method, request.url
        data = parse_qs(request.body or "") if method == "POST" else {}
        calls.append((method, url, data))
        response = requests.Response()
        response.status_code = 200
        response.url = url
        response.request = request
        if method == "POST":
            body = {"access_token": "test-token", "token_type": "Bearer", "expires_in": 3600}
        elif url.endswith("/v1/config"):
            body = {"defaults": {}, "overrides": {"s3.region": "server-region"}}
        else:
            body = {
                "metadata-location": "s3://bucket/table/metadata/file.json",
                "metadata": metadata,
                "config": {"s3.region": "table-region"},
            }
        response._content = json.dumps(body).encode()
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    defaults = iceberg_catalog_defaults(
        {
            "iceberg.rest.oauth2.server-uri": "https://auth.example/token",
            "iceberg.rest.oauth2.scope": "catalog:read",
            "s3.region": "doris-region",
        },
        configured,
    )
    catalog = catalogs.load_catalog("lake", type="rest", uri="https://rest.example", **defaults)
    token_calls = [(url, data) for method, url, data in calls if method == "POST"]
    assert token_calls and all(url == "https://auth.example/token" for url, _ in token_calls)
    assert all(data["scope"] == ["catalog:read"] for _, data in token_calls)
    assert catalog.properties["s3.region"] == "server-region"
    assert catalog.load_table("db.table").io.properties["s3.region"] == "table-region"
    assert "secret" not in str(defaults)


def test_main_snapshot_is_selected_and_missing_interface_is_classified_narrowly():
    query = Mock(
        return_value=[
            {"name": "tag", "type": "TAG", "snapshot_id": 1},
            {"name": "main", "type": "BRANCH", "snapshot_id": 2},
        ]
    )
    assert doris_main_snapshot(None, query, "lake", parse_table("db.events")) == 2
    query.side_effect = pymysql.err.OperationalError(1105, "Table [events$refs] does not exist")
    assert doris_main_snapshot(None, query, "lake", parse_table("db.events")) is None
    for message in ("Table [events] does not exist", "permission denied for events$refs"):
        query.side_effect = pymysql.err.OperationalError(1105, message)
        with pytest.raises(pymysql.MySQLError):
            doris_main_snapshot(None, query, "lake", parse_table("db.events"))


@pytest.mark.parametrize(
    "rows",
    [
        [{"name": "tag", "type": "TAG", "snapshot_id": 1}],
        [{"name": "main", "type": "TAG", "snapshot_id": 2}],
        [{"name": "main", "type": "BRANCH", "snapshot_id": True}],
    ],
)
def test_invalid_ref_metadata_is_never_treated_as_absent(rows):
    with pytest.raises(DorisPlanningError):
        doris_main_snapshot(None, Mock(return_value=rows), "lake", parse_table("db.events"))


def test_empty_refs_retains_doris():
    with pytest.raises(_NativeReadNotApplicableError):
        doris_main_snapshot(None, Mock(return_value=[]), "lake", parse_table("db.events"))


def test_table_identity_requires_shared_metadata_and_matching_uuid(monkeypatch):
    serializer = Mock()
    serializer.FromInputFile.table_metadata.return_value.table_uuid = "uuid"
    monkeypatch.setattr("ray_doris._iceberg_metadata.import_module", Mock(return_value=serializer))
    table = Mock()
    table.metadata_location = "new.json"
    table.metadata.table_uuid = "uuid"
    table.metadata.metadata_log = [SimpleNamespace(metadata_file="old.json")]
    query = Mock(return_value=[{"file": "old.json"}])
    assert verify_table_identity(None, query, "lake", parse_table("db.events"), table) is True
    serializer.FromInputFile.table_metadata.return_value.table_uuid = "different"
    with pytest.raises(DorisPlanningError):
        verify_table_identity(None, query, "lake", parse_table("db.events"), table)
    query.return_value = [{"file": "unrelated.json"}]
    with pytest.raises(_NativeReadNotApplicableError):
        verify_table_identity(None, query, "lake", parse_table("db.events"), table)
    query.side_effect = pymysql.err.OperationalError(
        1105, "Table [events$metadata_log_entries] does not exist"
    )
    assert verify_table_identity(None, query, "lake", parse_table("db.events"), table) is False
    query.side_effect = pymysql.err.OperationalError(1142, "access denied")
    with pytest.raises(pymysql.MySQLError):
        verify_table_identity(None, query, "lake", parse_table("db.events"), table)


def _snapshot(snapshot_id, parent=None, manifest=None):
    return SimpleNamespace(
        snapshot_id=snapshot_id, parent_snapshot_id=parent, manifest_list=manifest
    )


def test_legacy_identity_matches_selected_snapshot_or_ancestor():
    table = Mock()
    current = _snapshot(3, 2, "s3://bucket/s3")
    parent = _snapshot(2, 1, "s3://bucket/s2")
    table.snapshot_by_id.return_value = parent
    query = Mock(return_value=[{"snapshot_id": 3, "manifest_list": "s3://bucket/s3"}])
    assert (
        verify_legacy_snapshot_identity(
            None, query, "lake", parse_table("db.events"), table, current
        )
        == "snapshot"
    )
    assert query.call_args.args[2] == ("lake.db.events",)
    query.return_value = [{"snapshot_id": 2, "manifest_list": "s3://bucket/s2"}]
    assert (
        verify_legacy_snapshot_identity(
            None, query, "lake", parse_table("db.events"), table, current
        )
        == "ancestor"
    )
    table.snapshot_by_id.assert_called_with(2)


@pytest.mark.parametrize("matched_id", [3, 2])
def test_legacy_identity_exposes_manifest_conflicts_before_older_match(matched_id):
    current = _snapshot(3, 2, "s3://bucket/s3")
    parent = _snapshot(2, 1, "s3://bucket/s2")
    table = Mock()
    table.snapshot_by_id.side_effect = lambda snapshot_id: {
        2: parent,
        1: _snapshot(1, None, "old"),
    }.get(snapshot_id)
    query = Mock(
        return_value=[
            {"snapshot_id": matched_id, "manifest_list": "different"},
            {"snapshot_id": 1, "manifest_list": "old"},
        ]
    )
    with pytest.raises(DorisPlanningError, match="manifests disagree"):
        verify_legacy_snapshot_identity(
            None, query, "lake", parse_table("db.events"), table, current
        )


@pytest.mark.parametrize("manifest", [None, "", "  "])
def test_legacy_identity_never_treats_missing_manifests_as_match(manifest):
    current = _snapshot(3, None, manifest)
    query = Mock(return_value=[{"snapshot_id": 3, "manifest_list": manifest}])
    with pytest.raises(_NativeReadNotApplicableError):
        verify_legacy_snapshot_identity(
            None, query, "lake", parse_table("db.events"), Mock(), current
        )


def test_legacy_identity_does_not_ignore_missing_current_manifest_for_ancestor():
    current = _snapshot(3, 2, "current")
    table = Mock()
    table.snapshot_by_id.return_value = _snapshot(2, None, "older")
    query = Mock(
        return_value=[
            {"snapshot_id": 3, "manifest_list": None},
            {"snapshot_id": 2, "manifest_list": "older"},
        ]
    )
    with pytest.raises(_NativeReadNotApplicableError):
        verify_legacy_snapshot_identity(
            None, query, "lake", parse_table("db.events"), table, current
        )
    table.snapshot_by_id.assert_not_called()


def test_legacy_identity_missing_parent_cycles_and_invalid_rows():
    current = _snapshot(3, 2, "new")
    table = Mock()
    table.snapshot_by_id.return_value = None
    query = Mock(return_value=[{"snapshot_id": 1, "manifest_list": "old"}])
    with pytest.raises(_NativeReadNotApplicableError):
        verify_legacy_snapshot_identity(
            None, query, "lake", parse_table("db.events"), table, current
        )
    table.snapshot_by_id.return_value = current
    with pytest.raises(DorisPlanningError, match="ancestry"):
        verify_legacy_snapshot_identity(
            None, query, "lake", parse_table("db.events"), table, current
        )
    query.return_value = [
        {"snapshot_id": 1, "manifest_list": "a"},
        {"snapshot_id": 1, "manifest_list": "b"},
    ]
    with pytest.raises(DorisPlanningError, match="metadata"):
        verify_legacy_snapshot_identity(
            None, query, "lake", parse_table("db.events"), table, current
        )


def test_legacy_identity_only_falls_back_for_absent_metadata_function():
    query = Mock(
        side_effect=pymysql.err.OperationalError(1105, "Can not found function 'iceberg_meta'")
    )
    with pytest.raises(_NativeReadNotApplicableError):
        verify_legacy_snapshot_identity(
            None, query, "lake", parse_table("db.events"), Mock(), _snapshot(3)
        )
    for error in (
        pymysql.err.OperationalError(1142, "SELECT denied"),
        pymysql.err.OperationalError(1105, "iceberg_meta permission denied"),
        pymysql.err.OperationalError(2003, "connection failure"),
    ):
        query.side_effect = error
        with pytest.raises(pymysql.MySQLError):
            verify_legacy_snapshot_identity(
                None, query, "lake", parse_table("db.events"), Mock(), _snapshot(3)
            )


def test_policy_check_requires_visible_builtin_policy_and_authorizes_projection():
    settings = [{"Key": "access_controller_type", "Value": "default"}]
    query = Mock(side_effect=[settings, [], []])
    verify_doris_policy(None, query, {}, "lake", parse_table("db.events"), ("id",))
    assert query.call_args_list[2].args[1] == "SELECT `id` FROM `lake`.`db`.`events` LIMIT 0"
    query = Mock(
        side_effect=[settings, [{"CatalogName": "lake", "DbName": "db", "TableName": "events"}]]
    )
    with pytest.raises(_NativeReadNotApplicableError):
        verify_doris_policy(None, query, {}, "lake", parse_table("db.events"), None)
    query = Mock(side_effect=pymysql.err.OperationalError(1227, "access denied"))
    with pytest.raises(_NativeReadNotApplicableError):
        verify_doris_policy(None, query, {}, "lake", parse_table("db.events"), None)
    query = Mock(side_effect=pymysql.err.OperationalError(2003, "connection failure"))
    with pytest.raises(pymysql.MySQLError):
        verify_doris_policy(None, query, {}, "lake", parse_table("db.events"), None)
