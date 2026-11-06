import logging
from dataclasses import replace
from functools import partial
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, create_autospec

import pyarrow as pa
import pymysql
import pytest
import ray.data

from ray_doris import read_doris
from ray_doris._errors import (
    DorisAuthenticationError,
    DorisPermissionError,
    DorisPlanningError,
    DorisReadError,
    _NativeReadNotApplicableError,
    _PinnedNativeFallbackError,
)
from ray_doris._iceberg import read_iceberg
from ray_doris._models import DorisInputSplit, DorisReadConfig, NativeReadOptions
from ray_doris._sql import parse_table


@pytest.fixture
def iceberg_environment(monkeypatch):
    import ray_doris._iceberg as iceberg

    schema = pa.schema([pa.field("id", pa.int64())])
    cursor = Mock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    monkeypatch.setattr(iceberg.pymysql, "connect", Mock(return_value=connection))
    reader = Mock()
    reader.catalog.return_value = {"iceberg.catalog.type": "rest", "uri": "https://rest"}
    monkeypatch.setattr(iceberg, "ExternalMetadataReader", Mock(return_value=reader))
    monkeypatch.setattr(iceberg, "build_arrow_schema", Mock(return_value=schema))
    monkeypatch.setattr(iceberg, "verify_doris_policy", Mock())
    monkeypatch.setattr(iceberg, "verify_table_identity", Mock())
    monkeypatch.setattr(iceberg, "doris_main_snapshot", Mock(return_value=123))
    table = Mock()
    table.metadata.table_uuid = "table-uuid"
    table.metadata.format_version = 2
    table.snapshot_by_id.return_value = SimpleNamespace(
        snapshot_id=123, schema_id=0, manifests=Mock(return_value=[SimpleNamespace(content=0)])
    )
    table.metadata.schemas = [SimpleNamespace(schema_id=0)]
    scan = table.scan.return_value
    scan.projection.return_value.as_arrow.return_value = schema
    scan.plan_files.return_value = [
        SimpleNamespace(
            file=SimpleNamespace(
                file_path="s3://bucket/a.parquet", file_format=SimpleNamespace(value="PARQUET")
            ),
            delete_files=[],
        )
    ]
    catalog = Mock()
    catalog.load_table.return_value = table
    monkeypatch.setattr(
        iceberg,
        "import_module",
        Mock(
            return_value=SimpleNamespace(
                load_catalog=Mock(return_value=catalog),
                Config=Mock(return_value=SimpleNamespace(get_catalog_config=Mock(return_value={}))),
            )
        ),
    )
    public_read = create_autospec(ray.data.read_iceberg, return_value=Mock())
    monkeypatch.setattr(iceberg.ray.data, "read_iceberg", public_read)
    config = DorisReadConfig.from_options(
        host="fe", user="reader", catalog="lake", table=parse_table("db.events")
    )
    return iceberg, config, table, scan, public_read


def invoke(env, **kwargs):
    _, config, _, _, _ = env
    return read_iceberg(
        config,
        "lake",
        NativeReadOptions(),
        concurrency=None,
        override_num_blocks=None,
        ray_remote_args=kwargs or None,
    )


def test_iceberg_read_forwards_pinned_snapshot_and_uses_identity_guard(iceberg_environment):
    _, _, table, _, public_read = iceberg_environment
    invoke(iceberg_environment)
    args = public_read.call_args.kwargs
    assert args["table_identifier"] == "db.events" and args["snapshot_id"] == 123
    assert args["catalog_kwargs"]["ray-doris.table-uuid"] == "table-uuid"
    assert args["catalog_kwargs"]["ray-doris.snapshot-id"] == "123"
    assert "ray-doris.schema-fingerprint" not in args["catalog_kwargs"]
    assert args["catalog_kwargs"]["auth"] == {"type": "noop"}
    table.scan.assert_called_once_with(snapshot_id=123, selected_fields=("*",))


def test_snapshot_without_schema_id_passes_schema_fingerprint(iceberg_environment):
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    iceberg, _, table, _, public_read = iceberg_environment
    table.snapshot_by_id.return_value = SimpleNamespace(
        snapshot_id=123, schema_id=None, manifests=Mock(return_value=[])
    )
    table.current_snapshot.return_value = SimpleNamespace(snapshot_id=123)
    table.schema.return_value = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False)
    )
    invoke(iceberg_environment)
    assert public_read.call_args.kwargs["catalog_kwargs"]["ray-doris.schema-fingerprint"] == (
        iceberg.iceberg_schema_fingerprint(table.schema.return_value)
    )


def test_doris_nonsecret_defaults_reach_both_catalog_loads(iceberg_environment):
    iceberg, _, _, _, public_read = iceberg_environment
    iceberg.ExternalMetadataReader.return_value.catalog.return_value = {
        "iceberg.catalog.type": "rest",
        "uri": "https://rest",
        "s3.endpoint": "https://s3.example",
        "s3.region": "region-1",
        "iceberg.rest.oauth2.server-uri": "https://auth.example/token",
        "iceberg.rest.oauth2.scope": "read",
        "iceberg.rest.oauth2.credential": "doris-secret",
        "s3.secret_key": "storage-secret",
    }
    config = iceberg.import_module.return_value.Config.return_value
    config.get_catalog_config.return_value = {"credential": "ray-secret"}
    invoke(iceberg_environment)
    catalog_args = iceberg.import_module.return_value.load_catalog.call_args.kwargs
    guarded = public_read.call_args.kwargs["catalog_kwargs"]
    expected = {
        "s3.endpoint": "https://s3.example",
        "s3.region": "region-1",
        "oauth2-server-uri": "https://auth.example/token",
        "scope": "read",
    }
    assert guarded["ray-doris.catalog-defaults"] == expected
    assert all(catalog_args[key] == value for key, value in expected.items())
    assert all(
        secret not in str(guarded) for secret in ("doris-secret", "storage-secret", "ray-secret")
    )


def test_missing_rest_identity_falls_back_before_optional_default_validation(iceberg_environment):
    iceberg, _, _, _, public_read = iceberg_environment
    iceberg.ExternalMetadataReader.return_value.catalog.return_value = {
        "iceberg.catalog.type": "rest",
        "uri": "https://rest",
        "iceberg.rest.security.type": "oauth2",
        "s3.endpoint": "not-a-qualified-endpoint",
    }
    with pytest.raises(_NativeReadNotApplicableError, match="identity is not available"):
        invoke(iceberg_environment)
    public_read.assert_not_called()


@pytest.mark.parametrize(
    "oauth,legacy",
    [({}, {}), ({"client_id": "id"}, {}), ({"client_secret": "secret"}, {"token": "unused"})],
)
def test_missing_oauth_manager_identity_falls_back_before_rest(iceberg_environment, oauth, legacy):
    iceberg, _, _, _, public_read = iceberg_environment
    iceberg.ExternalMetadataReader.return_value.catalog.return_value = {
        "iceberg.catalog.type": "rest",
        "uri": "https://rest",
        "iceberg.rest.security.type": "oauth2",
    }
    config = iceberg.import_module.return_value.Config.return_value
    config.get_catalog_config.return_value = {"auth": {"type": "oauth2", "oauth2": oauth}, **legacy}
    with pytest.raises(_NativeReadNotApplicableError, match="identity is not available"):
        invoke(iceberg_environment)
    iceberg.import_module.return_value.load_catalog.assert_not_called()
    public_read.assert_not_called()


@pytest.mark.parametrize(
    "auth",
    [
        {"type": "oauth2", "oauth2": {"client_id": "id", "client_secret": "secret"}},
        {"type": "google"},
    ],
)
def test_configured_oauth_or_environment_manager_is_not_rejected(iceberg_environment, auth):
    iceberg, _, _, _, public_read = iceberg_environment
    iceberg.ExternalMetadataReader.return_value.catalog.return_value = {
        "iceberg.catalog.type": "rest",
        "uri": "https://rest",
        "iceberg.rest.security.type": "oauth2",
    }
    config = iceberg.import_module.return_value.Config.return_value
    config.get_catalog_config.return_value = {"auth": auth}
    invoke(iceberg_environment)
    public_read.assert_called_once()


def test_projection_preserves_requested_order_when_iceberg_uses_schema_order(iceberg_environment):
    iceberg, config, table, scan, public_read = iceberg_environment
    config = replace(config, columns=("label", "id"))
    iceberg.build_arrow_schema.return_value = pa.schema(
        [("label", pa.string()), ("id", pa.int64())]
    )
    scan.projection.return_value.as_arrow.return_value = pa.schema(
        [("id", pa.int64()), ("label", pa.large_string())]
    )
    read_iceberg(
        config,
        "lake",
        NativeReadOptions(),
        concurrency=None,
        override_num_blocks=None,
        ray_remote_args=None,
    )
    table.scan.assert_called_once_with(snapshot_id=123, selected_fields=("label", "id"))
    assert public_read.call_args.kwargs["selected_fields"] == ("label", "id")


def test_iceberg_known_delete_failure_is_pinned_but_other_errors_surface(iceberg_environment):
    _, _, _, scan, public_read = iceberg_environment
    scan.plan_files.side_effect = ValueError("PyIceberg does not yet support equality deletes")
    with pytest.raises(_PinnedNativeFallbackError) as error:
        invoke(iceberg_environment)
    assert error.value.snapshot_id == 123
    scan.plan_files.side_effect = PermissionError("secret")
    with pytest.raises(DorisPlanningError, match="planning failed"):
        invoke(iceberg_environment)
    public_read.assert_not_called()


def test_unpinned_fallback_only_before_snapshot_and_missing_snapshot_errors(iceberg_environment):
    iceberg, _, table, _, public_read = iceberg_environment
    iceberg.verify_table_identity.side_effect = _NativeReadNotApplicableError("unconfirmed")
    with pytest.raises(_NativeReadNotApplicableError):
        invoke(iceberg_environment)
    iceberg.verify_table_identity.side_effect = None
    table.snapshot_by_id.return_value = None
    with pytest.raises(DorisPlanningError, match="absent"):
        invoke(iceberg_environment)
    public_read.assert_not_called()


def test_storage_format_and_options_obey_fallback_boundary(iceberg_environment):
    _, config, _, scan, public_read = iceberg_environment
    with pytest.raises(_NativeReadNotApplicableError):
        read_iceberg(
            config,
            "lake",
            NativeReadOptions(),
            concurrency=2,
            override_num_blocks=None,
            ray_remote_args=None,
        )
    scan.plan_files.return_value[0].file.file_format.value = "ORC"
    with pytest.raises(_PinnedNativeFallbackError) as error:
        invoke(iceberg_environment)
    assert error.value.snapshot_id == 123
    public_read.assert_not_called()


@pytest.mark.parametrize("version", [2, 3])
def test_delete_manifests_keep_the_selected_snapshot(iceberg_environment, version):
    _, _, table, scan, public_read = iceberg_environment
    table.metadata.format_version = version
    table.snapshot_by_id.return_value = Mock(
        schema_id=0, manifests=Mock(return_value=[SimpleNamespace(content=1)])
    )
    with pytest.raises(_PinnedNativeFallbackError) as error:
        invoke(iceberg_environment)
    assert error.value.snapshot_id == 123
    scan.plan_files.assert_not_called()
    public_read.assert_not_called()


def test_delete_manifest_inspection_error_does_not_fall_back(iceberg_environment):
    _, _, table, scan, public_read = iceberg_environment
    table.snapshot_by_id.return_value.manifests.side_effect = PermissionError("secret")
    with pytest.raises(DorisPlanningError, match="delete manifest inspection failed") as error:
        invoke(iceberg_environment)
    assert "secret" not in str(error.value)
    scan.plan_files.assert_not_called()
    public_read.assert_not_called()


def test_rest_snapshot_is_used_only_when_doris_refs_are_unavailable(iceberg_environment):
    iceberg, _, table, _, public_read = iceberg_environment
    iceberg.doris_main_snapshot.return_value = None
    table.current_snapshot.return_value = SimpleNamespace(snapshot_id=456)
    invoke(iceberg_environment)
    assert public_read.call_args.kwargs["snapshot_id"] == 456
    table.current_snapshot.return_value = None
    with pytest.raises(_NativeReadNotApplicableError):
        invoke(iceberg_environment)


@pytest.mark.parametrize("evidence", ["snapshot", "ancestor"])
def test_legacy_identity_uses_rest_snapshot_and_warns(iceberg_environment, caplog, evidence):
    iceberg, _, table, _, public_read = iceberg_environment
    iceberg.verify_table_identity.return_value = False
    iceberg.doris_main_snapshot.return_value = None
    table.current_snapshot.return_value = SimpleNamespace(snapshot_id=456)
    table.snapshot_by_id.return_value = SimpleNamespace(
        snapshot_id=456, schema_id=0, manifests=Mock(return_value=[])
    )
    iceberg.verify_legacy_snapshot_identity = Mock(return_value=evidence)
    with caplog.at_level(logging.WARNING):
        invoke(iceberg_environment)
    assert public_read.call_args.kwargs["snapshot_id"] == 456
    assert evidence in caplog.text and "UUID is unverified" in caplog.text
    assert ("fallback may fail" in caplog.text) == (evidence == "ancestor")
    assert "https://rest" not in caplog.text


def test_legacy_rejection_and_permission_error_stay_before_native_execution(iceberg_environment):
    iceberg, _, _, _, public_read = iceberg_environment
    iceberg.verify_table_identity.return_value = False
    iceberg.verify_legacy_snapshot_identity = Mock(
        side_effect=_NativeReadNotApplicableError("no shared evidence")
    )
    with pytest.raises(_NativeReadNotApplicableError):
        invoke(iceberg_environment)
    iceberg.verify_legacy_snapshot_identity.side_effect = pymysql.err.OperationalError(
        1142, "sensitive permission details"
    )
    with pytest.raises(DorisPermissionError) as error:
        invoke(iceberg_environment)
    assert "sensitive" not in str(error.value)
    public_read.assert_not_called()


def test_legacy_known_reader_incompatibility_keeps_selected_snapshot(iceberg_environment):
    iceberg, _, _, scan, public_read = iceberg_environment
    iceberg.verify_table_identity.return_value = False
    iceberg.verify_legacy_snapshot_identity = Mock(return_value="ancestor")
    scan.plan_files.side_effect = ValueError("PyIceberg does not yet support equality deletes")
    with pytest.raises(_PinnedNativeFallbackError) as error:
        invoke(iceberg_environment)
    assert error.value.snapshot_id == 123
    public_read.assert_not_called()


def test_only_recognized_v1_inline_manifests_fall_back(iceberg_environment, monkeypatch):
    from pydantic import ValidationError
    from pyiceberg.catalog.rest import TableResponse
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import SortOrder
    from pyiceberg.types import LongType, NestedField

    iceberg, _, _, _, public_read = iceberg_environment
    metadata = new_table_metadata(
        Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=False)),
        PartitionSpec(),
        SortOrder(),
        "s3://bucket/table",
        {"format-version": "1"},
    ).model_dump(by_alias=True, mode="json")
    metadata["snapshots"] = [
        {"snapshot-id": 123, "timestamp-ms": 1, "manifests": ["s3://bucket/manifest"]}
    ]
    metadata["current-snapshot-id"] = 123
    with pytest.raises(ValidationError) as error:
        TableResponse.model_validate({"metadata": metadata})
    assert iceberg._unsupported_v1_inline_manifests(error.value)
    monkeypatch.setattr(iceberg, "_load_table", Mock(side_effect=error.value))
    with pytest.raises(_NativeReadNotApplicableError):
        invoke(iceberg_environment)
    metadata["snapshots"][0].pop("manifests")
    with pytest.raises(ValidationError) as malformed:
        TableResponse.model_validate({"metadata": metadata})
    assert not iceberg._unsupported_v1_inline_manifests(malformed.value)
    public_read.assert_not_called()


def test_facade_propagates_snapshot_into_doris_fallback(monkeypatch):
    native = Mock()
    native.read_native.side_effect = _PinnedNativeFallbackError(
        123, pa.schema([("id", pa.int64())])
    )
    monkeypatch.setattr("ray_doris._api.import_module", Mock(return_value=native))
    read = Mock(return_value="dataset")
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", read)
    assert (
        read_doris(host="fe", table="db.events", catalog="lake", auto_native_read=True) == "dataset"
    )
    assert read.call_args.args[0].config.snapshot_id == 123
    assert read.call_args.args[0].supports_projection_pushdown() is False
    assert read.call_args.args[0].supports_predicate_pushdown() is False


def test_snapshot_fallback_failure_surfaces_without_unpinned_retry(monkeypatch):
    from ray_doris._readers import read_mysql

    schema = pa.schema([("id", pa.int64())])
    native = Mock()
    native.read_native.side_effect = _PinnedNativeFallbackError(123, schema)
    monkeypatch.setattr("ray_doris._api.import_module", Mock(return_value=native))
    cursor = Mock()
    cursor.execute.side_effect = pymysql.err.OperationalError(1105, "sensitive failure")
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr("ray_doris._readers.pymysql.connect", Mock(return_value=connection))
    read = Mock(
        side_effect=lambda source, **_: list(
            read_mysql(source.config, DorisInputSplit(None), schema)
        )
    )
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", read)

    with pytest.raises(DorisReadError) as error:
        read_doris(host="fe", table="db.events", catalog="lake", auto_native_read=True)
    assert "sensitive" not in str(error.value)
    assert read.call_count == 1 and cursor.execute.call_count == 1
    assert "FOR VERSION AS OF 123" in cursor.execute.call_args.args[0]


def test_deferred_catalog_checks_table_identity_and_snapshot(monkeypatch):
    import importlib
    from pathlib import Path

    original_import = importlib.import_module
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: (
            SimpleNamespace(RestCatalog=object)
            if name == "pyiceberg.catalog.rest"
            else original_import(name)
        ),
    )
    spec = importlib.util.spec_from_file_location(
        "ray_doris._iceberg_catalog_test",
        Path(__file__).parents[2] / "src/ray_doris/_iceberg_catalog.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    table = Mock()
    table.metadata.table_uuid = "uuid"
    catalog = Mock()
    catalog.load_table.return_value = table
    monkeypatch.setattr(
        module,
        "import_module",
        Mock(return_value=SimpleNamespace(load_catalog=Mock(return_value=catalog))),
    )
    guarded = module.PinnedRestCatalog(
        "guard",
        **{
            "ray-doris.catalog-name": "lake",
            "ray-doris.table-uuid": "uuid",
            "ray-doris.snapshot-id": "123",
            "uri": "https://rest",
        },
    )
    assert guarded.load_table("db.events") is table
    table.metadata.table_uuid = "replacement"
    with pytest.raises(DorisPlanningError, match="identity changed"):
        guarded.load_table("db.events")
    table.metadata.table_uuid = "uuid"
    table.snapshot_by_id.return_value = None
    with pytest.raises(DorisPlanningError, match="no longer available"):
        guarded.load_table("db.events")
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    original = Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=False))
    replacement = Schema(NestedField(field_id=2, name="id", field_type=LongType(), required=False))
    snapshot = SimpleNamespace(schema_id=None)
    table.snapshot_by_id.return_value = snapshot
    table.schema.return_value = original
    fingerprint = module.iceberg_schema_fingerprint(original)
    guarded = module.PinnedRestCatalog(
        "guard",
        **{
            "ray-doris.catalog-name": "lake",
            "ray-doris.table-uuid": "uuid",
            "ray-doris.snapshot-id": "123",
            "ray-doris.schema-fingerprint": fingerprint,
            "uri": "https://rest",
        },
    )
    assert guarded.load_table("db.events") is table
    table.schema.return_value = replacement
    with pytest.raises(DorisPlanningError, match="schema changed"):
        guarded.load_table("db.events")
    table.schema.return_value = original
    snapshot.schema_id = 1
    with pytest.raises(DorisPlanningError, match="schema changed"):
        guarded.load_table("db.events")
    names = ("UnauthorizedError", "AuthorizationExpiredError", "OAuthError", "ForbiddenError")
    monkeypatch.setattr(
        module,
        "import_module",
        Mock(return_value=SimpleNamespace(**dict.fromkeys(names, PermissionError))),
    )
    catalog.load_table.side_effect = ValueError("sensitive-source-config")
    with pytest.raises(DorisPlanningError) as error:
        guarded.load_table("db.events")
    assert "sensitive-source-config" not in str(error.value)


def test_deferred_catalog_reuses_defaults_and_rejects_configuration_change(monkeypatch):
    import ray_doris._iceberg_catalog as iceberg_catalog

    loader = Mock()
    config = Mock()
    config.get_catalog_config.return_value = {
        "auth": {"type": "oauth2", "oauth2": {"client_id": "id", "client_secret": "secret"}}
    }
    monkeypatch.setattr(
        iceberg_catalog,
        "import_module",
        lambda name: (
            SimpleNamespace(Config=Mock(return_value=config))
            if name == "pyiceberg.utils.config"
            else SimpleNamespace(load_catalog=loader)
        ),
    )
    defaults = {
        "s3.endpoint": "https://s3.example",
        "auth": {"type": "oauth2", "oauth2": {"token_url": "https://auth.example/token"}},
    }
    kwargs = {
        "ray-doris.catalog-name": "lake",
        "ray-doris.table-uuid": "uuid",
        "ray-doris.snapshot-id": "123",
        "ray-doris.catalog-defaults": defaults,
        "uri": "https://rest",
    }
    iceberg_catalog.PinnedRestCatalog("guard", **kwargs)
    assert loader.call_args.kwargs["s3.endpoint"] == "https://s3.example"
    assert loader.call_args.kwargs["auth"] == defaults["auth"]
    assert "secret" not in str(loader.call_args.kwargs)
    config.get_catalog_config.return_value = {"s3.endpoint": "https://other.example"}
    with pytest.raises(DorisPlanningError, match="configuration changed"):
        iceberg_catalog.PinnedRestCatalog("guard", **kwargs)
    assert loader.call_count == 1


def test_vending_retry_only_for_explicit_missing_provider(monkeypatch):
    import ray_doris._iceberg as iceberg

    class BadRequestError(Exception):
        pass

    monkeypatch.setattr(
        iceberg,
        "import_module",
        Mock(return_value=SimpleNamespace(BadRequestError=BadRequestError)),
    )
    table = object()
    loader = Mock()
    loader.return_value.load_table.side_effect = [
        BadRequestError("No credential provider found for path s3://bucket/table"),
        table,
    ]
    rest = {"type": "rest", "uri": "https://rest"}
    assert iceberg._load_table(loader, "lake", rest, "db.events", {}) is table
    assert rest["header.X-Iceberg-Access-Delegation"] == ""
    loader.return_value.load_table.side_effect = PermissionError("denied")
    with pytest.raises(PermissionError):
        iceberg._load_table(
            loader, "lake", {"type": "rest", "uri": "https://rest"}, "db.events", {}
        )
    assert loader.call_count == 3


@pytest.mark.parametrize("source", ["rest", "configured"])
def test_vending_retry_preserves_explicit_case_insensitive_header(source):
    from pyiceberg.exceptions import BadRequestError

    import ray_doris._iceberg as iceberg

    loader = Mock()
    loader.return_value.load_table.side_effect = BadRequestError(
        "No credential provider found for path s3://bucket/table"
    )
    rest = {"type": "rest", "uri": "https://rest"}
    configured = {}
    target = rest if source == "rest" else configured
    target["header.x-iceberg-access-delegation"] = "vended-credentials"
    with pytest.raises(BadRequestError):
        iceberg._load_table(loader, "lake", rest, "db.events", configured)
    loader.assert_called_once()
    assert "header.X-Iceberg-Access-Delegation" not in rest


def test_named_delegation_configuration_reaches_retry_check(iceberg_environment, monkeypatch):
    import importlib
    import json
    import os

    import pyiceberg.catalog as catalogs
    import requests
    from pyiceberg.utils.config import Config

    iceberg, _, _, _, public_read = iceberg_environment
    for key in list(os.environ):
        if key.lower().startswith("pyiceberg_"):
            monkeypatch.delenv(key)
    delegation = "vended-credentials,remote-signing"
    monkeypatch.setenv("PYICEBERG_CATALOG__LAKE__HEADER__X_ICEBERG_ACCESS_DELEGATION", delegation)
    monkeypatch.setattr(Config, "_from_configuration_files", staticmethod(lambda: {}))
    monkeypatch.setattr(catalogs, "_ENV_CONFIG", Config())
    monkeypatch.setattr(iceberg, "import_module", importlib.import_module)
    calls = []

    def send(_adapter, request, **_kwargs):
        calls.append((request.url, request.headers.get("X-Iceberg-Access-Delegation")))
        response = requests.Response()
        response.url = request.url
        response.request = request
        if request.url.endswith("/v1/config"):
            response.status_code = 200
            body = {"defaults": {}, "overrides": {}}
        else:
            response.status_code = 400
            body = {
                "error": {
                    "type": "BadRequestException",
                    "message": "No credential provider found for path s3://bucket/table",
                    "code": 400,
                }
            }
        response._content = json.dumps(body).encode()
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with pytest.raises(DorisPlanningError, match="REST catalog discovery failed"):
        invoke(iceberg_environment)
    assert calls == [
        ("https://rest/v1/config", delegation),
        ("https://rest/v1/namespaces/db/tables/events", delegation),
    ]
    public_read.assert_not_called()


@pytest.mark.parametrize(
    "name,expected",
    [("UnauthorizedError", DorisAuthenticationError), ("ForbiddenError", DorisPermissionError)],
)
def test_rest_auth_failures_are_visible_and_sanitized(
    iceberg_environment, monkeypatch, name, expected
):
    iceberg, _, _, _, reader = iceberg_environment

    class AuthError(Exception):
        pass

    class OtherError(Exception):
        pass

    errors = SimpleNamespace(
        UnauthorizedError=AuthError if name == "UnauthorizedError" else OtherError,
        ForbiddenError=AuthError if name == "ForbiddenError" else OtherError,
        AuthorizationExpiredError=OtherError,
        OAuthError=OtherError,
        BadRequestError=OtherError,
    )
    monkeypatch.setattr(iceberg, "_load_table", Mock(side_effect=AuthError("secret-token")))
    iceberg.import_module.side_effect = lambda module: (
        errors
        if module == "pyiceberg.exceptions"
        else SimpleNamespace(
            load_catalog=Mock(),
            Config=Mock(return_value=SimpleNamespace(get_catalog_config=Mock(return_value={}))),
        )
    )
    with pytest.raises(expected) as error:
        invoke(iceberg_environment)
    assert "secret-token" not in str(error.value)
    reader.assert_not_called()


@pytest.mark.parametrize("stage", ["planning", "deferred"])
@pytest.mark.parametrize(
    "status,expected",
    [
        (401, DorisAuthenticationError),
        (403, DorisPermissionError),
        (500, DorisPlanningError),
        (None, DorisPlanningError),
    ],
)
def test_oauth_manager_http_failures_are_classified_without_fallback(
    iceberg_environment, monkeypatch, stage, status, expected
):
    import requests
    from pyiceberg.catalog.rest.auth import OAuth2AuthManager

    from ray_doris._iceberg_catalog import _safe_catalog_call

    if status is None:
        monkeypatch.setattr(requests, "post", Mock(side_effect=requests.ConnectionError("secret")))
    else:
        response = requests.Response()
        response.status_code = status
        response.url = "https://auth.example/token?secret=hidden"
        monkeypatch.setattr(requests, "post", Mock(return_value=response))
    manager = OAuth2AuthManager("id", "secret", "https://auth.example/token")
    iceberg, _, _, _, public_read = iceberg_environment
    if stage == "planning":
        monkeypatch.setattr(
            iceberg, "_load_table", Mock(side_effect=lambda *_args: manager.auth_header())
        )
        # The fixture's import stub represents all PyIceberg modules; HTTP 500
        # reaches the existing PyIceberg exception classification as well.
        real_exceptions = __import__("pyiceberg.exceptions", fromlist=["exceptions"])
        stub = iceberg.import_module.return_value
        monkeypatch.setattr(
            iceberg,
            "import_module",
            lambda module: real_exceptions if module == "pyiceberg.exceptions" else stub,
        )
        action = partial(invoke, iceberg_environment)
    else:
        action = partial(_safe_catalog_call, manager.auth_header)
    with pytest.raises(expected) as error:
        action()
    assert type(error.value) is expected
    assert "secret" not in str(error.value)
    public_read.assert_not_called()


def test_missing_historical_schema_fails_before_native_execution(iceberg_environment):
    _, _, table, _, public_read = iceberg_environment
    table.snapshot_by_id.return_value.schema_id = 17
    with pytest.raises(DorisPlanningError, match="schema is unavailable"):
        invoke(iceberg_environment)
    public_read.assert_not_called()
