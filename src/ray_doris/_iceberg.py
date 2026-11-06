"""Plan eligible Iceberg REST reads while preserving Doris' chosen snapshot."""

from __future__ import annotations

import logging
from functools import partial
from importlib import import_module
from typing import Any, Mapping, Optional, cast
from urllib.parse import urlsplit
from uuid import uuid4

import pyarrow as pa
import pymysql
import ray.data
from ray.data import Dataset

from ray_doris._compat import ensure_iceberg_ray_version
from ray_doris._errors import (
    DorisAuthenticationError,
    DorisConfigurationError,
    DorisError,
    DorisPermissionError,
    DorisPlanningError,
    _NativeReadNotApplicableError,
    _PinnedNativeFallbackError,
)
from ray_doris._iceberg_metadata import (
    doris_main_snapshot,
    iceberg_catalog_defaults,
    iceberg_http_access_error,
    iceberg_schema_fingerprint,
    rest_catalog_properties,
    verify_doris_policy,
    verify_legacy_snapshot_identity,
    verify_table_identity,
)
from ray_doris._metadata import ExternalMetadataReader, metadata_rows, named_row
from ray_doris._models import DorisReadConfig, NativeReadOptions
from ray_doris._native import _canonicalize_batch
from ray_doris._planner import _mysql_access_error, _mysql_connection_kwargs, _mysql_error_code
from ray_doris._schema import build_arrow_schema

logger = logging.getLogger(__name__)
_DELEGATION = "header.X-Iceberg-Access-Delegation"


def _load_table(load_catalog: Any, catalog: str, rest: dict[str, Any], identifier: str) -> Any:
    try:
        return load_catalog(catalog, **rest).load_table(identifier)
    except Exception as error:
        # Gravitino explicitly returns this 400 when vending was requested but
        # no provider exists; 401/403 and other failures must remain failures.
        exceptions = import_module("pyiceberg.exceptions")
        if (
            not isinstance(error, exceptions.BadRequestError)
            or "No credential provider found for path " not in str(error)
            or _DELEGATION in rest
        ):
            raise
        rest[_DELEGATION] = ""
        return load_catalog(catalog, **rest).load_table(identifier)


def _unsupported_v1_inline_manifests(error: Exception) -> bool:
    """Recognize only PyIceberg's missing manifest-list validation for v1 snapshots."""
    from pydantic import ValidationError

    if not isinstance(error, ValidationError):
        return False
    errors = error.errors(include_url=False)
    return bool(errors) and all(
        item.get("type") == "missing"
        and len(item.get("loc", ())) == 5
        and item["loc"][:3] == ("metadata", 1, "snapshots")
        and isinstance(item["loc"][3], int)
        and item["loc"][4] == "manifest-list"
        and isinstance(item.get("input"), dict)
        and "manifest-list" not in item["input"]
        and isinstance(item["input"].get("manifests"), list)
        and all(isinstance(path, str) and path for path in item["input"]["manifests"])
        for item in errors
    )


def read_iceberg(
    config: DorisReadConfig,
    catalog: str,
    options: NativeReadOptions,
    *,
    concurrency: Optional[int],
    override_num_blocks: Optional[int],
    ray_remote_args: Optional[Mapping[str, Any]],
) -> Dataset:
    """Select a snapshot only after source identity and policy checks succeed."""
    if concurrency is not None or options != NativeReadOptions():
        raise _NativeReadNotApplicableError("Iceberg reader cannot preserve these options")
    ensure_iceberg_ray_version()
    try:
        load_catalog = import_module("pyiceberg.catalog").load_catalog
    except ImportError:
        raise _NativeReadNotApplicableError("PyIceberg is not installed") from None
    kwargs = _mysql_connection_kwargs(config, streaming=False)
    kwargs.pop("database", None)
    kwargs.update(cursorclass=pymysql.cursors.DictCursor)
    kwargs.setdefault("read_timeout", options.metadata_timeout)
    kwargs.setdefault("write_timeout", options.metadata_timeout)
    stage = "connection"
    try:
        with pymysql.connect(**kwargs) as connection, connection.cursor() as cursor:
            reader = ExternalMetadataReader(cursor)
            stage = "catalog"
            properties = reader.catalog(catalog, expected_kind="iceberg")
            rest: dict[str, Any] = dict(rest_catalog_properties(properties))
            configured = (
                import_module("pyiceberg.utils.config").Config().get_catalog_config(catalog) or {}
            )
            auth = configured.get("auth")
            has_identity = bool(
                configured.get("credential")
                or configured.get("token")
                or (isinstance(auth, dict) and auth.get("type") not in (None, "noop"))
            )
            if isinstance(auth, dict) and auth.get("type") == "oauth2":
                oauth = auth.get("oauth2", {})
                if isinstance(oauth, dict):
                    # This manager does not use the legacy top-level token or credential.
                    has_identity = bool(oauth.get("client_id") and oauth.get("client_secret"))
            requires_oauth = properties.get("iceberg.rest.security.type", "").lower() == "oauth2"
            requires_oauth = requires_oauth or any(
                properties.get(key)
                for key in (
                    "credential",
                    "token",
                    "iceberg.rest.oauth2.credential",
                    "iceberg.rest.oauth2.token",
                )
            )
            if requires_oauth and not has_identity:
                raise _NativeReadNotApplicableError("REST identity is not available")
            if not has_identity and not auth:
                # PyIceberg 0.10's legacy manager otherwise sends "Bearer None".
                rest["auth"] = {"type": "noop"}
            if (
                properties.get("external_catalog.name")
                or properties.get("meta_names_mapping")
                or properties.get("lower_case_meta_names", "false").lower() != "false"
            ):
                raise _NativeReadNotApplicableError("Iceberg remote name mapping is unsupported")
            stage = "policy"
            verify_doris_policy(
                cursor, metadata_rows, properties, catalog, config.table, config.columns
            )
            stage = "schema"
            columns = reader.columns(config.table, catalog)
            schema = build_arrow_schema(columns, config.columns)
            if any(pa.types.is_nested(field.type) for field in schema):
                raise _NativeReadNotApplicableError("nested Iceberg columns require Doris")
            if any(pa.types.is_timestamp(field.type) for field in schema):
                timezone_rows = metadata_rows(cursor, "SELECT @@time_zone AS timezone")
                if len(timezone_rows) != 1:
                    raise DorisPlanningError("Doris returned invalid session time zone")
                timezone = named_row(timezone_rows[0]).get("timezone")
                if timezone not in ("UTC", "GMT", "Etc/UTC", "Etc/GMT", "+00:00", "-00:00"):
                    raise _NativeReadNotApplicableError("Iceberg timestamps require a UTC session")
            stage = "REST catalog"
            defaults = iceberg_catalog_defaults(properties, configured)
            rest.update(defaults)
            try:
                table = _load_table(
                    load_catalog, catalog, rest, f"{config.table.database}.{config.table.table}"
                )
            except Exception as error:
                if _unsupported_v1_inline_manifests(error):
                    raise _NativeReadNotApplicableError(
                        "Iceberg v1 inline manifests are unsupported by the reader"
                    ) from None
                access_error = iceberg_http_access_error(error)
                if access_error is not None:
                    raise access_error from None
                exceptions = import_module("pyiceberg.exceptions")
                if isinstance(
                    error,
                    (
                        exceptions.UnauthorizedError,
                        exceptions.AuthorizationExpiredError,
                        exceptions.OAuthError,
                    ),
                ):
                    raise DorisAuthenticationError("Iceberg REST authentication failed") from None
                if isinstance(error, exceptions.ForbiddenError):
                    raise DorisPermissionError("Iceberg REST permission denied") from None
                raise DorisPlanningError("Iceberg REST catalog discovery failed") from None
            stage = "identity"
            try:
                identity_verified = verify_table_identity(
                    cursor, metadata_rows, catalog, config.table, table
                )
            except (DorisError, pymysql.MySQLError):
                raise
            except Exception:
                raise DorisPlanningError("Iceberg table identity verification failed") from None
            stage = "snapshot"
            snapshot_id = doris_main_snapshot(cursor, metadata_rows, catalog, config.table)
            if snapshot_id is None:
                current = table.current_snapshot()
                if current is None:
                    raise _NativeReadNotApplicableError("Iceberg table has no current snapshot")
                snapshot_id = current.snapshot_id
            if (
                isinstance(snapshot_id, bool)
                or not isinstance(snapshot_id, int)
                or not 0 < snapshot_id < 1 << 63
            ):
                raise DorisPlanningError("Iceberg returned an invalid current snapshot")
            snapshot = table.snapshot_by_id(snapshot_id)
            if snapshot is None:
                raise DorisPlanningError("Doris snapshot is absent from the Iceberg REST table")
            legacy_identity = None
            if not identity_verified:
                stage = "legacy identity"
                legacy_identity = verify_legacy_snapshot_identity(
                    cursor, metadata_rows, catalog, config.table, table, snapshot
                )
            if snapshot.schema_id is not None and not any(
                schema_entry.schema_id == snapshot.schema_id
                for schema_entry in table.metadata.schemas
            ):
                raise DorisPlanningError("selected Iceberg snapshot schema is unavailable")
            expected_schema_fingerprint = None
            if snapshot.schema_id is None:
                current = table.current_snapshot()
                if current is None or current.snapshot_id != snapshot_id:
                    raise DorisPlanningError("historical Iceberg snapshot schema is unavailable")
                expected_schema_fingerprint = iceberg_schema_fingerprint(table.schema())
            # Everything after this point must keep the selected snapshot or fail.
            try:
                scan = table.scan(snapshot_id=snapshot_id, selected_fields=config.columns or ("*",))
                projection = scan.projection().as_arrow()
                pa.Table.from_batches([], schema=projection).select(schema.names).cast(
                    schema, safe=True
                )
            except Exception:
                raise DorisPlanningError(
                    "selected Iceberg snapshot schema differs from Doris"
                ) from None
            if table.metadata.format_version >= 3:
                try:
                    manifests = snapshot.manifests(table.io)
                    has_deletes = any(
                        getattr(item.content, "value", item.content) == 1 for item in manifests
                    )
                except Exception:
                    raise DorisPlanningError("Iceberg delete manifest inspection failed") from None
                if has_deletes:
                    raise _PinnedNativeFallbackError(snapshot_id, schema)
            try:
                tasks = list(scan.plan_files())
            except (NotImplementedError, ValueError) as error:
                if "does not yet support equality deletes" in str(error).lower():
                    raise _PinnedNativeFallbackError(snapshot_id, schema) from None
                raise DorisPlanningError("selected Iceberg snapshot is unsupported") from None
            except Exception:
                raise DorisPlanningError("Iceberg snapshot planning failed") from None
            if len(tasks) > options.max_files:
                raise _PinnedNativeFallbackError(snapshot_id, schema)
            for task in tasks:
                path = task.file.file_path
                files = [task.file, *task.delete_files]
                if urlsplit(path).scheme not in ("s3", "s3a", "hdfs") or any(
                    getattr(file.file_format, "value", "") != "PARQUET"
                    or urlsplit(file.file_path).scheme not in ("s3", "s3a", "hdfs")
                    for file in files
                ):
                    raise _PinnedNativeFallbackError(snapshot_id, schema)
            expected_uuid = str(table.metadata.table_uuid)
    except pymysql.MySQLError as exc:
        access_error = _mysql_access_error(
            exc, operation="Iceberg metadata", context="external table"
        )
        if access_error is not None:
            raise access_error from None
        raise DorisPlanningError(
            f"Doris Iceberg metadata request failed during {stage} "
            f"(MySQL error {_mysql_error_code(exc)})"
        ) from None

    if config.columns is not None and any(name not in schema.names for name in config.columns):
        raise DorisConfigurationError("Iceberg projection includes an unknown column")
    if legacy_identity is not None:
        logger.warning(
            "Iceberg native read uses %s manifest evidence; Doris table UUID is unverified "
            "and REST and Doris caches may show different snapshots%s",
            legacy_identity,
            "; a snapshot-pinned Doris fallback may fail" if legacy_identity == "ancestor" else "",
        )
    if not tasks:
        return cast(Dataset, ray.data.from_arrow(pa.Table.from_batches([], schema=schema)))
    guarded: dict[str, Any] = {
        "name": "ray_doris_" + uuid4().hex,
        "py-catalog-impl": "ray_doris._iceberg_catalog.PinnedRestCatalog",
        "ray-doris.catalog-name": catalog,
        "ray-doris.table-uuid": expected_uuid,
        "ray-doris.snapshot-id": str(snapshot_id),
        "uri": rest["uri"],
    }
    if "warehouse" in rest:
        guarded["warehouse"] = rest["warehouse"]
    if expected_schema_fingerprint is not None:
        guarded["ray-doris.schema-fingerprint"] = expected_schema_fingerprint
    if defaults:
        guarded["ray-doris.catalog-defaults"] = defaults
    if _DELEGATION in rest:
        guarded[_DELEGATION] = rest[_DELEGATION]
    if rest.get("auth") == {"type": "noop"}:
        guarded["auth"] = rest["auth"]
    dataset = ray.data.read_iceberg(
        table_identifier=f"{config.table.database}.{config.table.table}",
        snapshot_id=snapshot_id,
        selected_fields=config.columns or ("*",),
        catalog_kwargs=guarded,
        override_num_blocks=override_num_blocks,
        ray_remote_args=dict(ray_remote_args) if ray_remote_args is not None else {},
    )
    logger.info("Doris read path=native experimental=true reader=read_iceberg")
    return dataset.map_batches(
        partial(_canonicalize_batch, schema=schema),
        batch_format="pyarrow",
        batch_size=None,
        **(dict(ray_remote_args) if ray_remote_args is not None else {}),
    )
