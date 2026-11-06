"""Doris Iceberg REST catalog and snapshot metadata helpers."""

from __future__ import annotations

import re
from hashlib import sha256
from importlib import import_module
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit

import pymysql

from ray_doris._errors import (
    DorisAuthenticationError,
    DorisPermissionError,
    DorisPlanningError,
    _NativeReadNotApplicableError,
)
from ray_doris._metadata import MetadataQuery, named_row
from ray_doris._models import QualifiedTable
from ray_doris._sql import build_select_sql, quote_identifier


def iceberg_schema_fingerprint(schema: Any) -> str:
    """Identify an Iceberg schema, including field IDs, without exposing it in the plan."""
    return sha256(schema.model_dump_json(by_alias=True).encode("utf-8")).hexdigest()


def iceberg_http_access_error(error: Exception) -> Optional[DorisPlanningError]:
    """Classify HTTP authentication failures without exposing the response."""
    http_error = import_module("requests.exceptions").HTTPError
    if isinstance(error, http_error) and error.response is not None:
        if error.response.status_code == 401:
            return DorisAuthenticationError("Iceberg REST authentication failed")
        if error.response.status_code == 403:
            return DorisPermissionError("Iceberg REST permission denied")
    return None


def _nonsecret_value(properties: Mapping[str, str], keys: tuple[str, ...]) -> Optional[str]:
    values = {
        properties[key].strip()
        for key in keys
        if key in properties
        and properties[key].strip()
        and properties[key].strip().upper() not in {"*XXX", "******", "********"}
    }
    if len(values) > 1:
        raise DorisPlanningError("Doris Iceberg non-sensitive properties disagree")
    return next(iter(values), None)


def _safe_endpoint(value: str) -> str:
    try:
        parsed = urlsplit(value)
        valid = (
            parsed.scheme in ("http", "https")
            and bool(parsed.hostname)
            and not parsed.username
            and not parsed.password
            and not parsed.query
            and not parsed.fragment
            and (parsed.port is None or 0 < parsed.port < 65536)
        )
    except ValueError:
        valid = False
    if not valid:
        raise DorisPlanningError("Doris returned an invalid non-sensitive endpoint")
    return value


def iceberg_catalog_defaults(
    properties: Mapping[str, str], configured: Mapping[str, Any]
) -> dict[str, Any]:
    """Derive only safe defaults absent from the named PyIceberg configuration."""
    defaults: dict[str, Any] = {}
    for target, aliases in (
        ("s3.endpoint", ("s3.endpoint", "AWS_ENDPOINT", "aws.endpoint")),
        ("s3.region", ("s3.region", "AWS_REGION", "aws.region")),
    ):
        explicit = ("s3.region", "client.region") if target == "s3.region" else (target,)
        if any(key in configured for key in explicit):
            continue
        value = _nonsecret_value(properties, aliases)
        if value is None:
            continue
        if target == "s3.endpoint":
            value = _safe_endpoint(value)
        elif not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", value):
            raise DorisPlanningError("Doris returned an invalid S3 region")
        defaults[target] = value

    auth = configured.get("auth")
    if isinstance(auth, Mapping) and auth.get("type") == "oauth2":
        oauth = auth.get("oauth2", {})
        if not isinstance(oauth, Mapping):
            return defaults
        fields = (
            ("token_url", ("iceberg.rest.oauth2.server-uri", "oauth2-server-uri")),
            ("scope", ("iceberg.rest.oauth2.scope", "scope")),
        )
    elif not auth and configured.get("credential"):
        oauth = configured
        fields = (
            ("oauth2-server-uri", ("iceberg.rest.oauth2.server-uri", "oauth2-server-uri")),
            ("scope", ("iceberg.rest.oauth2.scope", "scope")),
        )
    else:
        return defaults
    oauth_defaults: dict[str, str] = {}
    for target, oauth_aliases in fields:
        if target in oauth:
            continue
        value = _nonsecret_value(properties, oauth_aliases)
        if value is not None:
            if target in ("token_url", "oauth2-server-uri"):
                value = _safe_endpoint(value)
            elif any(ord(char) < 32 or ord(char) == 127 for char in value):
                raise DorisPlanningError("Doris returned an invalid OAuth2 scope")
            oauth_defaults[target] = value
    if isinstance(auth, Mapping):
        if oauth_defaults:
            defaults["auth"] = {"type": "oauth2", "oauth2": oauth_defaults}
    else:
        defaults.update(oauth_defaults)
    return defaults


def validate_iceberg_catalog_defaults(
    defaults: Mapping[str, Any], configured: Mapping[str, Any]
) -> None:
    """Do not silently use different explicit settings after planning."""
    if any(key in defaults for key in ("oauth2-server-uri", "scope")) and configured.get("auth"):
        raise DorisPlanningError("Iceberg catalog configuration changed after planning")
    for key, value in defaults.items():
        if key == "auth":
            auth = configured.get("auth")
            if auth is None:
                continue
            if not isinstance(auth, Mapping) or auth.get("type") != "oauth2":
                raise DorisPlanningError("Iceberg catalog configuration changed after planning")
            oauth = auth.get("oauth2", {})
            if not isinstance(oauth, Mapping):
                raise DorisPlanningError("Iceberg catalog configuration changed after planning")
            for field, default in value["oauth2"].items():
                if field in oauth and oauth[field] != default:
                    raise DorisPlanningError("Iceberg catalog configuration changed after planning")
        else:
            explicit = ("s3.region", "client.region") if key == "s3.region" else (key,)
            if any(name in configured and configured[name] != value for name in explicit):
                raise DorisPlanningError("Iceberg catalog configuration changed after planning")


def rest_catalog_properties(properties: Mapping[str, str]) -> dict[str, str]:
    """Extract the REST connection, rejecting ambiguous Doris catalog metadata."""
    if properties.get("iceberg.catalog.type", "").lower() != "rest":
        raise _NativeReadNotApplicableError("Iceberg catalog is not backed by REST")
    values = [properties[key].strip() for key in ("uri", "iceberg.rest.uri") if key in properties]
    if not values or not any(values):
        raise _NativeReadNotApplicableError("Iceberg REST URI is unavailable")
    if any(not value for value in values) or len(set(values)) != 1:
        raise DorisPlanningError("Doris Iceberg REST URI properties disagree")
    uri = values[0]
    try:
        parsed = urlsplit(uri)
        valid = (
            parsed.scheme in ("http", "https")
            and bool(parsed.hostname)
            and not parsed.username
            and not parsed.password
            and not parsed.query
            and not parsed.fragment
            and (parsed.port is None or 0 < parsed.port < 65536)
        )
    except ValueError:
        valid = False
    if not valid:
        raise DorisPlanningError("Doris returned an invalid Iceberg REST URI")
    result = {"type": "rest", "uri": uri}
    if "warehouse" in properties:
        if not properties["warehouse"]:
            raise DorisPlanningError("Doris returned an empty Iceberg warehouse")
        result["warehouse"] = properties["warehouse"]
    return result


def doris_main_snapshot(
    cursor: Any, query: MetadataQuery, catalog: str, table: QualifiedTable
) -> Optional[int]:
    """Return Doris' main ref, or None only when the refs interface is absent."""
    table_ref = ".".join(
        quote_identifier(part) for part in (catalog, table.database, table.table + "$refs")
    )
    try:
        rows = query(cursor, f"SELECT name, `type`, snapshot_id FROM {table_ref}", None)
    except pymysql.MySQLError as error:
        code = error.args[0] if error.args else None
        message = " ".join(v for v in error.args[1:] if isinstance(v, str)).lower()
        # On 3.0.6.2 this exact system-table request reports the suffixed table
        # as absent. Other missing objects and permission failures are not capability misses.
        if (
            code == 1105
            and f"{table.table.lower()}$refs" in message
            and "does not exist" in message
            and not any(word in message for word in ("denied", "permission", "unauthorized"))
        ):
            return None
        raise
    normalized = [named_row(row) for row in rows]
    main = [row for row in normalized if row.get("name") == "main"]
    if not main:
        if not normalized:
            raise _NativeReadNotApplicableError("Iceberg table has no main snapshot")
        raise DorisPlanningError("Doris returned Iceberg refs without a main branch")
    if len(main) != 1 or str(main[0].get("type", "")).lower() != "branch":
        raise DorisPlanningError("Doris returned invalid Iceberg main ref")
    snapshot = main[0].get("snapshot_id")
    if isinstance(snapshot, bool) or not isinstance(snapshot, int) or not 0 < snapshot < 1 << 63:
        raise DorisPlanningError("Doris returned invalid Iceberg snapshot ID")
    return snapshot


def verify_table_identity(
    cursor: Any, query: MetadataQuery, catalog: str, table: QualifiedTable, rest_table: Any
) -> bool:
    """Confirm shared metadata and UUID; return False only when the interface is absent."""
    name = table.table + "$metadata_log_entries"
    table_ref = ".".join(quote_identifier(part) for part in (catalog, table.database, name))
    try:
        rows = query(cursor, f"SELECT file FROM {table_ref}", None)
    except pymysql.MySQLError as error:
        code = error.args[0] if error.args else None
        message = " ".join(v for v in error.args[1:] if isinstance(v, str)).lower()
        if (
            code == 1105
            and name.lower() in message
            and "does not exist" in message
            and not any(word in message for word in ("denied", "permission", "unauthorized"))
        ):
            return False
        raise
    files = [named_row(row).get("file") for row in rows]
    if not files or any(not isinstance(file, str) or not file for file in files):
        raise DorisPlanningError("Doris returned invalid Iceberg metadata file identities")
    rest_files = {rest_table.metadata_location}
    rest_files.update(entry.metadata_file for entry in rest_table.metadata.metadata_log)
    common = next((file for file in files if file in rest_files), None)
    if common is None:
        raise _NativeReadNotApplicableError("Doris and REST table identity cannot be confirmed")
    reader = import_module("pyiceberg.serializers").FromInputFile
    metadata = reader.table_metadata(rest_table.io.new_input(common))
    if metadata.table_uuid != rest_table.metadata.table_uuid:
        raise DorisPlanningError("Doris and REST Iceberg table identities disagree")
    return True


def verify_legacy_snapshot_identity(
    cursor: Any,
    query: MetadataQuery,
    catalog: str,
    table: QualifiedTable,
    rest_table: Any,
    snapshot: Any,
) -> str:
    """Match a snapshot or ancestor; this does not verify Doris' table UUID."""
    try:
        rows = query(
            cursor,
            "SELECT snapshot_id, manifest_list FROM "
            "iceberg_meta('table'=%s, 'query_type'='snapshots')",
            (f"{catalog}.{table.database}.{table.table}",),
        )
    except pymysql.MySQLError as error:
        code = error.args[0] if error.args else None
        message = " ".join(value for value in error.args[1:] if isinstance(value, str)).lower()
        if (
            code == 1105
            and "can not found function 'iceberg_meta'" in message
            and not any(word in message for word in ("denied", "permission", "unauthorized"))
        ):
            raise _NativeReadNotApplicableError(
                "Doris Iceberg snapshot metadata is unavailable"
            ) from None
        raise

    manifests: dict[int, Optional[str]] = {}
    for item in rows:
        row = named_row(item)
        snapshot_id, manifest = row.get("snapshot_id"), row.get("manifest_list")
        if (
            isinstance(snapshot_id, bool)
            or not isinstance(snapshot_id, int)
            or not 0 < snapshot_id < 1 << 63
            or snapshot_id in manifests
            or (manifest is not None and not isinstance(manifest, str))
        ):
            raise DorisPlanningError("Doris returned invalid Iceberg snapshot metadata")
        manifests[snapshot_id] = manifest if manifest and manifest.strip() else None

    seen: set[int] = set()
    current = snapshot
    while current is not None:
        snapshot_id = current.snapshot_id
        if (
            isinstance(snapshot_id, bool)
            or not isinstance(snapshot_id, int)
            or not 0 < snapshot_id < 1 << 63
            or snapshot_id in seen
        ):
            raise DorisPlanningError("Iceberg snapshot ancestry is invalid")
        seen.add(snapshot_id)
        manifest = current.manifest_list
        if manifest is not None and not isinstance(manifest, str):
            raise DorisPlanningError("Iceberg snapshot manifest is invalid")
        doris_manifest = manifests.get(snapshot_id)
        if doris_manifest and manifest and manifest.strip():
            if doris_manifest != manifest:
                raise DorisPlanningError("Doris and REST Iceberg snapshot manifests disagree")
            return "snapshot" if current is snapshot else "ancestor"
        if current is snapshot and snapshot_id in manifests:
            break
        parent_id = current.parent_snapshot_id
        if parent_id is None:
            break
        if (
            isinstance(parent_id, bool)
            or not isinstance(parent_id, int)
            or not 0 < parent_id < 1 << 63
        ):
            raise DorisPlanningError("Iceberg snapshot ancestry is invalid")
        current = rest_table.snapshot_by_id(parent_id)
    raise _NativeReadNotApplicableError("Doris and REST snapshot identity cannot be confirmed")


def verify_doris_policy(
    cursor: Any,
    query: MetadataQuery,
    properties: Mapping[str, str],
    catalog: str,
    table: QualifiedTable,
    columns: Optional[tuple[str, ...]],
) -> None:
    """Use only profiles where Doris' built-in policies can be established."""
    if properties.get("access_controller.class") or any(
        key.startswith("access_controller.properties.") for key in properties
    ):
        raise _NativeReadNotApplicableError("catalog authorization requires Doris")
    try:
        config = query(cursor, "ADMIN SHOW FRONTEND CONFIG LIKE 'access_controller_type'", None)
        policies = query(cursor, "SHOW ROW POLICY", None)
    except pymysql.MySQLError as error:
        code = error.args[0] if error.args else None
        message = " ".join(v for v in error.args[1:] if isinstance(v, str)).lower()
        if code in (1044, 1142, 1227) or (code == 1105 and "denied" in message):
            raise _NativeReadNotApplicableError(
                "Doris policy applicability is not visible"
            ) from None
        raise
    if len(config) != 1:
        raise _NativeReadNotApplicableError("Doris authorization source is not visible")
    settings = named_row(config[0])
    if settings.get("key") != "access_controller_type" or settings.get("value") != "default":
        raise _NativeReadNotApplicableError("Doris authorization source requires Doris")
    for item in policies:
        row = named_row(item)
        if not all(isinstance(row.get(key), str) for key in ("catalogname", "dbname", "tablename")):
            raise DorisPlanningError("Doris returned malformed row policies")
        if (row["catalogname"], row["dbname"], row["tablename"]) == (
            catalog,
            table.database,
            table.table,
        ):
            raise _NativeReadNotApplicableError("Doris row policies require Doris")
    # Ask Doris to authorize the actual projection; this also prevents source
    # credentials from granting more columns than the Doris identity can read.
    query(cursor, build_select_sql(table, columns, None, None, catalog) + " LIMIT 0", None)
