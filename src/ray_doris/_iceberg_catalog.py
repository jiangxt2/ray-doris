"""Identity guard for Ray's deferred public Iceberg catalog load."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Callable

from ray_doris._errors import DorisAuthenticationError, DorisPermissionError, DorisPlanningError
from ray_doris._iceberg_metadata import (
    iceberg_http_access_error,
    iceberg_schema_fingerprint,
    validate_iceberg_catalog_defaults,
)

if TYPE_CHECKING:

    class _RestCatalog:
        pass

else:
    _RestCatalog = import_module("pyiceberg.catalog.rest").RestCatalog


def _safe_catalog_call(operation: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    try:
        return operation(*args, **kwargs)
    except Exception as error:
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
        raise DorisPlanningError("deferred Iceberg REST catalog request failed") from None


class PinnedRestCatalog(_RestCatalog):
    """Reload a configured REST catalog without passing its credentials to Ray's plan."""

    def __init__(self, name: str, **properties: Any) -> None:
        del name
        self._source_name = properties["ray-doris.catalog-name"]
        self._expected_uuid = properties["ray-doris.table-uuid"]
        self._snapshot_id = int(properties["ray-doris.snapshot-id"])
        self._expected_schema_fingerprint = properties.get("ray-doris.schema-fingerprint")
        source: dict[str, Any] = {"type": "rest", "uri": properties["uri"]}
        if "warehouse" in properties:
            source["warehouse"] = properties["warehouse"]
        if defaults := properties.get("ray-doris.catalog-defaults"):
            configured = (
                import_module("pyiceberg.utils.config")
                .Config()
                .get_catalog_config(self._source_name)
                or {}
            )
            validate_iceberg_catalog_defaults(defaults, configured)
            source.update(defaults)
        if properties.get("auth") == {"type": "noop"}:
            source["auth"] = properties["auth"]
        if "header.X-Iceberg-Access-Delegation" in properties:
            source["header.X-Iceberg-Access-Delegation"] = properties[
                "header.X-Iceberg-Access-Delegation"
            ]
        load_catalog = import_module("pyiceberg.catalog").load_catalog
        self._source = _safe_catalog_call(load_catalog, self._source_name, **source)

    def load_table(self, identifier: Any) -> Any:
        table = _safe_catalog_call(self._source.load_table, identifier)
        if str(table.metadata.table_uuid) != self._expected_uuid:
            raise DorisPlanningError("Iceberg table identity changed after planning")
        snapshot = table.snapshot_by_id(self._snapshot_id)
        if snapshot is None:
            raise DorisPlanningError("selected Iceberg snapshot is no longer available")
        if self._expected_schema_fingerprint is not None and (
            snapshot.schema_id is not None
            or iceberg_schema_fingerprint(table.schema()) != self._expected_schema_fingerprint
        ):
            raise DorisPlanningError("Iceberg snapshot schema changed after planning")
        return table
