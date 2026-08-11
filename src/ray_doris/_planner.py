"""Driver-side Doris schema discovery and tablet planning."""

from __future__ import annotations

import base64
import http.client
import json
import logging
import math
import ssl
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pyarrow as pa
import pymysql

from ray_doris._errors import (
    DorisAuthenticationError,
    DorisConfigurationError,
    DorisPermissionError,
    DorisPlanningError,
    DorisSchemaError,
)
from ray_doris._models import (
    DorisInputSplit,
    DorisPlan,
    DorisPlanningSnapshot,
    DorisReadConfig,
)
from ray_doris._schema import build_arrow_schema, parse_describe_rows
from ray_doris._sql import build_describe_sql, build_select_sql

_MYSQL_AUTHENTICATION_ERROR_CODES = {1045}
_MYSQL_PERMISSION_ERROR_CODES = {1044, 1142, 1143, 1227}

logger = logging.getLogger(__name__)


def _table_context(config: DorisReadConfig) -> str:
    return f"{config.table.database}.{config.table.table}"


def _mysql_error_code(exc: pymysql.MySQLError) -> Any:
    return exc.args[0] if exc.args and isinstance(exc.args[0], int) else None


def _mysql_access_error(
    exc: pymysql.MySQLError, *, operation: str, context: str
) -> Optional[DorisPlanningError]:
    code = _mysql_error_code(exc)
    if code in _MYSQL_AUTHENTICATION_ERROR_CODES:
        return DorisAuthenticationError(
            f"Doris rejected {operation} credentials for {context} (MySQL error {code})"
        )
    if code in _MYSQL_PERMISSION_ERROR_CODES:
        return DorisPermissionError(f"Doris denied {operation} for {context} (MySQL error {code})")
    return None


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse redirects for authenticated query-plan POST requests."""

    def redirect_request(
        self,
        req: Any,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def _open_query_plan_request(
    request: urllib.request.Request,
    timeout: float,
    ssl_context: Optional[ssl.SSLContext],
) -> Any:
    handlers: List[Any] = [_NoRedirectHandler()]
    if ssl_context is not None:
        handlers.append(urllib.request.HTTPSHandler(context=ssl_context))
    return urllib.request.build_opener(*handlers).open(request, timeout=timeout)


def _query_plan_ssl_context(config: DorisReadConfig) -> Optional[ssl.SSLContext]:
    if config.http_scheme != "https":
        return None
    try:
        return ssl.create_default_context(cafile=config.http_ca_file)
    except (OSError, ValueError):
        raise DorisConfigurationError(
            f"Doris query-plan TLS configuration is invalid for {_table_context(config)}"
        ) from None


def _contains_tls_error(exc: BaseException) -> bool:
    candidates = [exc]
    seen = set()
    while candidates:
        candidate = candidates.pop()
        if id(candidate) in seen:
            continue
        seen.add(id(candidate))
        if isinstance(candidate, (ssl.SSLError, ssl.CertificateError)):
            return True
        for related in (
            candidate.__cause__,
            candidate.__context__,
            getattr(candidate, "reason", None),
        ):
            if isinstance(related, BaseException):
                candidates.append(related)
    return False


def _contains_mysql_tls_error(exc: pymysql.MySQLError) -> bool:
    if _contains_tls_error(exc):
        return True
    markers = (
        "certificate verify failed",
        "certificate_verify_failed",
        "hostname mismatch",
        "ssl handshake",
        "tls handshake",
    )
    return any(
        marker in argument.lower()
        for argument in exc.args
        if isinstance(argument, str)
        for marker in markers
    )


def _mysql_tls_is_configured(config: DorisReadConfig) -> bool:
    options = config.mysql_options()
    return "ssl" in options or any(key.startswith("ssl_") for key in options)


def _mysql_connection_kwargs(config: DorisReadConfig, *, streaming: bool) -> Dict[str, Any]:
    options = dict(config.mysql_options())
    kwargs: Dict[str, Any] = {
        "host": config.host,
        "port": config.mysql_port,
        "user": config.user,
        "password": config.resolve_password(),
        "database": config.table.database,
        "charset": "utf8mb4",
        "connect_timeout": config.connect_timeout,
    }
    if streaming:
        kwargs["cursorclass"] = pymysql.cursors.SSCursor
    kwargs.update(options)
    return kwargs


class QueryPlanClient:
    """A typed client for Doris FE's table query-plan endpoint."""

    def __init__(self, config: DorisReadConfig) -> None:
        self._config = config

    def fetch_tablet_ids(self, sql: str) -> Tuple[int, ...]:
        """Return the unique tablets retained by Doris predicate pruning."""
        config = self._config
        url = (
            f"{config.http_scheme}://{config.host}:{config.http_port}/api/"
            f"{config.table.database}/{config.table.table}/_query_plan"
        )
        password = config.resolve_password()
        ssl_context = _query_plan_ssl_context(config)
        request = urllib.request.Request(
            url,
            data=json.dumps({"sql": sql}).encode("utf-8"),
            headers={
                "Authorization": "Basic "
                + base64.b64encode(f"{config.user}:{password}".encode()).decode(),
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with _open_query_plan_request(
                request,
                timeout=config.effective_query_plan_timeout,
                ssl_context=ssl_context,
            ) as response:
                payload_bytes = response.read()
        except urllib.error.HTTPError as exc:
            if 300 <= exc.code < 400:
                raise DorisConfigurationError(
                    f"Doris query-plan endpoint redirected HTTP {exc.code} for "
                    f"{_table_context(config)}; configure the endpoint scheme and "
                    "host explicitly"
                ) from None
            if exc.code == 401:
                raise DorisAuthenticationError(
                    f"Doris query-plan authentication failed for {_table_context(config)} "
                    "(HTTP 401)"
                ) from None
            if exc.code == 403:
                raise DorisPermissionError(
                    f"Doris query-plan permission check failed for {_table_context(config)} "
                    "(HTTP 403)"
                ) from None
            raise DorisPlanningError(
                f"Doris query-plan endpoint returned HTTP {exc.code} for {_table_context(config)}"
            ) from None
        except (http.client.HTTPException, OSError) as exc:
            if config.http_scheme == "https" and _contains_tls_error(exc):
                raise DorisConfigurationError(
                    f"Doris query-plan TLS validation failed for "
                    f"{_table_context(config)}; refusing planning fallback"
                ) from None
            raise DorisPlanningError(
                f"Doris query-plan endpoint is unavailable for {_table_context(config)}"
            ) from None
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise DorisPlanningError(
                f"Doris query-plan endpoint returned invalid JSON for {_table_context(config)}"
            ) from None
        try:
            return self._parse_response(payload)
        except DorisAuthenticationError as exc:
            raise DorisAuthenticationError(f"{exc} for {_table_context(config)}") from None
        except DorisPermissionError as exc:
            raise DorisPermissionError(f"{exc} for {_table_context(config)}") from None
        except DorisPlanningError as exc:
            raise DorisPlanningError(f"{exc} for {_table_context(config)}") from None

    @staticmethod
    def _parse_response(payload: Any) -> Tuple[int, ...]:
        if not isinstance(payload, dict):
            raise DorisPlanningError("Doris query-plan response must be an object")
        outer_code = payload.get("code")
        if isinstance(outer_code, bool) or not isinstance(outer_code, int):
            raise DorisPlanningError("Doris query-plan response has an invalid body code")
        if outer_code == 401:
            raise DorisAuthenticationError("Doris rejected query-plan credentials (code 401)")
        if outer_code != 0:
            raise DorisPlanningError(
                f"Doris query-plan request failed with body code {outer_code!r}"
            )
        data = payload.get("data")
        if not isinstance(data, dict):
            raise DorisPlanningError("Doris query-plan response has no data object")
        plan_status = data.get("status")
        exception = str(data.get("exception", ""))
        if isinstance(plan_status, int) and not isinstance(plan_status, bool):
            if plan_status != 200:
                raise DorisPlanningError(
                    f"Doris query-plan request failed with body status {plan_status}"
                )
        elif plan_status == "1":
            if exception.startswith("Access denied;"):
                raise DorisPermissionError("Doris denied the query-plan request")
            raise DorisPlanningError("Doris query-plan service failed with body status '1'")
        else:
            raise DorisPlanningError("Doris query-plan response has an invalid body status")
        partitions = data.get("partitions")
        if not isinstance(partitions, dict):
            raise DorisPlanningError("Doris query-plan response has no partitions object")
        tablet_ids = []
        for raw_tablet_id in partitions:
            try:
                tablet_id = int(raw_tablet_id)
            except (TypeError, ValueError):
                raise DorisPlanningError("Doris query-plan returned an invalid tablet id") from None
            if tablet_id <= 0:
                raise DorisPlanningError("Doris query-plan returned an invalid tablet id")
            tablet_ids.append(tablet_id)
        return tuple(sorted(set(tablet_ids)))


def group_tablets(
    tablet_ids: Sequence[int], *, tablet_size: int, parallelism: int
) -> Tuple[DorisInputSplit, ...]:
    """Build deterministic adjacent tablet groups within Ray's requested cap."""
    _validate_parallelism(parallelism)
    if isinstance(tablet_size, bool) or not isinstance(tablet_size, int) or tablet_size <= 0:
        raise DorisConfigurationError("tablet_size must be a positive integer")
    if not tablet_ids:
        return ()
    group_count = min(parallelism, math.ceil(len(tablet_ids) / tablet_size))
    base_size, larger_groups = divmod(len(tablet_ids), group_count)
    splits = []
    offset = 0
    for group_index in range(group_count):
        group_size = base_size + (1 if group_index < larger_groups else 0)
        next_offset = offset + group_size
        splits.append(DorisInputSplit(tuple(tablet_ids[offset:next_offset])))
        offset = next_offset
    return tuple(splits)


def _validate_parallelism(parallelism: int) -> None:
    if isinstance(parallelism, bool) or not isinstance(parallelism, int) or parallelism <= 0:
        raise DorisConfigurationError("parallelism must be a positive integer")


class DorisPlanner:
    """Discover a canonical schema and build tablet-level worker splits."""

    def __init__(self, config: DorisReadConfig) -> None:
        self._config = config

    def discover(self) -> DorisPlanningSnapshot:
        """Discover schema and tablets once for one Ray datasource instance."""
        schema = self._describe_schema()
        base_sql = build_select_sql(
            self._config.table,
            self._config.columns,
            self._config.filter,
            tablet_ids=None,
        )
        try:
            tablet_ids = QueryPlanClient(self._config).fetch_tablet_ids(base_sql)
        except (DorisAuthenticationError, DorisPermissionError):
            raise
        except DorisPlanningError as exc:
            if self._config.on_query_plan_error == "error":
                raise
            logger.warning(
                "Doris query-plan failed for %s; using one unpartitioned task (%s)",
                _table_context(self._config),
                type(exc).__name__,
            )
            return DorisPlanningSnapshot(schema=schema, tablet_ids=None)
        return DorisPlanningSnapshot(schema=schema, tablet_ids=tablet_ids)

    def plan_from_snapshot(self, snapshot: DorisPlanningSnapshot, parallelism: int) -> DorisPlan:
        """Group one discovery snapshot for Ray's current parallelism hint."""
        _validate_parallelism(parallelism)
        if snapshot.tablet_ids is None:
            return DorisPlan(
                schema=snapshot.schema,
                splits=(DorisInputSplit(None),),
            )
        return DorisPlan(
            schema=snapshot.schema,
            splits=group_tablets(
                snapshot.tablet_ids,
                tablet_size=self._config.tablet_size,
                parallelism=parallelism,
            ),
        )

    def plan(self, parallelism: int) -> DorisPlan:
        """Plan a read without counting or sampling table data."""
        _validate_parallelism(parallelism)
        return self.plan_from_snapshot(self.discover(), parallelism)

    def _describe_schema(self) -> pa.Schema:
        try:
            connection = pymysql.connect(**_mysql_connection_kwargs(self._config, streaming=False))
            try:
                cursor = connection.cursor()
                try:
                    cursor.execute(build_describe_sql(self._config.table))
                    rows: List[Sequence[Any]] = []
                    while True:
                        batch = cursor.fetchmany(256)
                        if not batch:
                            break
                        rows.extend(batch)
                finally:
                    cursor.close()
            finally:
                connection.close()
        except DorisConfigurationError:
            raise
        except OSError:
            context = _table_context(self._config)
            if _mysql_tls_is_configured(self._config):
                raise DorisConfigurationError(
                    f"Doris MySQL TLS configuration is invalid for {context}"
                ) from None
            raise DorisPlanningError(f"failed to discover Doris schema for {context}") from None
        except pymysql.MySQLError as exc:
            code = _mysql_error_code(exc)
            context = _table_context(self._config)
            if _contains_mysql_tls_error(exc):
                raise DorisConfigurationError(
                    f"Doris MySQL TLS validation failed for {context}"
                ) from None
            access_error = _mysql_access_error(exc, operation="schema-discovery", context=context)
            if access_error is not None:
                raise access_error from None
            raise DorisPlanningError(
                f"failed to discover Doris schema for {context} (MySQL error {code!r})"
            ) from None
        try:
            columns = parse_describe_rows(rows)
            return build_arrow_schema(columns, self._config.columns)
        except DorisSchemaError as exc:
            raise DorisSchemaError(
                f"failed to build Arrow schema for {_table_context(self._config)}: {exc}"
            ) from exc
