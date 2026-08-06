"""Driver-side Doris schema discovery and tablet planning."""

from __future__ import annotations

import base64
import json
import math
import urllib.error
import urllib.request
from typing import Any, Dict, List, Sequence, Tuple

import pyarrow as pa
import pymysql

from ray_doris._errors import (
    DorisAuthenticationError,
    DorisConfigurationError,
    DorisPermissionError,
    DorisPlanningError,
    DorisSchemaError,
)
from ray_doris._models import DorisInputSplit, DorisPlan, DorisReadConfig
from ray_doris._schema import build_arrow_schema, parse_describe_rows
from ray_doris._sql import build_describe_sql, build_select_sql

_MYSQL_AUTHENTICATION_ERROR_CODES = {1045}
_MYSQL_PERMISSION_ERROR_CODES = {1044, 1142, 1143, 1227}


def _table_context(config: DorisReadConfig) -> str:
    return f"{config.table.database}.{config.table.table}"


def _mysql_error_code(exc: pymysql.MySQLError) -> Any:
    return exc.args[0] if exc.args and isinstance(exc.args[0], int) else None


def _mysql_connection_kwargs(config: DorisReadConfig, *, streaming: bool) -> Dict[str, Any]:
    options = dict(config.mysql_options())
    kwargs: Dict[str, Any] = {
        "host": config.host,
        "port": config.mysql_port,
        "user": config.user,
        "password": config.password,
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
        request = urllib.request.Request(
            url,
            data=json.dumps({"sql": sql}).encode("utf-8"),
            headers={
                "Authorization": "Basic "
                + base64.b64encode(f"{config.user}:{config.password}".encode()).decode(),
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=config.connect_timeout) as response:
                status = response.status
                payload_bytes = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise DorisAuthenticationError(
                    f"Doris query-plan authentication failed for {_table_context(config)} "
                    "(HTTP 401)"
                ) from exc
            if exc.code == 403:
                raise DorisPermissionError(
                    f"Doris query-plan permission check failed for {_table_context(config)} "
                    "(HTTP 403)"
                ) from exc
            raise DorisPlanningError(
                f"Doris query-plan endpoint returned HTTP {exc.code} for {_table_context(config)}"
            ) from exc
        except (OSError, TimeoutError) as exc:
            raise DorisPlanningError(
                f"Doris query-plan endpoint is unavailable for {_table_context(config)}"
            ) from exc
        if status == 401:
            raise DorisAuthenticationError(
                f"Doris query-plan authentication failed for {_table_context(config)} (HTTP 401)"
            )
        if status == 403:
            raise DorisPermissionError(
                f"Doris query-plan permission check failed for {_table_context(config)} (HTTP 403)"
            )
        if status != 200:
            raise DorisPlanningError(
                f"Doris query-plan endpoint returned HTTP {status} for {_table_context(config)}"
            )
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DorisPlanningError(
                f"Doris query-plan endpoint returned invalid JSON for {_table_context(config)}"
            ) from exc
        try:
            return self._parse_response(payload)
        except DorisAuthenticationError as exc:
            raise DorisAuthenticationError(f"{exc} for {_table_context(config)}") from exc
        except DorisPermissionError as exc:
            raise DorisPermissionError(f"{exc} for {_table_context(config)}") from exc
        except DorisPlanningError as exc:
            raise DorisPlanningError(f"{exc} for {_table_context(config)}") from exc

    @staticmethod
    def _parse_response(payload: Any) -> Tuple[int, ...]:
        if not isinstance(payload, dict):
            raise DorisPlanningError("Doris query-plan response must be an object")
        outer_code = payload.get("code")
        if isinstance(outer_code, bool) or not isinstance(outer_code, int):
            raise DorisPlanningError(
                f"Doris query-plan response has invalid body code {outer_code!r}"
            )
        if outer_code == 401:
            raise DorisAuthenticationError("Doris rejected query-plan credentials (code 401)")
        if outer_code != 0:
            raise DorisPlanningError(
                f"Doris query-plan request failed with body code {outer_code!r}: "
                f"{payload.get('msg', 'unknown error')}"
            )
        data = payload.get("data")
        if not isinstance(data, dict):
            raise DorisPlanningError("Doris query-plan response has no data object")
        plan_status = data.get("status")
        exception = str(data.get("exception", "unknown error"))
        if isinstance(plan_status, int) and not isinstance(plan_status, bool):
            if plan_status != 200:
                raise DorisPlanningError(
                    f"Doris query-plan request failed with body status {plan_status}: {exception}"
                )
        elif plan_status == "1":
            if exception.startswith("Access denied;"):
                raise DorisPermissionError(exception)
            raise DorisPlanningError(
                f"Doris query-plan service failed with body status '1': {exception}"
            )
        else:
            raise DorisPlanningError(
                f"Doris query-plan response has invalid body status {plan_status!r}"
            )
        partitions = data.get("partitions")
        if not isinstance(partitions, dict):
            raise DorisPlanningError("Doris query-plan response has no partitions object")
        tablet_ids = []
        for raw_tablet_id in partitions:
            try:
                tablet_id = int(raw_tablet_id)
            except (TypeError, ValueError) as exc:
                raise DorisPlanningError(
                    f"Doris query-plan returned invalid tablet id {raw_tablet_id!r}"
                ) from exc
            if tablet_id <= 0:
                raise DorisPlanningError(
                    f"Doris query-plan returned invalid tablet id {raw_tablet_id!r}"
                )
            tablet_ids.append(tablet_id)
        return tuple(sorted(set(tablet_ids)))


def group_tablets(
    tablet_ids: Sequence[int], *, tablet_size: int, parallelism: int
) -> Tuple[DorisInputSplit, ...]:
    """Build deterministic adjacent tablet groups within Ray's requested cap."""
    _validate_parallelism(parallelism)
    if not tablet_ids:
        return ()
    group_size = max(tablet_size, math.ceil(len(tablet_ids) / parallelism))
    return tuple(
        DorisInputSplit(tuple(tablet_ids[index : index + group_size]))
        for index in range(0, len(tablet_ids), group_size)
    )


def _validate_parallelism(parallelism: int) -> None:
    if isinstance(parallelism, bool) or not isinstance(parallelism, int) or parallelism <= 0:
        raise DorisConfigurationError("parallelism must be a positive integer")


class DorisPlanner:
    """Discover a canonical schema and build tablet-level worker splits."""

    def __init__(self, config: DorisReadConfig) -> None:
        self._config = config

    def plan(self, parallelism: int) -> DorisPlan:
        """Plan a read without counting or sampling table data."""
        _validate_parallelism(parallelism)
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
        except DorisPlanningError:
            if self._config.on_query_plan_error == "error":
                raise
            return DorisPlan(schema=schema, splits=(DorisInputSplit(None),))
        return DorisPlan(
            schema=schema,
            splits=group_tablets(
                tablet_ids,
                tablet_size=self._config.tablet_size,
                parallelism=parallelism,
            ),
        )

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
        except pymysql.MySQLError as exc:
            code = _mysql_error_code(exc)
            context = _table_context(self._config)
            if code in _MYSQL_AUTHENTICATION_ERROR_CODES:
                raise DorisAuthenticationError(
                    f"Doris rejected schema-discovery credentials for {context} "
                    f"(MySQL error {code})"
                ) from exc
            if code in _MYSQL_PERMISSION_ERROR_CODES:
                raise DorisPermissionError(
                    f"Doris denied schema discovery for {context} (MySQL error {code})"
                ) from exc
            raise DorisPlanningError(
                f"failed to discover Doris schema for {context} (MySQL error {code!r})"
            ) from exc
        try:
            columns = parse_describe_rows(rows)
            return build_arrow_schema(columns, self._config.columns)
        except DorisSchemaError as exc:
            raise DorisSchemaError(
                f"failed to build Arrow schema for {_table_context(self._config)}: {exc}"
            ) from exc
