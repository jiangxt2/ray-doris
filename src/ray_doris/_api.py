"""Convenience entry point mirroring Ray Data reader functions."""

from __future__ import annotations

import logging
from importlib import import_module
from typing import Any, Mapping, Optional, Sequence, cast

import ray.data
from ray.data import Dataset

from ray_doris._errors import DorisConfigurationError, _NativeReadNotApplicableError
from ray_doris._models import (
    FlightScheme,
    HttpScheme,
    NativeReadOptions,
    QueryPlanPolicy,
    Transport,
)
from ray_doris.datasource import DorisDatasource

logger = logging.getLogger(__name__)
_NATIVE_MYSQL_OPTIONS = {
    "read_timeout",
    "write_timeout",
    "ssl",
    "ssl_ca",
    "ssl_cert",
    "ssl_key",
    "ssl_verify_cert",
    "ssl_verify_identity",
    "ssl_key_password",
    "ssl_disabled",
}


def read_doris(
    *,
    table: str,
    host: str,
    auto_native_read: bool = False,
    catalog: Optional[str] = None,
    native_options: Optional[NativeReadOptions] = None,
    mysql_port: int = 9030,
    http_port: int = 8030,
    flight_port: int = 8070,
    http_scheme: HttpScheme = "http",
    flight_scheme: FlightScheme = "grpc",
    user: str = "root",
    password: str = "",
    password_env: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
    filter: Optional[str] = None,
    transport: Transport = "mysql",
    on_query_plan_error: QueryPlanPolicy = "single_task",
    tablet_size: int = 1,
    batch_size: int = 10_000,
    connect_timeout: float = 10.0,
    query_plan_timeout: Optional[float] = None,
    http_ca_file: Optional[str] = None,
    client_kwargs: Optional[Mapping[str, Any]] = None,
    flight_options: Optional[Mapping[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    ray_remote_args: Optional[Mapping[str, Any]] = None,
) -> Dataset:
    """Read a table through Doris, optionally selecting native Hive Parquet reads.

    ``auto_native_read`` is disabled by default. When enabled, a compatible Hive
    table can use Ray's public Parquet reader with independent HMS/storage access
    and immutable published files. Known unsupported cases retain Doris reading;
    authentication, storage and inconsistent-metadata errors remain visible.
    This API accepts a table identifier, not a complete SQL statement. A selected
    native read never replays through Doris after execution begins.

    See {doc}`the native Hive Parquet guide </user-guide/native-read>` for
    per-file footer overhead, version limits and independent source authorization.
    Native reads do not execute Doris row filters or column masking.
    """
    if not isinstance(auto_native_read, bool):
        raise DorisConfigurationError("auto_native_read must be a boolean")
    if native_options is not None and not isinstance(native_options, NativeReadOptions):
        raise DorisConfigurationError("native_options must be NativeReadOptions")
    if not auto_native_read and native_options is not None:
        raise DorisConfigurationError("native_options require auto_native_read=True")
    datasource = DorisDatasource(
        table=table,
        host=host,
        catalog=catalog,
        mysql_port=mysql_port,
        http_port=http_port,
        flight_port=flight_port,
        http_scheme=http_scheme,
        flight_scheme=flight_scheme,
        user=user,
        password=password,
        password_env=password_env,
        columns=columns,
        filter=filter,
        transport=transport,
        on_query_plan_error=on_query_plan_error,
        tablet_size=tablet_size,
        batch_size=batch_size,
        connect_timeout=connect_timeout,
        query_plan_timeout=query_plan_timeout,
        http_ca_file=http_ca_file,
        client_kwargs=client_kwargs,
        flight_options=flight_options,
    )
    if auto_native_read:
        try:
            if catalog is None or catalog == "internal":
                raise _NativeReadNotApplicableError("native reads require an external Hive catalog")
            if (
                datasource.config.filter is not None
                or transport != "mysql"
                or tablet_size != 1
                or batch_size != 10_000
                or flight_options is not None
                or query_plan_timeout is not None
                or on_query_plan_error != "single_task"
                or http_port != 8030
                or flight_port != 8070
                or http_scheme != "http"
                or flight_scheme != "grpc"
                or http_ca_file is not None
                or set(datasource.config.mysql_options()).difference(_NATIVE_MYSQL_OPTIONS)
            ):
                raise _NativeReadNotApplicableError("explicit read options require Doris execution")
            native = import_module("ray_doris._native")
            return cast(
                Dataset,
                native.read_native(
                    datasource.config,
                    catalog,
                    native_options or NativeReadOptions(),
                    concurrency=concurrency,
                    override_num_blocks=override_num_blocks,
                    ray_remote_args=ray_remote_args,
                ),
            )
        except _NativeReadNotApplicableError as exc:
            logger.info("Doris read path=doris reason=%s", exc)
    else:
        logger.info("Doris read path=doris reason=automatic native reads disabled")
    return cast(
        Dataset,
        ray.data.read_datasource(
            datasource,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
            ray_remote_args=dict(ray_remote_args) if ray_remote_args is not None else None,
        ),
    )
