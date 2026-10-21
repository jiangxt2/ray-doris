"""Convenience entry point mirroring Ray Data reader functions."""

from __future__ import annotations

from importlib import import_module
from typing import Any, Literal, Mapping, Optional, Sequence, cast

import ray.data
from ray.data import Dataset

from ray_doris._errors import DorisConfigurationError
from ray_doris._models import (
    DorisReadConfig,
    FlightScheme,
    HttpScheme,
    NativeReadOptions,
    QueryPlanPolicy,
    Transport,
)
from ray_doris._sql import normalize_columns, parse_table
from ray_doris.datasource import DorisDatasource


def read_doris(
    *,
    table: str,
    host: str,
    read_mode: Literal["query", "native"] = "query",
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
    """Read an internal Doris table, or explicitly opt into native Hive Parquet reads.

    ``native`` delegates to Ray's public Parquet reader and requires a catalog,
    independent HMS/storage access and immutable published files. SQL filters
    and tablet/Flight settings are query-only. It never falls back to SQL.
    """
    if read_mode not in ("query", "native"):
        raise DorisConfigurationError("read_mode must be 'query' or 'native'")
    if read_mode == "native":
        if catalog is None:
            raise DorisConfigurationError("native reads require catalog")
        if (
            filter is not None
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
        ):
            raise DorisConfigurationError("native reads reject SQL filters and query-only options")
        if native_options is not None and not isinstance(native_options, NativeReadOptions):
            raise DorisConfigurationError("native_options must be NativeReadOptions")
        native = import_module("ray_doris._native")

        config = DorisReadConfig.from_options(
            table=parse_table(table),
            host=host,
            mysql_port=mysql_port,
            user=user,
            password=password,
            password_env=password_env,
            columns=normalize_columns(columns),
            connect_timeout=connect_timeout,
            client_options=client_kwargs,
        )
        return cast(
            Dataset,
            native.read_native(
                config,
                catalog,
                native_options or NativeReadOptions(),
                concurrency=concurrency,
                override_num_blocks=override_num_blocks,
                ray_remote_args=ray_remote_args,
            ),
        )
    if catalog is not None or native_options is not None:
        raise DorisConfigurationError("catalog and native_options require read_mode='native'")
    datasource = DorisDatasource(
        table=table,
        host=host,
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
    return cast(
        Dataset,
        ray.data.read_datasource(
            datasource,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
            ray_remote_args=dict(ray_remote_args) if ray_remote_args is not None else None,
        ),
    )
