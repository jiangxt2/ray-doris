"""Convenience entry point mirroring Ray Data reader functions."""

from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence, cast

import ray.data
from ray.data import Dataset

from ray_doris._models import FlightScheme, HttpScheme, QueryPlanPolicy, Transport
from ray_doris.datasource import DorisDatasource


def read_doris(
    *,
    table: str,
    host: str,
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
    """Create a Ray Dataset by reading a Doris table in streaming tablet splits."""
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
