"""Ray Data V1 adapter for the shared Doris planner and readers."""

from __future__ import annotations

from functools import partial
from typing import Any, Callable, List, Mapping, Optional, Sequence, cast

import pyarrow as pa
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask

from ray_doris._compat import make_read_task
from ray_doris._models import (
    DorisPlanningSnapshot,
    DorisReadConfig,
    FlightScheme,
    HttpScheme,
    QueryPlanPolicy,
    Transport,
)
from ray_doris._planner import DorisPlanner, _validate_parallelism
from ray_doris._readers import _ensure_flight_available, read_split
from ray_doris._sql import normalize_columns, normalize_filter, parse_table


def _read_empty(schema: pa.Schema) -> List[pa.Table]:
    """Return one schema-carrying Arrow block without querying Doris."""
    return [pa.Table.from_batches([], schema=schema)]


class DorisDatasource(Datasource):
    """A public Ray V1 datasource that reads one internal-catalog Doris table.

    Each instance caches the schema and tablet discovery result from its first
    planning call. Create a new instance to discover later table changes. This
    planning cache does not provide snapshot isolation.
    """

    def __init__(
        self,
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
        columns: Optional[Sequence[str]] = None,
        filter: Optional[str] = None,
        transport: Transport = "mysql",
        on_query_plan_error: QueryPlanPolicy = "single_task",
        tablet_size: int = 1,
        batch_size: int = 10_000,
        connect_timeout: float = 10.0,
        client_kwargs: Optional[Mapping[str, Any]] = None,
        flight_options: Optional[Mapping[str, Any]] = None,
    ) -> None:
        # Ray exposes an untyped Datasource.__init__ across supported releases.
        initialize_datasource = cast(Callable[[], None], super().__init__)
        initialize_datasource()
        self._config = DorisReadConfig.from_options(
            table=parse_table(table),
            host=host,
            mysql_port=mysql_port,
            http_port=http_port,
            flight_port=flight_port,
            http_scheme=http_scheme,
            flight_scheme=flight_scheme,
            user=user,
            password=password,
            columns=normalize_columns(columns),
            filter=normalize_filter(filter),
            transport=transport,
            on_query_plan_error=on_query_plan_error,
            tablet_size=tablet_size,
            batch_size=batch_size,
            connect_timeout=connect_timeout,
            client_options=client_kwargs,
            flight_options=flight_options,
        )
        self._planning_snapshot: Optional[DorisPlanningSnapshot] = None
        if self._config.transport == "flight" or (
            self._config.transport == "auto" and self._config.flight_scheme == "grpc+tls"
        ):
            _ensure_flight_available()

    @property
    def config(self) -> DorisReadConfig:
        """Return the immutable datasource configuration."""
        return self._config

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Avoid an expensive COUNT or sampling query during Ray auto-parallelism."""
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[Any] = None,
    ) -> List[ReadTask]:
        """Plan tablet splits on the driver and create serializable worker reads."""
        del data_context
        _validate_parallelism(parallelism)
        planner = DorisPlanner(self._config)
        if self._planning_snapshot is None:
            self._planning_snapshot = planner.discover()
        plan = planner.plan_from_snapshot(self._planning_snapshot, parallelism)
        if not plan.splits:
            metadata = BlockMetadata(
                num_rows=0,
                size_bytes=0,
                exec_stats=None,
                input_files=None,
            )
            return [
                make_read_task(
                    partial(_read_empty, plan.schema),
                    metadata,
                    plan.schema,
                    per_task_row_limit,
                )
            ]
        tasks = []
        for split in plan.splits:
            read_fn = partial(read_split, self._config, split, plan.schema)
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                exec_stats=None,
                input_files=None,
            )
            tasks.append(make_read_task(read_fn, metadata, plan.schema, per_task_row_limit))
        return tasks
