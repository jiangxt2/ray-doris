"""Ray Data V1 adapter for the shared Doris planner and readers."""

from __future__ import annotations

from contextlib import closing
from copy import copy
from dataclasses import replace
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    cast,
)

import pyarrow as pa
from ray import cloudpickle
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask

from ray_doris._compat import (
    ensure_supported_ray_version,
    make_read_task,
    project_empty_block,
    supports_projection_pushdown,
)
from ray_doris._errors import DorisConfigurationError
from ray_doris._models import (
    DorisInputSplit,
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


def _read_zero_columns(
    config: DorisReadConfig, split: DorisInputSplit, schema: pa.Schema
) -> Iterator[pa.Table]:
    """Read with the existing physical schema and apply Ray's native empty projection."""
    # All transports return generators. Closing the wrapper must also close an
    # active reader when a consumer stops before EOF or projection fails.
    with closing(
        cast(Generator[pa.Table, None, None], read_split(config, split, schema))
    ) as blocks:
        for block in blocks:
            yield project_empty_block(block)


def _validate_config_serialization(config: DorisReadConfig) -> None:
    """Reject worker configuration that Ray cannot serialize without exposing values."""
    serialize = cast(Callable[[Any], bytes], cloudpickle.dumps)
    try:
        serialize(config)
    except Exception:
        raise DorisConfigurationError(
            "datasource configuration must contain only copyable and serializable values"
        ) from None


class DorisDatasource(Datasource):
    """A Ray V1 datasource that reads one internal-catalog Doris table.

    This extension uses Ray's documented ``Datasource`` API and the
    ``ReadTask`` DeveloperAPI. It never imports Ray Data private modules.

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
    ) -> None:
        ensure_supported_ray_version()
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
            password_env=password_env,
            columns=normalize_columns(columns),
            filter=normalize_filter(filter),
            transport=transport,
            on_query_plan_error=on_query_plan_error,
            tablet_size=tablet_size,
            batch_size=batch_size,
            connect_timeout=connect_timeout,
            query_plan_timeout=query_plan_timeout,
            http_ca_file=http_ca_file,
            client_options=client_kwargs,
            flight_options=flight_options,
        )
        _validate_config_serialization(self._config)
        self._projection_columns = self._config.columns
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

    def supports_projection_pushdown(self) -> bool:
        """Advertise only the Ray releases with verified projection contracts."""
        return supports_projection_pushdown()

    def get_projection_map(self) -> Optional[Dict[str, str]]:
        """Return a fresh identity map without exposing mutable source state."""
        if self._projection_columns is None:
            return None
        return {name: name for name in self._projection_columns}

    def apply_projection(self, projection_map: Optional[Dict[str, str]]) -> DorisDatasource:
        """Clone this source with a narrower, ordered projection and isolated planning."""
        columns: Optional[Tuple[str, ...]] = self._projection_columns
        if projection_map is not None:
            columns = normalize_columns(projection_map) if projection_map else ()
            if self._projection_columns is not None and any(
                name not in self._projection_columns for name in columns or ()
            ):
                raise DorisConfigurationError("projection cannot restore columns outside this read")
        clone = copy(self)
        if columns != self._projection_columns:
            clone._projection_columns = columns
            # A zero-column projection retains the previous physical read. Its
            # blocks are projected with Ray's public API after bounded reading.
            if columns:
                clone._config = replace(self._config, columns=columns)
            clone._planning_snapshot = None
        return clone

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
        zero_columns = self._projection_columns == ()
        schema = (
            project_empty_block(pa.Table.from_batches([], schema=plan.schema)).schema
            if zero_columns
            else plan.schema
        )
        if not plan.splits:
            metadata = BlockMetadata(
                num_rows=0,
                size_bytes=0,
                exec_stats=None,
                input_files=None,
            )
            return [
                make_read_task(
                    partial(_read_empty, schema),
                    metadata,
                    schema,
                    per_task_row_limit,
                )
            ]
        tasks = []
        for split in plan.splits:
            reader = _read_zero_columns if zero_columns else read_split
            read_fn = partial(reader, self._config, split, plan.schema)
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                exec_stats=None,
                input_files=None,
            )
            tasks.append(make_read_task(read_fn, metadata, schema, per_task_row_limit))
        return tasks
