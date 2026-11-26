"""Coordinate a streaming Iceberg read and its snapshot-pinned Doris fallback."""

from __future__ import annotations

import logging
from dataclasses import replace
from functools import partial
from typing import Any, Callable, Iterable, Iterator, Optional, cast

import pyarrow as pa
import ray.data
from ray.data import Dataset
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask

from ray_doris._compat import ensure_iceberg_stream_context, make_read_task
from ray_doris._errors import (
    DorisReadError,
    _diagnostic_suffix,
    _UnsafeIcebergFallbackError,
)
from ray_doris._models import DorisInputSplit, DorisReadConfig
from ray_doris._readers import read_split

logger = logging.getLogger(__name__)


def _stream_iceberg_or_doris(
    dataset: Dataset, config: DorisReadConfig, schema: pa.Schema, check_identity: Callable[[], None]
) -> Iterator[pa.Table]:
    """Switch once before this datasource yields its first block to Ray."""
    emitted = False
    try:
        check_identity()
        ensure_iceberg_stream_context(ray.data.DataContext.get_current())
        # Ray's generic UDF wrapper discards the underlying exception type when
        # serialized; preserve it so identity conflicts cannot become fallbacks.
        dataset.context.raise_original_map_exception = True
        batches = iter(
            cast(
                Iterable[pa.Table],
                dataset.iter_batches(batch_size=None, batch_format="pyarrow", prefetch_batches=0),
            )
        )
        try:
            for batch in batches:
                emitted = True
                yield batch
        finally:
            close = getattr(batches, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    logger.warning("Failed to close the Iceberg batch iterator")
    except Exception as error:
        if emitted:
            raise DorisReadError(
                f"Iceberg native read failed after output{_diagnostic_suffix(error)}; "
                "disable auto_native_read and retry"
            ) from None
        if isinstance(error, _UnsafeIcebergFallbackError):
            raise
        logger.warning(
            "Iceberg native read failed before output; falling back to the pinned Doris snapshot "
            "error_type=%s",
            type(error).__name__,
        )
        yield from read_split(config, DorisInputSplit(None), schema)


class _IcebergStreamDatasource(Datasource):
    def __init__(
        self,
        dataset: Dataset,
        config: DorisReadConfig,
        schema: pa.Schema,
        check_identity: Callable[[], None],
    ) -> None:
        initialize = cast(Callable[[], None], super().__init__)
        initialize()
        self._dataset = dataset
        self._config = config
        self._schema = schema
        self._check_identity = check_identity

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[object] = None,
    ) -> list[ReadTask]:
        del parallelism, data_context
        metadata = BlockMetadata(num_rows=None, size_bytes=None, exec_stats=None, input_files=None)
        return [
            make_read_task(
                partial(
                    _stream_iceberg_or_doris,
                    self._dataset,
                    self._config,
                    self._schema,
                    self._check_identity,
                ),
                metadata,
                self._schema,
                per_task_row_limit,
            )
        ]


def stream_iceberg_with_fallback(
    dataset: Dataset,
    config: DorisReadConfig,
    snapshot_id: int,
    schema: pa.Schema,
    check_identity: Callable[[], None],
    runtime_env: Any = None,
    scheduling_strategy: Any = None,
) -> Dataset:
    """Return a lazy Dataset with one non-retrying streaming coordinator."""
    ensure_iceberg_stream_context(dataset.context)
    source = _IcebergStreamDatasource(
        dataset, replace(config, snapshot_id=snapshot_id), schema, check_identity
    )
    remote_args: dict[str, Any] = {"num_cpus": 0, "max_retries": 0}
    if runtime_env is not None:
        remote_args["runtime_env"] = runtime_env
    if scheduling_strategy is not None:
        remote_args["scheduling_strategy"] = scheduling_strategy
    result = cast(
        Dataset,
        ray.data.read_datasource(source, override_num_blocks=1, ray_remote_args=remote_args),
    )
    ensure_iceberg_stream_context(result.context)
    return result
