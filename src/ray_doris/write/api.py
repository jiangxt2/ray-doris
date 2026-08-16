"""Public facade for Ray Data Stream Load writes."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from ray.data import Dataset

from ray_doris._compat import prepare_write_remote_args
from ray_doris._errors import DorisWriteError
from ray_doris.write.connection import DorisConnection, DorisTable
from ray_doris.write.datasink import DorisDatasink, DorisWriteResult
from ray_doris.write.options import DorisWriteOptions, WriteFormat, WriteOperation


def write_doris(
    dataset: Dataset,
    *,
    connection: DorisConnection,
    table: DorisTable,
    operation: WriteOperation = "load",
    format: Optional[WriteFormat] = None,
    batch_rows: int = 65_536,
    batch_bytes: int = 64 * 1024 * 1024,
    max_filter_ratio: float = 0.0,
    strict_mode: bool = True,
    request_timeout_seconds: Optional[float] = None,
    load_properties: Optional[Mapping[str, str]] = None,
    ray_remote_args: Optional[Mapping[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> DorisWriteResult:
    """Write a Dataset through Ray's public ``Dataset.write_datasink`` API."""
    options = DorisWriteOptions.from_mapping(
        operation=operation,
        format=format,
        batch_rows=batch_rows,
        batch_bytes=batch_bytes,
        max_filter_ratio=max_filter_ratio,
        strict_mode=strict_mode,
        request_timeout_seconds=request_timeout_seconds,
        load_properties=load_properties,
    )
    sink = DorisDatasink(connection, table, options)
    dataset.write_datasink(
        sink,
        ray_remote_args=prepare_write_remote_args(ray_remote_args),
        concurrency=concurrency,
    )
    if sink.result is None:
        raise DorisWriteError("Doris Datasink completed without a driver write result")
    return sink.result
