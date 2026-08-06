"""Small, explicit compatibility layer for supported Ray V1 releases."""

from __future__ import annotations

import inspect
from typing import Any, Callable, Iterable, Optional

import pyarrow as pa
from ray.data.block import BlockMetadata
from ray.data.datasource import ReadTask

from ray_doris._errors import DorisConfigurationError


def make_read_task(
    read_fn: Callable[[], Iterable[pa.Table]],
    metadata: BlockMetadata,
    schema: pa.Schema,
    per_task_row_limit: Optional[int],
) -> ReadTask:
    """Construct a ReadTask without hiding version or read-function TypeErrors."""
    parameters = inspect.signature(ReadTask).parameters
    kwargs: dict[str, Any] = {"read_fn": read_fn, "metadata": metadata}
    if "schema" not in parameters:
        raise DorisConfigurationError(
            "ray-doris requires Ray 2.48 or newer ReadTask schema support"
        )
    kwargs["schema"] = schema
    if "per_task_row_limit" in parameters:
        kwargs["per_task_row_limit"] = per_task_row_limit
    elif per_task_row_limit is not None:
        raise DorisConfigurationError(
            "this Ray version does not support non-null per_task_row_limit"
        )
    return ReadTask(**kwargs)
