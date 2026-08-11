"""Small, explicit compatibility layer for supported Ray V1 releases."""

from __future__ import annotations

import inspect
import re
from typing import Any, Callable, Iterable, Optional

import pyarrow as pa
import ray
from ray.data.block import BlockMetadata
from ray.data.datasource import ReadTask

from ray_doris._errors import DorisConfigurationError

_MIN_RAY_VERSION = (2, 49, 2)
_MAX_RAY_VERSION = (2, 57, 0)
_RAY_VERSION = ray.__version__
_FINAL_RELEASE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:\+[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*)?$")


def ensure_supported_ray_version() -> None:
    """Reject Ray releases outside the compatibility matrix before network access."""
    match = _FINAL_RELEASE.fullmatch(_RAY_VERSION)
    release = tuple(int(part) for part in match.groups()) if match is not None else ()
    if not _MIN_RAY_VERSION <= release < _MAX_RAY_VERSION:
        raise DorisConfigurationError(
            "ray-doris supports final Ray releases >=2.49.2,<2.57 "
            f"(local build suffixes are allowed); found Ray {_RAY_VERSION!r}"
        )


def make_read_task(
    read_fn: Callable[[], Iterable[pa.Table]],
    metadata: BlockMetadata,
    schema: pa.Schema,
    per_task_row_limit: Optional[int],
) -> ReadTask:
    """Construct a ReadTask without hiding version or read-function TypeErrors."""
    ensure_supported_ray_version()
    parameters = inspect.signature(ReadTask).parameters
    kwargs: dict[str, Any] = {"read_fn": read_fn, "metadata": metadata}
    if "schema" not in parameters:
        raise DorisConfigurationError(
            "installed Ray ReadTask lacks required schema support within the final "
            "Ray >=2.49.2,<2.57 compatibility window"
        )
    kwargs["schema"] = schema
    if "per_task_row_limit" in parameters:
        kwargs["per_task_row_limit"] = per_task_row_limit
    elif per_task_row_limit is not None:
        raise DorisConfigurationError(
            "this Ray version does not support non-null per_task_row_limit"
        )
    return ReadTask(**kwargs)
