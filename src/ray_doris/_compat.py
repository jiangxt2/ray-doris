"""Small, explicit compatibility layer for supported Ray V1 releases."""

from __future__ import annotations

import inspect
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal, Mapping, Optional

import pyarrow as pa
import ray
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasink, ReadTask

from ray_doris._errors import DorisConfigurationError

_MIN_RAY_VERSION = (2, 49, 2)
_MAX_RAY_VERSION = (2, 59, 0)
_RAY_VERSION = ray.__version__
_FINAL_RELEASE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:\+[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*)?$")


def ensure_supported_ray_version() -> None:
    """Reject Ray releases outside the compatibility matrix before network access."""
    match = _FINAL_RELEASE.fullmatch(_RAY_VERSION)
    release = tuple(int(part) for part in match.groups()) if match is not None else ()
    if not _MIN_RAY_VERSION <= release < _MAX_RAY_VERSION:
        raise DorisConfigurationError(
            "ray-doris supports final Ray releases >=2.49.2,<2.59; "
            "PEP 440 local build suffixes are allowed, but prerelease and post-release builds "
            f"are unsupported; found Ray {_RAY_VERSION!r}"
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
            "Ray >=2.49.2,<2.59 compatibility window"
        )
    kwargs["schema"] = schema
    if "per_task_row_limit" in parameters:
        kwargs["per_task_row_limit"] = per_task_row_limit
    elif per_task_row_limit is not None:
        raise DorisConfigurationError(
            "this Ray version does not support non-null per_task_row_limit"
        )
    return ReadTask(**kwargs)


@dataclass(frozen=True)
class DatasinkCompatibility:
    """Public Ray Datasink lifecycle facts for one supported Ray generation."""

    on_write_start_has_schema: bool
    on_write_start_on_empty_dataset: bool
    schema_source: Literal["first_input_bundle", "worker_blocks"]


def _parse_final_release(version: str) -> tuple[int, int, int]:
    match = _FINAL_RELEASE.fullmatch(version)
    if match is None:
        raise DorisConfigurationError(f"unsupported Ray release format: {version!r}")
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def datasink_compatibility(version: Optional[str] = None) -> DatasinkCompatibility:
    """Return the explicit Datasink callback contract for a supported Ray release."""
    ensure_supported_ray_version()
    release = _parse_final_release(_RAY_VERSION if version is None else version)
    if release < (2, 53, 0):
        return DatasinkCompatibility(False, True, "worker_blocks")
    return DatasinkCompatibility(True, False, "first_input_bundle")


def validate_datasink_signature() -> DatasinkCompatibility:
    """Validate the installed public Datasink signature against the release matrix."""
    contract = datasink_compatibility()
    parameters = inspect.signature(Datasink.on_write_start).parameters
    has_schema = "schema" in parameters
    if has_schema != contract.on_write_start_has_schema:
        raise DorisConfigurationError(
            "installed Ray Datasink.on_write_start signature does not match the supported "
            f"Ray {_RAY_VERSION} compatibility contract"
        )
    return contract


def prepare_write_remote_args(
    ray_remote_args: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """Freeze write task options and reject configurations that can replay side effects."""
    args = dict(ray_remote_args) if ray_remote_args is not None else {}
    for name in ("max_retries", "max_task_retries"):
        if name not in args:
            continue
        value = args[name]
        if isinstance(value, bool) or not isinstance(value, int) or value != 0:
            raise DorisConfigurationError(
                "Doris writes require Ray write task retry options to be exactly zero"
            )
    if args.get("retry_exceptions"):
        raise DorisConfigurationError(
            "Doris writes do not support Ray exception retries because a request may already "
            "have reached Doris"
        )
    args["max_retries"] = 0
    return args
