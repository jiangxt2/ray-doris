"""Small, explicit compatibility layer for supported Ray V1 releases."""

from __future__ import annotations

import inspect
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal, Mapping, Optional

import pyarrow as pa
import ray
from ray.data.block import BlockAccessor, BlockMetadata
from ray.data.datasource import Datasink, Datasource, ReadTask
from ray.data.datasource.partitioning import Partitioning, PartitionStyle

from ray_doris._errors import DorisConfigurationError, _NativeReadNotApplicableError

_MIN_RAY_VERSION = (2, 49, 2)
_MAX_RAY_VERSION = (2, 60, 0)
_RAY_VERSION = ray.__version__
_FINAL_RELEASE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:\+[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*)?$")
_PROJECTION_RELEASES = {(2, 58, 0), (2, 59, 0)}


def supports_projection_pushdown() -> bool:
    """Enable projection only for releases with verified public read contracts."""
    if _parse_final_release(_RAY_VERSION) not in _PROJECTION_RELEASES:
        return False
    for name, parameters in (
        ("supports_projection_pushdown", ("self",)),
        ("get_projection_map", ("self",)),
        ("apply_projection", ("self", "projection_map")),
    ):
        method = getattr(Datasource, name, None)
        if not callable(method) or tuple(inspect.signature(method).parameters) != parameters:
            return False
    return tuple(inspect.signature(BlockAccessor.select).parameters) == ("self", "columns")


def project_empty_block(block: pa.Table) -> pa.Table:
    """Use Ray's public block API to preserve native empty-projection row counts."""
    selected = BlockAccessor.for_block(block).select([])
    return BlockAccessor.for_block(selected).to_arrow()


def ensure_supported_ray_version() -> None:
    """Reject Ray releases outside the compatibility matrix before network access."""
    match = _FINAL_RELEASE.fullmatch(_RAY_VERSION)
    release = tuple(int(part) for part in match.groups()) if match is not None else ()
    if not _MIN_RAY_VERSION <= release < _MAX_RAY_VERSION:
        raise DorisConfigurationError(
            "ray-doris supports final Ray releases >=2.49.2,<2.60; "
            "PEP 440 local build suffixes are allowed, but prerelease and post-release builds "
            f"are unsupported; found Ray {_RAY_VERSION!r}"
        )


def ensure_native_ray_version() -> None:
    """Check public reader capabilities within the package compatibility window."""
    ensure_supported_ray_version()
    required = {
        "paths",
        "filesystem",
        "columns",
        "partitioning",
        "file_extensions",
        "concurrency",
        "override_num_blocks",
        "ray_remote_args",
    }
    if not required <= inspect.signature(ray.data.read_parquet).parameters.keys():
        raise _NativeReadNotApplicableError("Ray lacks required public Parquet reader parameters")
    if (
        not {"batch_format", "batch_size", "concurrency"}
        <= inspect.signature(ray.data.Dataset.map_batches).parameters.keys()
    ):
        raise _NativeReadNotApplicableError("Ray lacks required public batch mapping parameters")


def native_uses_datasource_v2() -> bool:
    """Identify the configured reader or the supported legacy public signature."""
    ensure_native_ray_version()
    context = ray.data.DataContext.get_current()
    if hasattr(context, "use_datasource_v2"):
        value = context.use_datasource_v2
        if not isinstance(value, bool):
            raise _NativeReadNotApplicableError(
                "Ray reader selection is not a known boolean setting"
            )
        return value
    if "ignore_missing_paths" not in inspect.signature(ray.data.read_parquet).parameters:
        return False
    raise _NativeReadNotApplicableError("Ray reader generation cannot be established")


def native_parquet_options(
    *, base_dir: str, filesystem: Any, partitioning: Optional[Partitioning]
) -> dict[str, Any]:
    """Adapt public parameters without leaking new kwargs to legacy Arrow."""
    ensure_native_ray_version()
    options: dict[str, Any] = {}
    if "ignore_missing_paths" in inspect.signature(ray.data.read_parquet).parameters:
        options["ignore_missing_paths"] = False
    elif partitioning is None:
        # Legacy readers cannot combine projected columns with partitioning=None.
        # An explicit empty scheme retains projection without discovering parent keys.
        ensure_native_partitioning()
        partitioning = Partitioning(
            PartitionStyle.HIVE,
            base_dir=base_dir,
            field_names=[],
            field_types={},
            filesystem=filesystem,
        )
    options["partitioning"] = partitioning
    return options


def ensure_native_partitioning() -> None:
    """Require typed partition metadata only for partitioned sources."""
    if (
        not {"base_dir", "field_names", "field_types", "filesystem"}
        <= inspect.signature(Partitioning).parameters.keys()
    ):
        raise _NativeReadNotApplicableError("Ray lacks required typed Hive partitioning parameters")


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
            "Ray >=2.49.2,<2.60 compatibility window"
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


def supports_native_predicates() -> bool:
    """Enable only Ray minors with verified V1 expression and rejection contracts."""
    ensure_supported_ray_version()
    return (2, 57, 0) <= _parse_final_release(_RAY_VERSION) < (2, 60, 0)


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
