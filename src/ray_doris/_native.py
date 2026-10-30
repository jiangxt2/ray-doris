"""A bounded metadata bridge to public Ray readers; no custom file decoder."""

from __future__ import annotations

import logging
import posixpath
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Mapping, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import ray.data
from ray.data import Dataset
from ray.data.datasource.partitioning import Partitioning, PartitionStyle, PathPartitionParser

from ray_doris._compat import (
    ensure_native_partitioning,
    ensure_native_ray_version,
    native_parquet_options,
    native_uses_datasource_v2,
)
from ray_doris._errors import (
    DorisConfigurationError,
    DorisPlanningError,
    DorisSchemaError,
    _NativeReadNotApplicableError,
)
from ray_doris._external_metadata import ExternalMetadata, discover_external, partition_value
from ray_doris._models import DorisReadConfig, NativeReadOptions
from ray_doris._sql import validate_identifier

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NativeReadPlan:
    schema: pa.Schema
    files: Tuple[str, ...] = field(repr=False)
    filesystem: pafs.FileSystem = field(repr=False)
    base_dir: str = field(repr=False)
    partition_keys: Tuple[Tuple[str, str], ...]


def _location(value: str) -> Tuple[str, str, str]:
    parsed = urlparse(value)
    scheme = "s3" if parsed.scheme == "s3a" else parsed.scheme
    if scheme not in ("s3", "hdfs"):
        raise _NativeReadNotApplicableError("native reads require HDFS or S3 storage")
    if not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise DorisPlanningError("native reads require qualified HDFS or S3 locations")
    return scheme, parsed.netloc, parsed.path


def plan_files(metadata: ExternalMetadata, options: NativeReadOptions) -> NativeReadPlan:
    if metadata.partition_keys:
        ensure_native_partitioning()
    scheme, authority, root = _location(metadata.storage.location)
    normalized_uri = urlunparse((scheme, authority, root, "", "", ""))
    try:
        if options.filesystem is None:
            filesystem, base = pafs.FileSystem.from_uri(normalized_uri)
        else:
            filesystem = options.filesystem
            base = authority + root if scheme == "s3" else root
        if filesystem.type_name != scheme:
            raise DorisConfigurationError("filesystem must match the HMS storage scheme")
        base = filesystem.normalize_path(base).rstrip("/")
        files = []
        paths = set()
        if metadata.partition_keys:
            sources = tuple((part.storage.location, part.values) for part in metadata.partitions)
        else:
            sources = ((metadata.storage.location, ()),)
        for uri, values in sources:
            part_scheme, part_authority, path = _location(uri)
            if (part_scheme, part_authority) != (scheme, authority):
                raise _NativeReadNotApplicableError("mixed storage authorities are unsupported")
            location = filesystem.normalize_path(authority + path if scheme == "s3" else path)
            location = location.rstrip("/")
            if metadata.partition_keys:
                if not location.startswith(base + "/"):
                    raise _NativeReadNotApplicableError("custom partition LOCATION is unsupported")
                relative = location[len(base) + 1 :].split("/")
                if len(relative) != len(metadata.partition_keys):
                    raise _NativeReadNotApplicableError(
                        "partition LOCATION must use standard Hive levels"
                    )
                # Use Ray's documented parser for the same mapping used by its reader.
                parser = PathPartitionParser(
                    Partitioning(
                        PartitionStyle.HIVE,
                        base_dir=base,
                        field_names=[key for key, _ in metadata.partition_keys],
                        field_types={
                            key: int if dtype.lower() == "bigint" else str
                            for key, dtype in metadata.partition_keys
                        },
                        filesystem=filesystem,
                    )
                )
                parsed = parser(location + "/native-placeholder")
                expected = {
                    key: partition_value(value, dtype)
                    for (key, dtype), value in zip(metadata.partition_keys, values)
                }
                if parsed != expected:
                    raise DorisPlanningError("partition path and registered values differ")
            if location in paths:
                raise _NativeReadNotApplicableError("duplicate partition locations are unsupported")
            paths.add(location)
            for info in filesystem.get_file_info(pafs.FileSelector(location, recursive=False)):
                if posixpath.basename(info.path).startswith((".", "_")):
                    continue
                if info.type != pafs.FileType.File:
                    raise _NativeReadNotApplicableError(
                        "native reads do not support visible data subdirectories"
                    )
                files.append(info.path)
                if len(files) > options.max_files:
                    raise _NativeReadNotApplicableError("file enumeration exceeds max_files")
        return NativeReadPlan(
            metadata.schema, tuple(sorted(files)), filesystem, base, metadata.partition_keys
        )
    except (DorisPlanningError, DorisConfigurationError):
        raise
    except Exception:
        raise DorisPlanningError(
            "storage metadata discovery failed; verify source access"
        ) from None


def _validate_file_schemas(plan: NativeReadPlan) -> int:
    """Check every physical schema and return footer row counts without decoding data."""
    partition_names = {name for name, _ in plan.partition_keys}
    expected = {
        column.name: column.type for column in plan.schema if column.name not in partition_names
    }
    rows = 0
    for path in plan.files:
        try:
            with plan.filesystem.open_input_file(path) as source:
                parquet = pq.ParquetFile(source)
                physical = parquet.schema_arrow
                rows += parquet.metadata.num_rows
        except Exception:
            raise DorisPlanningError(
                "Parquet footer discovery failed; verify source access"
            ) from None
        if len(physical.names) != len(set(physical.names)) or set(physical.names) != set(expected):
            raise DorisSchemaError("Parquet data column identities differ from Doris/HMS")
        for column in physical:
            target = expected[column.name]
            timestamp = (
                pa.types.is_timestamp(column.type)
                and pa.types.is_timestamp(target)
                and column.type.tz in (None, "UTC")
            )
            if column.type != target and not timestamp:
                raise DorisSchemaError("Parquet data column types differ from Doris/HMS")
    return rows


def _canonicalize_batch(batch: pa.Table, *, schema: pa.Schema) -> pa.Table:
    """Restore logical order/nullability after Ray adds path-derived partition values."""
    return batch.select(schema.names).cast(schema, safe=True)


def read_native_plan(
    plan: NativeReadPlan,
    columns: Optional[Tuple[str, ...]],
    *,
    concurrency: Optional[int],
    override_num_blocks: Optional[int],
    ray_remote_args: Optional[Mapping[str, Any]],
) -> Dataset:
    selected = (
        plan.schema if columns is None else pa.schema([plan.schema.field(c) for c in columns])
    )
    if not plan.files:
        return ray.data.from_arrow(pa.Table.from_batches([], schema=selected))
    use_v2 = native_uses_datasource_v2()
    partition_names = {name for name, _ in plan.partition_keys}
    if use_v2 and "path" in plan.schema.names and "path" not in partition_names:
        raise _NativeReadNotApplicableError(
            "the configured Ray V2 reader cannot preserve the data column 'path'; "
            "explicitly configure "
            "DataContext.use_datasource_v2=False to read this table"
        )
    if use_v2 and any(not path.endswith(".parquet") for path in plan.files):
        raise _NativeReadNotApplicableError(
            "the configured Ray V2 reader filters extensionless Parquet; explicitly configure "
            "DataContext.use_datasource_v2=False to read this layout"
        )
    if _validate_file_schemas(plan) == 0:
        return ray.data.from_arrow(pa.Table.from_batches([], schema=selected))
    partitioning = None
    if plan.partition_keys:
        ensure_native_partitioning()
        partitioning = Partitioning(
            PartitionStyle.HIVE,
            base_dir=plan.base_dir,
            field_names=[key for key, _ in plan.partition_keys],
            field_types={
                key: int if dtype.lower() == "bigint" else str for key, dtype in plan.partition_keys
            },
            filesystem=plan.filesystem,
        )
    dataset = ray.data.read_parquet(
        list(plan.files),
        filesystem=plan.filesystem,
        columns=list(columns) if columns is not None else None,
        file_extensions=None,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
        ray_remote_args=dict(ray_remote_args) if ray_remote_args is not None else {},
        **native_parquet_options(
            base_dir=plan.base_dir, filesystem=plan.filesystem, partitioning=partitioning
        ),
    )
    # Let Ray infer the physical data schema and synthesize partitions first.
    # Pinning the full logical schema makes V1 retain NULL partition placeholders.
    return dataset.map_batches(
        partial(_canonicalize_batch, schema=selected),
        batch_format="pyarrow",
        batch_size=None,
        concurrency=concurrency,
        **(dict(ray_remote_args) if ray_remote_args is not None else {}),
    )


def read_native(
    config: DorisReadConfig,
    catalog: str,
    options: NativeReadOptions,
    *,
    concurrency: Optional[int],
    override_num_blocks: Optional[int],
    ray_remote_args: Optional[Mapping[str, Any]],
) -> Dataset:
    ensure_native_ray_version()
    validate_identifier(catalog, kind="catalog name")
    metadata = discover_external(config, catalog, options)
    if config.columns is not None and any(c not in metadata.schema.names for c in config.columns):
        raise DorisConfigurationError("native projection includes an unknown column")
    plan = plan_files(metadata, options)
    dataset = read_native_plan(
        plan,
        config.columns,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
        ray_remote_args=ray_remote_args,
    )
    logger.info(
        "Doris read path=native experimental=true reader=read_parquet "
        "doris_version=%s partitions=%d files=%d",
        ".".join(str(part) for part in metadata.version) or "unknown",
        len(metadata.partitions),
        len(plan.files),
    )
    return dataset
