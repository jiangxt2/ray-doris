"""Real Parquet and public Ray reader contracts, independent of HMS services."""

import os
from datetime import datetime, timezone

os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")

import pyarrow as pa  # noqa: E402
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pytest
import ray

from ray_doris import DorisSchemaError
from ray_doris._errors import _NativeReadNotApplicableError
from ray_doris._native import NativeReadPlan, read_native_plan

pytestmark = pytest.mark.contract
HAS_READER_SWITCH = hasattr(ray.data.DataContext.get_current(), "use_datasource_v2")


@pytest.fixture(scope="module", autouse=True)
def native_ray():
    ray.init(num_cpus=2, include_dashboard=False, _temp_dir="/tmp/rd-native-contract")
    yield
    ray.shutdown()


@pytest.fixture(params=[False, True] if HAS_READER_SWITCH else [False])
def reader_version(request):
    context = ray.data.DataContext.get_current()
    previous = context.use_datasource_v2 if HAS_READER_SWITCH else None
    if HAS_READER_SWITCH:
        context.use_datasource_v2 = request.param
    yield request.param
    if HAS_READER_SWITCH:
        context.use_datasource_v2 = previous


@pytest.mark.parametrize("keys", [(), (("p", "bigint"),), (("p", "BIGINT"),)])
@pytest.mark.parametrize("projection", [None, ("id",), ("p", "id"), ("p",)])
def test_real_native_reader_preserves_schema_and_projection(
    tmp_path, keys, projection, reader_version
):
    partitioned = bool(keys)
    columns = [("id", pa.int32()), ("p", pa.int64())]
    schema = pa.schema(columns)
    directory = tmp_path / "p=7" if partitioned else tmp_path
    directory.mkdir(exist_ok=True)
    file = directory / "data.parquet"
    data = {"id": pa.array([1, 2], type=pa.int32())}
    if not partitioned:
        data["p"] = pa.array([7, 7], type=pa.int64())
    pq.write_table(pa.table(data), file)
    plan = NativeReadPlan(schema, (str(file),), fs.LocalFileSystem(), str(tmp_path), keys)
    ds = read_native_plan(
        plan, projection, concurrency=2, override_num_blocks=2, ray_remote_args=None
    )
    rows = ds.take_all()
    expected = [{"id": 1, "p": 7}, {"id": 2, "p": 7}]
    if projection:
        expected = [{key: row[key] for key in projection} for row in expected]
    assert sorted(rows, key=lambda r: r.get("id", 0)) == expected
    assert list(rows[0]) == list(projection or schema.names)
    assert ds.schema().base_schema == (
        schema if projection is None else pa.schema([schema.field(c) for c in projection])
    )


def test_native_empty_dataset_preserves_projected_schema(tmp_path):
    schema = pa.schema([("id", pa.int32()), ("p", pa.string())])
    plan = NativeReadPlan(schema, (), fs.LocalFileSystem(), str(tmp_path), (("p", "string"),))
    ds = read_native_plan(
        plan, ("p",), concurrency=None, override_num_blocks=None, ray_remote_args=None
    )
    assert ds.count() == 0
    assert ds.schema().base_schema == pa.schema([("p", pa.string())])


def test_unpartitioned_projection_ignores_partition_like_parent(tmp_path, reader_version):
    root = tmp_path / "p=parent"
    root.mkdir()
    file = root / "data.parquet"
    schema = pa.schema([("id", pa.int32()), ("p", pa.string())])
    pq.write_table(pa.Table.from_pylist([{"id": 7, "p": "physical"}], schema=schema), file)
    plan = NativeReadPlan(schema, (str(file),), fs.LocalFileSystem(), str(root), ())
    dataset = read_native_plan(
        plan, ("p", "id"), concurrency=1, override_num_blocks=1, ray_remote_args=None
    )
    assert dataset.take_all() == [{"p": "physical", "id": 7}]
    assert dataset.schema().base_schema == pa.schema([schema.field("p"), schema.field("id")])


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("projection", [None, ("p", "id")])
def test_zero_row_parquet_files_preserve_logical_schema(
    tmp_path, partitioned, projection, reader_version
):
    schema = pa.schema([("id", pa.int32()), ("p", pa.int64())])
    physical = pa.schema([schema.field("id")]) if partitioned else schema
    directory = tmp_path / "p=7" if partitioned else tmp_path
    directory.mkdir(exist_ok=True)
    file = directory / "empty.parquet"
    pq.write_table(pa.Table.from_batches([], schema=physical), file)
    plan = NativeReadPlan(
        schema,
        (str(file),),
        fs.LocalFileSystem(),
        str(tmp_path),
        (("p", "bigint"),) if partitioned else (),
    )
    dataset = read_native_plan(
        plan, projection, concurrency=1, override_num_blocks=1, ray_remote_args=None
    )
    assert dataset.count() == 0
    assert dataset.schema().base_schema == (
        schema if projection is None else pa.schema([schema.field(name) for name in projection])
    )


def test_extensionless_v2_rejected_and_public_v1_works(tmp_path):
    file = tmp_path / "extensionless"
    schema = pa.schema([("id", pa.int32())])
    pq.write_table(pa.table({"id": pa.array([1], type=pa.int32())}), file)
    plan = NativeReadPlan(schema, (str(file),), fs.LocalFileSystem(), str(tmp_path), ())
    context = ray.data.DataContext.get_current()
    previous = context.use_datasource_v2 if HAS_READER_SWITCH else None
    try:
        if HAS_READER_SWITCH:
            context.use_datasource_v2 = True
            with pytest.raises(_NativeReadNotApplicableError, match="extensionless"):
                read_native_plan(
                    plan, None, concurrency=None, override_num_blocks=None, ray_remote_args=None
                )
            context.use_datasource_v2 = False
        assert read_native_plan(
            plan, None, concurrency=None, override_num_blocks=None, ray_remote_args=None
        ).take_all() == [{"id": 1}]
    finally:
        if HAS_READER_SWITCH:
            context.use_datasource_v2 = previous


@pytest.mark.parametrize("projection", [None, ("path",), ("path", "id")])
def test_data_column_path_is_rejected_on_v2_and_preserved_on_v1(
    tmp_path, projection, reader_version
):
    table = pa.table({"id": pa.array([7], type=pa.int32()), "path": ["source-value"]})
    file = tmp_path / "data.parquet"
    pq.write_table(table, file)
    plan = NativeReadPlan(table.schema, (str(file),), fs.LocalFileSystem(), str(tmp_path), ())
    if reader_version:
        with pytest.raises(_NativeReadNotApplicableError, match="data column 'path'"):
            read_native_plan(
                plan, projection, concurrency=1, override_num_blocks=1, ray_remote_args=None
            )
    else:
        dataset = read_native_plan(
            plan, projection, concurrency=1, override_num_blocks=1, ray_remote_args=None
        )
        names = list(projection or table.schema.names)
        assert dataset.take_all() == [{name: table.to_pylist()[0][name] for name in names}]
        assert dataset.schema().names == names


@pytest.mark.parametrize("physical_name", ["ID", "renamed"])
def test_physical_field_mismatch_never_becomes_null(tmp_path, physical_name, reader_version):
    file = tmp_path / "data.parquet"
    pq.write_table(pa.table({physical_name: pa.array([7], type=pa.int32())}), file)
    plan = NativeReadPlan(
        pa.schema([("id", pa.int32())]), (str(file),), fs.LocalFileSystem(), str(tmp_path), ()
    )
    with pytest.raises(DorisSchemaError, match="column identities"):
        read_native_plan(plan, None, concurrency=1, override_num_blocks=1, ray_remote_args=None)


def test_every_file_footer_is_checked(tmp_path, reader_version):
    schema = pa.schema([("id", pa.int32())])
    files = []
    for name, column in [("first", "id"), ("second", "ID")]:
        file = tmp_path / (name + ".parquet")
        pq.write_table(pa.table({column: pa.array([7], type=pa.int32())}), file)
        files.append(str(file))
    plan = NativeReadPlan(schema, tuple(files), fs.LocalFileSystem(), str(tmp_path), ())
    with pytest.raises(DorisSchemaError, match="column identities"):
        read_native_plan(plan, None, concurrency=1, override_num_blocks=1, ray_remote_args=None)


@pytest.mark.parametrize("kind", ["type", "partition-column"])
def test_incompatible_physical_fields_are_rejected(tmp_path, kind, reader_version):
    data = {"id": ["seven"]} if kind == "type" else {"id": pa.array([7], type=pa.int32()), "p": [9]}
    file = tmp_path / "data.parquet"
    pq.write_table(pa.table(data), file)
    schema = pa.schema([("id", pa.int32())] + ([("p", pa.int64())] if kind != "type" else []))
    plan = NativeReadPlan(
        schema,
        (str(file),),
        fs.LocalFileSystem(),
        str(tmp_path),
        (("p", "bigint"),) if kind != "type" else (),
    )
    with pytest.raises(DorisSchemaError):
        read_native_plan(plan, None, concurrency=1, override_num_blocks=1, ray_remote_args=None)


def test_utc_timestamp_and_logical_nullability_are_preserved(tmp_path, reader_version):
    file = tmp_path / "data.parquet"
    physical = pa.schema(
        [pa.field("id", pa.int32(), nullable=False), ("ts", pa.timestamp("us", tz="UTC"))]
    )
    pq.write_table(
        pa.Table.from_pylist(
            [{"id": 7, "ts": datetime(2026, 10, 20, 12, tzinfo=timezone.utc)}], schema=physical
        ),
        file,
    )
    logical = pa.schema([("id", pa.int32()), ("ts", pa.timestamp("us"))])
    plan = NativeReadPlan(logical, (str(file),), fs.LocalFileSystem(), str(tmp_path), ())
    dataset = read_native_plan(
        plan, None, concurrency=1, override_num_blocks=1, ray_remote_args=None
    )
    assert dataset.take_all() == [{"id": 7, "ts": datetime(2026, 10, 20, 12)}]
    assert dataset.schema().base_schema == logical
