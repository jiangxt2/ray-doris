"""Real Spark/HMS/HDFS/S3/Doris and distributed native reader integration."""

from __future__ import annotations

import importlib
import inspect
import json
import os
import time
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pymysql
import pytest
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from ray_doris import (
    DorisPermissionError,
    DorisPlanningError,
    NativeReadOptions,
    _external_metadata,
    _metadata,
    _native,
    read_doris,
)
from ray_doris._hms import open_hms

pytestmark = pytest.mark.integration
BASE = "hdfs://hms:9000/native"
PARQUET_SIGNATURE = inspect.signature(ray.data.read_parquet)
DATA_COLUMNS = [
    ("id", "int"),
    ("payload", "string"),
    ("amount", "decimal(12,2)"),
    ("day", "date"),
    ("ts", "timestamp"),
    ("flag", "boolean"),
    ("tiny", "tinyint"),
    ("small", "smallint"),
    ("large", "bigint"),
    ("real", "float"),
    ("score", "double"),
    ("maybe", "string"),
]


def sql(query, params=None):
    __tracebackhide__ = True
    try:
        with pymysql.connect(host="fe", port=9030, user="root", autocommit=True) as con:
            with con.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
    except pymysql.MySQLError:
        raise RuntimeError("integration SQL failed; inspect sanitized service logs") from None


def s3():
    return fs.S3FileSystem(
        access_key=os.environ["NATIVE_S3_USER"],
        secret_key=os.environ["NATIVE_S3_PASSWORD"],
        endpoint_override="minio:9000",
        scheme="http",
        region="us-east-1",
        allow_bucket_creation=True,
    )


def hms_fixture_call(client, method, *args):
    __tracebackhide__ = True
    try:
        return getattr(client, method)(*args)
    except Exception as error:
        message = str(error)
        for name in ["NATIVE_S3_USER", "NATIVE_S3_PASSWORD"]:
            message = message.replace(os.environ[name], "<redacted>")
        raise DorisPlanningError(
            f"fixture HMS {method}: {type(error).__name__}: {message[:1200]}"
        ) from None


def register_table(
    client, name, location, keys=(), params=None, table_type="EXTERNAL_TABLE", file_format="parquet"
):
    types = importlib.import_module("pymetastore.hive_metastore.ttypes")
    table = types.Table(
        tableName=name,
        dbName="native_it",
        owner="root",
        tableType=table_type,
        parameters={"EXTERNAL": "TRUE", **(params or {})},
        partitionKeys=[types.FieldSchema(name=k, type=t) for k, t in keys],
        sd=types.StorageDescriptor(
            cols=[types.FieldSchema(name=k, type=t) for k, t in DATA_COLUMNS],
            location=location,
            inputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            outputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            serdeInfo=types.SerDeInfo(
                serializationLib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            ),
        ),
    )
    if file_format == "orc":
        table.sd.inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
        table.sd.outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
        table.sd.serdeInfo.serializationLib = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
    hms_fixture_call(client, "create_table", table)
    return table


@pytest.fixture(scope="module", autouse=True)
def cluster():
    deadline = time.monotonic() + 300
    while True:
        try:
            assert sql("SELECT 1") == ((1,),)
            with open_hms("thrift://hms:9083", 10):
                pass
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)
    types = importlib.import_module("pymetastore.hive_metastore.ttypes")
    sql("SET GLOBAL time_zone = 'UTC'")
    sql("SET GLOBAL hive_parquet_use_column_names = true")
    hdfs = fs.HadoopFileSystem("hms", 9000, user="root")
    leading_ids = set()
    store = s3()
    store.create_dir("native-bucket")
    # The exact same Parquet bytes produced by Spark are shared through S3 too.
    fs.copy_files(
        "/native", "native-bucket/native", source_filesystem=hdfs, destination_filesystem=store
    )
    for filesystem, root in [(hdfs, "/native"), (store, "native-bucket/native")]:
        filesystem.create_dir(root + "/empty")
        with filesystem.open_output_stream(root + "/plain/_temporary") as stream:
            stream.write(b"not parquet")
        # A valid, unregistered partition must never contribute data.
        filesystem.create_dir(root + "/partitioned/p=decoy/q=99")
        files = filesystem.get_file_info(fs.FileSelector(root + "/plain"))
        source = next(
            i.path for i in files if i.type == fs.FileType.File and i.path.endswith("parquet")
        )
        filesystem.copy_file(source, root + "/partitioned/p=decoy/q=99/decoy.parquet")
    with open_hms("thrift://hms:9083", 10) as hms:
        client = hms._client
        if "native_it" in hms_fixture_call(client, "get_all_databases"):
            hms_fixture_call(client, "drop_database", "native_it", False, True)
        hms_fixture_call(
            client, "create_database", types.Database(name="native_it", locationUri=BASE)
        )
        for medium, root in [("hdfs", BASE), ("s3", "s3a://native-bucket/native")]:
            register_table(client, medium + "_plain", root + "/plain")
            register_table(client, medium + "_orc", root + "/orc", file_format="orc")
            register_table(client, medium + "_int96", root + "/int96")
            table = register_table(
                client,
                medium + "_partitioned",
                root + "/partitioned",
                (("p", "string"), ("q", "bigint")),
            )
            partitions = []
            for p in ["east", "west", "north/south"]:
                for q in ["0", "1", "2"]:
                    sd = deepcopy(table.sd)
                    sd.location += f"/p={quote(p, safe='')}/q={q}"
                    partitions.append(
                        types.Partition(
                            values=[p, q],
                            dbName="native_it",
                            tableName=medium + "_partitioned",
                            sd=sd,
                        )
                    )
            hms_fixture_call(client, "add_partitions", partitions)
            register_table(client, medium + "_empty", root + "/empty")
            empty_partition = register_table(
                client, medium + "_empty_partition", root + "/empty_partition", (("p", "string"),)
            )
            empty_partition.sd.location += "/p=empty"
            hms_fixture_call(
                client,
                "add_partition",
                types.Partition(
                    values=["empty"],
                    dbName="native_it",
                    tableName=medium + "_empty_partition",
                    sd=empty_partition.sd,
                ),
            )
        leading = register_table(client, "leading_zero", BASE + "/leading_zero", (("q", "bigint"),))
        source_files = sorted(
            info.path
            for info in hdfs.get_file_info(fs.FileSelector("/native/plain"))
            if info.type == fs.FileType.File and info.path.endswith(".parquet")
        )
        register_table(client, "extensionless", BASE + "/extensionless")
        hdfs.copy_file(source_files[0], "/native/extensionless/part")
        path_table = register_table(client, "reserved_path", BASE + "/reserved_path")
        path_table.sd.cols.append(types.FieldSchema(name="path", type="string"))
        hms_fixture_call(client, "alter_table", "native_it", "reserved_path", path_table)
        data = pq.read_table(source_files[0], filesystem=hdfs)
        data = data.append_column("path", pa.array(["stored-value"] * data.num_rows))
        with hdfs.open_output_stream("/native/reserved_path/data.parquet") as output:
            pq.write_table(data, output)
        for value, source_file in zip(["01", "1"], source_files[:2]):
            descriptor = deepcopy(leading.sd)
            descriptor.location += "/q=" + value
            hms_fixture_call(
                client,
                "add_partition",
                types.Partition(
                    values=[value], dbName="native_it", tableName="leading_zero", sd=descriptor
                ),
            )
            destination = "/native/leading_zero/q=" + value + "/data.parquet"
            hdfs.copy_file(source_file, destination)
            leading_ids.update(
                pq.read_table(source_file, filesystem=hdfs, columns=["id"])["id"].to_pylist()
            )
        missing_partition = register_table(
            client, "missing_partition", BASE + "/missing_partition", (("p", "string"),)
        )
        missing_partition.sd.location += "/p=gone"
        hms_fixture_call(
            client,
            "add_partition",
            types.Partition(
                values=["gone"],
                dbName="native_it",
                tableName="missing_partition",
                sd=missing_partition.sd,
            ),
        )
        hdfs.delete_dir("/native/missing_partition/p=gone")
        register_table(
            client,
            "acid",
            BASE + "/plain",
            params={
                "transactional": "true",
                "transactional_properties": "insert_only",
                "EXTERNAL": "FALSE",
            },
            table_type="MANAGED_TABLE",
        )
        register_table(client, "missing", BASE + "/missing")
        # HMS creates an external table's location during registration. Remove
        # it afterwards so this really tests a missing, still-registered path.
        hdfs.delete_dir("/native/missing")
        register_table(client, "nested", BASE + "/nested")
        hdfs.create_dir("/native/nested/visible-child")
        source = next(
            i.path
            for i in hdfs.get_file_info(fs.FileSelector("/native/plain"))
            if i.path.endswith(".parquet")
        )
        hdfs.copy_file(source, "/native/nested/visible-child/data.parquet")
        hdfs.create_dir("/native/runtime_failure")
        hdfs.copy_file(source, "/native/runtime_failure/data.parquet")
        register_table(client, "runtime_failure", BASE + "/runtime_failure")
        for name, value, location in [
            ("custom_location", "east", BASE + "/plain"),
            ("null_partition", "__HIVE_DEFAULT_PARTITION__", BASE + "/plain"),
        ]:
            table = register_table(client, name, BASE + "/" + name, (("p", "string"),))
            table.sd.location = location
            hms_fixture_call(
                client,
                "add_partition",
                types.Partition(values=[value], dbName="native_it", tableName=name, sd=table.sd),
            )
    sql(
        "CREATE CATALOG native_hive PROPERTIES ('type'='hms', "
        "'hive.metastore.uris'='thrift://hms:9083','hadoop.username'='root',"
        "'s3.endpoint'='http://minio:9000','s3.region'='us-east-1','use_path_style'='true',"
        "'s3.access_key'=%s,'s3.secret_key'=%s)",
        (os.environ["NATIVE_S3_USER"], os.environ["NATIVE_S3_PASSWORD"]),
    )
    sql(
        "CREATE CATALOG native_hive_table_schema PROPERTIES ('type'='hms', "
        "'hive.metastore.uris'='thrift://hms:9083','hadoop.username'='root',"
        "'get_schema_from_table'='true')"
    )
    ray.init(address="auto", log_to_driver=False)
    deadline = time.monotonic() + 90
    while len([n for n in ray.nodes() if n["Alive"] and n["Resources"].get("CPU", 0) > 0]) < 2:
        if time.monotonic() > deadline:
            raise RuntimeError("two Ray workers did not become available")
        time.sleep(1)
    yield {"leading_ids": leading_ids}
    ray.shutdown()


def reader(table, medium, *, catalog="native_hive", auto_native_read=True, **extra):
    return read_doris(
        host="fe",
        table="native_it." + table,
        catalog=catalog,
        auto_native_read=auto_native_read,
        native_options=NativeReadOptions(
            filesystem=s3() if medium == "s3" else fs.HadoopFileSystem("hms", 9000, user="root")
        )
        if auto_native_read
        else None,
        **extra,
    )


@pytest.mark.parametrize("medium", ["hdfs", "s3"])
@pytest.mark.parametrize("kind", ["plain", "partitioned", "int96"])
def test_native_matches_real_doris_and_spark(medium, kind):
    name = medium + "_" + kind
    statements = []
    original = _external_metadata._rows

    def record(cursor, statement, params=None):
        statements.append(statement)
        return original(cursor, statement, params)

    with (
        patch.object(_external_metadata, "_rows", record),
        patch(
            "ray_doris.datasource.DorisDatasource.get_read_tasks",
            side_effect=AssertionError("Doris data path used"),
        ),
    ):
        ds = reader(name, medium, override_num_blocks=8, concurrency=2)
        native = sorted(ds.take_all(), key=lambda row: row["id"])
    columns = ds.schema().names
    expected_schema = pa.schema(
        [
            ("id", pa.int32()),
            ("payload", pa.string()),
            ("amount", pa.decimal128(12, 2)),
            ("day", pa.date32()),
            ("ts", pa.timestamp("us")),
            ("flag", pa.bool_()),
            ("tiny", pa.int8()),
            ("small", pa.int16()),
            ("large", pa.int64()),
            ("real", pa.float32()),
            ("score", pa.float64()),
            ("maybe", pa.string()),
        ]
        + ([("p", pa.string()), ("q", pa.int64())] if kind == "partitioned" else [])
    )
    assert ds.schema().base_schema == expected_schema
    reference = sql(
        "SELECT "
        + ",".join(f"`{c}`" for c in columns)
        + f" FROM native_hive.native_it.{name} ORDER BY id"
    )
    assert [tuple(row[c] for c in columns) for row in native] == list(reference)
    assert len(native) == 240
    assert native[0]["amount"] == Decimal("0.00")
    assert native[-1]["id"] == 239
    assert all(
        "TABLET" not in s and name not in s.replace(name + "$partitions", "metadata")
        for s in statements
        if "information_schema" not in s
    )


@pytest.mark.parametrize("medium", ["hdfs", "s3"])
def test_projection_and_empty_schema(medium):
    rows = reader(medium + "_partitioned", medium, columns=["q", "id"]).take_all()
    assert len(rows) == 240 and list(rows[0]) == ["q", "id"]
    empty = reader(medium + "_empty", medium, columns=["id", "payload"])
    assert empty.count() == 0
    assert empty.schema().base_schema == pa.schema([("id", pa.int32()), ("payload", pa.string())])
    empty_partition = reader(medium + "_empty_partition", medium, columns=["p", "id"])
    assert empty_partition.count() == 0
    assert empty_partition.schema().base_schema == pa.schema(
        [("p", pa.string()), ("id", pa.int32())]
    )


def test_schema_from_table_catalog_uses_doris_columns():
    dataset = reader(
        "hdfs_partitioned", "hdfs", catalog="native_hive_table_schema", columns=["q", "id"]
    )
    actual = sorted((row["q"], row["id"]) for row in dataset.take_all())
    expected = sql(
        "SELECT q,id FROM native_hive_table_schema.native_it.hdfs_partitioned ORDER BY q,id"
    )
    assert actual == list(expected)
    assert dataset.schema().base_schema == pa.schema([("q", pa.int64()), ("id", pa.int32())])


@pytest.mark.parametrize("provider", ["catalog", "columns", "session", "partitions", "version"])
def test_metadata_capabilities_use_real_secondary_interfaces(provider):
    original = _external_metadata._rows
    statements = []

    def query(cursor, statement, params=None):
        statements.append(statement)
        if provider == "version" and "@@version_comment" in statement:
            return [{"build": "vendor build without a Doris version tag"}]
        if provider == "catalog" and "FROM catalogs()" in statement:
            raise pymysql.err.OperationalError(1305, "FUNCTION catalogs does not exist")
        if provider == "columns" and "information_schema" in statement:
            raise pymysql.err.OperationalError(1054, "Unknown column COLUMN_TYPE")
        if provider == "session" and "@@hive_parquet_use_column_names" in statement:
            raise pymysql.err.OperationalError(1193, "Unknown system variable")
        if provider == "partitions" and "$partitions" in statement:
            raise pymysql.err.OperationalError(1105, "Unknown table leading_zero$partitions")
        return original(cursor, statement, params)

    table = "leading_zero" if provider == "partitions" else "hdfs_partitioned"
    with (
        patch.object(_external_metadata, "_rows", query),
        patch(
            "ray_doris.datasource.DorisDatasource.get_read_tasks",
            side_effect=AssertionError("Doris data path used"),
        ),
    ):
        dataset = reader(table, "hdfs")
        names = dataset.schema().names
        rows = sorted(dataset.take_all(), key=lambda row: row["id"])
    expected = sql(
        "SELECT "
        + ",".join(f"`{name}`" for name in names)
        + f" FROM native_hive.native_it.{table} ORDER BY id"
    )
    assert [tuple(row[name] for name in names) for row in rows] == list(expected)
    assert rows
    required = {
        "catalog": "SHOW CATALOG",
        "columns": "DESCRIBE",
        "session": "SHOW VARIABLES",
        "partitions": "SHOW PARTITIONS",
        "version": "SELECT @@version_comment",
    }
    assert any(statement.startswith(required[provider]) for statement in statements)


def test_doris_schema_uses_real_describe_when_columns_interface_is_absent():
    original = _metadata.metadata_rows
    statements = []

    def query(cursor, statement, params=None):
        statements.append(statement)
        if "information_schema" in statement:
            raise pymysql.err.OperationalError(1054, "Unknown column COLUMN_TYPE")
        return original(cursor, statement, params)

    with patch.object(_metadata, "metadata_rows", query):
        dataset = reader("hdfs_plain", "hdfs", auto_native_read=False)
        names = dataset.schema().names
        rows = sorted(dataset.take_all(), key=lambda row: row["id"])
    expected = sql(
        "SELECT "
        + ",".join(f"`{name}`" for name in names)
        + " FROM native_hive.native_it.hdfs_plain ORDER BY id"
    )
    assert [tuple(row[name] for name in names) for row in rows] == list(expected)
    assert len(rows) == 240
    assert any(statement.startswith("DESCRIBE") for statement in statements)


def test_typed_partition_collisions_preserve_original_hms_locations(cluster):
    assert sorted(sql("SELECT q FROM native_hive.native_it.`leading_zero$partitions`")) == [
        (1,),
        (1,),
    ]
    rows = reader("leading_zero", "hdfs", columns=["q", "id"]).take_all()
    assert {row["id"] for row in rows} == cluster["leading_ids"]
    assert len(rows) == len(cluster["leading_ids"])
    assert {row["q"] for row in rows} == {1}


def test_partition_limit_retains_full_doris_read():
    with patch(
        "ray_doris._native.ray.data.read_parquet", __signature__=PARQUET_SIGNATURE
    ) as native_reader:
        dataset = read_doris(
            host="fe",
            table="native_it.hdfs_partitioned",
            catalog="native_hive",
            auto_native_read=True,
            native_options=NativeReadOptions(max_partitions=1),
        )
        assert dataset.count() == 240
        native_reader.assert_not_called()


@pytest.mark.parametrize("table", ["missing", "missing_partition"])
def test_unsupported_locations_and_partition_values_fail_before_read(table):
    with patch(
        "ray_doris._native.ray.data.read_parquet", __signature__=PARQUET_SIGNATURE
    ) as native_reader:
        with pytest.raises(DorisPlanningError):
            reader(table, "hdfs")
        native_reader.assert_not_called()


def test_file_limit_retains_full_doris_read():
    with patch(
        "ray_doris._native.ray.data.read_parquet", __signature__=PARQUET_SIGNATURE
    ) as native_reader:
        dataset = read_doris(
            host="fe",
            table="native_it.hdfs_plain",
            catalog="native_hive",
            auto_native_read=True,
            native_options=NativeReadOptions(max_files=1),
        )
        assert dataset.count() == 240
        native_reader.assert_not_called()


def test_storage_denial_does_not_fall_back_to_doris():
    denied = fs.S3FileSystem(
        access_key="denied-user",
        secret_key=os.environ["NATIVE_S3_PASSWORD"],
        endpoint_override="minio:9000",
        scheme="http",
        region="us-east-1",
    )
    with pytest.raises(DorisPlanningError, match="storage metadata"):
        read_doris(
            host="fe",
            table="native_it.s3_plain",
            catalog="native_hive",
            auto_native_read=True,
            native_options=NativeReadOptions(filesystem=denied),
        )


def test_permission_error_is_not_version_fallback():
    sql("CREATE USER 'native_denied' IDENTIFIED BY %s", (os.environ["NATIVE_S3_PASSWORD"],))
    with pytest.raises(DorisPermissionError):
        read_doris(
            host="fe",
            user="native_denied",
            password=os.environ["NATIVE_S3_PASSWORD"],
            table="native_it.hdfs_plain",
            catalog="native_hive",
            auto_native_read=True,
        )


@pytest.mark.parametrize("medium", ["hdfs", "s3"])
def test_distributed_reads_use_two_workers(medium):
    def worker_identity(batch):
        return {"node": [ray.get_runtime_context().get_node_id()], "rows": [batch.num_rows]}

    workers = sorted(
        node["NodeID"]
        for node in ray.nodes()
        if node["Alive"] and node["Resources"].get("CPU", 0) > 0
    )
    assert len(workers) == 2
    evidence = []
    original_reader = _native.ray.data.read_parquet
    for node_id in workers:
        affinity = NodeAffinitySchedulingStrategy(node_id, soft=False)
        # Assert the real native reader receives the hard scheduling constraint;
        # downstream map placement alone would not prove source accessibility.
        with patch.object(
            _native.ray.data,
            "read_parquet",
            wraps=original_reader,
            __signature__=PARQUET_SIGNATURE,
        ) as native_reader:
            dataset = reader(
                medium + "_plain",
                medium,
                concurrency=1,
                override_num_blocks=2,
                ray_remote_args={"scheduling_strategy": affinity},
            )
            forwarded = native_reader.call_args.kwargs["ray_remote_args"]["scheduling_strategy"]
            assert forwarded.node_id == node_id and forwarded.soft is False
            rows = dataset.map_batches(
                worker_identity,
                batch_format="pyarrow",
                batch_size=None,
                concurrency=1,
                scheduling_strategy=affinity,
            ).take_all()
        assert {row["node"] for row in rows} == {node_id}
        assert sum(row["rows"] for row in rows) == 240
        evidence.append({"node": node_id, "rows": 240, "read_constraint": "hard"})
    Path(f"/evidence/read-nodes-{medium}.json").write_text(json.dumps(evidence, indent=2))
    Path("/evidence/version.txt").write_text(str(sql("SELECT @@version_comment")[0][0]))


def test_missing_file_after_planning_never_replays_through_sql():
    with patch(
        "ray_doris.datasource.DorisDatasource.get_read_tasks",
        side_effect=AssertionError("SQL replay"),
    ):
        dataset = reader("runtime_failure", "hdfs")
        fs.HadoopFileSystem("hms", 9000, user="root").delete_file(
            "/native/runtime_failure/data.parquet"
        )
        with pytest.raises((ray.exceptions.RayTaskError, OSError, ValueError)) as error:
            dataset.take_all()
        assert "/native/runtime_failure/data.parquet" in str(error.value)


@pytest.mark.parametrize("medium", ["hdfs", "s3"])
def test_disabled_switch_reads_through_doris_without_native_probe(medium):
    with patch("ray_doris._native.discover_external", side_effect=AssertionError("native probe")):
        data = reader(medium + "_plain", medium, auto_native_read=False).take_all()
        empty = reader(medium + "_empty", medium, auto_native_read=False, columns=["id"])
        assert len(data) == 240
        assert empty.count() == 0
        assert empty.schema().base_schema == pa.schema([("id", pa.int32())])


@pytest.mark.parametrize("medium", ["hdfs", "s3"])
def test_orc_falls_back_to_doris_and_preserves_values(medium):
    name = medium + "_orc"
    with patch(
        "ray_doris._native.ray.data.read_parquet", __signature__=PARQUET_SIGNATURE
    ) as native_reader:
        dataset = reader(name, medium)
        rows = sorted(dataset.take_all(), key=lambda row: row["id"])
        names = dataset.schema().names
        expected = sql(
            "SELECT "
            + ",".join(f"`{name}`" for name in names)
            + f" FROM native_hive.native_it.{name} ORDER BY id"
        )
        assert [tuple(row[name] for name in names) for row in rows] == list(expected)
        assert len(rows) == 240
        native_reader.assert_not_called()


@pytest.mark.parametrize("medium", ["hdfs", "s3"])
def test_predicate_and_custom_batch_keep_doris_execution(medium):
    with patch("ray_doris._native.discover_external", side_effect=AssertionError("native probe")):
        rows = reader(
            medium + "_plain", medium, columns=["id"], filter="id >= 230", batch_size=3
        ).take_all()
        assert sorted(row["id"] for row in rows) == list(range(230, 240))


@pytest.mark.parametrize("table", ["nested", "custom_location", "null_partition"])
def test_unsupported_native_layouts_retain_doris_results(table):
    with patch(
        "ray_doris._native.ray.data.read_parquet", __signature__=PARQUET_SIGNATURE
    ) as native_reader:
        dataset = reader(table, "hdfs", columns=["id"])
        rows = dataset.take_all()
        expected = sql(f"SELECT id FROM native_hive.native_it.{table}")
        assert sorted(row["id"] for row in rows) == sorted(row[0] for row in expected)
        assert dataset.schema().base_schema == pa.schema([("id", pa.int32())])
        native_reader.assert_not_called()


@pytest.mark.parametrize(
    "setting,value,reset",
    [
        ("hive_parquet_use_column_names", "false", "true"),
        ("time_zone", "'Asia/Shanghai'", "'UTC'"),
    ],
)
def test_session_semantics_outside_native_profile_retain_doris(setting, value, reset):
    sql(f"SET GLOBAL {setting} = {value}")
    try:
        with patch(
            "ray_doris._native.ray.data.read_parquet", __signature__=PARQUET_SIGNATURE
        ) as native_reader:
            dataset = reader("hdfs_plain", "hdfs", columns=["id", "ts"])
            rows = sorted((row["id"], row["ts"]) for row in dataset.take_all())
            assert rows == list(
                sql("SELECT id,ts FROM native_hive.native_it.hdfs_plain ORDER BY id")
            )
            assert len(rows) == 240
            native_reader.assert_not_called()
    finally:
        sql(f"SET GLOBAL {setting} = {reset}")


@pytest.mark.parametrize("table", ["extensionless", "reserved_path"])
def test_ray_v2_limitations_retain_doris_data(table):
    context = ray.data.DataContext.get_current()
    previous = context.use_datasource_v2
    context.use_datasource_v2 = True
    try:
        columns = ["path"] if table == "reserved_path" else ["id"]
        with patch(
            "ray_doris._native.ray.data.read_parquet", __signature__=PARQUET_SIGNATURE
        ) as native_reader:
            rows = reader(table, "hdfs", columns=columns).take_all()
            expected = sql(f"SELECT `{columns[0]}` FROM native_hive.native_it.{table}")
            assert rows and sorted(row[columns[0]] for row in rows) == sorted(
                r[0] for r in expected
            )
            native_reader.assert_not_called()
    finally:
        context.use_datasource_v2 = previous
