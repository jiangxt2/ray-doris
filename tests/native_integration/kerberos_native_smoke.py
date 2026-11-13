"""Exercise automatic Doris Hive authentication on two isolated Ray workers."""

import importlib
import os
import shutil
import time
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pymysql
import ray
import ray.data
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from ray_doris import read_doris
from ray_doris._hms import open_hms


def mark_worker(batch):
    node_id = ray.get_runtime_context().get_node_id()
    return batch.append_column("_worker", pa.array([node_id] * batch.num_rows))


def sql(statement, *, dictionaries=False):
    cursor_type = pymysql.cursors.DictCursor if dictionaries else pymysql.cursors.Cursor
    with pymysql.connect(
        host="fe", port=9030, user="root", autocommit=True, cursorclass=cursor_type
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            return cursor.fetchall()


def main():
    if not os.environ.get("CLASSPATH"):
        raise RuntimeError("integration driver requires the Hadoop CLASSPATH")
    realm = os.environ["RD_KRB_REALM"]
    principal = "reader@" + realm
    deadline = time.monotonic() + 300
    while True:
        try:
            assert sql("SELECT 1") == ((1,),)
            assert any(
                str(row["Alive"]).lower() == "true"
                for row in sql("SHOW BACKENDS", dictionaries=True)
            )
            with open_hms("thrift://hms:9083", 5):
                pass
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)
    types = importlib.import_module("pymetastore.hive_metastore.ttypes")
    with open_hms("thrift://hms:9083", 10) as hms:
        client = hms._client
        client.create_database(
            types.Database(name="krb_it", locationUri="hdfs://hdfs-probe:8020/lake")
        )
        client.create_table(
            types.Table(
                tableName="data",
                dbName="krb_it",
                owner="reader",
                tableType="EXTERNAL_TABLE",
                parameters={"EXTERNAL": "TRUE"},
                partitionKeys=[],
                sd=types.StorageDescriptor(
                    cols=[types.FieldSchema(name="id", type="int")],
                    location="hdfs://hdfs-probe:8020/lake/data",
                    inputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    outputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serdeInfo=types.SerDeInfo(
                        serializationLib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    ),
                ),
            )
        )
    sql("SET GLOBAL hive_parquet_use_column_names = true")
    sql(
        "CREATE CATALOG krb_hive PROPERTIES ("
        "'type'='hms', 'hive.metastore.uris'='thrift://hms:9083', "
        "'hive.metastore.authentication.type'='simple', "
        "'hive.metastore.sasl.enabled'='false', "
        "'get_schema_from_table'='true', 'fs.defaultFS'='hdfs://hdfs-probe:8020', "
        f"'dfs.namenode.kerberos.principal'='hdfs/hdfs-probe@{realm}', "
        "'dfs.data.transfer.protection'='authentication', "
        "'hadoop.security.authentication'='kerberos', "
        f"'hadoop.kerberos.principal'='{principal}', "
        "'hadoop.kerberos.keytab'='/fixture/reader.keytab')"
    )
    reference_error = None
    try:
        assert sql("SELECT id FROM krb_hive.krb_it.data ORDER BY id") == tuple(
            (value,) for value in range(40)
        )
    except Exception as error:
        # Preserve this failure while allowing independent Ray checks to run.
        reference_error = error
        print("Doris reference read failed; continuing independent Ray checks", flush=True)
    ray.init(address="ray-head:6379", log_to_driver=True)
    try:
        deadline = time.monotonic() + 90
        while True:
            workers = [
                node for node in ray.nodes() if node["Alive"] and node["Resources"].get("CPU", 0)
            ]
            if len(workers) == 2:
                break
            if time.monotonic() > deadline:
                raise RuntimeError("two Ray workers did not register")
            time.sleep(2)
        root_env = "RAY_DORIS_HDFS_KEYTAB_ROOT"
        original_root = os.environ[root_env]
        os.environ[root_env] = "/fixture/state"
        try:
            with patch(
                "ray_doris._api.ray.data.read_datasource", wraps=ray.data.read_datasource
            ) as doris:
                fallback = read_doris(
                    host="fe",
                    table="krb_it.data",
                    user="root",
                    catalog="krb_hive",
                    auto_native_read=True,
                )
                assert doris.call_count == 1
            assert sorted(row["id"] for row in fallback.take_all()) == list(range(40))
            print("driver keytab rejection fell back to Doris", flush=True)
        finally:
            os.environ[root_env] = original_root
        for node in workers:
            affinity = NodeAffinitySchedulingStrategy(node_id=node["NodeID"], soft=False)
            dataset = read_doris(
                host="fe",
                table="krb_it.data",
                user="root",
                catalog="krb_hive",
                auto_native_read=True,
                override_num_blocks=2,
                ray_remote_args={"scheduling_strategy": affinity},
            )
            rows = dataset.map_batches(
                mark_worker, batch_format="pyarrow", scheduling_strategy=affinity
            ).take_all()
            assert sorted(row["id"] for row in rows) == list(range(40))
            assert {row["_worker"] for row in rows} == {node["NodeID"]}
            print("native read passed on worker", node["NodeID"], flush=True)

        @ray.remote(num_cpus=1)
        class RenewalProbe:
            def __init__(self, principal):
                import pyarrow.fs as fs

                from ray_doris._hdfs_auth import HdfsKerberosConfig
                from ray_doris._hdfs_proxy import HdfsKerberosHandler

                self.handler = HdfsKerberosHandler(
                    "hdfs-probe",
                    8020,
                    HdfsKerberosConfig(principal, "/fixture/reader.keytab"),
                    {"hadoop.security.authentication": "kerberos"},
                )
                self.filesystem = fs.PyFileSystem(self.handler)

            def verify(self):
                info = self.filesystem.get_file_info("/lake/data/part-0.parquet")
                assert info.size > 0
                with self.filesystem.open_input_file("/lake/data/part-0.parquet") as stream:
                    assert stream.read(4) == b"PAR1"
                    initial = self.handler._lease.expiry
                    while time.time() <= initial + 2:
                        time.sleep(1)
                    assert self.handler._lease.expiry > initial
                    assert self.handler._lease.version > 1
                    assert self.filesystem.get_file_info("/lake/data/part-0.parquet").size > 0
                    stream.seek(0)
                    assert stream.read(4) == b"PAR1"
                with self.filesystem.open_input_file("/lake/data/part-0.parquet") as stream:
                    assert stream.read(4) == b"PAR1"
                return ray.get_runtime_context().get_node_id()

        probes = [
            RenewalProbe.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=node["NodeID"], soft=False
                )
            ).remote(principal)
            for node in workers
        ]
        observed = ray.get([probe.verify.remote() for probe in probes], timeout=100)
        assert set(observed) == {node["NodeID"] for node in workers}
        print("renewed HDFS access passed on both workers after original expiry", flush=True)
        for probe in probes:
            ray.kill(probe)

        failure_keytab = Path("/fixture/failure.keytab")
        shutil.copyfile("/fixture/reader.keytab", failure_keytab)
        sql(
            "CREATE CATALOG krb_failure PROPERTIES ("
            "'type'='hms', 'hive.metastore.uris'='thrift://hms:9083', "
            "'hive.metastore.authentication.type'='simple', "
            "'hive.metastore.sasl.enabled'='false', "
            "'get_schema_from_table'='true', 'fs.defaultFS'='hdfs://hdfs-probe:8020', "
            f"'dfs.namenode.kerberos.principal'='hdfs/hdfs-probe@{realm}', "
            "'dfs.data.transfer.protection'='authentication', "
            "'hadoop.security.authentication'='kerberos', "
            f"'hadoop.kerberos.principal'='{principal}', "
            "'hadoop.kerberos.keytab'='/fixture/failure.keytab')"
        )
        affinity = NodeAffinitySchedulingStrategy(node_id=workers[0]["NodeID"], soft=False)
        pending = read_doris(
            host="fe",
            table="krb_it.data",
            user="root",
            catalog="krb_failure",
            auto_native_read=True,
            ray_remote_args={"scheduling_strategy": affinity},
        )
        keytab = failure_keytab
        withheld = Path("/fixture/failure.keytab.withheld")
        keytab.rename(withheld)
        try:
            try:
                pending.take_all()
            except Exception as error:
                if "HDFS Kerberos keytab is not an authorized readable file" not in str(error):
                    raise AssertionError(
                        "native read failed before the intended credential check"
                    ) from error
                if "auto_native_read=False" not in str(error):
                    raise AssertionError(
                        "worker failure did not include the Doris retry hint"
                    ) from error
                print("worker keytab unavailability failed the native read", flush=True)
            else:
                raise AssertionError("native read succeeded without a worker keytab")
        finally:
            withheld.rename(keytab)
    finally:
        ray.shutdown()
    if reference_error is not None:
        raise AssertionError("Doris reference read failed") from reference_error


if __name__ == "__main__":
    main()
