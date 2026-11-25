"""Real Doris, REST and shared-storage snapshot checks."""

from __future__ import annotations

import functools
import logging
import os
import secrets
import time
from uuid import uuid4

import pyarrow as pa
import pymysql
import pytest
import ray
import ray.data
from pyiceberg.catalog import load_catalog
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from tests.native_integration.helpers import s3, sql

from ray_doris import read_doris
from ray_doris.datasource import DorisDatasource

pytestmark = pytest.mark.integration


def trace_runtime_env(marker):
    return {
        "worker_process_setup_hook": "tests.iceberg_integration.retry_hook.record_fallback",
        "env_vars": {"ICEBERG_FALLBACK_MARKER": marker},
    }


def trace_entries(marker):
    if not os.path.exists(marker):
        return []
    with open(marker, encoding="utf-8") as trace:
        return [line.split(" ", 1)[0] for line in trace.read().splitlines()]


@pytest.fixture(scope="module")
def tables(record_testsuite_property):
    record_testsuite_property("doris_build", str(sql("SELECT @@version_comment")[0][0]))
    s3().create_dir("native-bucket")
    result = {}
    for medium in ("s3", "hdfs"):
        name = "ice" + medium
        deadline = time.monotonic() + 180
        while True:
            try:
                catalog = load_catalog(
                    name,
                    type="rest",
                    auth={"type": "noop"},
                    **{"header.X-Iceberg-Access-Delegation": ""},
                )
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise RuntimeError("Iceberg REST fixture failed to start") from None
                time.sleep(2)
        namespace = "ice_" + medium + "_" + uuid4().hex[:8]
        catalog.create_namespace(namespace)
        table = catalog.create_table(
            namespace + ".events", schema=pa.schema([("id", pa.int64()), ("label", pa.string())])
        )
        table.append(
            pa.Table.from_pylist([{"id": 1, "label": "one"}], schema=table.schema().as_arrow())
        )
        endpoint = "http://iceberg-" + medium + ":9001/iceberg"
        sql(
            "CREATE CATALOG "
            + name
            + " PROPERTIES ('type'='iceberg','iceberg.catalog.type'='rest',"
            "'uri'=%s,'s3.endpoint'='http://minio:9000','s3.region'='us-east-1',"
            "'s3.path.style.access'='true','use_path_style'='true',"
            "'s3.access_key'=%s,'s3.secret_key'=%s,'hadoop.username'='root')",
            (endpoint, os.environ["NATIVE_S3_USER"], os.environ["NATIVE_S3_PASSWORD"]),
        )
        result[medium] = (name, namespace, table)
    result["fallback_marker"] = "/evidence/iceberg-fallback-" + uuid4().hex + ".log"
    ray.init(
        address="auto",
        log_to_driver=False,
        runtime_env=trace_runtime_env(result["fallback_marker"]),
    )
    yield result
    ray.shutdown()


def data(catalog, namespace, table="events", **kwargs):
    return read_doris(
        host="fe", catalog=catalog, table=namespace + "." + table, auto_native_read=True, **kwargs
    )


@pytest.fixture
def iceberg_reads(monkeypatch):
    original = ray.data.read_iceberg
    calls = []

    @functools.wraps(original)
    def record(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(ray.data, "read_iceberg", record)
    return calls


@pytest.fixture
def doris_reads(monkeypatch):
    original = ray.data.read_datasource
    snapshots = []

    @functools.wraps(original)
    def record(source, *args, **kwargs):
        if isinstance(source, DorisDatasource):
            snapshots.append(source.config.snapshot_id)
        return original(source, *args, **kwargs)

    monkeypatch.setattr(ray.data, "read_datasource", record)
    return snapshots


@pytest.fixture
def fallback_trace(tables):
    marker = tables["fallback_marker"]
    return marker, len(trace_entries(marker))


@pytest.mark.parametrize("medium", ["s3"])
def test_version_compatibility(tables, medium, iceberg_reads, doris_reads, fallback_trace):
    name, namespace, _ = tables[medium]
    marker, start = fallback_trace
    assert data(name, namespace).take_all() == [{"id": 1, "label": "one"}]
    assert len(iceberg_reads) == 1 and iceberg_reads[0]["snapshot_id"] > 0
    assert doris_reads == []
    assert trace_entries(marker)[start:] == []


def test_legacy_snapshot_and_cached_ancestor_use_native_read(
    tables, monkeypatch, caplog, doris_reads, fallback_trace
):
    if "3.0.6.2" not in str(sql("SELECT @@version_comment")[0][0]):
        pytest.skip("Doris 3.0.6.2-specific metadata interface")
    name, namespace, _ = tables["s3"]
    marker, start = fallback_trace
    _, table = scratch_table(name, namespace, "scratch_legacy")
    original = ray.data.read_iceberg
    selected = []

    @functools.wraps(original)
    def record(**kwargs):
        selected.append(kwargs["snapshot_id"])
        return original(**kwargs)

    monkeypatch.setattr(ray.data, "read_iceberg", record)
    with caplog.at_level(logging.WARNING, logger="ray_doris._iceberg"):
        assert data(name, namespace, table="scratch_legacy").take_all() == [{"id": 1}]
        assert "snapshot manifest evidence" in caplog.text
        first = selected[-1]
        table.append(pa.Table.from_pylist([{"id": 2}], schema=table.schema().as_arrow()))
        table.refresh()
        second = table.current_snapshot().snapshot_id
        rows = sql(
            "SELECT snapshot_id FROM iceberg_meta('table'=%s, 'query_type'='snapshots')",
            (name + "." + namespace + ".scratch_legacy",),
        )
        assert first in {row[0] for row in rows} and second not in {row[0] for row in rows}
        assert sorted(
            data(name, namespace, table="scratch_legacy").take_all(), key=lambda row: row["id"]
        ) == [{"id": 1}, {"id": 2}]
    assert selected == [first, second]
    assert doris_reads == []
    assert trace_entries(marker)[start:] == []
    assert "ancestor manifest evidence" in caplog.text
    assert "snapshot-pinned Doris fallback may fail" in caplog.text


@pytest.mark.parametrize("medium", ["s3", "hdfs"])
def test_projection_and_workers_match_doris_schema(
    tables, medium, iceberg_reads, doris_reads, fallback_trace
):
    name, namespace, _ = tables[medium]
    marker, start = fallback_trace
    expected = read_doris(
        host="fe", catalog=name, table=namespace + ".events", columns=["label", "id"]
    )
    expected_table = pa.concat_tables(ray.get(expected.to_arrow_refs()))
    assert doris_reads == [None]
    workers = [
        node["NodeID"]
        for node in ray.nodes()
        if node["Alive"] and node["Resources"].get("CPU", 0) > 0
    ]
    assert len(workers) == 2
    for index, node in enumerate(workers, start=1):
        actual = data(
            name,
            namespace,
            columns=["label", "id"],
            ray_remote_args={
                "scheduling_strategy": NodeAffinitySchedulingStrategy(node, soft=False)
            },
        )
        actual_table = pa.concat_tables(ray.get(actual.to_arrow_refs()))
        assert actual_table.schema == expected_table.schema
        assert actual_table.to_pylist() == expected_table.to_pylist()
        assert len(iceberg_reads) == index
        assert iceberg_reads[-1]["snapshot_id"] > 0
        assert iceberg_reads[-1]["ray_remote_args"]["scheduling_strategy"].node_id == node
        assert doris_reads == [None]
        assert trace_entries(marker)[start:] == []


def scratch_table(name, namespace, scratch, *, with_label=False):
    catalog = load_catalog(
        name, type="rest", auth={"type": "noop"}, **{"header.X-Iceberg-Access-Delegation": ""}
    )
    fields = [("id", pa.int64())]
    row = {"id": 1}
    if with_label:
        fields.append(("label", pa.string()))
        row["label"] = "one"
    table = catalog.create_table(namespace + "." + scratch, schema=pa.schema(fields))
    table.append(pa.Table.from_pylist([row], schema=table.schema().as_arrow()))
    return catalog, table


def test_worker_retry_keeps_snapshot_after_append(tables, monkeypatch, fallback_trace):
    name, namespace, _ = tables["s3"]
    fallback_marker, start = fallback_trace
    _, table = scratch_table(name, namespace, "scratch_retry")
    marker = "/evidence/iceberg-retry-" + uuid4().hex

    original_read = ray.data.read_iceberg
    snapshots = []

    @functools.wraps(original_read)
    def append_after_selection(**kwargs):
        snapshots.append(kwargs["snapshot_id"])
        table.append(pa.Table.from_pylist([{"id": 2}], schema=table.schema().as_arrow()))
        return original_read(**kwargs)

    monkeypatch.setattr(ray.data, "read_iceberg", append_after_selection)
    ray.shutdown()
    try:
        ray.init(
            address="auto",
            log_to_driver=False,
            runtime_env={
                "worker_process_setup_hook": "tests.iceberg_integration.retry_hook.setup",
                "env_vars": {
                    "ICEBERG_RETRY_MARKER": marker,
                    "ICEBERG_FALLBACK_MARKER": fallback_marker,
                },
            },
        )
        rows = data(
            name,
            namespace,
            table="scratch_retry",
            ray_remote_args={"max_retries": 1, "retry_exceptions": True},
        ).take_all()
    finally:
        ray.shutdown()
        ray.init(
            address="auto", log_to_driver=False, runtime_env=trace_runtime_env(fallback_marker)
        )
    assert rows == [{"id": 1}]
    assert len(snapshots) == 1 and snapshots[0] != table.current_snapshot().snapshot_id
    assert sql(
        f"SELECT id FROM {name}.{namespace}.scratch_retry FOR VERSION AS OF {snapshots[0]}"
    ) == ((1,),)
    assert os.path.isfile(marker)
    assert trace_entries(fallback_marker)[start:] == []


def test_worker_failure_falls_back_to_selected_snapshot(tables, monkeypatch, fallback_trace):
    name, namespace, _ = tables["s3"]
    marker, start = fallback_trace
    _, table = scratch_table(name, namespace, "scratch_stream_fallback")
    original = ray.data.read_iceberg
    snapshots = []

    def fail_native(_batch):
        raise RuntimeError("injected native failure")

    @functools.wraps(original)
    def inject_failure(**kwargs):
        snapshots.append(kwargs["snapshot_id"])
        return original(**kwargs).map_batches(fail_native, max_retries=0)

    monkeypatch.setattr(ray.data, "read_iceberg", inject_failure)
    password_env = "RAY_DORIS_IT_" + uuid4().hex
    monkeypatch.setenv(password_env, "")
    workers = [
        node["NodeID"]
        for node in ray.nodes()
        if node["Alive"] and node["Resources"].get("CPU", 0) > 0
    ]
    assert len(workers) == 2
    target_node = workers[0]
    dataset = data(
        name,
        namespace,
        table="scratch_stream_fallback",
        password_env=password_env,
        ray_remote_args={
            "runtime_env": {"env_vars": {password_env: ""}},
            "scheduling_strategy": NodeAffinitySchedulingStrategy(target_node, soft=False),
        },
    )
    table.append(pa.Table.from_pylist([{"id": 2}], schema=table.schema().as_arrow()))
    assert dataset.take_all() == [{"id": 1}]
    assert len(snapshots) == 1
    assert trace_entries(marker)[start:] == [str(snapshots[0])]
    with open(marker, encoding="utf-8") as trace:
        assert trace.read().splitlines()[start:] == [f"{snapshots[0]} {target_node}"]
    assert sql(
        f"SELECT id FROM {name}.{namespace}.scratch_stream_fallback "
        f"FOR VERSION AS OF {snapshots[0]}"
    ) == ((1,),)


def test_storage_auth_failure_falls_back_to_doris(tables, monkeypatch, caplog, doris_reads):
    import pyiceberg.catalog as catalogs
    from pyiceberg.utils.config import Config
    from tests.iceberg_integration.retry_hook import is_storage_authorization_error

    import ray_doris._iceberg as iceberg

    name, namespace, _ = tables["s3"]
    invalid = secrets.token_hex(24)
    monkeypatch.setenv("PYICEBERG_CATALOG__ICES3__S3__SECRET_ACCESS_KEY", invalid)
    monkeypatch.setattr(catalogs, "_ENV_CONFIG", Config())
    original = iceberg.verify_table_identity
    denied = []

    @functools.wraps(original)
    def verify(*args, **kwargs):
        assert args[4].io.properties.get("s3.secret-access-key") == invalid
        try:
            return original(*args, **kwargs)
        except OSError as error:
            assert is_storage_authorization_error(error), "Expected an S3 authorization failure"
            denied.append(True)
            raise

    monkeypatch.setattr(iceberg, "verify_table_identity", verify)
    with caplog.at_level(logging.WARNING, logger="ray_doris._iceberg"):
        assert data(name, namespace).take_all() == [{"id": 1, "label": "one"}]
    assert denied == [True] and doris_reads == [None]
    assert invalid not in caplog.text


def test_vended_s3_credentials_are_available_to_workers(
    tables, iceberg_reads, doris_reads, fallback_trace
):
    _, namespace, _ = tables["s3"]
    marker, start = fallback_trace
    sql(
        "CREATE CATALOG icevend PROPERTIES ('type'='iceberg','iceberg.catalog.type'='rest',"
        "'uri'='http://iceberg-vended:9001/iceberg','s3.endpoint'='http://minio:9000',"
        "'s3.region'='us-east-1','s3.path.style.access'='true','use_path_style'='true',"
        "'s3.access_key'=%s,'s3.secret_key'=%s)",
        (os.environ["NATIVE_S3_USER"], os.environ["NATIVE_S3_PASSWORD"]),
    )
    source = load_catalog("icevend", type="rest", auth={"type": "noop"})
    loaded = source.load_table(namespace + ".events")
    properties = loaded.io.properties
    if not properties.get("s3.access-key-id") or not properties.get("s3.secret-access-key"):
        raise AssertionError("REST catalog did not vend S3 credentials")
    from pyiceberg.utils.config import Config

    configured = Config().get_catalog_config("icevend") or {}
    assert not configured.get("s3.access-key-id") and not configured.get("s3.secret-access-key")
    rows = data("icevend", namespace).take_all()
    expected = sql(f"SELECT id, label FROM icevend.{namespace}.events ORDER BY id")
    assert sorted((row["id"], row["label"]) for row in rows) == list(expected)
    assert len(iceberg_reads) == 1
    assert doris_reads == []
    assert trace_entries(marker)[start:] == []


def test_unavailable_policy_visibility_keeps_doris_filter(
    tables, monkeypatch, record_testsuite_property, iceberg_reads, doris_reads
):
    import ray_doris._iceberg as iceberg
    from ray_doris._errors import _NativeReadNotApplicableError
    from ray_doris._metadata import named_row

    name, namespace, _ = tables["s3"]
    _, table = scratch_table(name, namespace, "scratch_policy_contract", with_label=True)
    table.append(
        pa.Table.from_pylist([{"id": 100, "label": "allowed"}], schema=table.schema().as_arrow())
    )
    sql(f"REFRESH TABLE {name}.{namespace}.scratch_policy_contract")
    sql("CREATE USER 'authprobe_reader' IDENTIFIED BY %s", (os.environ["NATIVE_S3_PASSWORD"],))
    sql(f"GRANT SELECT_PRIV ON {name}.{namespace}.scratch_policy_contract TO 'authprobe_reader'")
    sql(f"GRANT SELECT_PRIV ON {name}.information_schema.columns TO 'authprobe_reader'")
    sql(
        f"CREATE ROW POLICY authprobe_policy ON {name}.{namespace}.scratch_policy_contract "
        "AS RESTRICTIVE TO authprobe_reader USING (id = 100)"
    )
    with pymysql.connect(
        host="fe", port=9030, user="root", cursorclass=pymysql.cursors.DictCursor
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW ROW POLICY")
            rows = [named_row(row) for row in cursor.fetchall()]
    assert any(
        (row.get("catalogname"), row.get("dbname"), row.get("tablename"))
        == (name, namespace, "scratch_policy_contract")
        for row in rows
    )
    policy = iceberg.verify_doris_policy
    reasons = []

    def record_policy(*args, **kwargs):
        try:
            return policy(*args, **kwargs)
        except _NativeReadNotApplicableError as error:
            reasons.append(str(error))
            raise

    monkeypatch.setattr(iceberg, "verify_doris_policy", record_policy)
    admin_rows = data(name, namespace, table="scratch_policy_contract").take_all()
    restricted_rows = data(
        name,
        namespace,
        table="scratch_policy_contract",
        user="authprobe_reader",
        password=os.environ["NATIVE_S3_PASSWORD"],
    ).take_all()
    assert sorted(row["id"] for row in admin_rows) == [1, 100]
    assert restricted_rows == [{"id": 100, "label": "allowed"}]
    assert not iceberg_reads and doris_reads == [None, None]
    assert reasons == [
        "Doris row policies require Doris",
        "Doris policy applicability is not visible",
    ]
    record_testsuite_property(
        "policy_contract", "admin_nonempty_policy_detected;both_doris;restricted_filtered"
    )


def test_doris_position_delete(tables, iceberg_reads, doris_reads):
    from pyiceberg.manifest import DataFileContent, ManifestContent

    name, namespace, _ = tables["s3"]
    _, table = scratch_table(name, namespace, "scratch_delete", with_label=True)
    with table.transaction() as transaction:
        transaction.set_properties({"write.delete.mode": "merge-on-read"})
    table.refresh()
    table.append(
        pa.Table.from_pylist([{"id": 2, "label": "two"}], schema=table.schema().as_arrow())
    )
    sql(f"REFRESH TABLE {name}.{namespace}.scratch_delete")
    sql(f"DELETE FROM {name}.{namespace}.scratch_delete WHERE id = 1")
    sql(f"REFRESH TABLE {name}.{namespace}.scratch_delete")
    table.refresh()
    snapshot = table.current_snapshot()
    assert snapshot is not None
    delete_manifests = [
        manifest
        for manifest in snapshot.manifests(table.io)
        if manifest.content == ManifestContent.DELETES
    ]
    assert any(
        entry.data_file.content == DataFileContent.POSITION_DELETES
        for manifest in delete_manifests
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True)
    )
    actual = data(name, namespace, table="scratch_delete").take_all()
    assert doris_reads == [snapshot.snapshot_id]
    assert iceberg_reads == []
    expected = sql(
        f"SELECT id, label FROM {name}.{namespace}.scratch_delete "
        f"FOR VERSION AS OF {snapshot.snapshot_id}"
    )
    assert expected == ((2, "two"),)
    assert actual == [{"id": 2, "label": "two"}]
