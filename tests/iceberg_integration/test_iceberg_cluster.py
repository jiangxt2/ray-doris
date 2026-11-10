"""Real Doris, REST and shared-storage snapshot checks."""

from __future__ import annotations

import functools
import logging
import os
import secrets
import subprocess
import sys
import time
from uuid import uuid4

import pyarrow as pa
import pymysql
import pytest
import ray
import ray.data
from pyiceberg.catalog import load_catalog
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from ray_doris import DorisPlanningError, read_doris
from ray_doris.datasource import DorisDatasource

pytestmark = pytest.mark.integration


def sql(statement, parameters=None):
    __tracebackhide__ = True
    try:
        with pymysql.connect(host="fe", port=9030, user="root", autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, parameters)
                return cursor.fetchall()
    except pymysql.MySQLError as error:
        message = str(error)
        for key in ("NATIVE_S3_USER", "NATIVE_S3_PASSWORD"):
            message = message.replace(os.environ[key], "<redacted>")
        raise RuntimeError("Iceberg fixture SQL failed: " + message) from None


@pytest.fixture(scope="module")
def tables(record_testsuite_property):
    import pyarrow.fs as fs

    record_testsuite_property("doris_build", str(sql("SELECT @@version_comment")[0][0]))
    store = fs.S3FileSystem(
        access_key=os.environ["NATIVE_S3_USER"],
        secret_key=os.environ["NATIVE_S3_PASSWORD"],
        endpoint_override="minio:9000",
        scheme="http",
        region="us-east-1",
        allow_bucket_creation=True,
    )
    store.create_dir("native-bucket")
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
    ray.init(address="auto", log_to_driver=False)
    yield result
    ray.shutdown()


def data(catalog, namespace, table="events", **kwargs):
    return read_doris(
        host="fe", catalog=catalog, table=namespace + "." + table, auto_native_read=True, **kwargs
    )


@pytest.mark.parametrize("medium", ["s3", "hdfs"])
def test_version_compatibility(tables, medium, monkeypatch):
    name, namespace, _ = tables[medium]
    original = ray.data.read_iceberg
    calls = []

    @functools.wraps(original)
    def record(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(ray.data, "read_iceberg", record)
    assert data(name, namespace).take_all() == [{"id": 1, "label": "one"}]
    assert len(calls) == 1 and calls[0]["snapshot_id"] > 0


def test_legacy_snapshot_and_cached_ancestor_use_native_read(tables, monkeypatch, caplog):
    if "3.0.6.2" not in str(sql("SELECT @@version_comment")[0][0]):
        pytest.skip("Doris 3.0.6.2-specific metadata interface")
    name, namespace, _ = tables["s3"]
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
    assert "ancestor manifest evidence" in caplog.text
    assert "snapshot-pinned Doris fallback may fail" in caplog.text


def test_append_after_snapshot_selection_keeps_pinned_data(tables, monkeypatch):
    name, namespace, _ = tables["s3"]
    _, table = scratch_table(name, namespace, "scratch_append", with_label=True)
    original = ray.data.read_iceberg
    snapshots = []

    @functools.wraps(original)
    def append_after_selection(**kwargs):
        snapshots.append(kwargs["snapshot_id"])
        table.append(
            pa.Table.from_pylist([{"id": 2, "label": "two"}], schema=table.schema().as_arrow())
        )
        return original(**kwargs)

    monkeypatch.setattr(ray.data, "read_iceberg", append_after_selection)
    assert data(name, namespace, table="scratch_append").take_all() == [{"id": 1, "label": "one"}]
    assert sql(
        f"SELECT id, label FROM {name}.{namespace}.scratch_append FOR VERSION AS OF {snapshots[0]}"
    ) == ((1, "one"),)


@pytest.mark.parametrize("medium", ["s3", "hdfs"])
def test_projection_and_workers_match_doris_schema(tables, medium, monkeypatch):
    name, namespace, _ = tables[medium]
    native = ray.data.read_iceberg
    calls = []

    @functools.wraps(native)
    def record(**kwargs):
        calls.append(kwargs)
        return native(**kwargs)

    monkeypatch.setattr(ray.data, "read_iceberg", record)
    expected = read_doris(
        host="fe", catalog=name, table=namespace + ".events", columns=["label", "id"]
    )
    expected_table = pa.concat_tables(ray.get(expected.to_arrow_refs()))
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
        assert len(calls) == index
        assert calls[-1]["ray_remote_args"]["scheduling_strategy"].node_id == node


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


def test_snapshot_without_schema_id_rejects_later_field_id_change(tables, monkeypatch):
    from pyiceberg.serializers import ToOutputFile
    from pyiceberg.types import LongType

    name, namespace, _ = tables["s3"]
    identifier = namespace + ".scratch_schema_guard"
    catalog, table = scratch_table(name, namespace, "scratch_schema_guard")
    snapshot_id = table.current_snapshot().snapshot_id
    metadata = table.metadata.model_copy(
        update={
            "snapshots": [
                snapshot.model_copy(update={"schema_id": None})
                if snapshot.snapshot_id == snapshot_id
                else snapshot
                for snapshot in table.metadata.snapshots
            ]
        }
    )
    metadata_file = (
        table.metadata_location.rsplit("/", 1)[0] + "/guard-" + uuid4().hex + ".metadata.json"
    )
    ToOutputFile.table_metadata(metadata, table.io.new_output(metadata_file))
    catalog.drop_table(identifier)
    table = catalog.register_table(identifier, metadata_file)
    assert table.current_snapshot().schema_id is None
    original = ray.data.read_iceberg

    @functools.wraps(original)
    def change_schema(**kwargs):
        with table.update_schema() as update:
            update.delete_column("id")
            update.add_column("id", LongType())
        table.refresh()
        assert table.current_snapshot().snapshot_id == snapshot_id
        return original(**kwargs)

    monkeypatch.setattr(ray.data, "read_iceberg", change_schema)
    with pytest.raises(Exception, match="snapshot schema changed after planning"):
        data(name, namespace, table="scratch_schema_guard").take_all()


def test_empty_table_without_snapshot_keeps_doris(tables, monkeypatch):
    name, namespace, _ = tables["s3"]
    catalog = load_catalog(
        name, type="rest", auth={"type": "noop"}, **{"header.X-Iceberg-Access-Delegation": ""}
    )
    catalog.create_table(namespace + ".scratch_empty", schema=pa.schema([("id", pa.int64())]))
    original = ray.data.read_iceberg
    calls = []

    @functools.wraps(original)
    def record(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(ray.data, "read_iceberg", record)
    assert data(name, namespace, table="scratch_empty").take_all() == []
    assert not calls


def test_known_incompatibility_uses_same_snapshot_in_real_doris(tables, monkeypatch):
    from pyiceberg.table import DataScan

    name, namespace, _ = tables["s3"]
    _, table = scratch_table(name, namespace, "scratch_fallback")
    original = ray.data.read_datasource
    selected = []

    def unsupported_delete(scan):
        table.append(pa.Table.from_pylist([{"id": 2}], schema=table.schema().as_arrow()))
        raise ValueError("PyIceberg does not yet support equality deletes")

    @functools.wraps(original)
    def record(source, *args, **kwargs):
        if isinstance(source, DorisDatasource):
            selected.append(source.config.snapshot_id)
        return original(source, *args, **kwargs)

    monkeypatch.setattr(DataScan, "plan_files", unsupported_delete)
    monkeypatch.setattr(ray.data, "read_datasource", record)
    assert data(name, namespace, table="scratch_fallback").take_all() == [{"id": 1}]
    assert len(selected) == 1 and selected[0] != table.current_snapshot().snapshot_id
    assert sql(
        f"SELECT id FROM {name}.{namespace}.scratch_fallback FOR VERSION AS OF {selected[0]}"
    ) == ((1,),)


def test_historical_schema_mismatch_fails_without_unpinned_fallback(tables):
    from pyiceberg.types import StringType

    name, namespace, _ = tables["s3"]
    _, table = scratch_table(name, namespace, "scratch_schema")
    with table.update_schema() as update:
        update.add_column("note", StringType())
    sql(f"REFRESH TABLE {name}.{namespace}.scratch_schema")
    assert sql(f"SELECT id, note FROM {name}.{namespace}.scratch_schema") == ((1, None),)
    with pytest.raises(DorisPlanningError, match="snapshot schema differs"):
        data(name, namespace, table="scratch_schema")


@pytest.mark.parametrize("change", ["replace", "expire"])
def test_deferred_reader_rejects_changed_table_or_expired_snapshot(tables, monkeypatch, change):
    name, namespace, _ = tables["hdfs"]
    scratch = "scratch_" + change
    catalog, table = scratch_table(name, namespace, scratch)
    original = ray.data.read_iceberg

    @functools.wraps(original)
    def change_after_selection(**kwargs):
        if change == "replace":
            catalog.drop_table(namespace + "." + scratch)
            catalog.create_table(namespace + "." + scratch, schema=pa.schema([("id", pa.int64())]))
        else:
            table.append(pa.Table.from_pylist([{"id": 2}], schema=table.schema().as_arrow()))
            table.maintenance.expire_snapshots().by_id(kwargs["snapshot_id"]).commit()
        return original(**kwargs)

    monkeypatch.setattr(ray.data, "read_iceberg", change_after_selection)
    with pytest.raises(Exception, match="identity changed|no longer available"):
        data(name, namespace, table=scratch).take_all()


def test_worker_retry_keeps_snapshot_after_append(tables, monkeypatch):
    name, namespace, _ = tables["s3"]
    _, table = scratch_table(name, namespace, "scratch_retry")
    marker = "/evidence/iceberg-retry-" + uuid4().hex

    original_read = ray.data.read_iceberg

    @functools.wraps(original_read)
    def append_after_selection(**kwargs):
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
                "env_vars": {"ICEBERG_RETRY_MARKER": marker},
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
        ray.init(address="auto", log_to_driver=False)
    assert rows == [{"id": 1}]
    assert os.path.isfile(marker)


def test_started_native_failure_does_not_replay_through_doris(tables, monkeypatch):
    name, namespace, _ = tables["hdfs"]
    _, table = scratch_table(name, namespace, "scratch_missing")
    tasks = list(table.scan().plan_files())
    assert len(tasks) == 1
    original_read = ray.data.read_datasource
    doris_reads = []

    @functools.wraps(original_read)
    def record(source, *args, **kwargs):
        if isinstance(source, DorisDatasource):
            doris_reads.append(source)
        return original_read(source, *args, **kwargs)

    monkeypatch.setattr(ray.data, "read_datasource", record)
    dataset = data(name, namespace, table="scratch_missing", ray_remote_args={"max_retries": 0})
    table.io.delete(tasks[0].file.file_path)
    with pytest.raises(Exception, match="scratch_missing"):
        dataset.take_all()
    assert not doris_reads


def test_storage_auth_failure_does_not_fall_back(tables):
    name, namespace, _ = tables["s3"]
    env = dict(os.environ)
    invalid = secrets.token_hex(24)
    env["PYICEBERG_CATALOG__ICES3__S3__SECRET_ACCESS_KEY"] = invalid
    script = """
import sys
from ray_doris import DorisPlanningError, read_doris
try:
    read_doris(host='fe', catalog=sys.argv[1], table=sys.argv[2], auto_native_read=True)
except DorisPlanningError as error:
    assert 'identity verification failed' in str(error)
else:
    raise AssertionError('storage authorization failure was not surfaced')
"""
    result = subprocess.run(
        [sys.executable, "-c", script, name, namespace + ".events"],
        env=env,
        text=True,
        capture_output=True,
        timeout=90,
    )
    assert result.returncode == 0, "storage-denial subprocess failed"
    assert invalid not in result.stdout + result.stderr


def test_doris_nonsecret_defaults_oauth_and_s3(tables):
    import base64
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.parse import parse_qs

    import requests

    _, namespace, _ = tables["s3"]
    password = os.environ["NATIVE_S3_PASSWORD"]
    allowed_token = secrets.token_urlsafe(24)
    denied_token = secrets.token_urlsafe(24)
    scope = "catalog:read"
    seen = {"token": 0, "allowed": 0, "denied": 0}
    upstream = requests.Session()
    upstream.trust_env = False

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format, *args):
            pass

        def respond(self, status, body, content_type="application/json"):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_POST(self):
            if self.path != "/oauth/token":
                self.respond(404, b"{}")
                return
            data = parse_qs(self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode())
            client = data.get("client_id", [""])[0]
            secret = data.get("client_secret", [""])[0]
            header = self.headers.get("Authorization", "")
            if header.startswith("Basic "):
                client, _, secret = base64.b64decode(header[6:]).decode().partition(":")
            if (
                client not in ("allowed", "denied")
                or secret != password
                or data.get("scope") != [scope]
            ):
                self.respond(401, b'{"error":"invalid_client"}')
                return
            seen["token"] += 1
            token = allowed_token if client == "allowed" else denied_token
            self.respond(
                200,
                json.dumps(
                    {"access_token": token, "token_type": "Bearer", "expires_in": 3600}
                ).encode(),
            )

        def do_GET(self):
            self.forward("GET")

        def do_HEAD(self):
            self.forward("HEAD")

        def forward(self, method):
            auth = self.headers.get("Authorization", "")
            if auth == "Bearer " + denied_token:
                seen["denied"] += 1
                self.respond(
                    403, b'{"error":{"message":"forbidden","type":"Forbidden","code":403}}'
                )
                return
            if auth != "Bearer " + allowed_token:
                self.respond(
                    401, b'{"error":{"message":"unauthorized","type":"Unauthorized","code":401}}'
                )
                return
            seen["allowed"] += 1
            headers = {
                key: value
                for key, value in self.headers.items()
                if key.lower() not in ("authorization", "host", "connection")
            }
            response = upstream.request(
                method, "http://iceberg-s3:9001" + self.path, headers=headers, timeout=30
            )
            self.respond(
                response.status_code,
                response.content,
                response.headers.get("Content-Type", "application/json"),
            )

    server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    token_url = f"http://ray-head:{server.server_port}/oauth/token"
    rest_url = f"http://ray-head:{server.server_port}/iceberg"
    try:
        sql(
            "CREATE CATALOG iceoauth PROPERTIES ('type'='iceberg','iceberg.catalog.type'='rest',"
            "'uri'=%s,'iceberg.rest.security.type'='oauth2','iceberg.rest.oauth2.credential'=%s,"
            "'iceberg.rest.oauth2.server-uri'=%s,'iceberg.rest.oauth2.scope'=%s,"
            "'s3.endpoint'='http://minio:9000','s3.region'='us-east-1',"
            "'use_path_style'='true','s3.access_key'=%s,'s3.secret_key'=%s)",
            (
                rest_url,
                "allowed:" + password,
                token_url,
                scope,
                os.environ["NATIVE_S3_USER"],
                password,
            ),
        )
        env = dict(os.environ)
        env.update(
            PYICEBERG_CATALOG__ICEOAUTH__S3__ACCESS_KEY_ID=os.environ["NATIVE_S3_USER"],
            PYICEBERG_CATALOG__ICEOAUTH__S3__SECRET_ACCESS_KEY=password,
            PYICEBERG_CATALOG__ICEOAUTH__S3__FORCE_VIRTUAL_ADDRESSING="false",
            EXPECTED_TOKEN_URL=token_url,
        )
        script = """
import functools
import os
import sys
from unittest.mock import patch
import ray
import ray.data
from ray_doris import DorisAuthenticationError, DorisPermissionError, read_doris

mode, namespace = sys.argv[1:]
ray.init(address="auto", log_to_driver=False)
try:
    original = ray.data.read_iceberg
    observed = []
    @functools.wraps(original)
    def record(**kwargs):
        defaults = kwargs["catalog_kwargs"]["ray-doris.catalog-defaults"]
        assert defaults["oauth2-server-uri"] == os.environ["EXPECTED_TOKEN_URL"]
        assert defaults["scope"] == "catalog:read"
        assert defaults["s3.endpoint"] == "http://minio:9000"
        assert defaults["s3.region"] == "us-east-1"
        assert os.environ["NATIVE_S3_PASSWORD"] not in str(kwargs["catalog_kwargs"])
        observed.append(True)
        return original(**kwargs)
    with patch("ray.data.read_iceberg", record):
        try:
            dataset = read_doris(
                host="fe", catalog="iceoauth", table=namespace + ".events", auto_native_read=True
            )
            rows = dataset.take_all()
        except (DorisAuthenticationError, DorisPermissionError) as error:
            assert mode != "allowed"
            expected = DorisAuthenticationError if mode == "invalid" else DorisPermissionError
            assert isinstance(error, expected)
            assert os.environ["NATIVE_S3_PASSWORD"] not in str(error)
        else:
            assert mode == "allowed" and observed and rows == [{"id": 1, "label": "one"}]
finally:
    ray.shutdown()
"""
        for mode, credential in (
            ("allowed", "allowed:" + password),
            ("invalid", "allowed:" + password + "x"),
            ("denied", "denied:" + password),
        ):
            env["PYICEBERG_CATALOG__ICEOAUTH__CREDENTIAL"] = credential
            result = subprocess.run(
                [sys.executable, "-c", script, mode, namespace],
                env=env,
                text=True,
                capture_output=True,
                timeout=120,
            )
            output = result.stdout + result.stderr
            if any(secret in output for secret in (password, allowed_token, denied_token)):
                raise AssertionError("OAuth probe exposed sensitive values")
            if result.returncode:
                raise AssertionError("OAuth probe failed for " + mode + ": " + output[-3000:])
        assert seen["token"] >= 2 and seen["allowed"] > 0 and seen["denied"] > 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)
        upstream.close()


def test_vended_s3_credentials_are_available_to_workers(tables, monkeypatch):
    _, namespace, _ = tables["s3"]
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
    original = ray.data.read_iceberg
    calls = []

    @functools.wraps(original)
    def record(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(ray.data, "read_iceberg", record)
    rows = data("icevend", namespace).take_all()
    expected = sql(f"SELECT id, label FROM icevend.{namespace}.events ORDER BY id")
    assert sorted((row["id"], row["label"]) for row in rows) == list(expected)
    assert len(calls) == 1


def test_unavailable_policy_visibility_keeps_doris_filter(
    tables, monkeypatch, record_testsuite_property
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

    native = ray.data.read_iceberg
    native_calls = []

    @functools.wraps(native)
    def record_native(**kwargs):
        native_calls.append(kwargs)
        return native(**kwargs)

    original = ray.data.read_datasource
    selected = []

    @functools.wraps(original)
    def record_datasource(source, *args, **kwargs):
        if isinstance(source, DorisDatasource):
            selected.append(source.config.snapshot_id)
        return original(source, *args, **kwargs)

    monkeypatch.setattr(iceberg, "verify_doris_policy", record_policy)
    monkeypatch.setattr(ray.data, "read_iceberg", record_native)
    monkeypatch.setattr(ray.data, "read_datasource", record_datasource)
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
    assert not native_calls and selected == [None, None]
    assert reasons == [
        "Doris row policies require Doris",
        "Doris policy applicability is not visible",
    ]
    record_testsuite_property(
        "policy_contract", "admin_nonempty_policy_detected;both_doris;restricted_filtered"
    )


def test_doris_position_delete(tables, monkeypatch):
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
    original = ray.data.read_datasource
    selected = []

    @functools.wraps(original)
    def record(source, *args, **kwargs):
        if isinstance(source, DorisDatasource):
            selected.append(source.config.snapshot_id)
        return original(source, *args, **kwargs)

    native = ray.data.read_iceberg
    native_calls = []

    @functools.wraps(native)
    def record_native(**kwargs):
        native_calls.append(kwargs)
        return native(**kwargs)

    monkeypatch.setattr(ray.data, "read_datasource", record)
    monkeypatch.setattr(ray.data, "read_iceberg", record_native)
    actual = data(name, namespace, table="scratch_delete").take_all()
    assert selected == [snapshot.snapshot_id]
    assert native_calls == []
    expected = sql(
        f"SELECT id, label FROM {name}.{namespace}.scratch_delete "
        f"FOR VERSION AS OF {snapshot.snapshot_id}"
    )
    assert expected == ((2, "two"),)
    assert actual == [{"id": 2, "label": "two"}]
