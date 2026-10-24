from contextlib import nullcontext
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pyarrow as pa
import pymysql
import pytest

from ray_doris import (
    DorisAuthenticationError,
    DorisConfigurationError,
    DorisPermissionError,
    DorisPlanningError,
    DorisSchemaError,
    NativeReadOptions,
    read_doris,
)
from ray_doris._errors import _NativeReadNotApplicableError
from ray_doris._external_metadata import (
    discover_external,
    doris_build_version,
    partition_value,
    validate_native_session,
    validate_storage,
    validate_table,
)
from ray_doris._hms import HiveMetadataClient, HivePartition, HiveStorage, HiveTable, open_hms
from ray_doris._models import DorisReadConfig
from ray_doris._native import NativeReadPlan, _canonicalize_batch, _validate_file_schemas
from ray_doris._schema import doris_type_to_arrow, information_schema_type
from ray_doris._sql import parse_table


@pytest.mark.parametrize(
    "kwargs",
    [
        {"auto_native_read": "yes"},
        {"auto_native_read": 1},
        {"auto_native_read": None},
        {"catalog": "invalid-name"},
        {"native_options": NativeReadOptions()},
        {"auto_native_read": True, "catalog": "hive", "native_options": {}},
    ],
)
def test_native_rejects_conflicting_options_before_network(kwargs):
    with pytest.raises(DorisConfigurationError):
        read_doris(host="unused", table="db.tbl", **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_partitions": 0},
        {"max_partitions": 32767},
        {"max_files": True},
        {"metadata_timeout": float("inf")},
        {"metadata_timeout": 0},
        {"filesystem": "s3"},
        {"hms_uri": ""},
    ],
)
def test_native_options_bounds(kwargs):
    with pytest.raises(DorisConfigurationError):
        NativeReadOptions(**kwargs)


def test_native_routes_without_scheduling_doris_reads(monkeypatch):
    delegate = Mock(return_value="native dataset")
    monkeypatch.setattr("ray_doris._native.read_native", delegate)
    forbidden = Mock(side_effect=AssertionError("Doris read path used"))
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", forbidden)
    assert (
        read_doris(
            host="fe",
            table="db.tbl",
            auto_native_read=True,
            catalog="hive",
            columns=["id"],
            concurrency=2,
        )
        == "native dataset"
    )
    assert delegate.call_args.args[0].columns == ("id",)
    assert delegate.call_args.kwargs["concurrency"] == 2
    forbidden.assert_not_called()


@pytest.mark.parametrize("applicable", [False, True])
def test_native_path_log_matches_final_selection(monkeypatch, caplog, applicable):
    metadata = SimpleNamespace(
        schema=pa.schema([("id", pa.int32())]), version=(4, 0, 6), partitions=()
    )
    plan = SimpleNamespace(files=("data.parquet",))
    monkeypatch.setattr("ray_doris._native.ensure_native_ray_version", Mock())
    monkeypatch.setattr("ray_doris._native.discover_external", Mock(return_value=metadata))
    monkeypatch.setattr("ray_doris._native.plan_files", Mock(return_value=plan))
    monkeypatch.setattr(
        "ray_doris._native.read_native_plan",
        Mock(
            return_value="native",
            side_effect=None if applicable else _NativeReadNotApplicableError("profile"),
        ),
    )
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", Mock(return_value="doris"))
    with caplog.at_level("INFO", logger="ray_doris"):
        result = read_doris(host="fe", table="db.t", catalog="hive", auto_native_read=True)
    assert result == ("native" if applicable else "doris")
    selected = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "doris.read.path_selected"
    ]
    assert len(selected) == 1
    assert selected[0].path == ("native" if applicable else "doris")


@pytest.mark.parametrize(
    "comment, expected",
    [
        ("Doris version doris-4.0.6-release-abcd", (4, 0, 6)),
        ("Doris version 3.0.6.2-abcd", (3, 0, 6, 2)),
    ],
)
def test_doris_build_version(comment, expected):
    assert doris_build_version(comment) == expected


@pytest.mark.parametrize(
    "comment, expected",
    [
        ("5.7.99", ()),
        ("Doris version 1.2.2", (1, 2, 2)),
        ("Doris version 3.0.6", (3, 0, 6)),
        ("Doris version 4.0.6.1", (4, 0, 6, 1)),
        ("Doris version 3.0.6.3", (3, 0, 6, 3)),
    ],
)
def test_doris_version_is_diagnostic_only(comment, expected):
    assert doris_build_version(comment) == expected


def storage():
    return HiveStorage(
        "s3://bucket/table",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        (("id", "int"),),
    )


@pytest.mark.parametrize(
    "params",
    [
        {"transactional": "true"},
        {"table_type": "ICEBERG"},
        {"hoodie.name": "x"},
        {"storage_handler": "custom"},
        {"delta.version": "2"},
        {"spark.sql.sources.provider": "delta"},
        {"spark.sql.sources.provider": "iceberg"},
        {"spark.sql.sources.provider": "hudi"},
    ],
)
def test_format_markers_cannot_masquerade_as_parquet(params):
    with pytest.raises(DorisPlanningError):
        validate_storage(storage(), params)


@pytest.mark.parametrize("provider", ["hive", "parquet", "PARQUET"])
def test_standard_spark_and_hive_providers_remain_eligible(provider):
    validate_storage(storage(), {"spark.sql.sources.provider": provider})


def test_invalid_hms_port_never_changes_to_another_endpoint(monkeypatch):
    sdk_import = Mock(side_effect=AssertionError("unexpected SDK access"))
    monkeypatch.setattr("ray_doris._hms.import_module", sdk_import)
    with pytest.raises(DorisConfigurationError, match="port"):
        with open_hms("thrift://hms:0", 1):
            pass
    sdk_import.assert_not_called()


def test_schema_uses_doris_mapping_and_rejects_hms_disagreement():
    table = HiveTable(storage(), (), (), "EXTERNAL_TABLE")
    validate_table(table, pa.schema([("id", pa.int32())]))
    with pytest.raises(DorisSchemaError):
        validate_table(table, pa.schema([("id", pa.int64())]))
    with pytest.raises(DorisPlanningError):
        validate_table(
            replace(table, storage=replace(storage(), serde="custom")),
            pa.schema([("id", pa.int32())]),
        )


def test_doris_mysql_display_boolean_is_not_a_real_tinyint():
    assert (
        doris_type_to_arrow(information_schema_type("tinyint(1)"), column_name="flag") == pa.bool_()
    )
    assert (
        doris_type_to_arrow(information_schema_type("tinyint(4)"), column_name="tiny") == pa.int8()
    )


@pytest.mark.parametrize("value", ["__HIVE_DEFAULT_PARTITION__", "abc", str(2**63)])
def test_bad_partition_value(value):
    with pytest.raises(DorisPlanningError):
        partition_value(value, "bigint")


def test_raw_partition_names_are_preserved_when_numbers_collide():
    assert partition_value("01", "bigint") == partition_value("1", "bigint") == 1
    client = Mock()
    client.get_partition_names.return_value = ["p=01", "p=1"]
    client.partition_name_to_vals.side_effect = [["01"], ["1"]]
    sd = SimpleNamespace(
        location="s3://bucket/t/p=1",
        inputFormat=storage().input_format,
        serdeInfo=SimpleNamespace(serializationLib=storage().serde),
        cols=[SimpleNamespace(name="id", type="int")],
    )
    client.get_partitions_by_names.return_value = [
        SimpleNamespace(values=["1"], sd=sd, parameters={}),
        SimpleNamespace(values=["01"], sd=sd, parameters={}),
    ]
    parts = HiveMetadataClient(client).partitions("db", "t", 5)
    assert [p.name for p in parts] == ["p=1", "p=01"]


def test_partition_overflow_fails_before_descriptor_requests():
    client = Mock()
    client.get_partition_names.return_value = ["p=a", "p=b"]
    with pytest.raises(DorisPlanningError, match="max_partitions"):
        HiveMetadataClient(client).partitions("db", "t", 1)
    client.get_partitions_by_names.assert_not_called()


def test_hms_connection_closes_and_redacts_failure(monkeypatch):
    transport = Mock()
    sdk = Mock()
    modules = {
        "pymetastore.hive_metastore.ThriftHiveMetastore": sdk,
        "thrift.transport.TSocket": Mock(),
        "thrift.transport.TTransport": SimpleNamespace(
            TBufferedTransport=Mock(return_value=transport)
        ),
        "thrift.protocol.TBinaryProtocol": Mock(),
    }
    monkeypatch.setattr("ray_doris._hms.import_module", modules.__getitem__)
    sdk.Client.return_value.get_table.side_effect = RuntimeError("secret-storage-key")
    with pytest.raises(DorisPlanningError, match="HMS metadata request failed") as error:
        with open_hms("thrift://hms:9083", 0.0001) as client:
            client.table("db", "t")
    assert "secret" not in str(error.value)
    transport.close.assert_called_once()
    modules["thrift.transport.TSocket"].TSocket.return_value.setTimeout.assert_called_with(1)


@pytest.mark.parametrize("code", [1045, 1221, 5087])
def test_driver_metadata_never_sets_internal_default_database(monkeypatch, code):
    connect = Mock(side_effect=__import__("pymysql").err.OperationalError(code, "secret"))
    monkeypatch.setattr("ray_doris._external_metadata.pymysql.connect", connect)
    config = DorisReadConfig(host="fe", table=parse_table("external_db.t"), user="reader")
    with pytest.raises(DorisAuthenticationError if code == 1045 else DorisPermissionError):
        discover_external(config, "hive", NativeReadOptions())
    assert "database" not in connect.call_args.kwargs
    assert connect.call_args.kwargs["read_timeout"] > 0


@pytest.mark.parametrize("version", ["2.49.2", "2.58.0"])
def test_native_ray_capabilities_are_independent_of_patch_whitelist(monkeypatch, version):
    monkeypatch.setattr("ray_doris._compat._RAY_VERSION", version)
    discover = Mock(side_effect=RuntimeError("metadata reached"))
    monkeypatch.setattr("ray_doris._native.discover_external", discover)
    with pytest.raises(RuntimeError, match="metadata reached"):
        read_doris(host="unused", table="db.t", catalog="hive", auto_native_read=True)
    discover.assert_called_once()


def test_disabled_flag_does_not_import_or_probe_native(monkeypatch):
    native_import = Mock(side_effect=AssertionError("native probe used"))
    read = Mock(return_value="doris dataset")
    monkeypatch.setattr("ray_doris._api.import_module", native_import)
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", read)
    assert read_doris(host="fe", table="db.t", catalog="hive") == "doris dataset"
    assert read.call_args.args[0].config.catalog == "hive"
    native_import.assert_not_called()


@pytest.mark.parametrize("catalog", [None, "internal"])
def test_internal_source_does_not_probe_files(monkeypatch, catalog):
    native_import = Mock(side_effect=AssertionError("native probe used"))
    monkeypatch.setattr("ray_doris._api.import_module", native_import)
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", Mock(return_value="doris"))
    assert read_doris(host="fe", table="db.t", catalog=catalog, auto_native_read=True) == "doris"
    native_import.assert_not_called()


def test_not_applicable_retains_same_doris_source_and_logs_reason(monkeypatch, caplog):
    caplog.set_level("INFO", logger="ray_doris._api")
    native = Mock(side_effect=_NativeReadNotApplicableError("unsupported layout"))
    read = Mock(return_value="doris")
    monkeypatch.setattr("ray_doris._native.read_native", native)
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", read)
    assert (
        read_doris(
            host="fe",
            table="db.t",
            catalog="hive",
            columns=["id"],
            auto_native_read=True,
            concurrency=2,
            override_num_blocks=3,
            ray_remote_args={"num_cpus": 0.25},
        )
        == "doris"
    )
    assert native.call_args.args[0] is read.call_args.args[0].config
    assert read.call_args.kwargs == {
        "concurrency": 2,
        "override_num_blocks": 3,
        "ray_remote_args": {"num_cpus": 0.25},
    }
    selected = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "doris.read.path_selected"
    ]
    assert selected[0].path == "doris"
    assert selected[0].reason_code == "native_profile_not_applicable"


@pytest.mark.parametrize(
    "error",
    [
        DorisPlanningError("metadata broken"),
        DorisSchemaError("contradictory schema"),
        DorisPermissionError("denied"),
        DorisAuthenticationError("bad credential"),
        DorisConfigurationError("bad source options"),
    ],
)
def test_real_native_errors_never_start_doris_fallback(monkeypatch, error):
    monkeypatch.setattr("ray_doris._native.read_native", Mock(side_effect=error))
    read = Mock(side_effect=AssertionError("Doris retry used"))
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", read)
    with pytest.raises(type(error)) as captured:
        read_doris(host="fe", table="db.t", catalog="hive", auto_native_read=True)
    assert captured.value is error
    read.assert_not_called()


@pytest.mark.parametrize(
    "options",
    [
        {"filter": "id > 1"},
        {"transport": "flight"},
        {"tablet_size": 2},
        {"batch_size": 2},
        {"query_plan_timeout": 5},
        {"on_query_plan_error": "error"},
        {"flight_options": {}},
        {"http_port": 18030},
        {"flight_port": 18070},
        {"http_scheme": "https"},
        {"flight_scheme": "grpc+tls"},
        {"client_kwargs": {"init_command": "SET time_zone = '+00:00'"}},
    ],
)
def test_explicit_doris_options_skip_native_discovery(monkeypatch, options):
    monkeypatch.setattr("ray_doris.datasource._ensure_flight_available", Mock())
    native_import = Mock(side_effect=AssertionError("native probe used"))
    monkeypatch.setattr("ray_doris._api.import_module", native_import)
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", Mock(return_value="doris"))
    assert (
        read_doris(host="fe", table="db.t", catalog="hive", auto_native_read=True, **options)
        == "doris"
    )
    native_import.assert_not_called()


@pytest.mark.parametrize(
    "property_name", ["access_controller.class", "access_controller.properties.x"]
)
def test_catalog_policy_controller_retains_doris_before_hms(monkeypatch, property_name):
    metadata_connection(
        monkeypatch,
        [
            [{"build": "Doris version 4.0.6"}],
            [{"CatalogType": "hms", "Property": property_name, "Value": "configured"}],
        ],
    )
    hms = Mock(side_effect=AssertionError("HMS used"))
    monkeypatch.setattr("ray_doris._external_metadata.open_hms", hms)
    with pytest.raises(_NativeReadNotApplicableError, match="access controller"):
        discover_external(
            DorisReadConfig(host="fe", table=parse_table("db.t"), user="reader"),
            "hive",
            NativeReadOptions(),
        )
    hms.assert_not_called()


@pytest.mark.parametrize(
    "message, denied",
    [
        ("errCode = 2, detailMessage = Access denied for user 'u' to catalog 'hive'", True),
        ("errCode = 2, detailMessage = Unknown catalog 'hive'", False),
    ],
)
def test_nereids_catalog_denial_is_distinct_from_other_generic_errors(monkeypatch, message, denied):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    monkeypatch.setattr(
        "ray_doris._external_metadata.pymysql.connect", Mock(return_value=connection)
    )
    rows = Mock(
        side_effect=[
            [{"build": "Doris version 4.0.6"}],
            [],
            pymysql.err.OperationalError(1105, message),
        ]
    )
    monkeypatch.setattr("ray_doris._external_metadata._rows", rows)
    config = DorisReadConfig(host="fe", table=parse_table("db.t"), user="u")
    with pytest.raises(DorisPlanningError) as error:
        discover_external(config, "hive", NativeReadOptions())
    assert isinstance(error.value, DorisPermissionError) is denied
    assert "secret" not in str(error.value)


def metadata_connection(monkeypatch, responses):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    monkeypatch.setattr(
        "ray_doris._external_metadata.pymysql.connect", Mock(return_value=connection)
    )
    monkeypatch.setattr("ray_doris._external_metadata._rows", Mock(side_effect=responses))
    monkeypatch.setattr("ray_doris._external_metadata.validate_native_session", Mock())
    return connection


@pytest.mark.parametrize("version", ["Doris version 3.1.4", "Doris version 4.0.7", "vendor build"])
def test_unknown_build_can_use_complete_metadata_capabilities(monkeypatch, version):
    metadata_connection(
        monkeypatch,
        [
            [{"build": version}],
            [
                {
                    "CatalogType": "hms",
                    "Property": "hive.metastore.uris",
                    "Value": "thrift://hms:9083",
                }
            ],
            [{"COLUMN_NAME": "id", "COLUMN_TYPE": "INT", "IS_NULLABLE": "YES"}],
        ],
    )
    client = Mock()
    client.table.return_value = HiveTable(storage(), (), (), "EXTERNAL_TABLE")
    monkeypatch.setattr("ray_doris._external_metadata.open_hms", lambda *args: nullcontext(client))
    result = discover_external(
        DorisReadConfig(host="fe", table=parse_table("db.t"), user="u"), "hive", NativeReadOptions()
    )
    assert result.schema == pa.schema([("id", pa.int32())])
    assert result.version == doris_build_version(version)


@pytest.mark.parametrize("shown,expected", [(["p=01", "p=1"], None), (["p=2"], DorisPlanningError)])
def test_partition_metadata_fallback_keeps_raw_hms_identities(monkeypatch, shown, expected):
    metadata_connection(
        monkeypatch,
        [
            [{"build": "unidentified build"}],
            [
                {
                    "CatalogType": "hms",
                    "Property": "hive.metastore.uris",
                    "Value": "thrift://hms:9083",
                }
            ],
            [
                {"COLUMN_NAME": "id", "COLUMN_TYPE": "INT", "IS_NULLABLE": "YES"},
                {"COLUMN_NAME": "p", "COLUMN_TYPE": "BIGINT", "IS_NULLABLE": "YES"},
            ],
            pymysql.err.OperationalError(1105, "Unknown table t$partitions"),
            [{"Partition": name} for name in shown],
        ],
    )
    client = Mock()
    client.table.return_value = HiveTable(storage(), (("p", "bigint"),), (), "EXTERNAL_TABLE")
    client.partitions.return_value = tuple(
        HivePartition("p=" + value, (value,), storage(), ()) for value in ("01", "1")
    )
    monkeypatch.setattr("ray_doris._external_metadata.open_hms", lambda *args: nullcontext(client))
    options = DorisReadConfig(host="fe", table=parse_table("db.t"), user="u")
    if expected is not None:
        with pytest.raises(expected, match="identities differ"):
            discover_external(options, "hive", NativeReadOptions())
    else:
        result = discover_external(options, "hive", NativeReadOptions())
        assert [part.name for part in result.partitions] == ["p=01", "p=1"]


def test_nested_native_schema_retains_doris_before_hms_access(monkeypatch):
    metadata_connection(
        monkeypatch,
        [
            [{"build": "Doris version 4.0.6"}],
            [
                {
                    "CatalogType": "hms",
                    "Property": "hive.metastore.uris",
                    "Value": "thrift://hms:9083",
                }
            ],
            [{"COLUMN_NAME": "items", "COLUMN_TYPE": "ARRAY<INT>", "IS_NULLABLE": "YES"}],
        ],
    )
    hms = Mock(side_effect=AssertionError("unexpected HMS access"))
    monkeypatch.setattr("ray_doris._external_metadata.open_hms", hms)
    with pytest.raises(_NativeReadNotApplicableError, match="nested"):
        discover_external(
            DorisReadConfig(host="fe", table=parse_table("db.t"), user="u"),
            "hive",
            NativeReadOptions(),
        )
    hms.assert_not_called()


@pytest.mark.parametrize(
    "use_names,timezone,timestamp,expected",
    [
        ("true", "UTC", True, None),
        (1, "+00:00", True, None),
        (True, "Etc/UTC", True, None),
        ("true", "Asia/Shanghai", False, None),
        (False, "UTC", False, _NativeReadNotApplicableError),
        ("0", "UTC", True, _NativeReadNotApplicableError),
        ("true", "Asia/Shanghai", True, _NativeReadNotApplicableError),
        ("unknown", "UTC", False, DorisPlanningError),
    ],
)
def test_native_session_semantics(monkeypatch, use_names, timezone, timestamp, expected):
    monkeypatch.setattr(
        "ray_doris._external_metadata._rows",
        Mock(
            return_value=[
                {
                    "use_names": use_names,
                    "timezone": timezone,
                }
            ]
        ),
    )
    schema = pa.schema([("value", pa.timestamp("us") if timestamp else pa.int32())])
    if expected is None:
        validate_native_session(Mock(), schema)
    else:
        with pytest.raises(expected):
            validate_native_session(Mock(), schema)


@pytest.mark.parametrize(
    "setting,value",
    [
        ("hive.metastore.type", "glue"),
        ("hive.metastore.sasl.enabled", "true"),
        ("lower_case_meta_names", "true"),
        ("meta_names_mapping", "configured"),
    ],
)
def test_unsupported_catalog_configuration_rejected_before_hms(monkeypatch, setting, value):
    metadata_connection(
        monkeypatch,
        [
            [{"build": "Doris version 4.0.6"}],
            [{"CatalogType": "hms", "Property": setting, "Value": value}],
        ],
    )
    hms = Mock(side_effect=AssertionError("unexpected HMS access"))
    monkeypatch.setattr("ray_doris._external_metadata.open_hms", hms)
    with pytest.raises(DorisPlanningError, match="authentication or remote-name"):
        discover_external(
            DorisReadConfig(host="fe", table=parse_table("db.t"), user="u"),
            "hive",
            NativeReadOptions(),
        )
    hms.assert_not_called()


def test_stale_doris_partition_values_do_not_select_a_different_hms_scope(monkeypatch):
    connection = metadata_connection(
        monkeypatch,
        [
            [{"build": "Doris version 4.0.6"}],
            [
                {
                    "CatalogType": "hms",
                    "Property": "hive.metastore.uris",
                    "Value": "thrift://hms:9083",
                }
            ],
            [
                {"IS_NULLABLE": "YES", "COLUMN_TYPE": "int(11)", "COLUMN_NAME": "id"},
                {"IS_NULLABLE": "YES", "COLUMN_TYPE": "bigint(20)", "COLUMN_NAME": "p"},
            ],
            [{"p": 2}],
        ],
    )
    client = Mock()
    client.table.return_value = HiveTable(storage(), (("p", "bigint"),), (), "EXTERNAL_TABLE")
    client.partitions.return_value = (HivePartition("p=1", ("1",), storage(), ()),)
    monkeypatch.setattr("ray_doris._external_metadata.open_hms", lambda *args: nullcontext(client))
    with pytest.raises(DorisPlanningError, match="registered partition values differ"):
        discover_external(
            DorisReadConfig(host="fe", table=parse_table("db.t"), user="u"),
            "hive",
            NativeReadOptions(),
        )
    connection.__exit__.assert_called_once()


def test_canonical_timestamp_cast_never_truncates_submicrosecond_values():
    batch = pa.table({"ts": pa.array([1], type=pa.timestamp("ns"))})
    with pytest.raises(pa.ArrowInvalid):
        _canonicalize_batch(batch, schema=pa.schema([("ts", pa.timestamp("us"))]))


def test_bad_footer_closes_input_and_redacts_storage_details():
    source = pa.BufferReader(b"not parquet")
    filesystem = Mock()
    filesystem.open_input_file.return_value = source
    plan = NativeReadPlan(pa.schema([("id", pa.int32())]), ("secret-source",), filesystem, "", ())
    with pytest.raises(DorisPlanningError, match="footer discovery") as error:
        _validate_file_schemas(plan)
    assert source.closed
    assert "secret-source" not in str(error.value)
