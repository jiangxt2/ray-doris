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
from ray_doris._external_metadata import (
    discover_external,
    doris_build_version,
    information_schema_type,
    partition_value,
    validate_storage,
    validate_table,
)
from ray_doris._hms import HiveMetadataClient, HivePartition, HiveStorage, HiveTable, open_hms
from ray_doris._models import DorisReadConfig
from ray_doris._native import NativeReadPlan, _canonicalize_batch, _validate_file_schemas
from ray_doris._schema import doris_type_to_arrow
from ray_doris._sql import parse_table


@pytest.mark.parametrize(
    "kwargs",
    [
        {"read_mode": "bad"},
        {"catalog": "hive"},
        {"native_options": NativeReadOptions()},
        {"read_mode": "native"},
        {"read_mode": "native", "catalog": "hive", "filter": "id=1"},
        {"read_mode": "native", "catalog": "hive", "transport": "flight"},
        {"read_mode": "native", "catalog": "hive", "tablet_size": 2},
        {"read_mode": "native", "catalog": "hive", "native_options": {}},
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


def test_native_routes_without_constructing_doris_datasource(monkeypatch):
    delegate = Mock(return_value="native dataset")
    monkeypatch.setattr("ray_doris._native.read_native", delegate)
    forbidden = Mock(side_effect=AssertionError("Doris read path used"))
    monkeypatch.setattr("ray_doris._api.DorisDatasource", forbidden)
    assert (
        read_doris(
            host="fe",
            table="db.tbl",
            read_mode="native",
            catalog="hive",
            columns=["id"],
            concurrency=2,
        )
        == "native dataset"
    )
    assert delegate.call_args.args[0].columns == ("id",)
    assert delegate.call_args.kwargs["concurrency"] == 2
    forbidden.assert_not_called()


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
    "comment",
    [
        "5.7.99",
        "Doris version 1.2.2",
        "Doris version 3.0.6",
        "Doris version 4.0.6.1",
        "Doris version 3.0.6.3",
    ],
)
def test_unverified_doris_build_rejected(comment):
    with pytest.raises(DorisPlanningError):
        doris_build_version(comment)


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
def test_native_unsupported_ray_rejected_before_io(monkeypatch, version):
    monkeypatch.setattr("ray_doris._compat._RAY_VERSION", version)
    connect = Mock(side_effect=AssertionError("unexpected metadata I/O"))
    monkeypatch.setattr("ray_doris._external_metadata.pymysql.connect", connect)
    with pytest.raises(DorisConfigurationError, match="Ray 2.59.0"):
        read_doris(host="unused", table="db.t", catalog="hive", read_mode="native")
    connect.assert_not_called()


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
    return connection


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
