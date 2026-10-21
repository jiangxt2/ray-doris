"""Metadata source priority, completeness and error boundaries."""

from unittest.mock import Mock

import pyarrow as pa
import pymysql
import pytest

from ray_doris import DorisPlanningError, DorisSchemaError
from ray_doris._errors import _NativeReadNotApplicableError
from ray_doris._metadata import ExternalMetadataReader, capability_missing
from ray_doris._schema import build_arrow_schema
from ray_doris._sql import parse_table


@pytest.mark.parametrize(
    "source,code,message,missing",
    [
        ("catalog", 1305, "FUNCTION catalogs does not exist", True),
        ("catalog", 1105, "Unknown table valued function catalogs", True),
        ("columns", 1054, "Unknown column COLUMN_TYPE", True),
        ("partitions", 1105, "Unknown table t$partitions", True),
        ("partitions", 1105, "Unknown table t", False),
        ("session", 1193, "Unknown system variable", True),
        ("catalog", 1105, "Access denied for user to catalog", False),
        ("columns", 1142, "Access denied", False),
        ("catalog", 2003, "Connection refused", False),
        ("catalog", 1105, "Unexpected engine failure", False),
        ("catalog", 1305, "Permission denied", False),
        ("show-catalog", 1064, "Syntax error near SHOW CATALOG", True),
        ("show-catalog", 1105, "Unknown catalog requested", False),
        ("show-partitions", 1105, "SHOW PARTITIONS is not supported", True),
        ("show-partitions", 1105, "Access denied for SHOW PARTITIONS", False),
    ],
)
def test_only_explicit_metadata_absence_is_a_capability_miss(source, code, message, missing):
    assert capability_missing(pymysql.err.OperationalError(code, message), source) is missing


@pytest.mark.parametrize("primary", [[], pymysql.err.OperationalError(1305, "missing catalogs")])
def test_catalog_falls_back_to_structured_show_and_caches(primary):
    query = Mock(
        side_effect=[
            primary,
            [{"Key": "hive.metastore.uris", "Value": "thrift://hms:9083"}],
            [{"CatalogName": "hive", "Type": "hms"}],
        ]
    )
    reader = ExternalMetadataReader(Mock(), query)
    expected = {"hive.metastore.uris": "thrift://hms:9083"}
    assert reader.catalog("hive") == expected
    assert reader.catalog("hive") == expected and query.call_count == 3
    assert query.call_args_list[1].args[1] == "SHOW CATALOG `hive`"
    assert query.call_args_list[2].args[1] == "SHOW CATALOGS"
    assert reader.origins == {"catalog": "show-catalog"}


@pytest.mark.parametrize("source", ["catalog", "columns", "native_settings"])
def test_real_errors_never_try_another_metadata_provider(source):
    denied = pymysql.err.OperationalError(1142, "secret denied")
    query = Mock(side_effect=denied)
    reader = ExternalMetadataReader(Mock(), query)
    with pytest.raises(pymysql.MySQLError) as caught:
        if source == "catalog":
            reader.catalog("hive")
        elif source == "columns":
            reader.columns(parse_table("db.t"), "hive")
        else:
            reader.native_settings(timestamp=True)
    assert caught.value is denied and query.call_count == 1


@pytest.mark.parametrize("primary", [[], pymysql.err.OperationalError(1054, "COLUMN_TYPE absent")])
@pytest.mark.parametrize("nullable,required", [("YES", "NO"), ("Yes", "No"), ("yes", "no")])
def test_columns_fall_back_to_describe_and_normalize_display_types(primary, nullable, required):
    query = Mock(
        side_effect=[
            primary,
            [
                {"Field": "value", "Type": "text", "Null": nullable},
                {"Field": "flag", "Type": "tinyint(1)", "Null": required},
            ],
        ]
    )
    reader = ExternalMetadataReader(Mock(), query)
    schema = build_arrow_schema(reader.columns(parse_table("db.t"), "hive"))
    assert schema == pa.schema(
        [("value", pa.string()), pa.field("flag", pa.bool_(), nullable=False)]
    )
    assert reader.columns(parse_table("db.t"), "hive")
    assert query.call_count == 2 and reader.origins["columns"] == "describe"


@pytest.mark.parametrize("nullable", ["unknown", None, True, [], 1])
def test_describe_normalization_does_not_accept_invalid_nullability(nullable):
    query = Mock(side_effect=[[], [{"Field": "id", "Type": "INT", "Null": nullable}]])
    with pytest.raises(DorisSchemaError, match="invalid external column metadata"):
        ExternalMetadataReader(Mock(), query).columns(parse_table("db.t"), "hive")
    assert query.call_count == 2


def test_malformed_primary_columns_are_not_replaced_by_a_better_result():
    query = Mock(return_value=[{"COLUMN_NAME": "id", "COLUMN_TYPE": "INT", "IS_NULLABLE": None}])
    with pytest.raises(DorisSchemaError):
        ExternalMetadataReader(Mock(), query).columns(parse_table("db.t"), "hive")
    assert query.call_count == 1


@pytest.mark.parametrize("timestamp", [False, True])
def test_session_falls_back_to_effective_show_variables(timestamp):
    query = Mock(
        side_effect=[
            pymysql.err.OperationalError(1193, "Unknown system variable"),
            [{"Variable_name": "hive_parquet_use_column_names", "Value": "true"}],
            [{"Variable_name": "time_zone", "Value": "UTC"}],
        ]
    )
    reader = ExternalMetadataReader(Mock(), query)
    assert reader.native_settings(timestamp=timestamp) == ("true", "UTC" if timestamp else None)
    assert query.call_count == (3 if timestamp else 2)
    assert reader.origins["session"] == "show-variables"


def test_missing_required_session_setting_retains_doris():
    query = Mock(side_effect=[pymysql.err.OperationalError(1193, "Unknown system variable"), []])
    with pytest.raises(_NativeReadNotApplicableError):
        ExternalMetadataReader(Mock(), query).native_settings(timestamp=False)


def test_malformed_catalog_is_not_a_capability_miss():
    query = Mock(return_value=[{"CatalogType": "hms", "Property": "key", "Value": None}])
    with pytest.raises(DorisPlanningError):
        ExternalMetadataReader(Mock(), query).catalog("hive")
    assert query.call_count == 1


def test_unavailable_final_catalog_interface_retains_doris():
    query = Mock(side_effect=[[], pymysql.err.OperationalError(1064, "SHOW CATALOG syntax")])
    with pytest.raises(_NativeReadNotApplicableError, match="catalog metadata"):
        ExternalMetadataReader(Mock(), query).catalog("hive")


@pytest.mark.parametrize("source", ["catalog", "columns", "native_settings"])
def test_secondary_provider_errors_remain_visible(source):
    denied = pymysql.err.OperationalError(1142, "access denied")
    first = (
        pymysql.err.OperationalError(1193, "Unknown variable")
        if source == "native_settings"
        else []
    )
    query = Mock(side_effect=[first, denied])
    reader = ExternalMetadataReader(Mock(), query)
    with pytest.raises(pymysql.MySQLError) as caught:
        if source == "catalog":
            reader.catalog("hive")
        elif source == "columns":
            reader.columns(parse_table("db.t"), "hive")
        else:
            reader.native_settings(timestamp=True)
    assert caught.value is denied and query.call_count == 2


def test_conflicting_catalog_type_is_not_downgraded():
    query = Mock(return_value=[{"CatalogType": "hms", "Property": "type", "Value": "iceberg"}])
    with pytest.raises(DorisPlanningError, match="disagree"):
        ExternalMetadataReader(Mock(), query).catalog("hive")
    assert query.call_count == 1


@pytest.mark.parametrize(
    "rows", [[{"Variable_name": "unrelated", "Value": "true"}], [{"use_names": "true"}]]
)
def test_malformed_secondary_session_metadata_is_not_a_capability_miss(rows):
    query = Mock(side_effect=[pymysql.err.OperationalError(1193, "Unknown variable"), rows])
    with pytest.raises(DorisPlanningError, match="malformed"):
        ExternalMetadataReader(Mock(), query).native_settings(timestamp=False)
