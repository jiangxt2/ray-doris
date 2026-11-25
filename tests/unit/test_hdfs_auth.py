"""Tests for HDFS Kerberos properties visible in Doris catalog metadata."""

import pytest

from ray_doris._errors import DorisPlanningError
from ray_doris._hdfs_auth import HdfsKerberosConfig, catalog_hdfs_kerberos


@pytest.mark.parametrize(
    "properties",
    [
        pytest.param(
            {
                "hdfs.authentication.type": "kerberos",
                "hdfs.authentication.kerberos.principal": "reader@EXAMPLE.COM",
                "hdfs.authentication.kerberos.keytab": "/secure/reader.keytab",
            },
            id="hdfs-properties",
        ),
        pytest.param(
            {
                "hadoop.security.authentication": "kerberos",
                "hadoop.kerberos.principal": "reader@EXAMPLE.COM",
                "hadoop.kerberos.keytab": "/secure/reader.keytab",
            },
            id="hadoop-properties",
        ),
    ],
)
def test_extract_catalog_kerberos(properties):
    result = catalog_hdfs_kerberos(properties)
    assert result == HdfsKerberosConfig("reader@EXAMPLE.COM", "/secure/reader.keytab")
    assert "reader" not in repr(result)


def test_simple_or_absent_auth_has_no_kerberos_identity():
    assert catalog_hdfs_kerberos({}) is None
    assert catalog_hdfs_kerberos({"hdfs.authentication.type": "simple"}) is None


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"hadoop.security.authentication": "simple"}, id="conflicting-auth"),
        pytest.param({"hdfs.authentication.kerberos.principal": "*XXX"}, id="masked-value"),
        pytest.param({"hdfs.authentication.kerberos.principal": "******"}, id="masked-stars"),
        pytest.param(
            {"hdfs.authentication.kerberos.keytab": "relative.keytab"}, id="relative-path"
        ),
        pytest.param(
            {"hdfs.authentication.kerberos.keytab": "/secure/../other.keytab"}, id="path-traversal"
        ),
        pytest.param(
            {"hdfs.authentication.kerberos.keytab": "/secure/invalid\x00.keytab"},
            id="control-character",
        ),
        pytest.param({"hdfs.authentication.kerberos.principal": ""}, id="empty-principal"),
        pytest.param({"hdfs.authentication.kerberos.principal": "-bad"}, id="option-principal"),
        pytest.param(
            {"hadoop.kerberos.principal": "other@EXAMPLE.COM"}, id="conflicting-principal"
        ),
    ],
)
def test_reject_ambiguous_or_invalid_catalog_auth(overrides):
    properties = {
        "hdfs.authentication.type": "kerberos",
        "hdfs.authentication.kerberos.principal": "reader@EXAMPLE.COM",
        "hdfs.authentication.kerberos.keytab": "/secure/reader.keytab",
    }
    properties.update(overrides)
    with pytest.raises(DorisPlanningError) as error:
        catalog_hdfs_kerberos(properties)
    assert "reader" not in str(error.value)


def test_reject_missing_identity():
    with pytest.raises(DorisPlanningError, match="invalid"):
        catalog_hdfs_kerberos({"hdfs.authentication.type": "kerberos"})


def test_reject_unknown_auth_type():
    with pytest.raises(DorisPlanningError, match="unsupported"):
        catalog_hdfs_kerberos({"hdfs.authentication.type": "unknown"})


def test_reject_identity_without_auth_type():
    with pytest.raises(DorisPlanningError, match="type is missing"):
        catalog_hdfs_kerberos({"hadoop.kerberos.principal": "reader@EXAMPLE.COM"})
