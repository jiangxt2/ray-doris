"""Validate Kerberos settings advertised by a Doris Hive catalog."""

from __future__ import annotations

import posixpath
from dataclasses import dataclass, field
from typing import Mapping, Optional

from ray_doris._errors import DorisPlanningError

KEYTAB_ROOT_ENV = "RAY_DORIS_HDFS_KEYTAB_ROOT"


@dataclass(frozen=True)
class HdfsKerberosConfig:
    """Catalog identity and keytab path, excluding all credential contents."""

    principal: str = field(repr=False)
    keytab: str = field(repr=False)


def _catalog_value(properties: Mapping[str, str], current: str, legacy: str) -> Optional[str]:
    values = [properties[key].strip() for key in (current, legacy) if key in properties]
    if not values:
        return None
    if (
        any(not value or value.upper() == "*XXX" or not value.strip("*") for value in values)
        or len(set(values)) != 1
    ):
        raise DorisPlanningError(
            "HDFS catalog authentication properties are incomplete or conflict"
        )
    return values[0]


def catalog_hdfs_kerberos(properties: Mapping[str, str]) -> Optional[HdfsKerberosConfig]:
    """Extract a Kerberos identity without exposing keytab or ticket contents.

    This validates catalog metadata only; it does not establish the trust or
    availability of the advertised path on the driver and worker nodes.
    """
    auth = _catalog_value(properties, "hdfs.authentication.type", "hadoop.security.authentication")
    if auth is None:
        if any(
            key in properties
            for key in (
                "hdfs.authentication.kerberos.principal",
                "hdfs.authentication.kerberos.keytab",
                "hadoop.kerberos.principal",
                "hadoop.kerberos.keytab",
            )
        ):
            raise DorisPlanningError("HDFS catalog authentication type is missing")
        return None
    if auth.lower() == "simple":
        return None
    if auth.lower() != "kerberos":
        raise DorisPlanningError("unsupported HDFS catalog authentication type")
    principal = _catalog_value(
        properties, "hdfs.authentication.kerberos.principal", "hadoop.kerberos.principal"
    )
    keytab = _catalog_value(
        properties, "hdfs.authentication.kerberos.keytab", "hadoop.kerberos.keytab"
    )
    if (
        principal is None
        or keytab is None
        or principal.startswith("-")
        or not posixpath.isabs(keytab)
        or ".." in keytab.split("/")
        or any(ord(char) < 32 or ord(char) == 127 for char in principal + keytab)
    ):
        raise DorisPlanningError("HDFS catalog Kerberos identity or keytab path is invalid")
    return HdfsKerberosConfig(principal, keytab)
