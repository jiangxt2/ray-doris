"""Shared Doris and S3 helpers for native integration tests."""

import os

import pyarrow.fs as fs
import pymysql


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
