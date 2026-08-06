from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Iterator

import pymysql
import pytest
import ray


@dataclass(frozen=True)
class DorisITConfig:
    host: str
    mysql_port: int
    http_port: int
    flight_port: int
    user: str
    password: str
    database: str = "ray_doris_it"
    table: str = "records"
    empty_table: str = "empty_records"
    reader_user: str = "ray_doris_reader"
    reader_password: str = "reader-password"

    def reader_kwargs(self, *, transport: str = "mysql", **kwargs: object) -> dict:
        values = {
            "table": f"{self.database}.{self.table}",
            "host": self.host,
            "mysql_port": self.mysql_port,
            "http_port": self.http_port,
            "flight_port": self.flight_port,
            "user": self.user,
            "password": self.password,
            "transport": transport,
        }
        values.update(kwargs)
        return values

    def minimal_reader_kwargs(self, *, transport: str = "mysql", **kwargs: object) -> dict:
        values = self.reader_kwargs(transport=transport, **kwargs)
        values.update(user=self.reader_user, password=self.reader_password)
        return values


def _config_from_environment() -> DorisITConfig:
    return DorisITConfig(
        host=os.environ.get("DORIS_HOST", "127.0.0.1"),
        mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "19030")),
        http_port=int(os.environ.get("DORIS_HTTP_PORT", "18030")),
        flight_port=int(os.environ.get("DORIS_FLIGHT_PORT", "18070")),
        user=os.environ.get("DORIS_USER", "root"),
        password=os.environ.get("DORIS_PASSWORD", ""),
    )


def _connect(config: DorisITConfig, *, database: str | None = None):
    return pymysql.connect(
        host=config.host,
        port=config.mysql_port,
        user=config.user,
        password=config.password,
        database=database,
        charset="utf8mb4",
        autocommit=True,
    )


def _wait_for_doris(config: DorisITConfig) -> None:
    deadline = time.monotonic() + 240
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            connection = _connect(config)
            try:
                cursor = connection.cursor()
                try:
                    cursor.execute("SHOW BACKENDS")
                    names = [description[0] for description in cursor.description]
                    alive_index = names.index("Alive")
                    rows = cursor.fetchmany(16)
                    if rows and any(str(row[alive_index]).lower() == "true" for row in rows):
                        return
                finally:
                    cursor.close()
            finally:
                connection.close()
        except (OSError, pymysql.MySQLError, ValueError) as exc:
            last_error = exc
        time.sleep(2)
    raise RuntimeError("Doris FE/BE did not become ready within 240 seconds") from last_error


def _execute(cursor, sql: str) -> None:
    cursor.execute(sql)


def _create_fixture_data(config: DorisITConfig) -> None:
    connection = _connect(config)
    try:
        cursor = connection.cursor()
        try:
            _execute(cursor, f"DROP DATABASE IF EXISTS `{config.database}`")
            _execute(cursor, f"CREATE DATABASE `{config.database}`")
            _execute(
                cursor,
                f"""
                CREATE TABLE `{config.database}`.`{config.table}` (
                    `id` BIGINT NOT NULL,
                    `category` VARCHAR(32) NULL,
                    `amount` DECIMALV3(20, 6) NULL,
                    `created_at` DATETIMEV2(6) NULL,
                    `event_date` DATEV2 NULL,
                    `active` BOOLEAN NULL,
                    `large_value` LARGEINT NULL,
                    `payload` JSON NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 4
                PROPERTIES ("replication_num" = "1")
                """,
            )
            _execute(
                cursor,
                f"""
                INSERT INTO `{config.database}`.`{config.table}` VALUES
                  (1, 'alpha', 1234.567890, '2025-01-01 01:02:03.123456', '2025-01-01',
                   TRUE, 170141183460469231731687303715884105727, '{{"source":"a"}}'),
                  (2, NULL, NULL, '2025-01-02 01:02:03.000001', '2025-01-02',
                   FALSE, -170141183460469231731687303715884105728, NULL),
                  (3, 'beta', 0.000001, NULL, NULL, NULL, 0, '{{"source":"b"}}'),
                  (4, 'alpha', 99999999999999.999999, '2025-01-04 04:05:06.654321',
                   '2025-01-04', TRUE, 42, '{{"source":"a"}}')
                """,
            )
            _execute(cursor, "DROP USER IF EXISTS 'ray_doris_no_select'@'%'")
            _execute(
                cursor,
                "CREATE USER 'ray_doris_no_select'@'%' IDENTIFIED BY 'no-select-password'",
            )
            _execute(cursor, f"DROP USER IF EXISTS '{config.reader_user}'@'%'")
            _execute(
                cursor,
                f"CREATE USER '{config.reader_user}'@'%' IDENTIFIED BY '{config.reader_password}'",
            )
            _execute(
                cursor,
                f"GRANT SELECT_PRIV ON internal.{config.database}.{config.table} "
                f"TO '{config.reader_user}'@'%'",
            )
            _execute(
                cursor,
                f"""
                CREATE TABLE `{config.database}`.`{config.empty_table}` (
                    `id` BIGINT NOT NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
                """,
            )
        finally:
            cursor.close()
    finally:
        connection.close()


def _cleanup_fixture_data(config: DorisITConfig) -> None:
    connection = _connect(config)
    try:
        cursor = connection.cursor()
        try:
            _execute(cursor, f"DROP DATABASE IF EXISTS `{config.database}`")
            _execute(cursor, "DROP USER IF EXISTS 'ray_doris_no_select'@'%'")
            _execute(cursor, f"DROP USER IF EXISTS '{config.reader_user}'@'%'")
        finally:
            cursor.close()
    finally:
        connection.close()


@pytest.fixture(scope="session")
def doris_config() -> Iterator[DorisITConfig]:
    config = _config_from_environment()
    _wait_for_doris(config)
    try:
        _create_fixture_data(config)
        yield config
    finally:
        _cleanup_fixture_data(config)


@pytest.fixture(scope="session", autouse=True)
def ray_runtime() -> Iterator[None]:
    ray.init(num_cpus=4, include_dashboard=False)
    try:
        yield
    finally:
        ray.shutdown()
