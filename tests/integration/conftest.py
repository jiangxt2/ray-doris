from __future__ import annotations

import math
import os
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Iterator

import pymysql
import pytest
import ray

from ray_doris import DorisConnection, DorisTable

READER_PASSWORD_ENV = "RAY_DORIS_IT_READER_PASSWORD"


@dataclass(frozen=True)
class DorisITConfig:
    host: str
    mysql_port: int
    http_port: int
    flight_port: int
    be_http_port: int
    user: str
    password: str
    database: str = "ray_doris_it"
    table: str = "records"
    empty_table: str = "empty_records"
    reader_user: str = "ray_doris_reader"
    reader_password: str = "reader-password"

    def write_connection(
        self, *, user: str | None = None, password: str | None = None
    ) -> DorisConnection:
        return DorisConnection(
            host=self.host,
            username=self.user if user is None else user,
            password=self.password if password is None else password,
            http_port=self.http_port,
            mysql_port=self.mysql_port,
            redirect_hosts=(self.host,),
            redirect_ports=(self.be_http_port,),
            redirect_policy="public",
        )

    def write_table(self, name: str) -> DorisTable:
        return DorisTable(self.database, name)

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

    def minimal_env_reader_kwargs(
        self,
        *,
        transport: str = "mysql",
        password_env: str = READER_PASSWORD_ENV,
        **kwargs: object,
    ) -> dict:
        values = self.reader_kwargs(transport=transport, **kwargs)
        values.update(user=self.reader_user, password="", password_env=password_env)
        return values


def _config_from_environment() -> DorisITConfig:
    return DorisITConfig(
        host=os.environ.get("DORIS_HOST", "127.0.0.1"),
        mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "19030")),
        http_port=int(os.environ.get("DORIS_HTTP_PORT", "18030")),
        flight_port=int(os.environ.get("DORIS_FLIGHT_PORT", "18070")),
        be_http_port=int(os.environ.get("DORIS_BE_HTTP_PORT", "18040")),
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


def _is_positive_capacity(value: object) -> bool:
    parts = str(value).split(maxsplit=1)
    if not parts:
        return False
    try:
        capacity = float(parts[0])
    except ValueError:
        return False
    return math.isfinite(capacity) and capacity > 0


def _doris_backends_ready(description: Sequence[str], rows: Iterable[Sequence[object]]) -> bool:
    try:
        alive_index = description.index("Alive")
        available_index = description.index("AvailCapacity")
        total_index = description.index("TotalCapacity")
    except ValueError:
        return False
    backend_rows = tuple(rows)
    if not backend_rows:
        return False
    for row in backend_rows:
        try:
            alive = str(row[alive_index]).casefold() == "true"
            has_available_capacity = _is_positive_capacity(row[available_index])
            has_total_capacity = _is_positive_capacity(row[total_index])
        except IndexError:
            return False
        if not (alive and has_available_capacity and has_total_capacity):
            return False
    return True


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
                    description = tuple(column[0] for column in cursor.description)
                    rows = cursor.fetchall()
                    if _doris_backends_ready(description, rows):
                        return
                finally:
                    cursor.close()
            finally:
                connection.close()
        except (OSError, pymysql.MySQLError, ValueError) as exc:
            last_error = exc
        time.sleep(2)
    raise RuntimeError("Doris FE/BE did not become ready within 240 seconds") from last_error


def _configure_stream_load_endpoint(config: DorisITConfig) -> None:
    connection = _connect(config)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("SHOW BACKENDS")
            names = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            if not rows:
                raise RuntimeError("Doris did not report a backend")
            host = str(rows[0][names.index("Host")])
            heartbeat_port = int(rows[0][names.index("HeartbeatPort")])
            endpoint = f"{config.host}:{config.be_http_port}"
            cursor.execute(
                f"ALTER SYSTEM MODIFY BACKEND '{host}:{heartbeat_port}' SET "
                f"('tag.location' = 'default', 'tag.public_endpoint' = '{endpoint}')"
            )
        finally:
            cursor.close()
    finally:
        connection.close()


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
                CREATE TABLE `{config.database}`.`write_events` (
                    `id` BIGINT NOT NULL,
                    `category` VARCHAR(32) NULL,
                    `score` INT NULL,
                    `payload` JSON NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
                """,
            )
            _execute(
                cursor,
                f"""
                CREATE TABLE `{config.database}`.`unique_events` (
                    `id` BIGINT NOT NULL,
                    `category` VARCHAR(32) NULL,
                    `score` INT NULL,
                    `payload` JSON NULL
                ) ENGINE=OLAP
                UNIQUE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
                """,
            )
            _execute(
                cursor,
                f"""
                CREATE TABLE `{config.database}`.`partial_events` (
                    `id` BIGINT NOT NULL,
                    `category` VARCHAR(32) NULL,
                    `score` INT NULL,
                    `payload` JSON NULL
                ) ENGINE=OLAP
                UNIQUE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
                )
                """,
            )
            _execute(
                cursor,
                f"""
                CREATE TABLE `{config.database}`.`aggregate_events` (
                    `id` BIGINT NOT NULL,
                    `score` INT SUM
                ) ENGINE=OLAP
                AGGREGATE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
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
    _configure_stream_load_endpoint(config)
    try:
        _create_fixture_data(config)
        yield config
    finally:
        _cleanup_fixture_data(config)


@pytest.fixture(scope="session", autouse=True)
def ray_runtime(doris_config: DorisITConfig) -> Iterator[None]:
    previous_password = os.environ.get(READER_PASSWORD_ENV)
    os.environ[READER_PASSWORD_ENV] = doris_config.reader_password
    ray.init(num_cpus=4, include_dashboard=False)
    try:
        yield
    finally:
        ray.shutdown()
        if previous_password is None:
            os.environ.pop(READER_PASSWORD_ENV, None)
        else:
            os.environ[READER_PASSWORD_ENV] = previous_password
