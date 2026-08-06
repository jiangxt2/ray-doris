from __future__ import annotations

import csv
import os
import time
import urllib.request
from dataclasses import dataclass
from typing import Any

import pymysql

BUCKET_COUNT = 48
DATABASE = "ray_doris_slow_it"
DISTRIBUTED_TABLE = "distributed_records"
REPLICATED_TABLE = "replicated_records"
FLIGHT_PROXY_BACKENDS = ("be-1", "be-2", "be-3")


def _positive_env_int(name: str, default: int, maximum: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if not 1 <= value <= maximum:
        raise RuntimeError(f"{name} must be between 1 and {maximum}")
    return value


@dataclass(frozen=True)
class FlightProxyBackendStats:
    status: str
    total_sessions: int
    bytes_in: int
    bytes_out: int


@dataclass(frozen=True)
class SlowITConfig:
    host: str
    mysql_port: int
    https_port: int
    flight_port: int
    flight_proxy_stats_url: str
    tls_ca: str
    row_count: int
    stress_seconds: int
    worker_ips: tuple[str, ...]
    database: str = DATABASE
    distributed_table: str = DISTRIBUTED_TABLE
    replicated_table: str = REPLICATED_TABLE

    @classmethod
    def from_environment(cls) -> SlowITConfig:
        worker_ips = tuple(
            value.strip()
            for value in os.environ.get(
                "RAY_WORKER_IPS",
                "172.31.128.11,172.31.128.12,172.31.128.13",
            ).split(",")
            if value.strip()
        )
        if len(worker_ips) != 3:
            raise RuntimeError("RAY_WORKER_IPS must contain exactly three addresses")
        return cls(
            host=os.environ.get("DORIS_HOST", "flight-proxy"),
            mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "19030")),
            https_port=int(os.environ.get("DORIS_HTTPS_PORT", "18443")),
            flight_port=int(os.environ.get("DORIS_FLIGHT_PORT", "18070")),
            flight_proxy_stats_url=os.environ.get(
                "DORIS_FLIGHT_PROXY_STATS_URL",
                "http://flight-proxy:8404/stats;csv",
            ),
            tls_ca=os.environ.get("DORIS_TLS_CA", "/tls/ca.pem"),
            row_count=_positive_env_int("RAY_DORIS_ROW_COUNT", 10_000, 50_000_000),
            stress_seconds=_positive_env_int(
                "RAY_DORIS_STRESS_SECONDS",
                5,
                86_400,
            ),
            worker_ips=worker_ips,
        )

    @property
    def expected_id_sum(self) -> int:
        return self.row_count * (self.row_count - 1) // 2

    def reader_kwargs(
        self,
        *,
        table: str,
        transport: str = "flight",
        **kwargs: Any,
    ) -> dict[str, Any]:
        values: dict[str, Any] = {
            "table": f"{self.database}.{table}",
            "host": self.host,
            "mysql_port": self.mysql_port,
            "http_port": self.https_port,
            "http_scheme": "https",
            "flight_port": self.flight_port,
            "flight_scheme": "grpc",
            "user": "root",
            "password": "",
            "transport": transport,
            "on_query_plan_error": "error",
            "connect_timeout": 30.0,
            "client_kwargs": {
                "ssl": {
                    "ca": self.tls_ca,
                    "check_hostname": True,
                },
                "read_timeout": 180,
                "write_timeout": 180,
            },
            "flight_options": {
                "adbc.flight.sql.rpc.timeout_seconds.query": "180",
                "adbc.flight.sql.rpc.timeout_seconds.fetch": "180",
            },
        }
        values.update(kwargs)
        return values


def mysql_connection(config: SlowITConfig):
    return pymysql.connect(
        host=config.host,
        port=config.mysql_port,
        user="root",
        password="",
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30,
        read_timeout=180,
        write_timeout=180,
        ssl={"ca": config.tls_ca, "check_hostname": True},
    )


def query_rows(config: SlowITConfig, sql: str) -> list[dict[str, Any]]:
    connection = mysql_connection(config)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            rows: list[dict[str, Any]] = []
            while True:
                batch = cursor.fetchmany(256)
                if not batch:
                    return rows
                rows.extend(batch)
        finally:
            cursor.close()
    finally:
        connection.close()


def flight_proxy_backend_stats(
    config: SlowITConfig,
) -> dict[str, FlightProxyBackendStats]:
    request = urllib.request.Request(
        config.flight_proxy_stats_url,
        headers={"Accept": "text/csv"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        lines = response.read().decode("utf-8").splitlines()
    if not lines or not lines[0].startswith("# "):
        raise RuntimeError("HAProxy returned an invalid stats document")

    fieldnames = lines[0][2:].split(",")
    stats: dict[str, FlightProxyBackendStats] = {}
    for row in csv.DictReader(lines[1:], fieldnames=fieldnames):
        backend = row.get("svname")
        if row.get("pxname") != "doris_be_flight" or backend not in FLIGHT_PROXY_BACKENDS:
            continue
        assert backend is not None
        stats[backend] = FlightProxyBackendStats(
            status=row["status"],
            total_sessions=int(row["stot"] or 0),
            bytes_in=int(row["bin"] or 0),
            bytes_out=int(row["bout"] or 0),
        )
    if set(stats) != set(FLIGHT_PROXY_BACKENDS):
        raise RuntimeError("HAProxy stats are missing Doris BE servers")
    return stats


def wait_for_flight_proxy_backend_count(
    config: SlowITConfig,
    expected_up: int,
    *,
    timeout_seconds: int = 300,
) -> dict[str, FlightProxyBackendStats]:
    if not 0 <= expected_up <= len(FLIGHT_PROXY_BACKENDS):
        raise ValueError("expected_up is outside the configured BE count")
    deadline = time.monotonic() + timeout_seconds
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            stats = flight_proxy_backend_stats(config)
            if sum(value.status == "UP" for value in stats.values()) == expected_up:
                return stats
        except (KeyError, OSError, RuntimeError, ValueError) as exc:
            last_error = exc
        time.sleep(2)
    raise RuntimeError(
        f"HAProxy did not report exactly {expected_up} healthy Doris backends"
    ) from last_error


def execute(config: SlowITConfig, sql: str) -> None:
    connection = mysql_connection(config)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()
    finally:
        connection.close()


def alive_backends(config: SlowITConfig) -> list[dict[str, Any]]:
    return [
        row for row in query_rows(config, "SHOW BACKENDS") if str(row["Alive"]).lower() == "true"
    ]


def wait_for_backend_count(
    config: SlowITConfig,
    expected: int,
    *,
    timeout_seconds: int = 300,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_seconds
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            rows = alive_backends(config)
            if len(rows) == expected:
                return rows
        except (OSError, pymysql.MySQLError, KeyError) as exc:
            last_error = exc
        time.sleep(2)
    raise RuntimeError(f"Doris did not report exactly {expected} alive backends") from last_error


def setup_tables(config: SlowITConfig) -> None:
    wait_for_backend_count(config, 3)
    execute(config, f"DROP DATABASE IF EXISTS `{config.database}`")
    execute(config, f"CREATE DATABASE `{config.database}`")
    execute(
        config,
        f"""
        CREATE TABLE `{config.database}`.`{config.distributed_table}` (
            `id` BIGINT NOT NULL,
            `shard` INT NOT NULL,
            `payload` VARCHAR(128) NULL,
            `ray_worker` VARCHAR(64) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS {BUCKET_COUNT}
        PROPERTIES ("replication_num" = "1")
        """,
    )
    execute(
        config,
        f"""
        CREATE TABLE `{config.database}`.`{config.replicated_table}` (
            `id` BIGINT NOT NULL,
            `shard` INT NOT NULL,
            `payload` VARCHAR(128) NULL,
            `ray_worker` VARCHAR(64) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS {BUCKET_COUNT}
        PROPERTIES ("replication_num" = "3")
        """,
    )
    execute(
        config,
        f"""
        INSERT INTO `{config.database}`.`{config.distributed_table}`
        SELECT
            number,
            CAST(number % 1024 AS INT),
            CONCAT('payload-', CAST(number AS STRING)),
            NULL
        FROM numbers("number" = "{config.row_count}")
        """,
    )
    execute(
        config,
        f"""
        INSERT INTO `{config.database}`.`{config.replicated_table}`
        SELECT *
        FROM `{config.database}`.`{config.distributed_table}`
        """,
    )
    rows = query_rows(
        config,
        f"""
        SELECT COUNT(*) AS row_count, SUM(id) AS id_sum
        FROM `{config.database}`.`{config.distributed_table}`
        """,
    )
    if len(rows) != 1:
        raise RuntimeError("Doris fixture validation returned an unexpected row count")
    if int(rows[0]["row_count"]) != config.row_count:
        raise RuntimeError("Doris fixture row count is incomplete")
    if int(rows[0]["id_sum"]) != config.expected_id_sum:
        raise RuntimeError("Doris fixture checksum is incomplete")


def replica_distribution(
    config: SlowITConfig,
    table: str,
) -> list[dict[str, Any]]:
    return query_rows(
        config,
        f"SHOW REPLICA DISTRIBUTION FROM `{config.database}`.`{table}`",
    )
