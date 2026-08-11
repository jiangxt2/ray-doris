from __future__ import annotations

import os
import ssl
import time
import traceback
from collections.abc import Callable, Iterable, Iterator
from functools import partial
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import ray
import ray.data
from _cluster import (
    BUCKET_COUNT,
    SlowITConfig,
    alive_backends,
    flight_proxy_backend_stats,
    query_rows,
    reader_mysql_connection,
    replica_distribution,
    wait_for_backend_count,
)
from ray.data.aggregate import Count, Max, Min, Sum
from ray.data.datasource import ReadTask
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from ray_doris import DorisConfigurationError, DorisDatasource, read_doris
from ray_doris._models import DorisReadConfig
from ray_doris._planner import QueryPlanClient
from ray_doris._sql import build_select_sql, parse_table

pytestmark = [
    pytest.mark.slow_integration,
    pytest.mark.timeout(0),
]

WORKER_COLUMN = "ray_worker"
RAY_FAILURE_MARKER = "/state/ray-read-started"
BE_FAILURE_MARKER = "/state/be-failure-ready"


def _assert_redacted_exception(exception: BaseException, caplog, sentinel: str) -> None:
    rendered_traceback = "".join(
        traceback.format_exception(type(exception), exception, exception.__traceback__)
    )
    assert sentinel not in str(exception)
    assert sentinel not in rendered_traceback
    assert sentinel not in caplog.text
    assert exception.__cause__ is None


def _alive_ray_nodes() -> list[dict[str, Any]]:
    return [node for node in ray.nodes() if node["Alive"]]


def _ray_worker_nodes(config: SlowITConfig) -> list[dict[str, Any]]:
    expected_addresses = set(config.worker_ips)
    return [node for node in _alive_ray_nodes() if node["NodeManagerAddress"] in expected_addresses]


def _claim_marker(path: str, node_id: str) -> bool:
    try:
        descriptor = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o644,
        )
    except FileExistsError:
        return False
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(node_id)
    return True


def _annotate_table(table: pa.Table, node_id: str) -> pa.Table:
    column_index = table.schema.get_field_index(WORKER_COLUMN)
    if column_index < 0:
        raise RuntimeError(f"missing instrumentation column {WORKER_COLUMN!r}")
    field = table.schema.field(column_index)
    values = pa.array([node_id] * table.num_rows, type=field.type)
    return table.set_column(column_index, field, values)


def _instrument_read(
    read_fn: Callable[[], Iterable[pa.Table]],
    *,
    marker_path: str | None,
    pause_seconds: float,
    hold_after_first_batch: float,
) -> Iterator[pa.Table]:
    node_id = ray.get_runtime_context().get_node_id()
    for index, table in enumerate(read_fn()):
        yield _annotate_table(table, node_id)
        if index != 0:
            continue
        if marker_path is not None and _claim_marker(marker_path, node_id):
            time.sleep(pause_seconds)
        elif hold_after_first_batch > 0:
            time.sleep(hold_after_first_batch)


class InstrumentedDorisDatasource(DorisDatasource):
    def __init__(
        self,
        *,
        marker_path: str | None = None,
        pause_seconds: float = 0,
        hold_after_first_batch: float = 0,
        **kwargs: Any,
    ) -> None:
        self._marker_path = marker_path
        self._pause_seconds = pause_seconds
        self._hold_after_first_batch = hold_after_first_batch
        super().__init__(**kwargs)

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None = None,
        data_context: Any | None = None,
    ) -> list[ReadTask]:
        tasks = super().get_read_tasks(
            parallelism,
            per_task_row_limit=per_task_row_limit,
            data_context=data_context,
        )
        return [
            ReadTask(
                read_fn=partial(
                    _instrument_read,
                    task.read_fn,
                    marker_path=self._marker_path,
                    pause_seconds=self._pause_seconds,
                    hold_after_first_batch=self._hold_after_first_batch,
                ),
                metadata=task.metadata,
                schema=task.schema,
                per_task_row_limit=task.per_task_row_limit,
            )
            for task in tasks
        ]


def _assert_summary(
    summary: dict[str, Any],
    *,
    row_count: int,
    id_sum: int,
) -> None:
    assert summary == {
        "row_count": row_count,
        "id_sum": id_sum,
        "minimum_id": 0,
        "maximum_id": row_count - 1,
    }


def _aggregate_ids(dataset: ray.data.Dataset) -> dict[str, Any]:
    summary = dataset.aggregate(
        Count(alias_name="row_count"),
        Sum("id", alias_name="id_sum"),
        Min("id", alias_name="minimum_id"),
        Max("id", alias_name="maximum_id"),
    )
    assert isinstance(summary, dict)
    return summary


def test_cluster_topology_and_replica_distribution(
    slow_config: SlowITConfig,
) -> None:
    ray_nodes = _alive_ray_nodes()
    worker_nodes = _ray_worker_nodes(slow_config)
    assert len(ray_nodes) == 4
    assert len(worker_nodes) == 3
    assert {node["NodeManagerAddress"] for node in worker_nodes} == set(slow_config.worker_ips)
    assert all(node["Resources"].get("CPU") == 1.0 for node in worker_nodes)

    frontends = [
        row
        for row in query_rows(slow_config, "SHOW FRONTENDS")
        if str(row["Alive"]).lower() == "true"
    ]
    backends = alive_backends(slow_config)
    assert len(frontends) == 1
    assert len(backends) == 3
    assert all(value.status == "UP" for value in flight_proxy_backend_stats(slow_config).values())

    backend_ids = {int(row["BackendId"]) for row in backends}
    distributed = replica_distribution(
        slow_config,
        slow_config.distributed_table,
    )
    replicated = replica_distribution(
        slow_config,
        slow_config.replicated_table,
    )
    assert {int(row["BackendId"]) for row in distributed} == backend_ids
    assert sum(int(row["ReplicaNum"]) for row in distributed) == BUCKET_COUNT
    assert {int(row["BackendId"]) for row in replicated} == backend_ids
    assert sum(int(row["ReplicaNum"]) for row in replicated) == BUCKET_COUNT * 3
    assert all(int(row["ReplicaNum"]) == BUCKET_COUNT for row in replicated)


def test_https_gateway_mysql_tls_and_explicit_flight(
    slow_config: SlowITConfig,
) -> None:
    connection = reader_mysql_connection(slow_config)
    try:
        assert isinstance(connection._sock, ssl.SSLSocket)
        assert connection._sock.cipher() is not None
    finally:
        connection.close()

    expected_rows = min(slow_config.row_count, 1_000)
    expected_sum = expected_rows * (expected_rows - 1) // 2
    for transport in ("mysql", "flight"):
        dataset = read_doris(
            **slow_config.reader_kwargs(
                table=slow_config.distributed_table,
                transport=transport,
                columns=["id"],
                filter=f"id < {expected_rows}",
                tablet_size=1,
            ),
            concurrency=3,
            override_num_blocks=BUCKET_COUNT,
            ray_remote_args={
                "num_cpus": 1,
                "scheduling_strategy": "SPREAD",
                "max_retries": 3,
            },
        )
        _assert_summary(
            _aggregate_ids(dataset),
            row_count=expected_rows,
            id_sum=expected_sum,
        )


def test_tls_rejects_wrong_ca_hostname_and_plaintext_endpoint(
    slow_config: SlowITConfig,
    caplog,
) -> None:
    sentinel = os.environ["RAY_DORIS_READER_PASSWORD"]
    wrong_ca_kwargs = slow_config.reader_kwargs(
        table=slow_config.distributed_table,
        http_ca_file="/tls/wrong-ca.pem",
        client_kwargs={
            "ssl": {"ca": "/tls/wrong-ca.pem", "check_hostname": True},
            "read_timeout": 180,
            "write_timeout": 180,
        },
    )
    with pytest.raises(DorisConfigurationError, match="MySQL TLS validation failed") as captured:
        DorisDatasource(**wrong_ca_kwargs).get_read_tasks(parallelism=1)
    _assert_redacted_exception(captured.value, caplog, sentinel)

    wrong_hostname_kwargs = slow_config.reader_kwargs(
        table=slow_config.distributed_table,
        host="doris-ingress",
    )
    with pytest.raises(DorisConfigurationError, match="MySQL TLS validation failed") as captured:
        DorisDatasource(**wrong_hostname_kwargs).get_read_tasks(parallelism=1)
    _assert_redacted_exception(captured.value, caplog, sentinel)

    query_plan_config = DorisReadConfig.from_options(
        table=parse_table(f"{slow_config.database}.{slow_config.distributed_table}"),
        host="doris-ingress",
        mysql_port=slow_config.mysql_port,
        http_port=slow_config.https_port,
        http_scheme="https",
        http_ca_file=slow_config.tls_ca,
        user="ray_doris_reader",
        password_env="RAY_DORIS_READER_PASSWORD",
        on_query_plan_error="error",
    )
    query_plan_sql = build_select_sql(query_plan_config.table, None, None, None)
    with pytest.raises(
        DorisConfigurationError, match="query-plan TLS validation failed"
    ) as captured:
        QueryPlanClient(query_plan_config).fetch_tablet_ids(query_plan_sql)
    _assert_redacted_exception(captured.value, caplog, sentinel)

    plaintext_config = DorisReadConfig.from_options(
        table=query_plan_config.table,
        host="fe",
        http_port=8030,
        http_scheme="https",
        http_ca_file=slow_config.tls_ca,
        user=query_plan_config.user,
        password_env="RAY_DORIS_READER_PASSWORD",
        on_query_plan_error="error",
    )
    with pytest.raises(
        DorisConfigurationError, match="query-plan TLS validation failed"
    ) as captured:
        QueryPlanClient(plaintext_config).fetch_tablet_ids(query_plan_sql)
    _assert_redacted_exception(captured.value, caplog, sentinel)


def _assert_read_executes_on_all_ray_workers(
    slow_config: SlowITConfig,
    *,
    transport: str,
) -> None:
    proxy_before = flight_proxy_backend_stats(slow_config) if transport == "flight" else None
    datasource = InstrumentedDorisDatasource(
        **slow_config.reader_kwargs(
            table=slow_config.distributed_table,
            transport=transport,
            columns=["id", WORKER_COLUMN],
            tablet_size=1,
            batch_size=5_000,
        ),
        hold_after_first_batch=0.5,
    )
    assert len(datasource.get_read_tasks(parallelism=BUCKET_COUNT)) == BUCKET_COUNT

    dataset = ray.data.read_datasource(
        datasource,
        concurrency=3,
        override_num_blocks=BUCKET_COUNT,
        ray_remote_args={
            "num_cpus": 1,
            "scheduling_strategy": "SPREAD",
            "max_retries": 3,
        },
    )
    observed_node_ids: set[str] = set()
    row_count = 0
    id_sum = 0
    for batch in dataset.iter_batches(batch_size=10_000, batch_format="pyarrow"):
        assert isinstance(batch, pa.Table)
        row_count += batch.num_rows
        batch_sum = pc.sum(batch["id"]).as_py()
        id_sum += int(batch_sum or 0)
        observed_node_ids.update(pc.unique(batch[WORKER_COLUMN]).to_pylist())

    expected_node_ids = {node["NodeID"] for node in _ray_worker_nodes(slow_config)}
    assert observed_node_ids == expected_node_ids
    assert row_count == slow_config.row_count
    assert id_sum == slow_config.expected_id_sum

    if proxy_before is not None:
        proxy_after = flight_proxy_backend_stats(slow_config)
        for backend in proxy_before:
            previous = proxy_before[backend]
            current = proxy_after[backend]
            assert current.status == "UP"
            assert current.total_sessions > previous.total_sessions
            assert current.bytes_in + current.bytes_out > previous.bytes_in + previous.bytes_out


def test_mysql_read_executes_on_all_ray_workers(
    slow_config: SlowITConfig,
) -> None:
    _assert_read_executes_on_all_ray_workers(slow_config, transport="mysql")


def test_flight_read_executes_on_all_ray_workers(
    slow_config: SlowITConfig,
) -> None:
    _assert_read_executes_on_all_ray_workers(slow_config, transport="flight")


def test_repeated_flight_reads(
    slow_config: SlowITConfig,
) -> None:
    started = time.monotonic()
    deadline = started + slow_config.stress_seconds
    iterations = 0
    while time.monotonic() < deadline or iterations == 0:
        dataset = read_doris(
            **slow_config.reader_kwargs(
                table=slow_config.distributed_table,
                transport="flight",
                columns=["id"],
                tablet_size=1,
                batch_size=10_000,
            ),
            concurrency=3,
            override_num_blocks=BUCKET_COUNT,
            ray_remote_args={
                "num_cpus": 1,
                "scheduling_strategy": "SPREAD",
                "max_retries": 3,
            },
        )
        _assert_summary(
            _aggregate_ids(dataset),
            row_count=slow_config.row_count,
            id_sum=slow_config.expected_id_sum,
        )
        iterations += 1
        print(
            "validated Flight pressure iteration "
            f"{iterations} after {time.monotonic() - started:.1f}s",
            flush=True,
        )
    assert iterations >= 1
    assert time.monotonic() - started >= slow_config.stress_seconds


def test_worker_retry_and_backend_failover(
    slow_config: SlowITConfig,
) -> None:
    worker_one = next(
        node
        for node in _ray_worker_nodes(slow_config)
        if node["NodeManagerAddress"] == slow_config.worker_ips[0]
    )
    assert not Path(RAY_FAILURE_MARKER).exists()
    retry_rows = min(slow_config.row_count, 200_000)
    retry_sum = retry_rows * (retry_rows - 1) // 2
    datasource = InstrumentedDorisDatasource(
        **slow_config.reader_kwargs(
            table=slow_config.replicated_table,
            transport="mysql",
            columns=["id", WORKER_COLUMN],
            filter=f"id < {retry_rows}",
            tablet_size=BUCKET_COUNT,
            batch_size=2_000,
        ),
        marker_path=RAY_FAILURE_MARKER,
        pause_seconds=600,
    )
    dataset = ray.data.read_datasource(
        datasource,
        concurrency=1,
        override_num_blocks=1,
        ray_remote_args={
            "num_cpus": 1,
            "scheduling_strategy": NodeAffinitySchedulingStrategy(
                node_id=worker_one["NodeID"],
                soft=True,
            ),
            "max_retries": 3,
        },
    )
    observed_node_ids: set[str] = set()
    row_count = 0
    id_sum = 0
    for batch in dataset.iter_batches(batch_size=10_000, batch_format="pyarrow"):
        assert isinstance(batch, pa.Table)
        row_count += batch.num_rows
        batch_sum = pc.sum(batch["id"]).as_py()
        id_sum += int(batch_sum or 0)
        observed_node_ids.update(pc.unique(batch[WORKER_COLUMN]).to_pylist())

    initial_node_id = Path(RAY_FAILURE_MARKER).read_text(encoding="utf-8")
    assert initial_node_id == worker_one["NodeID"]
    assert row_count == retry_rows
    assert id_sum == retry_sum
    assert observed_node_ids - {initial_node_id}

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline and len(_ray_worker_nodes(slow_config)) != 2:
        time.sleep(2)
    assert len(_ray_worker_nodes(slow_config)) == 2

    Path(BE_FAILURE_MARKER).write_text("ready\n", encoding="utf-8")
    surviving_backends = wait_for_backend_count(
        slow_config,
        2,
        timeout_seconds=180,
    )
    assert len(surviving_backends) == 2
    dataset = read_doris(
        **slow_config.reader_kwargs(
            table=slow_config.replicated_table,
            transport="mysql",
            columns=["id"],
            tablet_size=1,
            batch_size=10_000,
        ),
        concurrency=2,
        override_num_blocks=BUCKET_COUNT,
        ray_remote_args={
            "num_cpus": 1,
            "scheduling_strategy": "SPREAD",
            "max_retries": 3,
        },
    )
    _assert_summary(
        _aggregate_ids(dataset),
        row_count=slow_config.row_count,
        id_sum=slow_config.expected_id_sum,
    )
