from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import ray
import ray.data
from _cluster import (
    SlowITConfig,
    setup_tables,
    wait_for_flight_proxy_backend_count,
)
from tests.logging_capture import install, records


def _wait_for_ray_workers(
    config: SlowITConfig,
    *,
    timeout_seconds: int = 180,
) -> None:
    expected_addresses = set(config.worker_ips)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        alive_addresses = {node["NodeManagerAddress"] for node in ray.nodes() if node["Alive"]}
        if expected_addresses <= alive_addresses:
            return
        time.sleep(2)
    raise RuntimeError("Ray did not register all three worker nodes")


@pytest.fixture(scope="session")
def slow_config() -> SlowITConfig:
    return SlowITConfig.from_environment()


@pytest.fixture(scope="session", autouse=True)
def distributed_cluster(slow_config: SlowITConfig) -> Iterator[None]:
    setup_tables(slow_config)
    wait_for_flight_proxy_backend_count(slow_config, 3)
    install("/state")
    ray.init(
        address="auto",
        runtime_env={
            "env_vars": {
                "RAY_DORIS_TEST_LOG_DIR": "/state",
                "PYTHONPATH": "/workspace:/workspace/tests/slow_integration",
            },
            "worker_process_setup_hook": "tests.logging_capture.setup",
        },
    )
    try:
        ray.data.DataContext.get_current()
        _wait_for_ray_workers(slow_config)
        yield
    finally:
        ray.shutdown()
        Path("/state/diagnostics.json").write_text(json.dumps(records("/state")), encoding="utf-8")
