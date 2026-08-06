from __future__ import annotations

import time
from collections.abc import Iterator

import pytest
import ray
from _cluster import (
    SlowITConfig,
    setup_tables,
    wait_for_flight_proxy_backend_count,
)


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
    ray.init(address="auto")
    try:
        _wait_for_ray_workers(slow_config)
        yield
    finally:
        ray.shutdown()
