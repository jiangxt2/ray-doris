from __future__ import annotations

import json
import os
import platform
from pathlib import Path
from typing import Any

import ray
from _cluster import BUCKET_COUNT, SlowITConfig


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required to write slow-suite evidence")
    return value


def write_result(path: Path) -> None:
    config = SlowITConfig.from_environment()
    result: dict[str, Any] = {
        "schema_version": 2,
        "commit_sha": _required_environment("RAY_DORIS_SLOW_COMMIT_SHA"),
        "workflow_run_id": int(_required_environment("RAY_DORIS_SLOW_RUN_ID")),
        "profile": _required_environment("RAY_DORIS_SLOW_PROFILE"),
        "status": "passed",
        "transport": "mysql",
        "endpoint_mode": "logical",
        "doris_version": "4.0.6",
        "ray_version": ray.__version__,
        "python_version": platform.python_version(),
        "initial_frontend_count": 1,
        "initial_backend_count": 3,
        "initial_ray_worker_count": len(config.worker_ips),
        "row_count": config.row_count,
        "parameters": {
            "be_memory_limit": _required_environment("RAY_DORIS_BE_MEMORY_LIMIT"),
            "row_count": config.row_count,
            "stress_seconds": config.stress_seconds,
        },
        "image_ids": {
            "doris_be": _required_environment("RAY_DORIS_SLOW_DORIS_BE_IMAGE_ID"),
            "doris_fe": _required_environment("RAY_DORIS_SLOW_DORIS_FE_IMAGE_ID"),
            "flight_proxy": _required_environment("RAY_DORIS_SLOW_FLIGHT_PROXY_IMAGE_ID"),
            "ray": _required_environment("RAY_DORIS_SLOW_RAY_IMAGE_ID"),
            "ray_base": _required_environment("RAY_DORIS_SLOW_RAY_BASE_IMAGE_ID"),
        },
        "topology": {
            "doris_backend_count": 3,
            "doris_frontend_count": 1,
            "distributed_table_buckets": BUCKET_COUNT,
            "ray_head_count": 1,
            "ray_worker_count": len(config.worker_ips),
            "replicated_table_replication_num": 3,
        },
        "scenarios": [
            "be_failure",
            "logical_endpoint_tls",
            "mysql_all_workers",
            "ray_worker_retry",
            "stream_load_transport_fault",
            "stream_load_write_all_workers",
        ],
    }
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    write_result(Path("/state/slow-result.json"))
