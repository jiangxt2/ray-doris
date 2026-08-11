"""Validate slow-integration release evidence."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

_COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_PYTHON_RELEASE = re.compile(r"^3\.12\.\d+$")


def _required_string(data: dict[str, Any], name: str) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"slow result field {name!r} must be a non-empty string")
    return value


def _required_positive_integer(data: dict[str, Any], name: str) -> int:
    value = data.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimeError(f"slow result field {name!r} must be a positive integer")
    return value


def verify_slow_result(path: Path, *, expected_commit: str, expected_run_id: int) -> None:
    """Require successful full-profile evidence for the exact release commit."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise RuntimeError("slow result manifest is unreadable") from None
    if not isinstance(data, dict):
        raise RuntimeError("slow result manifest must contain a JSON object")
    if type(data.get("schema_version")) is not int or data["schema_version"] != 1:
        raise RuntimeError("slow result manifest has an unsupported schema version")

    commit = _required_string(data, "commit_sha")
    if _COMMIT_SHA.fullmatch(commit) is None or commit != expected_commit:
        raise RuntimeError("slow result commit does not match the release commit")
    expected_values = {
        "profile": "full",
        "status": "passed",
        "transport": "mysql",
        "endpoint_mode": "logical",
        "doris_version": "4.0.6",
    }
    for name, expected in expected_values.items():
        if _required_string(data, name) != expected:
            raise RuntimeError(f"slow result field {name!r} must equal {expected!r}")

    if _required_string(data, "ray_version") != "2.55.1":
        raise RuntimeError("slow result field 'ray_version' must equal '2.55.1'")
    python_version = _required_string(data, "python_version")
    if _PYTHON_RELEASE.fullmatch(python_version) is None:
        raise RuntimeError("slow result field 'python_version' must be a Python 3.12 release")
    if _required_positive_integer(data, "workflow_run_id") != expected_run_id:
        raise RuntimeError("slow result workflow run does not match its artifact source")
    _required_positive_integer(data, "row_count")
    expected_counts = {
        "initial_frontend_count": 1,
        "initial_backend_count": 3,
        "initial_ray_worker_count": 3,
    }
    for name, expected in expected_counts.items():
        if _required_positive_integer(data, name) != expected:
            raise RuntimeError(f"slow result field {name!r} must equal {expected}")
    expected_scenarios = [
        "be_failure",
        "logical_endpoint_tls",
        "mysql_all_workers",
        "ray_worker_retry",
    ]
    scenarios = data.get("scenarios")
    if scenarios != expected_scenarios:
        raise RuntimeError(
            "slow result scenarios do not cover the enterprise-candidate MySQL profile"
        )


def main() -> int:
    """Validate a manifest supplied by the release workflow."""
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-run-id", required=True, type=int)
    arguments = parser.parse_args()
    verify_slow_result(
        arguments.manifest,
        expected_commit=arguments.expected_commit,
        expected_run_id=arguments.expected_run_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
