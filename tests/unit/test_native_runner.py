"""Lifecycle checks for the native IT runner without starting Docker."""

import io
import json
import runpy
import signal
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

RUNNER = Path(__file__).resolve().parents[1] / "native_integration/run.py"


@pytest.mark.parametrize("outcome", ["success", "exception", "sigterm"])
def test_cleanup_runs_once_for_exit_paths(outcome):
    source = f"""
import os, runpy, signal
guard = runpy.run_path({str(RUNNER)!r})['cleanup_on_termination']
with guard(lambda: print('CLEANUP_DONE', flush=True)):
    if {outcome!r} == 'exception':
        raise RuntimeError('expected fixture failure')
    if {outcome!r} == 'sigterm':
        os.kill(os.getpid(), signal.SIGTERM)
"""
    result = subprocess.run(
        [sys.executable, "-c", source], capture_output=True, text=True, timeout=10
    )
    assert result.stdout.count("CLEANUP_DONE") == 1
    assert (
        result.returncode
        == {"success": 0, "exception": 1, "sigterm": 128 + signal.SIGTERM}[outcome]
    )


@pytest.mark.parametrize("resource", ["containers", "networks", "volumes", "images"])
def test_cleanup_verification_detects_each_owned_resource(resource):
    verify = runpy.run_path(str(RUNNER))["verify_cleanup"]
    responses = {
        "containers": "",
        "networks": "",
        "volumes": "unrelated-volume",
        "labeled_volumes": "",
        "images": "shared:base",
    }
    responses[resource] += (
        "\n"
        + {
            "containers": "container",
            "networks": "network",
            "volumes": "owned-volume",
            "images": "owned:runtime",
        }[resource]
    )
    run = Mock(side_effect=list(responses.values()))
    with pytest.raises(RuntimeError, match=resource):
        verify(run, {"owned-volume"}, ["owned:runtime"])


def test_cleanup_accepts_already_absent_resources_and_preserves_shared_resources():
    verify = runpy.run_path(str(RUNNER))["verify_cleanup"]
    run = Mock(side_effect=["", "", "shared-volume", "", "shared:base"])
    result = verify(run, {"owned-volume"}, ["owned:runtime"])
    assert json.loads(json.dumps(result)) == {
        "containers": [],
        "networks": [],
        "volumes": [],
        "images": [],
    }


def test_cleanup_detects_labeled_volumes_after_partial_start():
    verify = runpy.run_path(str(RUNNER))["verify_cleanup"]
    run = Mock(side_effect=["", "", "anonymous", "anonymous", ""])
    with pytest.raises(RuntimeError, match="volumes"):
        verify(run, set(), [])


def test_cleanup_verification_does_not_hide_docker_failure():
    verify = runpy.run_path(str(RUNNER))["verify_cleanup"]
    run = Mock(side_effect=RuntimeError("Docker is unavailable"))
    with pytest.raises(RuntimeError, match="Docker is unavailable"):
        verify(run, set(), [])


@pytest.mark.parametrize(
    "arguments,message",
    [
        (
            ["--iceberg", "--iceberg-compat-only", "--versions", "4.0.6"],
            "requires non-4.0.6 versions",
        ),
        (["--doris-regression-only", "--versions", "4.1.5"], "requires only --versions 4.0.6"),
        (
            ["--doris-regression-only", "--skip-query-regression", "--versions", "4.0.6"],
            "requires only --versions 4.0.6",
        ),
        (
            ["--iceberg", "--iceberg-cases", "../other", "--versions", "4.0.6"],
            "valid test function names",
        ),
    ],
)
def test_runner_rejects_invalid_modes(tmp_path, arguments, message):
    result = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            *arguments,
            "--logs",
            str(tmp_path / "logs"),
            "--hadoop-source",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode != 0
    assert message in result.stderr


def test_iceberg_retry_hook_uses_a_serializable_runtime_environment():
    from ray.runtime_env import RuntimeEnv

    runtime = RuntimeEnv(
        worker_process_setup_hook="tests.iceberg_integration.retry_hook.setup",
        env_vars={"ICEBERG_RETRY_MARKER": "/evidence/marker"},
    )
    serialized = json.loads(runtime.serialize())
    assert serialized["worker_process_setup_hook"].endswith("retry_hook.setup")
    assert serialized["env_vars"]["ICEBERG_RETRY_MARKER"] == "/evidence/marker"


def test_iceberg_retry_hook_fails_once_and_then_uses_real_reader(monkeypatch, tmp_path):
    from types import ModuleType

    from tests.iceberg_integration import retry_hook

    class FakeScan:
        def to_record_batches(self, tasks):
            return tasks

    fake = ModuleType("pyiceberg.io.pyarrow")
    fake.ArrowScan = FakeScan
    monkeypatch.setitem(sys.modules, "pyiceberg", ModuleType("pyiceberg"))
    monkeypatch.setitem(sys.modules, "pyiceberg.io", ModuleType("pyiceberg.io"))
    monkeypatch.setitem(sys.modules, "pyiceberg.io.pyarrow", fake)
    monkeypatch.setenv("ICEBERG_RETRY_MARKER", str(tmp_path / "marker"))
    retry_hook.setup()
    with pytest.raises(RuntimeError, match="injected Iceberg read retry"):
        FakeScan().to_record_batches([1])
    assert FakeScan().to_record_batches([2]) == [2]


def test_streamed_command_redacts_and_closes_output():
    stream = runpy.run_path(str(RUNNER))["stream_command"]
    process = Mock(stdout=io.StringIO("fixture-secret\n"))
    process.wait.return_value = 0
    out = io.StringIO()
    assert stream(process, out, ["fixture-secret"]) == 0
    assert out.getvalue() == "<redacted>\n"
    assert process.stdout.closed


@pytest.mark.parametrize("timeout", [False, True])
def test_interrupted_child_is_reaped_before_cleanup(timeout):
    stream = runpy.run_path(str(RUNNER))["stream_command"]
    process = Mock(stdout=MagicMock())
    process.stdout.__iter__.side_effect = KeyboardInterrupt
    if timeout:
        process.wait.side_effect = [subprocess.TimeoutExpired("fixture", 10), 0]
    with pytest.raises(KeyboardInterrupt):
        stream(process, io.StringIO(), [])
    process.terminate.assert_called_once()
    assert process.kill.call_count == int(timeout)
    process.stdout.close.assert_called_once()
