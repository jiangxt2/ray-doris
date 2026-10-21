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
    run = Mock(side_effect=["", "", "shared-volume", "shared:base"])
    result = verify(run, {"owned-volume"}, ["owned:runtime"])
    assert json.loads(json.dumps(result)) == {
        "containers": [],
        "networks": [],
        "volumes": [],
        "images": [],
    }


def test_cleanup_verification_does_not_hide_docker_failure():
    verify = runpy.run_path(str(RUNNER))["verify_cleanup"]
    run = Mock(side_effect=RuntimeError("Docker is unavailable"))
    with pytest.raises(RuntimeError, match="Docker is unavailable"):
        verify(run, set(), [])


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
