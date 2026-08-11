import json
import traceback
from pathlib import Path
from unittest.mock import Mock

import pytest
from tools import check_release
from tools.check_slow_result import verify_slow_result


def _assert_redacted_exception(exception: BaseException, caplog, sentinel: str) -> None:
    rendered_traceback = "".join(
        traceback.format_exception(type(exception), exception, exception.__traceback__)
    )
    assert sentinel not in str(exception)
    assert sentinel not in rendered_traceback
    assert sentinel not in caplog.text
    assert exception.__cause__ is None


def write_pyproject(path: Path, version: str = "0.1.0a1") -> Path:
    pyproject = path / "pyproject.toml"
    pyproject.write_text(
        f'[project]\nname = "ray-doris"\nversion = "{version}"\n',
        encoding="utf-8",
    )
    return pyproject


def test_verify_release_accepts_matching_tag_on_master(monkeypatch, tmp_path) -> None:
    run = Mock(return_value=Mock(returncode=0))
    monkeypatch.setattr(check_release.subprocess, "run", run)

    check_release.verify_release(
        tag="v0.1.0a1",
        commit="candidate-sha",
        pyproject=write_pyproject(tmp_path),
    )

    run.assert_called_once_with(
        ["git", "merge-base", "--is-ancestor", "candidate-sha", "origin/master"],
        check=False,
    )


def test_verify_release_rejects_version_mismatch_before_git(monkeypatch, tmp_path) -> None:
    run = Mock()
    monkeypatch.setattr(check_release.subprocess, "run", run)

    with pytest.raises(RuntimeError, match="package version tag"):
        check_release.verify_release(
            tag="v0.1.0",
            commit="candidate-sha",
            pyproject=write_pyproject(tmp_path),
        )

    run.assert_not_called()


def test_verify_release_rejects_commit_outside_master(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        check_release.subprocess,
        "run",
        Mock(return_value=Mock(returncode=1)),
    )

    with pytest.raises(RuntimeError, match="origin/master"):
        check_release.verify_release(
            tag="v0.1.0a1",
            commit="other-sha",
            pyproject=write_pyproject(tmp_path),
        )


def test_release_main_requires_github_tag_environment(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_REF_NAME", raising=False)
    monkeypatch.delenv("GITHUB_SHA", raising=False)
    with pytest.raises(RuntimeError, match="GITHUB_REF_NAME"):
        check_release.main()


def write_slow_result(path: Path, **overrides: object) -> Path:
    data = {
        "schema_version": 1,
        "commit_sha": "a" * 40,
        "workflow_run_id": 42,
        "profile": "full",
        "status": "passed",
        "transport": "mysql",
        "endpoint_mode": "logical",
        "doris_version": "4.0.6",
        "ray_version": "2.55.1",
        "python_version": "3.12.12",
        "initial_frontend_count": 1,
        "initial_backend_count": 3,
        "initial_ray_worker_count": 3,
        "row_count": 10_000,
        "scenarios": [
            "be_failure",
            "logical_endpoint_tls",
            "mysql_all_workers",
            "ray_worker_retry",
        ],
    }
    data.update(overrides)
    manifest = path / "slow-result.json"
    manifest.write_text(json.dumps(data), encoding="utf-8")
    return manifest


def test_slow_result_accepts_exact_full_profile_evidence(tmp_path) -> None:
    verify_slow_result(
        write_slow_result(tmp_path),
        expected_commit="a" * 40,
        expected_run_id=42,
    )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"commit_sha": "b" * 40}, "release commit"),
        ({"profile": "core"}, "profile"),
        ({"status": "failed"}, "status"),
        ({"transport": "flight"}, "transport"),
        ({"endpoint_mode": "multi-fe"}, "endpoint_mode"),
        ({"schema_version": True}, "schema version"),
        ({"ray_version": "2.56.1"}, "ray_version"),
        ({"python_version": "3.11.9"}, "python_version"),
        ({"initial_frontend_count": 2}, "initial_frontend_count"),
        ({"initial_frontend_count": True}, "initial_frontend_count"),
        ({"initial_backend_count": 2}, "initial_backend_count"),
        ({"initial_ray_worker_count": 2}, "initial_ray_worker_count"),
        ({"row_count": 0}, "row_count"),
        ({"workflow_run_id": True}, "workflow_run_id"),
        ({"scenarios": ["mysql_all_workers"]}, "scenarios"),
        (
            {
                "scenarios": [
                    "be_failure",
                    "logical_endpoint_tls",
                    "mysql_all_workers",
                    "ray_worker_retry",
                    "ray_worker_retry",
                ]
            },
            "scenarios",
        ),
    ],
)
def test_slow_result_rejects_invalid_release_evidence(
    tmp_path,
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        verify_slow_result(
            write_slow_result(tmp_path, **overrides),
            expected_commit="a" * 40,
            expected_run_id=42,
        )


def test_slow_result_rejects_unreadable_manifest_without_parser_details(tmp_path, caplog) -> None:
    sentinel = "secret-json-sentinel"
    manifest = tmp_path / "slow-result.json"
    manifest.write_text("{" + sentinel, encoding="utf-8")
    with pytest.raises(RuntimeError, match="manifest is unreadable") as captured:
        verify_slow_result(manifest, expected_commit="a" * 40, expected_run_id=42)
    _assert_redacted_exception(captured.value, caplog, sentinel)
