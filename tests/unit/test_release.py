from pathlib import Path
from unittest.mock import Mock

import pytest
from tools import check_release


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
