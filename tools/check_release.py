"""Validate release source and version without publishing artifacts."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

_PROJECT_VERSION = re.compile(r'^version\s*=\s*"([^"]+)"\s*$', re.MULTILINE)


def project_version(pyproject: Path) -> str:
    """Read the static project version from pyproject.toml."""
    match = _PROJECT_VERSION.search(pyproject.read_text(encoding="utf-8"))
    if match is None:
        raise RuntimeError("pyproject.toml has no static project version")
    return match.group(1)


def verify_release(
    *,
    tag: str,
    commit: str,
    default_branch: str = "origin/master",
    pyproject: Path = Path("pyproject.toml"),
) -> None:
    """Require a matching version tag on the configured default branch history."""
    expected_tag = f"v{project_version(pyproject)}"
    if tag != expected_tag:
        raise RuntimeError(f"release tag must equal package version tag {expected_tag!r}")
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, default_branch],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"release commit must belong to {default_branch}")


def main() -> int:
    """Validate the GitHub tag environment used by the release workflow."""
    tag = os.environ.get("GITHUB_REF_NAME")
    commit = os.environ.get("GITHUB_SHA")
    if not tag or not commit:
        raise RuntimeError("GITHUB_REF_NAME and GITHUB_SHA are required")
    verify_release(tag=tag, commit=commit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
