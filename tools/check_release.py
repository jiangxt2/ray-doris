"""Validate release source and version without publishing artifacts."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.9 and 3.10
    import tomli as tomllib

_PROJECT_VERSION = re.compile(r'^version\s*=\s*"([^"]+)"\s*$', re.MULTILINE)
_PACKAGE_VERSION = re.compile(r'^__version__\s*=\s*"([^"]+)"\s*$', re.MULTILINE)
_TWO_COMPONENT_VERSION = re.compile(r"^[0-9]+\.[0-9]+$")


def _validate_version(value: str, source: str) -> str:
    if _TWO_COMPONENT_VERSION.fullmatch(value) is None:
        raise RuntimeError(f"{source} must use a two-component major.minor version")
    return value


def project_version(pyproject: Path) -> str:
    """Read the static project version from pyproject.toml."""
    match = _PROJECT_VERSION.search(pyproject.read_text(encoding="utf-8"))
    if match is None:
        raise RuntimeError("pyproject.toml has no static project version")
    return _validate_version(match.group(1), "project version")


def package_version(package_init: Path) -> str:
    """Read the package's exported version from its initializer."""
    match = _PACKAGE_VERSION.search(package_init.read_text(encoding="utf-8"))
    if match is None:
        raise RuntimeError("package initializer has no static __version__")
    return _validate_version(match.group(1), "package version")


def _git(*arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError("release candidate Git validation failed")
    return result.stdout.strip()


def _git_file(candidate_sha: str, path: str) -> str:
    result = subprocess.run(
        ["git", "show", f"{candidate_sha}:{path}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"release candidate is missing {path}")
    return result.stdout


def _candidate_version(candidate_sha: str) -> tuple[str, str]:
    project = tomllib.loads(_git_file(candidate_sha, "pyproject.toml"))["project"]
    project_value = _validate_version(str(project["version"]), "candidate project version")
    package_text = _git_file(candidate_sha, "src/ray_doris/__init__.py")
    package_match = _PACKAGE_VERSION.search(package_text)
    if package_match is None:
        raise RuntimeError("release candidate package initializer has no static __version__")
    package_value = _validate_version(package_match.group(1), "candidate package version")
    if project_value != package_value:
        raise RuntimeError("release candidate versions do not match")
    return project_value, package_value


def _require_release_notes(candidate_sha: str, version: str) -> str:
    path = f"release-notes/v{version}.md"
    content = _git_file(candidate_sha, path)
    if not content.strip():
        raise RuntimeError(f"release candidate release notes are empty: {path}")
    return path


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


def verify_candidate(
    *,
    candidate_sha: str,
    mode: str,
    expected_version: str = "",
    tag: str = "",
    master_ref: str = "origin/master",
    event_sha: str = "",
    event_created: bool = False,
    event_deleted: bool = False,
    event_forced: bool = False,
) -> tuple[str, str, str]:
    """Validate a dry-run or tag candidate and return its identity tuple."""
    resolved_sha = _git("rev-parse", f"{candidate_sha}^{{commit}}")
    master_sha = _git("rev-parse", f"{master_ref}^{{commit}}")
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", resolved_sha, master_sha],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"release commit must belong to {master_ref}")
    version, _ = _candidate_version(resolved_sha)
    if expected_version and expected_version != version:
        raise RuntimeError("release candidate version does not match package metadata")
    expected_tag = f"v{version}"
    _require_release_notes(resolved_sha, version)
    if mode == "tag":
        event_commit_sha = _git("rev-parse", f"{event_sha}^{{commit}}")
        if tag != expected_tag or event_commit_sha != resolved_sha:
            raise RuntimeError(
                "release tag identity does not match the candidate commit and version"
            )
        if not event_created or event_deleted or event_forced:
            raise RuntimeError("release tag must be a newly created, non-forced tag")
    elif mode == "dry-run":
        if tag or event_sha or event_created or event_deleted or event_forced:
            raise RuntimeError("dry-run candidates must not include tag event identity")
    else:
        raise RuntimeError(f"unsupported release mode: {mode}")
    return resolved_sha, version, master_sha


def main(arguments: Sequence[str] | None = None) -> int:
    """Validate a workflow-dispatch candidate or a newly created release tag."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("tag", "dry-run"), required=True)
    parser.add_argument("--candidate-ref", required=True)
    parser.add_argument("--expected-version", default="")
    parser.add_argument("--tag", default="")
    parser.add_argument("--master-ref", default="origin/master")
    parser.add_argument("--event-sha", default="")
    parser.add_argument("--event-created", default="false")
    parser.add_argument("--event-deleted", default="false")
    parser.add_argument("--event-forced", default="false")
    parser.add_argument("--github-output", default="")
    options = parser.parse_args([] if arguments is None else arguments)
    candidate_sha, version, master_sha = verify_candidate(
        candidate_sha=options.candidate_ref,
        mode=options.mode,
        expected_version=options.expected_version,
        tag=options.tag,
        master_ref=options.master_ref,
        event_sha=options.event_sha,
        event_created=options.event_created.lower() == "true",
        event_deleted=options.event_deleted.lower() == "true",
        event_forced=options.event_forced.lower() == "true",
    )
    if options.github_output:
        output = Path(options.github_output)
        with output.open("a", encoding="utf-8") as stream:
            stream.write(f"mode={options.mode}\nsha={candidate_sha}\nversion={version}\n")
            stream.write(
                f"tag={options.tag if options.mode == 'tag' else ''}\nmaster_sha={master_sha}\n"
            )
    print(f"release candidate verified: {candidate_sha} ({version})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
