"""Enforce the release workflow's fail-closed orchestration invariants."""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from pathlib import Path


def _job(workflow: str, job_id: str) -> str:
    jobs_start = workflow.find("\njobs:\n")
    if jobs_start < 0:
        return ""
    jobs = workflow[jobs_start:]
    match = re.search(
        rf"(?ms)^  {re.escape(job_id)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        jobs,
    )
    return "" if match is None else match.group(1)


def release_policy_failures(ci_workflow: str, release_workflow: str) -> tuple[str, ...]:
    """Return release policy violations for the two workflows."""
    failures: list[str] = []

    def require(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    require("workflow_call:" in ci_workflow, "CI must expose workflow_call for release gates")
    require(
        "ref: ${{ inputs.candidate_sha || github.sha }}" in ci_workflow,
        "CI must accept an immutable candidate SHA",
    )
    for job_id in ("unit", "quality", "docs", "integration", "package"):
        require(bool(_job(ci_workflow, job_id)), f"CI is missing the {job_id} job")
    integration_job = _job(ci_workflow, "integration")
    compose = "docker compose -f tests/integration/docker-compose.yml"
    require(
        f"{compose} build fe" in integration_job,
        "CI integration must build the Doris FE image exactly once before startup",
    )
    require(
        f"{compose} build be" in integration_job,
        "CI integration must build the Doris BE image exactly once before startup",
    )
    require(
        f"{compose} up -d --no-build" in integration_job,
        "CI integration must start Doris with --no-build",
    )
    require(
        f"{compose} up -d --build" not in integration_job,
        "CI integration must not rebuild Doris images during startup",
    )
    require('tags: ["v*"]' in release_workflow, "release must trigger on version tags")
    require("workflow_dispatch:" in release_workflow, "release must support a candidate dry run")
    require("formal_publish:" in release_workflow, "release must expose explicit formal recovery")
    require(
        "release_tag:" in release_workflow, "formal recovery must name the existing release tag"
    )
    for tool in (
        "tools/check_release.py",
        "tools/verify_release_artifacts.py",
        "tools/verify_release_tag.py",
    ):
        require(tool in release_workflow, f"release must use {tool}")
    for job_id in (
        "candidate",
        "verify",
        "build",
        "install-smoke",
        "testpypi-publish",
        "testpypi-smoke",
        "publish",
        "github-release",
    ):
        require(bool(_job(release_workflow, job_id)), f"release is missing the {job_id} job")
    require(
        "uses: ./.github/workflows/ci.yml" in _job(release_workflow, "verify"),
        "release gates must call CI",
    )
    require(
        "candidate_sha: ${{ needs.candidate.outputs.sha }}" in _job(release_workflow, "verify"),
        "release gates must use the validated SHA",
    )
    require(
        '"build==1.3.0" "twine==6.2.0"' in _job(release_workflow, "build"),
        "release build must use the verified build and Twine pins",
    )
    formal_jobs = ("testpypi-publish", "testpypi-smoke", "publish", "github-release")
    for job_id in formal_jobs:
        job = _job(release_workflow, job_id)
        require(
            "github.event_name == 'workflow_dispatch' && inputs.formal_publish == true" in job,
            f"{job_id} must allow only explicit formal recovery dispatches besides tag pushes",
        )
        require(
            "github.ref == 'refs/heads/master'" in job,
            f"{job_id} recovery dispatch must run from master",
        )
    require(
        "environment: testpypi" in _job(release_workflow, "testpypi-publish"),
        "TestPyPI upload must use the testpypi Environment",
    )
    require(
        "repository-url: https://test.pypi.org/legacy/"
        in _job(release_workflow, "testpypi-publish"),
        "TestPyPI upload must use the TestPyPI repository",
    )
    require(
        "needs.testpypi-publish.result == 'success'" in _job(release_workflow, "publish"),
        "PyPI publish must require TestPyPI upload success",
    )
    require(
        "needs.testpypi-smoke.result == 'success'" in _job(release_workflow, "publish"),
        "PyPI publish must require TestPyPI smoke success",
    )
    require(
        "id-token: write" in _job(release_workflow, "testpypi-publish"), "TestPyPI must use OIDC"
    )
    require("id-token: write" in _job(release_workflow, "publish"), "PyPI publish must use OIDC")
    require(
        "contents: write" in _job(release_workflow, "github-release"),
        "GitHub Release must have contents write",
    )
    require(
        "environment: github-release" in _job(release_workflow, "github-release"),
        "GitHub Release must use the github-release Environment",
    )
    require(
        "id-token: write" not in _job(release_workflow, "github-release"),
        "GitHub Release must not have OIDC write",
    )
    require(
        "needs.publish.result == 'success'" in _job(release_workflow, "github-release"),
        "GitHub Release must require PyPI success",
    )
    require(
        '--notes-file "release-notes/v${{ needs.candidate.outputs.version }}.md"'
        in _job(release_workflow, "github-release"),
        "GitHub Release must use the committed release notes file",
    )
    require(
        "--generate-notes" not in _job(release_workflow, "github-release"),
        "GitHub Release must not generate unreviewed notes",
    )
    require(
        "distribution: sdist" in _job(release_workflow, "install-smoke"),
        "install smoke must cover the source distribution",
    )
    require(
        "packages-dir: release/packages/" in _job(release_workflow, "publish"),
        "PyPI publish must exclude the manifest",
    )
    require(
        "packages-dir: release/packages/" in _job(release_workflow, "testpypi-publish"),
        "TestPyPI publish must exclude the manifest",
    )
    return tuple(failures)


def main(arguments: Sequence[str] | None = None) -> int:
    """Check CI and release workflow policy."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ci", type=Path)
    parser.add_argument("release", type=Path)
    options = parser.parse_args(arguments)
    failures = release_policy_failures(
        options.ci.read_text(encoding="utf-8"),
        options.release.read_text(encoding="utf-8"),
    )
    if failures:
        print("release workflow policy failed:")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print("release workflow policy verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
