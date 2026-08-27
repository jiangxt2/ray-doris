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
    docs_job = _job(ci_workflow, "docs")
    linkcheck_paths = "README.md CONTRIBUTING.md SECURITY.md release-notes doc/source"
    require(
        linkcheck_paths in docs_job,
        "CI documentation linkcheck must cover versioned release notes",
    )
    require(
        "CHANGELOG.md" not in docs_job,
        "CI documentation linkcheck must not reference CHANGELOG.md",
    )
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
    require(
        "release_profile:" in release_workflow, "release must expose Alpha and enterprise profiles"
    )
    require("formal_publish:" in release_workflow, "release must expose explicit formal recovery")
    require(
        "release_tag:" in release_workflow, "formal recovery must name the existing release tag"
    )
    for tool in (
        "tools/check_release.py",
        "tools/check_slow_result.py",
        "tools/find_slow_evidence.py",
        "tools/verify_release_artifacts.py",
        "tools/verify_release_tag.py",
    ):
        require(tool in release_workflow, f"release must use {tool}")
    for job_id in (
        "candidate",
        "verify",
        "slow-evidence",
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
    candidate_job = _job(release_workflow, "candidate")
    require(
        '--release-profile "${RELEASE_PROFILE}"' in candidate_job,
        "release candidate validation must receive the selected release profile",
    )
    require(
        "slow_required: ${{ steps.candidate.outputs.slow_required }}" in candidate_job,
        "release candidate must expose whether slow evidence is required",
    )
    require(
        "candidate_sha: ${{ needs.candidate.outputs.sha }}" in _job(release_workflow, "verify"),
        "release gates must use the validated SHA",
    )
    require(
        "tools/find_slow_evidence.py" in _job(release_workflow, "slow-evidence"),
        "release must locate the exact-SHA slow artifact",
    )
    slow_job = _job(release_workflow, "slow-evidence")
    require(
        "slow_required: ${{ steps.slow_mode.outputs.slow_required }}" in slow_job,
        "release slow evidence must expose its required/optional mode",
    )
    require(
        "RELEASE_PROFILE" in slow_job and "slow_required=true" in slow_job,
        "enterprise release profile must require slow evidence",
    )
    require(
        "slow_required == 'true'" in slow_job,
        "optional Alpha releases must skip slow artifact operations",
    )
    require(
        "FORMAL_HISTORICAL_RECOVERY" in _job(release_workflow, "slow-evidence")
        and "inputs.release_tag == 'v1.0'" in _job(release_workflow, "slow-evidence")
        and "needs.candidate.outputs.version == '1.0'" in _job(release_workflow, "slow-evidence"),
        "release must scope the slow-evidence exception to formal v1.0 recovery",
    )
    require(
        "steps.slow_mode.outputs.historical_recovery != 'true'"
        in _job(release_workflow, "slow-evidence"),
        "release slow evidence steps must honor the explicit historical exception",
    )
    require(
        "artifact-ids: ${{ steps.slow_run.outputs.artifact-id }}"
        in _job(release_workflow, "slow-evidence"),
        "release must download the selected slow artifact by ID",
    )
    require(
        "run_id: ${{ steps.slow_run.outputs.run-id }}" in _job(release_workflow, "slow-evidence"),
        "release must export the selected slow workflow run ID",
    )
    require(
        "tools/check_slow_result.py" in _job(release_workflow, "slow-evidence"),
        "release must validate the slow artifact",
    )
    require(
        "needs.slow-evidence.outputs.artifact_id" in _job(release_workflow, "build"),
        "release build must consume the verified slow artifact",
    )
    require(
        "needs.slow-evidence.outputs.slow_required == 'true'" in _job(release_workflow, "build"),
        "release build must only consume slow evidence when required",
    )
    require(
        "tools/check_slow_result.py" in _job(release_workflow, "build"),
        "release build must revalidate the slow artifact",
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
            "needs.slow-evidence.result == 'success'" in job,
            f"{job_id} must require exact-SHA slow evidence",
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


def slow_policy_failures(
    slow_workflow: str,
    slow_runner: str,
    slow_compose: str,
    ray_dockerfile: str,
) -> tuple[str, ...]:
    """Return slow-runner and exact-evidence policy violations."""
    failures: list[str] = []

    def require(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    require("workflow_call:" in slow_workflow, "slow workflow must expose workflow_call")
    require("workflow_dispatch:" in slow_workflow, "slow workflow must support manual runs")
    require("schedule:" in slow_workflow, "slow workflow must retain scheduled runs")
    require(
        "runs-on: [self-hosted, linux, x64, ray-doris-slow-it, ray-doris-slow-it-node24]"
        in slow_workflow,
        "slow workflow must use the dedicated self-hosted runner labels",
    )
    require(
        "Verify runner capacity and tools" in slow_workflow,
        "slow workflow must verify runner capacity before checkout",
    )
    require(
        'RAY_DORIS_SLOW_COMMIT_SHA}" =~ ^[0-9a-f]{40}$' in slow_workflow,
        "slow workflow must validate the evidence commit SHA",
    )
    require(
        "persist-credentials: false" in slow_workflow,
        "slow workflow checkout must not persist credentials",
    )
    require(
        "ray-doris-slow-result-${{ inputs.candidate_sha || github.sha }}" in slow_workflow,
        "slow result artifact must bind to the tested commit",
    )
    require(
        "docker compose --project-name ray-doris-it" in slow_runner,
        "slow runner must use the isolated Compose project",
    )
    require(
        'RAY_DORIS_SLOW_COMMIT_SHA}" =~ ^[0-9a-f]{40}$' in slow_runner,
        "slow runner must validate the evidence commit SHA",
    )
    require(
        "network ls" in slow_runner,
        "slow runner must reject stale Compose networks",
    )
    require(
        "name=ray-doris-it-" in slow_runner,
        "slow runner must reject stale Compose containers",
    )
    require(
        "volume ls" in slow_runner,
        "slow runner must reject stale Compose volumes",
    )
    require(
        "build ray-head fe be-1" in slow_runner,
        "slow runner must build each custom image before startup",
    )
    require(
        "up -d --no-build" in slow_runner,
        "slow runner must start Compose with --no-build",
    )
    require(
        "up -d --build" not in slow_runner,
        "slow runner must not rebuild images during startup",
    )
    require(
        "image ls --filter dangling=true" in slow_runner,
        "slow runner must record dangling-image state",
    )
    require(
        "system df" in slow_runner,
        "slow runner must record Docker disk usage",
    )
    require(
        'RAY_BASE_IMAGE: "${RAY_BASE_IMAGE:-rayproject/ray:2.58.0-py312-cpu}"' in slow_compose,
        "slow Compose must default to Ray 2.58.0",
    )
    require(
        "RAY_DORIS_SLOW_RAY_IMAGE_ID" in slow_compose,
        "slow Compose must pass image identity to the evidence manifest",
    )
    require(
        'assert ray.__version__ == "2.58.0"' in ray_dockerfile,
        "slow Ray image must assert Ray 2.58.0",
    )
    return tuple(failures)


def main(arguments: Sequence[str] | None = None) -> int:
    """Check CI and release workflow policy."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ci", type=Path)
    parser.add_argument("release", type=Path)
    parser.add_argument("slow", type=Path)
    parser.add_argument("slow_runner", type=Path)
    parser.add_argument("slow_compose", type=Path)
    parser.add_argument("ray_dockerfile", type=Path)
    options = parser.parse_args(arguments)
    failures = release_policy_failures(
        options.ci.read_text(encoding="utf-8"),
        options.release.read_text(encoding="utf-8"),
    )
    failures += slow_policy_failures(
        options.slow.read_text(encoding="utf-8"),
        options.slow_runner.read_text(encoding="utf-8"),
        options.slow_compose.read_text(encoding="utf-8"),
        options.ray_dockerfile.read_text(encoding="utf-8"),
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
