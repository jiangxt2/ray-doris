import json
import traceback
from pathlib import Path
from unittest.mock import Mock, call

import pytest
from tools import check_release
from tools.check_release_workflow import release_policy_failures, slow_policy_failures
from tools.check_slow_result import verify_slow_result
from tools.find_slow_evidence import SlowEvidence, select_unique_evidence


def _assert_redacted_exception(exception: BaseException, caplog, sentinel: str) -> None:
    rendered_traceback = "".join(
        traceback.format_exception(type(exception), exception, exception.__traceback__)
    )
    assert sentinel not in str(exception)
    assert sentinel not in rendered_traceback
    assert sentinel not in caplog.text
    assert exception.__cause__ is None


def write_pyproject(path: Path, version: str = "1.0") -> Path:
    pyproject = path / "pyproject.toml"
    pyproject.write_text(
        f'[project]\nname = "ray-doris"\nversion = "{version}"\n',
        encoding="utf-8",
    )
    return pyproject


@pytest.mark.parametrize("version", ["1.0", "1.10", "2.0"])
def test_project_version_accepts_two_components(tmp_path: Path, version: str) -> None:
    assert check_release.project_version(write_pyproject(tmp_path, version)) == version


@pytest.mark.parametrize("version", ["1", "1.0.0", "v1.0", "1.0rc1"])
def test_project_version_rejects_non_two_component_versions(
    tmp_path: Path,
    version: str,
) -> None:
    with pytest.raises(RuntimeError, match=r"two-component major\.minor"):
        check_release.project_version(write_pyproject(tmp_path, version))


def test_package_version_rejects_three_components(tmp_path: Path) -> None:
    package_init = tmp_path / "__init__.py"
    package_init.write_text('__version__ = "1.0.0"\n', encoding="utf-8")
    with pytest.raises(RuntimeError, match=r"two-component major\.minor"):
        check_release.package_version(package_init)


def _workflow_sources() -> tuple[str, str]:
    root = Path(__file__).parents[2]
    return (
        (root / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"),
        (root / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8"),
    )


def _slow_workflow_sources() -> tuple[str, str, str, str]:
    root = Path(__file__).parents[2]
    return (
        (root / ".github" / "workflows" / "slow-integration.yml").read_text(encoding="utf-8"),
        (root / "tests" / "slow_integration" / "run.sh").read_text(encoding="utf-8"),
        (root / "tests" / "slow_integration" / "docker-compose.yml").read_text(encoding="utf-8"),
        (root / "tests" / "slow_integration" / "docker" / "ray.Dockerfile").read_text(
            encoding="utf-8"
        ),
    )


def test_slow_policy_requires_runner_and_image_lifecycle_contract() -> None:
    sources = _slow_workflow_sources()
    assert slow_policy_failures(*sources) == ()

    invalid_runner = sources[1].replace("up -d --no-build", "up -d --build", 1)
    failures = slow_policy_failures(sources[0], invalid_runner, sources[2], sources[3])
    assert any("must not rebuild images" in failure for failure in failures)

    for command, message in (
        ("network ls", "reject stale Compose networks"),
        ("volume ls", "reject stale Compose volumes"),
    ):
        invalid_runner = sources[1].replace(command, "missing-resource-command", 2)
        failures = slow_policy_failures(sources[0], invalid_runner, sources[2], sources[3])
        assert any(message in failure for failure in failures)


class _FakeActionsApi:
    def __init__(self, summaries, runs, artifacts) -> None:
        self.summaries = summaries
        self.runs = runs
        self.artifacts = artifacts

    def list_successful_runs(self, head_sha):
        return self.summaries

    def get_run(self, run_id):
        return self.runs[run_id]

    def list_artifacts(self, run_id, name):
        return self.artifacts.get(run_id, [])


def _slow_run(candidate_sha: str, *, path: str = ".github/workflows/slow-integration.yml"):
    return {
        "head_sha": candidate_sha,
        "conclusion": "success",
        "path": path,
    }


def test_select_unique_slow_evidence_ignores_expired_and_unrelated_runs() -> None:
    candidate_sha = "a" * 40
    api = _FakeActionsApi(
        summaries=[{"id": 1}, {"id": 2}],
        runs={
            1: _slow_run(candidate_sha),
            2: _slow_run(candidate_sha, path=".github/workflows/other.yml"),
        },
        artifacts={
            1: [
                {"id": 41, "name": f"ray-doris-slow-result-{candidate_sha}", "expired": True},
                {"id": 42, "name": f"ray-doris-slow-result-{candidate_sha}", "expired": False},
            ],
            2: [{"id": 43, "name": f"ray-doris-slow-result-{candidate_sha}", "expired": False}],
        },
    )

    assert select_unique_evidence(api, candidate_sha) == SlowEvidence(run_id=1, artifact_id=42)


def test_select_unique_slow_evidence_accepts_reusable_workflow() -> None:
    candidate_sha = "a" * 40
    api = _FakeActionsApi(
        summaries=[{"id": 7}],
        runs={
            7: {
                **_slow_run(candidate_sha, path=".github/workflows/caller.yml"),
                "referenced_workflows": [
                    {
                        "path": ".github/workflows/slow-integration.yml",
                        "sha": candidate_sha,
                    }
                ],
            }
        },
        artifacts={
            7: [{"id": 70, "name": f"ray-doris-slow-result-{candidate_sha}", "expired": False}]
        },
    )

    assert select_unique_evidence(api, candidate_sha) == SlowEvidence(run_id=7, artifact_id=70)


@pytest.mark.parametrize(
    ("artifacts", "message"),
    [
        ({1: []}, "no successful full slow result"),
        (
            {
                1: [
                    {"id": 41, "name": "ignored", "expired": False},
                    {"id": 42, "name": "ignored", "expired": False},
                ]
            },
            "no successful full slow result",
        ),
        (
            {
                1: [{"id": 43, "name": "ray-doris-slow-result-" + "a" * 40}],
            },
            "no successful full slow result",
        ),
        (
            {
                1: [
                    {
                        "id": 44,
                        "name": "ray-doris-slow-result-" + "a" * 40,
                        "expired": None,
                    }
                ],
            },
            "no successful full slow result",
        ),
    ],
)
def test_select_unique_slow_evidence_rejects_missing_artifact(artifacts, message: str) -> None:
    candidate_sha = "a" * 40
    api = _FakeActionsApi(
        summaries=[{"id": 1}],
        runs={1: _slow_run(candidate_sha)},
        artifacts=artifacts,
    )
    with pytest.raises(RuntimeError, match=message):
        select_unique_evidence(api, candidate_sha)


def test_select_unique_slow_evidence_rejects_duplicate_runs() -> None:
    candidate_sha = "a" * 40
    artifact_name = f"ray-doris-slow-result-{candidate_sha}"
    api = _FakeActionsApi(
        summaries=[{"id": 1}, {"id": 2}],
        runs={1: _slow_run(candidate_sha), 2: _slow_run(candidate_sha)},
        artifacts={
            1: [{"id": 41, "name": artifact_name, "expired": False}],
            2: [{"id": 42, "name": artifact_name, "expired": False}],
        },
    )
    with pytest.raises(RuntimeError, match="expected one successful full slow result"):
        select_unique_evidence(api, candidate_sha)


def test_select_unique_slow_evidence_rejects_duplicate_artifacts() -> None:
    candidate_sha = "a" * 40
    artifact_name = f"ray-doris-slow-result-{candidate_sha}"
    api = _FakeActionsApi(
        summaries=[{"id": 1}],
        runs={1: _slow_run(candidate_sha)},
        artifacts={
            1: [
                {"id": 41, "name": artifact_name, "expired": False},
                {"id": 42, "name": artifact_name, "expired": False},
            ]
        },
    )
    with pytest.raises(RuntimeError, match="multiple active slow-result artifacts"):
        select_unique_evidence(api, candidate_sha)


def test_select_unique_slow_evidence_rejects_invalid_sha() -> None:
    api = _FakeActionsApi([], {}, {})
    with pytest.raises(RuntimeError, match="full lowercase commit SHA"):
        select_unique_evidence(api, "not-a-sha")


@pytest.mark.parametrize(
    ("command", "message"),
    [
        (
            "docker compose -f tests/integration/docker-compose.yml build fe",
            "build the Doris FE image",
        ),
        (
            "docker compose -f tests/integration/docker-compose.yml build be",
            "build the Doris BE image",
        ),
        (
            "docker compose -f tests/integration/docker-compose.yml up -d --no-build",
            "start Doris with --no-build",
        ),
    ],
)
def test_release_policy_requires_prebuilt_doris_integration_images(
    command: str,
    message: str,
) -> None:
    ci_workflow, release_workflow = _workflow_sources()
    assert release_policy_failures(ci_workflow, release_workflow) == ()

    invalid_ci = ci_workflow.replace(command, "missing-doris-command", 1)
    assert any(
        message in failure for failure in release_policy_failures(invalid_ci, release_workflow)
    )


def test_release_policy_rejects_compose_rebuild_during_startup() -> None:
    ci_workflow, release_workflow = _workflow_sources()
    invalid_ci = ci_workflow.replace("up -d --no-build", "up -d --build", 1)

    assert any(
        "must not rebuild Doris images" in failure
        for failure in release_policy_failures(invalid_ci, release_workflow)
    )


def test_release_policy_scopes_slow_exception_to_v1_0_recovery() -> None:
    ci_workflow, release_workflow = _workflow_sources()
    assert release_policy_failures(ci_workflow, release_workflow) == ()

    invalid_release = release_workflow.replace(
        "inputs.release_tag == 'v1.0'", "inputs.release_tag == 'v1.1'", 1
    )
    assert any(
        "scope the slow-evidence exception" in failure
        for failure in release_policy_failures(ci_workflow, invalid_release)
    )


def test_release_policy_uses_versioned_release_notes_for_linkcheck() -> None:
    ci_workflow, release_workflow = _workflow_sources()
    invalid_ci = ci_workflow.replace("release-notes", "missing-notes", 1)
    assert any(
        "linkcheck must cover versioned release notes" in failure
        for failure in release_policy_failures(invalid_ci, release_workflow)
    )


def test_release_policy_rejects_reintroduced_changelog_reference() -> None:
    ci_workflow, release_workflow = _workflow_sources()
    invalid_ci = ci_workflow.replace("release-notes", "CHANGELOG.md release-notes", 1)
    assert any(
        "must not reference CHANGELOG.md" in failure
        for failure in release_policy_failures(invalid_ci, release_workflow)
    )


def test_verify_release_accepts_matching_tag_on_master(monkeypatch, tmp_path) -> None:
    run = Mock(return_value=Mock(returncode=0))
    monkeypatch.setattr(check_release.subprocess, "run", run)

    check_release.verify_release(
        tag="v1.0",
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
            tag="v1.0",
            commit="other-sha",
            pyproject=write_pyproject(tmp_path),
        )


def test_release_main_requires_candidate_arguments() -> None:
    with pytest.raises(SystemExit):
        check_release.main([])


def test_verify_candidate_accepts_matching_dry_run(monkeypatch) -> None:
    candidate_sha = "a" * 40
    master_sha = "b" * 40
    monkeypatch.setattr(
        check_release,
        "_git",
        Mock(side_effect=[candidate_sha, master_sha]),
    )
    monkeypatch.setattr(
        check_release,
        "_git_file",
        lambda _sha, path: (
            '[project]\nname = "ray-doris"\nversion = "1.0"\n'
            if path == "pyproject.toml"
            else '__version__ = "1.0"\n'
            if path == "src/ray_doris/__init__.py"
            else "# release notes\n"
        ),
    )
    monkeypatch.setattr(check_release.subprocess, "run", Mock(return_value=Mock(returncode=0)))

    assert check_release.verify_candidate(
        candidate_sha="candidate-ref",
        mode="dry-run",
        expected_version="1.0",
    ) == (candidate_sha, "1.0", master_sha)


def test_verify_candidate_peels_annotated_tag_event_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_sha = "a" * 40
    master_sha = "b" * 40
    event_tag_object_sha = "c" * 40
    git = Mock(side_effect=[candidate_sha, master_sha, candidate_sha])
    monkeypatch.setattr(check_release, "_git", git)
    monkeypatch.setattr(
        check_release,
        "_git_file",
        lambda _sha, path: (
            '[project]\nname = "ray-doris"\nversion = "1.0"\n'
            if path == "pyproject.toml"
            else '__version__ = "1.0"\n'
            if path == "src/ray_doris/__init__.py"
            else "# release notes\n"
        ),
    )
    monkeypatch.setattr(check_release.subprocess, "run", Mock(return_value=Mock(returncode=0)))

    assert check_release.verify_candidate(
        candidate_sha="candidate-ref",
        mode="tag",
        expected_version="1.0",
        tag="v1.0",
        event_sha=event_tag_object_sha,
        event_created=True,
    ) == (candidate_sha, "1.0", master_sha)

    assert git.call_args_list == [
        call("rev-parse", "candidate-ref^{commit}"),
        call("rev-parse", "origin/master^{commit}"),
        call("rev-parse", f"{event_tag_object_sha}^{{commit}}"),
    ]


def write_slow_result(path: Path, **overrides: object) -> Path:
    data = {
        "schema_version": 2,
        "commit_sha": "a" * 40,
        "workflow_run_id": 42,
        "profile": "full",
        "status": "passed",
        "transport": "mysql",
        "endpoint_mode": "logical",
        "doris_version": "4.0.6",
        "ray_version": "2.58.0",
        "python_version": "3.12.12",
        "initial_frontend_count": 1,
        "initial_backend_count": 3,
        "initial_ray_worker_count": 3,
        "row_count": 10_000,
        "parameters": {
            "be_memory_limit": "2g",
            "row_count": 10_000,
            "stress_seconds": 5,
        },
        "image_ids": {
            "doris_be": "sha256:" + "1" * 64,
            "doris_fe": "sha256:" + "2" * 64,
            "flight_proxy": "sha256:" + "3" * 64,
            "ray": "sha256:" + "4" * 64,
            "ray_base": "sha256:" + "5" * 64,
        },
        "topology": {
            "doris_backend_count": 3,
            "doris_frontend_count": 1,
            "distributed_table_buckets": 48,
            "ray_head_count": 1,
            "ray_worker_count": 3,
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


def test_slow_result_writer_emits_release_manifest(monkeypatch, tmp_path) -> None:
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1] / "slow_integration"))
    import write_result

    environment = {
        "RAY_DORIS_SLOW_COMMIT_SHA": "a" * 40,
        "RAY_DORIS_SLOW_RUN_ID": "42",
        "RAY_DORIS_SLOW_PROFILE": "full",
        "RAY_DORIS_ROW_COUNT": "10000",
        "RAY_DORIS_STRESS_SECONDS": "5",
        "RAY_DORIS_BE_MEMORY_LIMIT": "2g",
        "RAY_DORIS_SLOW_DORIS_BE_IMAGE_ID": "sha256:" + "1" * 64,
        "RAY_DORIS_SLOW_DORIS_FE_IMAGE_ID": "sha256:" + "2" * 64,
        "RAY_DORIS_SLOW_FLIGHT_PROXY_IMAGE_ID": "sha256:" + "3" * 64,
        "RAY_DORIS_SLOW_RAY_IMAGE_ID": "sha256:" + "4" * 64,
        "RAY_DORIS_SLOW_RAY_BASE_IMAGE_ID": "sha256:" + "5" * 64,
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(write_result.ray, "__version__", "2.58.0")

    manifest = tmp_path / "slow-result.json"
    write_result.write_result(manifest)
    verify_slow_result(manifest, expected_commit="a" * 40, expected_run_id=42)


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
        ({"parameters": {}}, "be_memory_limit"),
        (
            {
                "parameters": {
                    "be_memory_limit": "4g",
                    "row_count": 10_000,
                    "stress_seconds": 5,
                }
            },
            "be_memory_limit",
        ),
        (
            {
                "image_ids": {
                    "doris_be": "sha256:" + "1" * 64,
                    "doris_fe": "sha256:" + "2" * 64,
                    "flight_proxy": "sha256:" + "3" * 64,
                    "ray": "not-an-image-id",
                    "ray_base": "sha256:" + "5" * 64,
                }
            },
            "malformed",
        ),
        ({"topology": {}}, "doris_backend_count"),
        ({"workflow_run_id": True}, "workflow_run_id"),
        ({"scenarios": ["mysql_all_workers"]}, "scenarios"),
        (
            {
                "scenarios": [
                    "be_failure",
                    "logical_endpoint_tls",
                    "mysql_all_workers",
                    "ray_worker_retry",
                    "stream_load_transport_fault",
                    "stream_load_write_all_workers",
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
