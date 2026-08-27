"""Select one exact-SHA slow-integration release artifact from GitHub Actions."""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

_COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_PAGE_SIZE = 100
_SLOW_WORKFLOW = ".github/workflows/slow-integration.yml"


class ActionsApi(Protocol):
    """Subset of the GitHub Actions API needed by the selector."""

    def list_successful_runs(self, head_sha: str) -> Iterable[Mapping[str, Any]]: ...

    def get_run(self, run_id: int) -> Mapping[str, Any]: ...

    def list_artifacts(self, run_id: int, name: str) -> Iterable[Mapping[str, Any]]: ...


@dataclass(frozen=True)
class SlowEvidence:
    """Identity of a selected successful slow workflow artifact."""

    run_id: int
    artifact_id: int


def _positive_integer(data: Mapping[str, Any], name: str) -> int:
    value = data.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimeError(f"slow evidence API returned an invalid {name}")
    return value


def _workflow_path(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.split("@", 1)[0]


def _is_slow_run(run: Mapping[str, Any], candidate_sha: str) -> bool:
    if run.get("head_sha") != candidate_sha or run.get("conclusion") != "success":
        return False
    if _workflow_path(run.get("path")) == _SLOW_WORKFLOW:
        return True
    referenced = run.get("referenced_workflows")
    if not isinstance(referenced, list):
        return False
    return any(
        (
            _workflow_path(workflow.get("path")) == _SLOW_WORKFLOW
            or _workflow_path(workflow.get("path")).endswith(f"/{_SLOW_WORKFLOW}")
        )
        and workflow.get("sha") == candidate_sha
        for workflow in referenced
        if isinstance(workflow, Mapping)
    )


def select_unique_evidence(api: ActionsApi, candidate_sha: str) -> SlowEvidence:
    """Select exactly one non-expired full slow artifact for a candidate SHA."""
    if _COMMIT_SHA.fullmatch(candidate_sha) is None:
        raise RuntimeError("candidate SHA must be a full lowercase commit SHA")

    artifact_name = f"ray-doris-slow-result-{candidate_sha}"
    matches: list[SlowEvidence] = []
    for summary in api.list_successful_runs(candidate_sha):
        run_id = _positive_integer(summary, "id")
        run = api.get_run(run_id)
        if not _is_slow_run(run, candidate_sha):
            continue
        artifacts = [
            artifact
            for artifact in api.list_artifacts(run_id, artifact_name)
            if artifact.get("name") == artifact_name and artifact.get("expired") is False
        ]
        if len(artifacts) > 1:
            raise RuntimeError(f"multiple active slow-result artifacts exist for run {run_id}")
        if len(artifacts) == 1:
            matches.append(
                SlowEvidence(
                    run_id=run_id,
                    artifact_id=_positive_integer(artifacts[0], "id"),
                )
            )

    if not matches:
        raise RuntimeError(f"no successful full slow result exists for {candidate_sha}")
    if len(matches) != 1:
        raise RuntimeError(
            f"expected one successful full slow result for {candidate_sha}, found {len(matches)}"
        )
    return matches[0]


class GitHubActionsClient:
    """Small, fail-closed GitHub Actions REST client."""

    def __init__(self, *, api_url: str, repository: str, token: str) -> None:
        if not repository or not token:
            raise RuntimeError("GITHUB_REPOSITORY and GH_TOKEN are required")
        self._base_url = api_url.rstrip("/")
        self._repository = repository
        self._token = token

    def _get(self, path: str, **parameters: str) -> Mapping[str, Any]:
        query = urllib.parse.urlencode(parameters)
        url = f"{self._base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError, urllib.error.URLError):
            raise RuntimeError("GitHub Actions API request failed") from None
        if not isinstance(payload, Mapping):
            raise RuntimeError("GitHub Actions API returned an invalid response")
        return payload

    def _list(self, path: str, key: str, **parameters: str) -> Iterator[Mapping[str, Any]]:
        page = 1
        while True:
            payload = self._get(
                path,
                **parameters,
                page=str(page),
                per_page=str(_PAGE_SIZE),
            )
            values = payload.get(key)
            if not isinstance(values, list):
                raise RuntimeError("GitHub Actions API returned an invalid collection")
            for value in values:
                if not isinstance(value, Mapping):
                    raise RuntimeError("GitHub Actions API returned an invalid collection item")
                yield value
            if len(values) < _PAGE_SIZE:
                return
            page += 1

    def list_successful_runs(self, head_sha: str) -> Iterator[Mapping[str, Any]]:
        return self._list(
            f"/repos/{self._repository}/actions/runs",
            "workflow_runs",
            head_sha=head_sha,
            status="success",
        )

    def get_run(self, run_id: int) -> Mapping[str, Any]:
        return self._get(f"/repos/{self._repository}/actions/runs/{run_id}")

    def list_artifacts(self, run_id: int, name: str) -> Iterator[Mapping[str, Any]]:
        return self._list(
            f"/repos/{self._repository}/actions/runs/{run_id}/artifacts",
            "artifacts",
            name=name,
        )


def main(arguments: Sequence[str] | None = None) -> int:
    """Find one exact-SHA slow artifact and write its identity to GitHub output."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--github-output", default="")
    options = parser.parse_args(arguments)
    api = GitHubActionsClient(
        api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
        repository=os.environ.get("GITHUB_REPOSITORY", ""),
        token=os.environ.get("GH_TOKEN", ""),
    )
    evidence = select_unique_evidence(api, options.candidate_sha)
    if options.github_output:
        with Path(options.github_output).open("a", encoding="utf-8") as stream:
            stream.write(f"run-id={evidence.run_id}\nartifact-id={evidence.artifact_id}\n")
    print(f"exact slow evidence selected: run {evidence.run_id}, artifact {evidence.artifact_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
