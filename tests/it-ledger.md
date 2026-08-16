# Integration test ledger

This ledger records the infrastructure and code state used for RFC-candidate evidence. A passing
result is reusable only while the production source, tests, Compose configuration, dependency
environment, and target image are unchanged.

| Code state | Suite/profile | Environment and coverage | Result |
| --- | --- | --- | --- |
| Baseline `086160e` plus approved write implementation (working tree), existing two-hour Compose stack | `tests/integration` | Same coverage as the final run; the session fixture failed before tests because the reused BE reported `MEM_ALLOC_FAILED` under its 2 GiB container limit | 30 setup errors; infrastructure failure recorded in `/tmp/ray-doris-it-final-failed.log` |
| Baseline `086160e` plus approved write implementation (working tree), freshly recreated Compose stack | `tests/integration` | Python 3.12.12, Ray 2.56.1, PyArrow 25.0.1, PyMySQL 2.2.8, Doris 4.0.6; existing reads plus Stream Load load/upsert/partial-update/aggregate, type round-trip, permissions, empty Dataset, and schema preflight | 30 passed in 51.95s; Compose log `/tmp/ray-doris-it-final.log` |
| Review-fixed working tree after P1/P2/Nit changes | `tests/integration` | Same real Doris 4.0.6 read/write, permissions, schema, redirect, and empty Dataset coverage | 30 passed in 52.23s; pytest log `/tmp/ray-doris-it-review-fixes.log`; Compose log `/tmp/ray-doris-it-review-fixes-compose.log` |
| Baseline `086160eb72c6533e507100521e8f2dae9b04c922` plus approved write implementation (working tree) | `tests/slow_integration/run.sh` (`full`) | Previous run before the review fixes; three Ray workers, three Doris BEs, strict TLS, distributed read/write, worker retry, BE failover, and read-side fault scenarios; local resource overrides `RAY_DORIS_ROW_COUNT=1000` and `RAY_DORIS_STRESS_SECONDS=1` | 8 passed, 8 warnings in 80.12s; superseded because the implementation and scenario manifest changed; logs `/tmp/ray-doris-slow-it-final-10` |
| Review-fixed working tree after P1/P2/Nit changes | `tests/slow_integration/run.sh` (`full`) | First attempt used the new three-worker/three-BE topology and deterministic transport-fault service; the fault service started before the shared state volume permission setup completed | Infrastructure startup failure; no pytest scenarios ran; log `/tmp/ray-doris-slow-it-review-fixes/compose.log` |
| Review-fixed working tree after startup-order fix | `tests/slow_integration/run.sh` (`full`) | Python 3.12.9, Ray 2.55.1, Doris 4.0.6; three Ray workers, three Doris BEs, strict TLS, distributed read/write, worker retry, BE failover, and deterministic post-send Stream Load transport fault; local resource overrides `RAY_DORIS_ROW_COUNT=1000` and `RAY_DORIS_STRESS_SECONDS=1` | 9 passed, 8 warnings in 93.78s; manifest `/tmp/ray-doris-slow-it-review-fixes-2/slow-result.json`; pytest/Compose logs `/tmp/ray-doris-slow-it-review-fixes-2` |

The final-run evidence below records the exact `git diff --no-ext-diff`, Python/Ray/PyArrow/PyMySQL
versions, Docker and Compose versions, image digests, Compose topology, command lines, and retained
pytest/Compose logs. All required rows have a passing result; the infrastructure-only failed attempt
is retained separately for diagnosis.

## Evidence manifest

- source revision: baseline `086160eb72c6533e507100521e8f2dae9b04c922` plus uncommitted worktree changes;
- Python: 3.12.12;
- Ray: 2.56.1;
- PyArrow: 25.0.1;
- PyMySQL: 2.2.8;
- Doris image digest: FE `sha256:830863e9ff8af4b354df5303b1235c11b2f822fa3125b83a1627e498c5c251cf`, BE `sha256:72e58021c2fa110350269e587d6c74a28579d3c1ed563023cb784a3824f4ad87`;
- Docker/Compose: Docker 29.7.2, Compose v2.29.7.2;
- logs: `/tmp/ray-doris-it-review-fixes-compose.log` and `/tmp/ray-doris-slow-it-review-fixes-2`;
- slow result checker: `.venv/bin/python tools/check_slow_result.py /tmp/ray-doris-slow-it-review-fixes-2/slow-result.json --expected-commit 086160eb72c6533e507100521e8f2dae9b04c922 --expected-run-id 1` passed;
- final slow command: `RAY_DORIS_SLOW_PROFILE=full RAY_DORIS_ROW_COUNT=1000 RAY_DORIS_STRESS_SECONDS=1 RAY_DORIS_SLOW_LOG_DIR=/tmp/ray-doris-slow-it-review-fixes-2 ./tests/slow_integration/run.sh`;
- unit: `tests/unit` — 240 passed in 1.54s after the final dead-code cleanup;
- Ray contract: `tests/contract` — 3 passed in 5.36s on Ray 2.56.1 after the final dead-code cleanup;
- static: Ruff format/lint, mypy (`18 source files`), and `git diff --check` passed;
- package checks: isolated Hatchling build completed and `.venv/bin/twine check dist/*` passed;
- documentation checks: `tools/check_docs.py`, Sphinx strict build, and spelling check passed.

The final slow profile used the recorded local resource overrides so the three-worker Ray cluster
could complete within the available Docker memory. The profile exercised every required scenario
after the review fixes and its manifest records the exact override values; this is not a claim about
the unmodified stress defaults. The only subsequent production-source edit removed an unreachable
zero-result helper; the current unit and contract runs cover that cleanup, while the long-running
Doris evidence remains valid under the no-runtime-change test reuse rule.
