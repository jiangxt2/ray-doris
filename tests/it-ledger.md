# Integration test ledger

This ledger records the infrastructure and code state used for RFC-candidate evidence. A passing
result is reusable only while the production source, tests, Compose configuration, dependency
environment, and target image are unchanged.

| Code state | Suite/profile | Environment and coverage | Result |
| --- | --- | --- | --- |
| Worktree `release-evidence-alignment` at base `ebbf2e55406c482f5f938a1e23c120d08d7c2a3e` plus current uncommitted workflow/slow-infrastructure changes | `tests/slow_integration/run.sh` (`core`) | Planned Ray 2.58.0, Python 3.12, Doris 4.0.6; runner preflight, one Ray head, three Ray workers, one FE, three BEs, isolated Compose build/start/cleanup, and tablet task/worker distribution | `Planned`; do not run until the code is committed to the candidate branch and a runner with both `ray-doris-slow-it` and `ray-doris-slow-it-node24` labels is available |
| Baseline `086160e` plus approved write implementation (working tree), existing two-hour Compose stack | `tests/integration` | Same coverage as the final run; the session fixture failed before tests because the reused BE reported `MEM_ALLOC_FAILED` under its 2 GiB container limit | 30 setup errors; infrastructure failure recorded in `/tmp/ray-doris-it-final-failed.log` |
| Baseline `086160e` plus approved write implementation (working tree), freshly recreated Compose stack | `tests/integration` | Python 3.12.12, Ray 2.56.1, PyArrow 25.0.1, PyMySQL 2.2.8, Doris 4.0.6; existing reads plus Stream Load load/upsert/partial-update/aggregate, type round-trip, permissions, empty Dataset, and schema preflight | 30 passed in 51.95s; Compose log `/tmp/ray-doris-it-final.log` |
| Review-fixed working tree after P1/P2/Nit changes | `tests/integration` | Same real Doris 4.0.6 read/write, permissions, schema, redirect, and empty Dataset coverage | 30 passed in 52.23s; pytest log `/tmp/ray-doris-it-review-fixes.log`; Compose log `/tmp/ray-doris-it-review-fixes-compose.log` |
| Baseline `086160eb72c6533e507100521e8f2dae9b04c922` plus approved write implementation (working tree) | `tests/slow_integration/run.sh` (`full`) | Previous run before the review fixes; three Ray workers, three Doris BEs, strict TLS, distributed read/write, worker retry, BE failover, and read-side fault scenarios; local resource overrides `RAY_DORIS_ROW_COUNT=1000` and `RAY_DORIS_STRESS_SECONDS=1` | 8 passed, 8 warnings in 80.12s; superseded because the implementation and scenario manifest changed; logs `/tmp/ray-doris-slow-it-final-10` |
| Review-fixed working tree after P1/P2/Nit changes | `tests/slow_integration/run.sh` (`full`) | First attempt used the new three-worker/three-BE topology and deterministic transport-fault service; the fault service started before the shared state volume permission setup completed | Infrastructure startup failure; no pytest scenarios ran; log `/tmp/ray-doris-slow-it-review-fixes/compose.log` |
| Review-fixed working tree after startup-order fix | `tests/slow_integration/run.sh` (`full`) | Python 3.12.9, Ray 2.55.1, Doris 4.0.6; three Ray workers, three Doris BEs, strict TLS, distributed read/write, worker retry, BE failover, and deterministic post-send Stream Load transport fault; local resource overrides `RAY_DORIS_ROW_COUNT=1000` and `RAY_DORIS_STRESS_SECONDS=1` | 9 passed, 8 warnings in 93.78s; manifest `/tmp/ray-doris-slow-it-review-fixes-2/slow-result.json`; pytest/Compose logs `/tmp/ray-doris-slow-it-review-fixes-2` |
| Base `40cd4b861a473988e328c84cc3d582693473854d` plus implementation diff SHA-256 `ec2e4af209d9380ac31e9bf487c4a8857d22b07d303557a0e7c0ad42fe93a158` | `tests/integration` | Python 3.12.12, Ray 2.58.0, PyArrow 25.0.1, PyMySQL 2.2.8, Doris 4.0.6; required read/write, schema, permissions, redirect, empty Dataset, Flight, and Stream Load coverage before the final constructor-time write guard | 30 passed in 56.00s; superseded for final-code evidence after static review added the empty-Dataset unsupported-Ray guard; JUnit `/tmp/ray-doris-ray258-required-it.xml`, Compose log `/tmp/ray-doris-ray258-required-it-compose.log` |
| Base `40cd4b861a473988e328c84cc3d582693473854d` plus final implementation diff SHA-256 `ce9ae55d87ab875dc8503ff8b05b1fd836aad30a863d138eea9070f816db01b4` | `tests/integration` | Python 3.12.12, Ray 2.58.0, PyArrow 25.0.1, PyMySQL 2.2.8, Doris 4.0.6; exact final worktree code after adding constructor-time write compatibility validation | 30 passed in 56.04s; JUnit `/tmp/ray-doris-ray258-required-it-final.xml`, Compose log `/tmp/ray-doris-ray258-required-it-final-compose.log` |

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

## Ray 2.58 compatibility evidence

- worktree: `/Users/jiangxintong/GitHub/workspace/ray-doris-ray-258-compatibility`, branch
  `ray-258-compatibility`;
- base revision: `40cd4b861a473988e328c84cc3d582693473854d`;
- runtime implementation diff: `/tmp/ray-doris-ray258-implementation-final.diff`, SHA-256
  `ce9ae55d87ab875dc8503ff8b05b1fd836aad30a863d138eea9070f816db01b4`; this digest excludes this
  evidence-only ledger update;
- final repository diff: `/tmp/ray-doris-ray258-repository-final.diff`, SHA-256
  `c86f64320767eff45af18fe5f51a1bfb8609235dd6cd887524a004aca44fa337`; changes after the runtime
  digest are limited to CI orchestration, public commands, the executable workflow-policy checker,
  its unit tests, and this ledger;
- official final compatibility points: Ray 2.57.0 and 2.58.0;
- minimum matrix: Python 3.9.25 / Ray 2.49.2, final full unit and contract suite 253 passed;
- intermediate matrix: Python 3.10.18 / Ray 2.57.0, final full unit and contract suite 253 passed;
- main matrix: Python 3.12.12 / Ray 2.58.0, final full unit and contract suite 253 passed;
- highest-Python matrix and wheel smoke: Python 3.13.7 / Ray 2.58.0, final full unit and contract
  suite 253 passed; wheel dependency check and import passed before the environment was switched to
  the editable CI layout;
- final constructor-time unsupported-Ray guard plus Datasink contract: 4 passed on each of Ray
  2.49.2, Ray 2.57.0, and Python 3.13 / Ray 2.58.0 after the production-code change;
- final main unit and contract suite: 253 passed with 85% aggregate coverage;
- static checks: Ruff format, Ruff lint, strict mypy for 18 source files, pre-commit, workflow action
  pins, release-workflow policy, and `git diff --check` passed;
- documentation: `tools/check_docs.py`, Sphinx strict, spelling, and the separate Sphinx linkcheck
  passed against the regenerated Ray 2.58.0 documentation lock;
- package: wheel and sdist built; Twine checks passed;
- required CI Docker lifecycle: FE and BE images are built separately, Compose startup uses
  `--no-build`, Docker proxy variables are explicitly cleared, and the release-policy checker plus
  four new unit cases enforce the contract;
- required Doris IT: the pre-guard implementation passed 30 tests in 56.00s. Static review then
  added constructor-time write compatibility validation so empty Dataset writes cannot bypass the
  unsupported-Ray guard. After renewed long-test approval, the final implementation diff passed all
  30 tests in 56.04s; both evidence rows and log paths are retained in the table. The later final
  repository changes do not alter production source, integration tests, Compose configuration, or
  images, and the passing run already used the now-documented separate-build/`--no-build` sequence,
  so the result remains reusable without another IT run;
- Doris base digests: FE
  `sha256:830863e9ff8af4b354df5303b1235c11b2f822fa3125b83a1627e498c5c251cf`, BE
  `sha256:72e58021c2fa110350269e587d6c74a28579d3c1ed563023cb784a3824f4ad87`;
- local test images: FE
  `sha256:df0687fa697a24df19e0e62dc85e93702e855c0ef21af056a210f9945b87082e`, BE
  `sha256:50232281bf40698f1a6935f5037f4f0c635e1f6f4c1ff33fadc6976f1d81aff0`;
- Docker: Engine 29.7.2, Compose v5.4.0;
- cleanup: the exact `ray-doris-it` containers, network, volumes, and local test images were removed;
  the pre-existing dangling image set was unchanged;
- slow full: not run; it remains assigned to the separate exact-candidate release-evidence work.
