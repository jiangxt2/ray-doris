# Integration test ledger

## Native V1 predicates: local validation

Protocol evidence was collected in worktree `predicate-pushdown` on base
`36efb8ff46c89efc5c16875bba3ab869c6534a70`, before commit/push. The protocol-evidence production SHA-256 is
`839cb37ea9337ce1ae08cfb115b0e73a013406278d1aada17ad4fc5a7d713086`.
The implementation plan records the exact runtime/test fingerprints and applicability checks.

| Suite | Environment / scope | Result |
| --- | --- | --- |
| Full unit and public contracts | Python 3.12.12, Ray 2.59.0, Arrow 26.0.0 | 340 passed, 3 upstream deprecation warnings, 13.14s; `/tmp/ray-doris-predicate-final-unit-contract.log` |
| Minimal compatibility contracts | Separate Python 3.12.12 environments for Ray 2.57.0, 2.58.0, 2.49.2 | Each 50 passed; native contracts on 2.57/2.58, disabled native hooks and legacy reads on 2.49.2; `/tmp/ray-doris-predicate-ray-<version>.log` |
| Complete required Doris IT | Python 3.12.12, Ray 2.59.0, Arrow 26.0.0, Doris 4.0.6; dedicated `ray-doris-predicate-it` project | 32 passed, 3 failed, 210.80s. The three SQL-recording cases failed before scans because workers could not import the test probe module. Functional MySQL/Flight native branch, trusted-filter, residual and limit cases passed. Probe now serializes by value and passed isolated deserialization; real SQL evidence remains pending an approved retry. Logs/XML: `/tmp/ray-doris-predicate-required-it/` |
| Distributed full | Fixed Ray 2.59.0 base, Doris 4.0.6, full profile including new predicate cases; temporary runner copy changes only absolute source directory and Compose/image namespace | Infrastructure startup failed with `Pool overlaps with other one on this address space` before pytest started. No distributed scenarios executed. Logs: `/tmp/ray-doris-predicate-slow-it/` and `/tmp/ray-doris-predicate-slow-launch.log` |
| Static and documentation | Ruff format/lint, strict mypy, documentation checker, Sphinx strict and spelling | Passed; documentation logs `/tmp/ray-doris-predicate-doc-{strict,spelling}-final.log` |
| Approved protocol retry | Same three cases only, Python 3.12.12 / Ray 2.59.0 / Doris 4.0.6; temporary CIDR `10.254.127.0/24` and BE mapped Flight port 28050 | 3 failed, 43.18s. Probe serialization is fixed; MySQL integer/null cases reached scans and FE audit shows bound boolean SQL. The BE reached its 2 GiB container / 1.8 GiB process limit (`MEM_ALLOC_FAILED` and Flight `MEM_LIMIT_EXCEEDED`); no result-assertion mismatch identified. Logs/XML: `/tmp/ray-doris-predicate-required-isolated/`. An initial bootstrap collision, before pytest, is retained in `/tmp/ray-doris-predicate-required-retry/`. |
| Approved distributed full retry | Python 3.12.14 / Ray 2.59.0 / Doris 4.0.6; 1 FE, 3 BE, 3 Ray workers, 3 GiB per BE, 10,000 rows and 5s stress; temporary CIDR `10.254.253.0/24` with matching priority networks/TLS SANs | 11 passed, 12 warnings, 120.36s. Both new MySQL/Flight native predicate cases and all existing fault/read/write scenarios passed. Runtime and Python tests are byte-identical to the worktree; only temporary network/port configuration differs. Logs and passed full manifest: `/tmp/ray-doris-predicate-slow-retry-it/`. |
| Final approved 3 GiB protocol retry | Same three MySQL/Flight/auto cases; Python 3.12.12 / Ray 2.59.0 / Arrow 26.0.0 / Doris 4.0.6; required BE `mem_limit: 3g`; runtime/test/config SHA-256 `2ff70fd843dfb8265450767984a140719e8d25727974ac49b53b764507d9335a` | 3 passed, 3 upstream deprecation warnings, 336.79s. Real planning SQL, MySQL bound values, Flight/auto literal SQL and row equivalence assertions passed. Docker inspect verified 3221225472 bytes for BE memory. Logs/XML: `/tmp/ray-doris-predicate-required-3g-final/`. A corrected temporary script-path bootstrap, before pytest, is retained in `/tmp/ray-doris-predicate-required-3g/`. |
| Pre-commit import-policy checks | Deferred public-module loading and conditional test imports only; no SQL, protocol, predicate, or assertion changes. Production SHA-256 `b9dd6c5a979d794cdd33796846b26c1e1c8a92f8188d32760f41ad9e206e11b6`; runtime/test/config SHA-256 `5c5bfd8e9973a2e8969b93f842bcef62c3b4282d246100c47cb3e3aa7f157530` | Full unit/public contracts: 340 passed in 13.40s. Minimal compiler/compatibility/read contracts: 36 passed on each Ray 2.57.0, 2.58.0 and 2.49.2 baseline. Logs `/tmp/ray-doris-predicate-precommit-unit-contract.log` and `/tmp/ray-doris-predicate-precommit-ray-<version>.log`. Ruff, mypy, docs checker and Python-safety layer passed. |

Both infrastructure attempts cleaned only their own project resources and custom images. Other
chats were concurrently using Docker. The additional untagged image observed after slow cleanup
has an existing registry digest, no project label, and an August creation date; it was preserved.
The network-overlap source was absent on subsequent inspection; no existing network was removed.
The user subsequently requested rollback of the required-IT memory increase: `be.mem_limit` is
restored to `2g`, and the Compose file matches HEAD. Production source, Python tests and CI/workflow
memory remain unchanged. The three 3 GiB protocol passes above are historical local evidence;
they do not prove the restored 2 GiB required/CI configuration passes. Its latest protocol run
failed on BE memory limits. The primary, compatibility, earlier 32 required and independently
configured full slow results retain their recorded applicability. No tests were rerun for the
rollback. All prior exact-project cleanup evidence is preserved.

The pre-commit import cleanup preserves lazy optional loading and legacy-version guards. After
normalizing only import loading, imported-symbol qualification and import placement, the functional
ASTs of all five affected files are unchanged; `/tmp/ray-doris-predicate-import-equivalence.json`
records that comparison. New unit/contract and compatibility checks cover the import changes; the
recorded database/slow evidence remains applicable to the unchanged SQL/protocol behavior.
The restored 2 GiB budget still has no passing protocol-recording result; no additional container
suite was run or claimed. Commit/push authorization does not change that validation limitation.

This ledger records the infrastructure and code state used for RFC-candidate evidence. A passing
result is reusable only while the production source, tests, Compose configuration, dependency
environment, and target image are unchanged.

| Code state | Suite/profile | Environment and coverage | Result |
| --- | --- | --- | --- |
| Merged `master` commit `a3d2ef2e90f9100f97ea646b7b59fe26fbdf1448` (the `release-evidence-alignment` worktree has been cleaned) | `tests/slow_integration/run.sh` (`core`) | Ray 2.58.0, Python 3.12, Doris 4.0.6; runner preflight, one Ray head, three Ray workers, one FE, three BEs, isolated Compose build/start/cleanup, and tablet task/worker distribution | `Code complete`; repository runner count is 0, so no `core` or `full` execution evidence exists. Run only for the final `1.1` candidate after a runner with both `ray-doris-slow-it` and `ray-doris-slow-it-node24` labels is available |
| Baseline `086160e` plus approved write implementation (working tree), existing two-hour Compose stack | `tests/integration` | Same coverage as the final run; the session fixture failed before tests because the reused BE reported `MEM_ALLOC_FAILED` under its 2 GiB container limit | 30 setup errors; infrastructure failure recorded in `/tmp/ray-doris-it-final-failed.log` |
| Baseline `086160e` plus approved write implementation (working tree), freshly recreated Compose stack | `tests/integration` | Python 3.12.12, Ray 2.56.1, PyArrow 25.0.1, PyMySQL 2.2.8, Doris 4.0.6; existing reads plus Stream Load load/upsert/partial-update/aggregate, type round-trip, permissions, empty Dataset, and schema preflight | 30 passed in 51.95s; Compose log `/tmp/ray-doris-it-final.log` |
| Review-fixed working tree after P1/P2/Nit changes | `tests/integration` | Same real Doris 4.0.6 read/write, permissions, schema, redirect, and empty Dataset coverage | 30 passed in 52.23s; pytest log `/tmp/ray-doris-it-review-fixes.log`; Compose log `/tmp/ray-doris-it-review-fixes-compose.log` |
| Baseline `086160eb72c6533e507100521e8f2dae9b04c922` plus approved write implementation (working tree) | `tests/slow_integration/run.sh` (`full`) | Previous run before the review fixes; three Ray workers, three Doris BEs, strict TLS, distributed read/write, worker retry, BE failover, and read-side fault scenarios; local resource overrides `RAY_DORIS_ROW_COUNT=1000` and `RAY_DORIS_STRESS_SECONDS=1` | 8 passed, 8 warnings in 80.12s; superseded because the implementation and scenario manifest changed; logs `/tmp/ray-doris-slow-it-final-10` |
| Review-fixed working tree after P1/P2/Nit changes | `tests/slow_integration/run.sh` (`full`) | First attempt used the new three-worker/three-BE topology and deterministic transport-fault service; the fault service started before the shared state volume permission setup completed | Infrastructure startup failure; no pytest scenarios ran; log `/tmp/ray-doris-slow-it-review-fixes/compose.log` |
| Review-fixed working tree after startup-order fix | `tests/slow_integration/run.sh` (`full`) | Python 3.12.9, Ray 2.55.1, Doris 4.0.6; three Ray workers, three Doris BEs, strict TLS, distributed read/write, worker retry, BE failover, and deterministic post-send Stream Load transport fault; local resource overrides `RAY_DORIS_ROW_COUNT=1000` and `RAY_DORIS_STRESS_SECONDS=1` | 9 passed, 8 warnings in 93.78s; manifest `/tmp/ray-doris-slow-it-review-fixes-2/slow-result.json`; pytest/Compose logs `/tmp/ray-doris-slow-it-review-fixes-2` |
| Base `40cd4b861a473988e328c84cc3d582693473854d` plus implementation diff SHA-256 `ec2e4af209d9380ac31e9bf487c4a8857d22b07d303557a0e7c0ad42fe93a158` | `tests/integration` | Python 3.12.12, Ray 2.58.0, PyArrow 25.0.1, PyMySQL 2.2.8, Doris 4.0.6; required read/write, schema, permissions, redirect, empty Dataset, Flight, and Stream Load coverage before the final constructor-time write guard | 30 passed in 56.00s; superseded for final-code evidence after static review added the empty-Dataset unsupported-Ray guard; JUnit `/tmp/ray-doris-ray258-required-it.xml`, Compose log `/tmp/ray-doris-ray258-required-it-compose.log` |
| Base `40cd4b861a473988e328c84cc3d582693473854d` plus final implementation diff SHA-256 `ce9ae55d87ab875dc8503ff8b05b1fd836aad30a863d138eea9070f816db01b4` | `tests/integration` | Python 3.12.12, Ray 2.58.0, PyArrow 25.0.1, PyMySQL 2.2.8, Doris 4.0.6; exact final worktree code after adding constructor-time write compatibility validation | 30 passed in 56.04s; JUnit `/tmp/ray-doris-ray258-required-it-final.xml`, Compose log `/tmp/ray-doris-ray258-required-it-final-compose.log` |

The historical required-write and slow-run evidence below records the exact `git diff --no-ext-diff`,
Python/Ray/PyArrow/PyMySQL versions, Docker and Compose versions, image digests, Compose topology,
command lines, and retained pytest/Compose logs. All completed required rows have a passing result; the
infrastructure-only failed attempt is retained separately for diagnosis. The current Ray 2.58 release
infrastructure row above has no runtime result because the repository has no eligible self-hosted runner.

## Historical evidence manifest

- historical source revision: baseline `086160eb72c6533e507100521e8f2dae9b04c922` plus uncommitted worktree changes;
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

The historical slow profile used the recorded local resource overrides so the three-worker Ray cluster
could complete within the available Docker memory. The profile exercised every required scenario
after the review fixes and its manifest records the exact override values; this is not a claim about
the unmodified stress defaults. The only subsequent production-source edit removed an unreachable
zero-result helper; the current unit and contract runs cover that cleanup, while the long-running
Doris evidence remains valid under the no-runtime-change test reuse rule.

## Ray 2.58 compatibility evidence

- historical worktree (cleaned): `/Users/jiangxintong/GitHub/workspace/ray-doris-ray-258-compatibility`, branch
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

## Python 3.11 CI compatibility evidence

- worktree: `/Users/jiangxintong/GitHub/workspace/ray-doris-python-311-ci`, branch
  `python-311-ci`;
- base revision: `72da8256a3c998a031897cf3a473058a9aee8bcc`; evidence applies to the committed
  tree containing this ledger and must be rebound after any runtime, dependency,
  test-infrastructure, or execution-config change;
- scope: CI matrix, package smoke, compatibility documentation, and a standard-library matrix
  consistency checker; production source, dependencies, Doris tests, Compose, and slow execution
  configuration are unchanged;
- Python 3.11.15 / Ray 2.58.0: full unit and public contract suite 309 passed with 85% aggregate
  coverage; three warnings are Ray deprecations from public contract execution;
- Python 3.12.12 / Ray 2.58.0: full unit and public contract suite 309 passed with 85% aggregate
  coverage; the same three Ray deprecation warnings were recorded;
- Python 3.11 final wheel smoke: base and Flight-extra installations succeeded in separate venvs;
  both `uv pip check` runs passed, and `ray_doris`, `adbc_driver_flightsql.dbapi`, and
  `adbc_driver_manager.dbapi` imported successfully from the built wheel;
- package: isolated wheel and sdist build succeeded after build dependency resolution; Twine checks
  passed for both artifacts; Python 3.12 wheel and sdist installs, dependency checks, and imports
  passed after the review fix added the missing `uv pip check` steps;
- static and policy checks: Ruff format/lint, strict mypy for 18 source files, pre-commit, workflow
  action pins, release-workflow policy, and the new nine-case matrix consistency suite passed;
- review fixes: the Flight smoke now requires exactly one wheel before constructing the direct
  reference; the CI indentation and missing-unit-job paths are independently tested; README and
  compatibility verification wording and source formatting are aligned;
- documentation: `tools/check_docs.py`, Sphinx strict, and spelling passed;
- required Doris IT, slow distributed IT, and benchmark: not run because production source,
  dependencies, protocol behavior, Doris tests, Compose, and execution configuration are unchanged;
  the final Python 3.12 / Ray 2.58.0 required IT result above remains reusable.

## Version 1.2 release preparation evidence

- worktree: `/Users/jiangxintong/GitHub/workspace/ray-doris-release-maturity`, branch
  `release-maturity`; base revision: `a1f1f60b740d28a4dd5625577e443c7fa3c86349`;
- scope: version metadata, current maturity wording, default release profile naming, workflow policy,
  and release unit tests; runtime behavior, dependency bounds, Doris tests, Compose, and distributed
  execution configuration are unchanged;
- Python 3.12.12 / Ray 2.58.0 / PyArrow 25.0.1 / PyMySQL 2.2.8: final full unit and public contract
  suite 318 passed with 85% aggregate coverage; the three warnings are Ray public-context
  deprecations; targeted release tests passed all 66 cases;
- static and policy checks: Ruff format/lint, strict mypy for 18 source files, pre-commit, workflow
  action pins, release-workflow policy, and `git diff --check` passed;
- documentation: `tools/check_docs.py`, Sphinx strict, spelling, and the separately executed external
  linkcheck passed;
- package: isolated version 1.2 wheel and sdist build, Twine checks, and SHA-256 manifest validation
  passed. Both distribution metadata records and embedded package versions match 1.2 and contain
  no Development Status classifier. Four isolated Python 3.12 wheel/sdist base/Flight installations
  passed dependency checks and imports outside the repository;
- logs: `/private/tmp/ray-doris-release-maturity.EFfzme`, including `unit-contract.xml`,
  `unit-contract.log`, `release-unit.log`, `static.log`, `docs.log`, `linkcheck.log`, `package.log`,
  `install-smoke.log`, and `SHA256SUMS`; package checks apply to the tested tree before this
  evidence-only ledger append;
- required Doris IT: not rerun locally; the existing final Python 3.12 / Ray 2.58.0 / Doris 4.0.6
  result above remains reusable because runtime behavior and infrastructure are unchanged;
- full distributed evidence: deferred under the standard release profile. Renaming that profile
  does not establish stable or enterprise distributed certification; the enterprise profile still
  requires a successful full manifest for the exact release candidate;
- release state: local preparation only. No 1.2 commit, push, tag, TestPyPI, PyPI, or GitHub Release
  has been created. Exact candidate SHA, workflow results, and published artifact hashes must be
  recorded after the separately approved external steps.

## Ray 2.59.0 compatibility validation

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| Doris 4.0.6 integration | Real MySQL, Flight SQL, types, permissions and Stream Load cases | Uncommitted `ray-259-compatibility` on base `4248a772a0012cad5e8b03ac6bc747c9971056fd`; Python 3.12.12, Ray 2.59.0, PyArrow 26.0.0 | Verify the new Ray minor against real database protocols | 30 passed in 64.89s. Logs `/tmp/ray-259-validation-20261013/doris-it/`; scoped cleanup completed. Later slow-fixture-only changes do not invalidate this result. |
| Slow full functional suite | Three Ray workers and three BEs, TLS/Flight, retries and post-send ambiguity | Ray 2.59.0 / Doris 4.0.6; 10,000 rows and 5-second stress; initial 2 GiB BE budget | Verify existing distributed semantics once, with no preceding core run | Initial attempts stopped during subnet/Ray startup and completed no cases. After startup repair: 3 passed, 6 failed in 20.08s. BE logs show RSS 1.79 GiB versus a 1.80 GiB process limit, causing `MEM_ALLOC_FAILED`. Fixture/workflow defaults now use 3 GiB; additional full verification pending authorization. Logs `/tmp/ray-259-validation-20261013/doris-slow-ready/`; cleanup completed. This is failed evidence, not a full pass or release artifact. |
| Approved 3 GiB/BE full follow-up | Same nine distributed cases, without a preceding core run | Base above plus cumulative diff SHA-256 `94a491a7276eda748f0b06b9fb267ed837a983077d890a55b870c7f1fd4e100b` excluding this ledger; Python 3.12.14, Ray 2.59.0, Doris 4.0.6, 10,000 rows, 5-second stress, 3 GiB/BE | Explicit user approval on 2026-10-13; replace failed allocator evidence after the BE budget change. Existing 30-case required IT is not repeated | 9 passed, 11 warnings in 97.72s; runner exit 0. Logs `/tmp/ray-259-validation-20261013/doris-slow-3g/` and `doris-slow-3g-run.log`; local `slow-result.json` emitted and exact project cleanup completed. Subsequent validator/test-only correction aligns the manifest budget to 3 GiB and does not invalidate runtime evidence. This uncommitted local result is not exact-release-commit evidence. |
| Manifest policy correction | Validate the actual 3 GiB manifest; reject legacy 2 GiB and other mismatched profiles | Only `tools/check_slow_result.py` and its unit fixtures changed after the full run | Static self-review found the former budget requirement had not been propagated; no runtime revalidation is needed | 67 release tests passed in 0.95s; `/tmp/ray-259-validation-20261013/doris-manifest-policy-3g.log`. Actual local manifest validation, changed-file Ruff format/lint and whitespace checks passed. |
| Approved additional Python baselines | Full unit/contract on Python 3.11 and 3.13, including real local-Ray Datasink contracts | Python 3.11.15 and 3.13.7 / Ray 2.59.0 / PyArrow 26.0.0; source unchanged; state and environment facts in `followup/python-final-results.json` | Close the explicitly approved Python/Ray validation gaps without repeating database IT | Each baseline passed all 321 cases, zero skipped, in 11.89s and 11.19s. Initial attempts had 318 passes and three Ray-setup errors because the local Unix socket path exceeded 103 bytes; shorter isolated paths resolved this environment issue. All original and final logs are preserved under `/tmp/ray-259-validation-20261013/followup/`, with final logs `ray-doris-py311-unit-contract-retry.log` and `ray-doris-py313-unit-contract-retry.log`. No Doris IT rerun. |

## Master development version validation

User-approved development identity `1.3.dev0` on 2026-10-13; source-state SHA-256 excluding this ledger: `0eb9014d10761750af78cd3018ee0893e5f56952c6c1632f85051803b931a37c`. Primary runtime: Python 3.12.12 / Ray 2.59.0. Full unit suite: 320 passed, zero skipped; Ruff format/lint, mypy, workflow/version policies and strict documentation/spelling passed. Wheel/sdist build, Twine checks and isolated non-editable installs passed, with exact development metadata, public imports, dependency checks and production source-byte comparisons. Logs, artifact hashes and XML: `/tmp/ray-259-development-versions-20261013/`.

This follow-up changes version metadata, release/CI validation and direct regressions, with no additional data-path or test-infrastructure change. Retain the recorded functional contracts and real database/cluster IT; no such IT was rerun. It does not certify an exact published release SHA or a remote Actions run. No commit, push or publication performed.

## V1 projection pushdown validation

Evidence recorded on 2026-10-21 for the uncommitted `projection-pushdown` worktree on
base `36efb8ff46c89efc5c16875bba3ab869c6534a70`. Production changes are limited to
`datasource.py` and `_compat.py`; the source fingerprints and complete logs are retained
under `/tmp/ray-doris-projection-validation/`. The approved implementation preserves the
installation window and enables automatic column pruning only on verified Ray 2.58.0/2.59.0.

| Suite | Runtime and scope | Result |
| --- | --- | --- |
| Primary unit and public contracts | Python 3.12.12 / Ray 2.59.0; complete suite | 352 passed, zero skipped. After the test recorder correction, the final 9-case projection contract passed again; repeated runs are not counted as additional coverage. |
| Compatibility | Ray 2.58.0 enabled; Ray 2.57.0 and 2.49.2 retain native behavior | 47 affected unit cases and 9 projection contracts passed on each target; the corrected recorder was exercised by all four Ray contract baselines. |
| Initial required IT | Doris 4.0.6 / Ray 2.59.0; checked-in 2 GiB BE limit | 27 passed, 15 failed in 137.96s. One new assertion used an iterable as an iterator; BE logs also showed 1.87 GiB RSS above a 1.80 GiB process limit, rejecting reads and existing writes. Retained in `doris/`; this is failed evidence. |
| Approved required IT recovery | Python 3.12.12 / Ray 2.59.0 / PyArrow 26.0.0 / PyMySQL 1.2.3 / ADBC 1.12.0 / Doris 4.0.6; corrected tests and temporary 3 GiB BE override | 42 passed, zero skipped, 214.14s. Includes actual MySQL/Flight execute-boundary SQL, cached branches, column order/nullability, filter dependencies, native empty projections, planning fallback and existing write regressions. Logs and source fingerprints: `doris-recovery/`. |
| Initial distributed core | Ray 2.59.0 / Doris 4.0.6; three workers and three 3 GiB BEs | 1 passed, 2 failed in 22.14s. Projected SQL, row counts and sums passed; node checks failed because the test recorder reported its actor node instead of the caller. Retained in `core/`. |
| Approved corrected core | Python 3.12.14 / fixed Ray 2.59.0 base / Doris 4.0.6; one head, three workers, one FE and three 3 GiB BEs | 3 passed, zero skipped, 21.59s. Actual projected MySQL and Flight SQL, expected row counts/sums and participation by all three workers passed. Logs and source fingerprints: `core-recovery/`. |

Both recovery suites ran once after explicit user approval and exited successfully. Compose
startup used `--no-build`; proxy variables were cleared for Docker operations. Per-run images
and the exact test project containers, networks and volumes were reclaimed. Shared base images
were retained. The core profile does not generate full slow-suite or release evidence.

Ruff lint/format, strict mypy, documentation contracts, Sphinx strict/spelling, shell syntax,
whitespace checks and cumulative static self-review passed. No remaining production-code
defect was identified. The checked-in required-IT BE default remains 2 GiB: successful local
evidence applies to the approved temporary 3 GiB profile, and the recorded 2 GiB memory
rejection remains a CI resource risk. No commit, push, PR, tag or release was performed.

Pre-push test-only import cleanup passed the same 56 affected unit/contract cases on each of
Ray 2.49.2, 2.57.0, 2.58.0 and 2.59.0. Production source, query execution, schema/row checks
and caller-node recording are unchanged. The lazy optional Flight import now reports missing
ADBC explicitly; the successful installed-ADBC path uses the same Cursor. The required IT and
core results above remain applicable without repeating those long suites.

## Predicate and projection merge validation

User-approved conflict resolution on 2026-10-21 merges projection base
`d795631536e1e2e5d7119ae48e9fff02b9db9ca0` into predicate head
`cd1d56e2eda8870cdcd684566148a72cb74aeda8`, without a commit or push. Both
datasource capabilities and both distributed test functions are retained. Synthetic
projection fixtures keep filtering in Ray; SQL-backed native contracts independently
exercise combined filtering, projection order, omitted dependency columns and empty
projections. Production SHA-256 is
`6774d754fbfaf7c898f0818c288549011ec974666dc1d7c7a422a2f85bef1cfd`.

| Suite | Coverage, code state and overlap | Result |
| --- | --- | --- |
| Focused diagnostics | Merged datasource, predicate units and both public read contracts; initial fixture corrections before final evidence | Final focused run: 27 passed in 18.39s. Initial diagnostics identified a native empty-projection representation assertion and a worker-side fixture patch; both were corrected without production logic changes. Sandbox Ray startup failure is retained separately. |
| Final unit and contracts | Python 3.12.12 / Ray 2.59.0 / Arrow 26.0.0; merged production and final tests | 375 passed, zero skipped, four upstream warnings, 25.32s. One complete primary run after focused corrections. |
| Required Doris IT | Exactly one complete suite on Doris 4.0.6 / Ray 2.59.0; required BE remains 2 GiB, confirmed as 2147483648 bytes | 29 passed, 20 failed, zero skipped, 391.20s. MySQL and Flight native SQL-recording cases passed; auto and later read/write cases failed with Doris runtime exceptions. BE logs show repeated `MEM_ALLOC_FAILED` and process memory above its 1.80 GiB limit. Combined real-protocol cases did not pass; no automatic retry or memory increase. |
| Distributed targeted | Exactly the two conflicting test functions, each on MySQL and Flight; three Ray workers, three BEs, Python 3.12.14 / Ray 2.59.0 / Doris 4.0.6; runtime-only BE override of 2 GiB for this request | Four failed in 11.25s with Doris read exceptions. Cluster logs confirm `MEM_ALLOC_FAILED` at the 1.80 GiB process limit; all three containers were inspected at 2147483648 bytes. No slow full run, release result or additional retry. |
| Static and documentation | Ruff lint/format, strict mypy, documentation checker, Sphinx strict/spelling, whitespace and cumulative self-review | Passed. Sphinx inventory downloads required host/proxy access; no documentation error remained. Six core datasource methods retain the corresponding predicate/projection parent ASTs. |
| Reused evidence | Unchanged SQL compiler/rendering/reader behavior and version gates; unaffected full slow scenarios and separate-feature compatibility checks | Retain earlier evidence with its recorded runtime and memory settings. Earlier 3 GiB results do not prove that the merged 2 GiB runs pass. |

Complete logs, JUnit XML, failure classification, source manifest, runtime versions,
AST comparison and actual diffs are under
`/tmp/ray-doris-predicate-merge-validation/`. Temporary configurations isolate network
addresses and service ports; Python source and tests match the worktree. Required
Compose and CI workflows are unchanged. The slow suite's checked-in 3 GiB default is
also unchanged; only this targeted invocation used `RAY_DORIS_BE_MEMORY_LIMIT=2g`.
Both precise Compose projects, their volumes/networks and custom images were cleaned;
the two pre-existing dangling images were retained. No tests were skipped, assertions
relaxed, infrastructure memory increased, commit created or remote branch pushed.
