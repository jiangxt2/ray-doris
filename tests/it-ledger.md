# Integration test ledger

This ledger records the infrastructure and exact code state used for RFC-candidate evidence.
Reuse requires unchanged covered behavior, configuration, dependencies and infrastructure;
any source change must have an explicit impact assessment and separate evidence for changed
branches. A prior fingerprint is never represented as a run of a different code state.

## Shared-storage native read development

### Confirmed review fixes and verification matrix

2026-10-21: the user requested fixes after approving a minimal runtime reproduction.
Python 3.12.12 / Ray 2.59.0 / Arrow 26.0.0 reproduced NULL filling for case-only
physical fields, V1 NULL partitions and reordered projections, and V2 loss of a
physical `path` column. Evidence: `/private/tmp/rd-repro-8jempz71/results.json` and
`reproduction.log`. The UTC timestamp control was correct; general timezone parity
is not added to this feature. The earlier statement that no defect remained is
superseded by these results and the corrective verification below.

| Suite | Code/config state and reason | Overlap and execution state |
| --- | --- | --- |
| Targeted native contracts and runner lifecycle units | Footer checks, native schema restoration, V2 guards, actual SIGTERM handling and cleanup verification | Initial reader contracts: 38 passed; runner lifecycle: 12 passed. Final full unit/contract suite: 446 passed, zero skipped, 13.28s |
| Doris 4.0.6 native final IT | Changed reader/schema bridge and fixtures invalidate M's native result; add exact read-node placement, source-schema configuration, raw partition collision and missing/empty registered partitions | 22 passed, 20 upstream warnings, 34.53s; one complete run |
| Doris 3.0.6.2 native version IT | The same changed native contracts require the approved second-version check | 22 passed, 20 upstream warnings, 30.91s; one complete run in the same matrix |
| Existing 4.0.6 query/write regression | The default API, planner, query readers, write code and legacy compatibility functions are unchanged by these fixes | Reuse M's 30-pass result; invoke the runner with `--skip-query-regression` to avoid duplicate long tests |

The final matrix must retain source/config fingerprint, per-version XML,
`read-nodes-hdfs.json`, `read-nodes-s3.json` and `cleanup.json`. Failed or interrupted
states must be diagnosed and recorded before any necessary retry. No benchmark,
additional Doris version or complete concurrent-DDL/cache-refresh matrix is added.

Corrective matrix: `/private/tmp/rd-native-fixes-20261021/matrix-final/`, fingerprint
`9a5156440053da5164495434931b6d6b50ce3bfc29e762bc0cc3f099e79d7821`.
Both native suites passed and runner exit was zero. `cleanup.json` confirms no owned
containers, networks, volumes or custom image tags remain. Node JSON records refer to
the shared Ray cluster; each version's XML separately records its successful read-node tests.

Final self-review found that the new normalization map lost schema for zero-row Parquet
files. The V1/V2 reproduction is in `empty-file-check.log`. The existing footer read now
also reads `num_rows` from the same public Arrow `ParquetFile` object and constructs a
typed empty Dataset when every file has zero rows. This uses the same constructor and
`schema_arrow` property as Arrow 26's `read_schema`; physical schema checks and the
nonempty reader path are unchanged. Eight new empty-file contracts and the final full
446-test suite verify that branch. The successful native profiles have nonempty files or
no files, so their existing evidence is reused rather than rerunning either long suite.

Final reviewed fingerprint: `bd2ba898ffa6bddfab31ad5b9976ce3ba3f8f2226e77bb3737c8dcd4af024ac4`.
Evidence and the impact assessment are in `/private/tmp/rd-native-fixes-20261021/`:
`reviewed-state.json`, `unit-contract-empty-fix-final.log`, final strict/spelling logs,
wheel/sdist build and Twine logs, and a wheel default-import check with optional HMS
imports blocked. Ruff lint/format, strict mypy, documentation contracts and `git diff --check`
passed. The first documentation attempt failed only sandbox DNS/inventory access;
the unchanged check passed through the configured proxy. No benchmark ran.

### Submission import cleanup

The required pre-push checker identified function-local imports in five files.
Move ordinary imports to module scope and retain the API's native-only lazy import
through the existing `import_module` pattern. An AST comparison confirms that the
other four modules change only import placement; API dispatch arguments, validation
and the query path remain equivalent. This is not a reader, metadata or fixture change,
so the two 22-case native results and 30-case query/write result remain applicable.

Runtime/config fingerprint: `ecd06fb89364fb3c4eef5a517a6a8f4a33b3c25e2fb4fe401764ac4caebd23a0`.
The full unit/contract suite passed again: 446 passed, zero skipped, 36 upstream
warnings, 15.27s (`unit-contract-precommit-final-host.log`). The sandbox attempt had
391 passed and 55 setup errors because Ray's process enumeration was denied before
initialization; no assertion failure occurred. The successful host run verifies all cases.
All pre-push static layers pass (`pre-push-static-final.log`); its duplicate pytest
layer is omitted in favor of the full suite. Wheel/sdist build without isolation and
Twine passed, and the final wheel passes default import/configuration with HMS SDK
imports blocked and the native module still unloaded. An isolated build attempt was
blocked while downloading Hatchling by sandbox DNS; the installed backend satisfies
the declared requirements. Logs are retained in the same evidence directory.

The user approved the minimal hook configuration fix after `check-yaml` rejected
Compose's valid `!reset` tags. Only the native Compose overlay uses the hook's
syntax-only `--unsafe` mode; every other YAML file retains safe loading. All seven
pre-commit hooks pass (`pre-commit-all-final.log`). Compose 5.6.0 configuration
validation confirms eight image-only services and removal of inherited FE/BE build
definitions (`compose-config-validation.json`), without starting containers.
This configuration-only change leaves runtime code, fixtures and all functional
test evidence unchanged; no unit or infrastructure suite is repeated.

The following 2026-10-20 entries retain the original development history. The corrected
results and explicit reuse assessment above are the current evidence.

Approved on 2026-10-20: implement Issue #12 and verify multiple cached Doris images. Base
`36efb8ff46c89efc5c16875bba3ab869c6534a70`; feature branch `shared-storage-native-read`.
All rows below are planned until an actual result and cumulative source/config fingerprint is recorded.

| Suite | Coverage | Environment | Reason and overlap | Result |
| --- | --- | --- | --- | --- |
| Native unit/public reader contracts | Routing, metadata limits, rejection/cleanup, real extensionless Parquet, schema/projection/empty data | Python 3.12 / Ray 2.59.0 | Small local checks during implementation; final full suite once stable | Pending |
| Native 4.0.6 final IT | Spark fixtures, real HMS/HDFS/S3, registered partitions, metadata-only FE access, multiple Ray workers, negative cases | Cached FE `ad353c79bf58`, BE `a851eab4facf`, Ray `b60863feb40c`; full IDs checked by runner | New path has no reusable prior IT evidence; no preceding complete sub-suite | Pending |
| Native 3.0.6.2 version IT | Same fixture, Doris structured schema/partition APIs and native-vs-SQL data equivalence | Cached FE `d6de824d7dbe`, BE `fe68ff98e015`; reuse native runtime and source fixture | Explicitly approved additional Doris baseline; exclude unrelated slow stress profiles | Pending |
| Existing 4.0.6 integration regression | Default query and existing MySQL/Flight/write/permissions behavior | Existing isolated integration services, stable feature code | Entry-point changes invalidate reuse of old API regression results | Pending |

Each long run records code/config fingerprint, exact images, command, logs and result. An identical
successful state is never rerun. Failed/incomplete runs are retained and repaired before retry;
the reason must explain why the previous evidence cannot be reused. No benchmark is authorized.
Preparation commands use exact `ray-doris-it` labels and names; shared base images are preserved.

Development evidence: 355 unit tests passed; eight real local Ray/Parquet contracts passed after
repairing the V2 extension-filter boundary. Initial local contract execution was blocked by the
macOS sandbox; the approved unsandboxed retry exposed six real extensionless-reader failures,
which were fixed by explicit V2 rejection and public V1 opt-in coverage. Logs:
`/private/tmp/rd-native/unit-final.log`, `local-contract-retry.log`, and
`local-contract-reader-fix.log` in the same directory.

Matrix preparation attempt A built the custom images and started FE/BE/HMS/HDFS/MinIO/Ray head.
No functional case ran: the readiness helper omitted port 9030 and tried the MySQL default 3306.
The exact readiness process was terminated (exit 143); runner cleanup completed. The helper now
uses 9030. Logs: `/private/tmp/rd-native/matrix-20261020-a/` and `matrix-driver-a.log`.
A retry is necessary because this attempt provides no functional evidence; added native negative
fixtures also change the test fingerprint. The next run will record the corrected state.

Attempt B reached Spark publication and pytest, but all 16 cases errored in the shared fixture
before connector assertions: the cached ARM64 Hive image bundles an x86-64 `libhdfs.so`.
Binary inspection confirmed the mismatch. Logs: `/private/tmp/rd-native/matrix-20261020-b/`.
The repair compiles upstream Hadoop 3.3.0 JNI sources for ARM64, verifies library loading in the
runtime image, and records each verified source blob in the evidence. HDFS/S3 remain real services.
Additional scalar/INT96 fixtures and explicit HMS S3A configuration are part of the corrected
state; the previous setup-only errors cannot establish functional compatibility.

Attempt C successfully compiled and loaded the ARM64 JNI library, then all 18 cases stopped in
fixture setup because `docker compose exec` bypasses the image entrypoint and did not inherit its
computed CLASSPATH. The runner now invokes the same entrypoint wrapper explicitly for driver
commands; readiness includes a real HDFS metadata call. No data assertion passed in attempt C.
Logs: `/private/tmp/rd-native/matrix-20261020-c/`. The retry reuses the verified JNI source/cache,
preserves the Ray cluster across Doris version changes, and uses a valid managed insert-only
transactional Hive fixture for the rejection case.

Attempt D passed real HDFS readiness and copied Spark files into S3, but stopped during HMS
fixture registration before connector assertions. The old HMS context manager masked the raw
fixture error. Error sanitization now wraps only SDK requests and connection setup, preserving
non-HMS exceptions (including Doris permission errors); fixture registration records a sanitized
operation-specific failure. Logs: `/private/tmp/rd-native/matrix-20261020-d/`. Runtime dependency
installation is now cached separately from source copies; source changes do not redownload the
same Arrow/SDK dependencies. The failed fixture provides no reusable functional evidence.

Attempt E identified the exact setup failure: HMS `create_table(s3_plain)` timed out. The cached
Hive S3A template sets credentials/endpoint but omits path-style access, causing requests to use
unresolvable bucket-prefixed service names. The scoped HMS entrypoint now enables path-style
access and HTTP for the isolated MinIO endpoint, with bounded connection timeouts. Logs:
`/private/tmp/rd-native/matrix-20261020-e/`. HDFS table registration completed; connector data
assertions still did not run. Retry F is required after this configuration correction.

Attempt F did not reach HMS readiness: the cached image invokes `start.sh` through Bash because
that file is not executable. The new wrapper now mirrors that invocation instead of executing
the file directly. The runner checks for services that have already exited before entering long
readiness polling. Logs: `/private/tmp/rd-native/matrix-20261020-f/`. No functional case ran;
the exact waiting process is terminated to trigger scoped cleanup before retry.

Attempt G completed HMS/S3 registration and entered the first actual reader call. The driver
terminated with SIGSEGV (exit 139) when constructing PyArrow HDFS after `ray.init()`, matching
Ray issue #36415 and the official JVM signal-chaining guidance. The runtime now preloads the
same JDK's `libjsig.so` before Python starts on all Ray nodes and the driver, preserving Ray's
failure handler. Logs: `/private/tmp/rd-native/matrix-20261020-g/`. No data equivalence assertion
completed; this failed result cannot replace the corrected functional run.

Attempt H fixed the JVM conflict and reached connector metadata discovery. FE logs showed Hive's
URI canonicalization turning `hms` into a Docker FQDN containing the default network underscore,
which Java rejects. The native Compose network now has a valid explicit DNS name with hyphens.
Logs: `/private/tmp/rd-native/matrix-20261020-h/`. The read failed before row/schema assertions;
retry is required after correcting this fixture network configuration.

Attempt I reached schema validation and correctly rejected a Doris/HMS type mismatch. Static
inspection of both target branches confirmed that `information_schema.columns.COLUMN_TYPE`
uses the MySQL display form `tinyint(1)` for BOOLEAN and `tinyint(4)` for real TINYINT. Native
metadata now normalizes only that verified representation before reusing the existing Doris
mapper; the legacy DESCRIBE path is unchanged. A dedicated regression test distinguishes both.
Logs: `/private/tmp/rd-native/matrix-20261020-i/`. Latest full unit/public contract state before
this display-normalization fix passed 372 cases, zero skipped, in 14.34s; current targeted tests
and the corrected integration run are required for the new state.

Attempt J: 9 native 4.0.6 cases passed, including HDFS/S3 plain, registered escaped partitions,
INT96, full scalar schema/content comparison, projection and empty results. The next negative
case failed because HMS had created the supposedly missing location during registration; the
fixture now explicitly removes that isolated directory after registration. This changes test
setup, not reader behavior. Logs: `/private/tmp/rd-native/matrix-20261020-j/`. A corrected final
run is necessary for the negative boundary and subsequent cases; the partial pass is retained.

Attempt K passed 15 native 4.0.6 cases. The denied-catalog case failed only its public error-class
assertion: Nereids wraps the catalog denial as MySQL 1105 instead of exposing 5087/1221. Native
metadata now recognizes that exact denied-catalog wording only during catalog discovery; other
1105 errors remain planning failures. Unit tests distinguish denial from unknown-catalog errors.
The read already failed without fallback; this repair improves its error classification. Logs:
`/private/tmp/rd-native/matrix-20261020-k/`. The remaining distributed/missing-file cases and
3.0.6.2 baseline still require a corrected complete run.

Attempt L passed 17 native 4.0.6 cases, including catalog denial, storage denial and two-worker
execution. The final fault test received the correct FileNotFoundError for the deleted file,
but its English substring assertion did not match a path-only message. The assertion now checks
the exact missing test path while retaining the public exception-type requirement and SQL-replay
guard. Static review also aligned BIGINT partition-type case handling with metadata validation;
real local contracts now cover both spellings. Logs: `/private/tmp/rd-native/matrix-20261020-l/`.

## Shared-storage native final functional matrix

Run M completed both approved versions and the existing regression suite. Cumulative source/config
fingerprint: `e93a5f4c5e88aaebf8efe0c74e48ce8d49d0223ca21173da4280977ab64d3ae5`.
Evidence: `/private/tmp/rd-native/matrix-20261020-m/` (`base-images.json`, `hadoop-source.json`,
`source-fingerprint.txt`, `results.json`, per-version XML, `query-regression.xml`, command logs).

| Suite | Result | Covered state |
| --- | --- | --- |
| Doris 4.0.6 native | 18 passed, 6 warnings, 32.12s | Real HDFS/S3, Spark Parquet including INT96, full scalar schema, escaped registered partitions, projection/empty tables, limits, unsupported/missing locations, source/catalog denial, two workers, missing-file failure without SQL replay |
| Existing 4.0.6 query/write regression | 30 passed, 5 warnings, 54.76s | Default API path, MySQL/Flight types, permissions, tablet planning and Stream Load |
| Doris 3.0.6.2 native | 18 passed, 6 warnings, 34.00s | The same native semantics and independent Doris SQL comparison on the second cached version |

The runner exited zero and removed its exact services, network, newly built image tags and recorded
owned volumes. Base images and unrelated images/volumes were preserved. Warnings are upstream
Ray context/projection and pytz deprecations. This is functional evidence, not a benchmark or release.

Post-matrix static review adds pure rejection guards for non-Hive/non-Parquet Spark providers and
HMS port zero, plus partition-only projection contracts. The tested standard Hive profiles (no
provider override, explicit port 9083), data reader, metadata queries and infrastructure behavior are
unchanged. These added branches use targeted unit/public-reader contracts; the successful long
matrix and unchanged query-path result are reused instead of repeating their service coverage.
The source-fetch helper now uses standard proxy environment variables; the already verified JNI
source manifest and binary are unchanged. Latest static/unit/document/package results follow below.

Final reviewed source/config fingerprint:
`07008c13940c034044f0c4806fb35e9e6cf0b9efa5f1e1d8a9849fd7c11fffcc`.
Full Python 3.12 / Ray 2.59.0 unit and public contract suite: **394 passed, zero skipped**, 24
upstream warnings, 12.17s. This includes stale Doris/HMS partition-scope rejection, unsupported
catalog settings, and the post-review provider/port/projection guards. Log:
`/private/tmp/rd-native/unit-contract-complete-final.log`.

Ruff lint, full format check (63 files), strict mypy (21 source files), `tools/check_docs.py`,
`git diff --check`, Sphinx strict build, spelling, isolated-environment source wheel/sdist build
using the cached declared Hatchling backend, and Twine checks passed. Build uses `--no-isolation`
in the dedicated feature virtualenv; it is not a separate hermetic release certification. Logs:
`docs-final-strict.log`, `docs-final-spelling.log`, `package-final.log`, `package-final-check.log`
under `/private/tmp/rd-native/`. External linkcheck was not rerun; the feature remains local,
uncommitted and unpublished. No benchmark or additional Python/CPU architecture matrix ran.

The built wheel was also imported directly with optional `pymetastore`/`thrift` imports blocked;
default datasource construction and the public NativeReadOptions export passed. Log:
`/private/tmp/rd-native/wheel-base-import.log`. Final static self-review covered the complete
cumulative source changes, format eligibility, typed metadata/partition association, permissions,
credential boundaries, close/error paths and reader delegation; no unresolved first-profile defect
remains. UTC-only timestamp fixture coverage and the deployment limits remain explicitly documented.

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
