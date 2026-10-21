# Integration test ledger

## Native metadata capability discovery

User-approved code update on base `533f787f51b798ea3d8d1092ad1264c0f39329b8`.
Native selection now probes structured metadata and public reader capabilities,
without exact Doris/Ray patch whitelists. The package Ray window is unchanged.
Authentication, connectivity, malformed results, source errors and contradictory
metadata remain errors. Discovery does not replay a running native read.

| Suite | Scope, state and reason | Result |
| --- | --- | --- |
| Primary unit/public contracts | Python 3.12.12, Ray 2.59.0, Arrow 26.0.0; shared metadata, legacy reader adapter and DESCRIBE nullability fix | 614 passed, zero skipped, 31.07s; `describe-fix/unit-contract.xml` |
| Minimum Ray native contracts | Python 3.9.25, Ray 2.49.2, Arrow 21.0.0; legacy V1, projection, typed partitions, empty schemas and error boundaries | 36 passed, zero skipped, 5.56s; `ray249-native-final.xml` |
| Previous Ray minor native contracts | Python 3.12.12, Ray 2.58.0, Arrow 26.0.0; public V1/V2 reader capabilities and actual files | 62 passed, zero skipped, 7.87s; `ray258-native-final.xml` |
| Native Doris 4.0.6 | Existing 32 cases plus 6 secondary-interface, unknown-build and shared DESCRIBE cases | Final approved retry: 38 passed, zero skipped, 59.70s; `matrix-final/native-4.0.6.xml` |
| Native Doris 3.0.6.2 | Same complete native suite on the second cached Doris baseline | Final approved retry: 38 passed, zero skipped, 98.48s; `matrix-final/native-3.0.6.2.xml` |
| Required Doris 4.0.6 read/write regression | Shared planner changed since the merge; 49 existing protocol, projection, predicate and write cases | Final approved retry: 49 passed, zero skipped, 662.93s; `matrix-final/query-regression.xml` |

Evidence root: `/private/tmp/rd-native-capability-20261021/`. Final runtime/test
fingerprint after the nullability fix: `1fa4ec07b68df1bb0eb2ef851f10ad65f033c8dbec8e6f2f16240db71652e426`;
the manifest excludes this evidence-only Markdown ledger. Earlier 94-case
infrastructure results predate these changes and cannot certify the new fallback
interfaces. The new cases inject only known absence at the preferred interface;
alternate SQL and full data comparisons run against real Doris after authorization.
They do not establish compatibility with an untested older Doris release.

Diagnostics remain separate: the first primary run passed 601 cases and the
first Ray 2.58.0 subset passed 58. Ray 2.49.2 initially passed 28 and failed 5:
its Parquet reader references an uninitialized partition variable for a projected
read with `partitioning=None`. The legacy public-signature adapter now supplies
an explicit empty partition scheme, preserving projection without synthesizing
parent keys. Final suites above cover the changed code; no runtime error is
swallowed, no private Ray implementation is imported and no test is skipped.

Ruff format/lint, strict mypy, static PR precheck and all applicable hooks passed.
Wheel/sdist and Twine checks passed; all 23 Python modules in the wheel match
the source. Final documentation checks and static self-review are recorded in
the same evidence directory. Both cached Doris versions and all fixture base
images are available; the isolated test project has no existing containers.
The user approved exactly one 125-case final infrastructure run. The runner
stopped after 10 passes and 1 failure; 114 cases were not executed. Its tested
fingerprint was `67dc6122e6d42b4ae36b1f251400f1542a305d5e59f725611fe9a33eac908e9a`.
The real Catalog alternate SQL passed. The DESCRIBE alternate failed because
Doris emits `Yes`/`No` but the adapter passed those values to the strict
`YES`/`NO` information-schema parser unchanged. Both Doris 4.0.6 and 3.0.6 tagged
`IndexSchemaProcNode` implementations confirm the presentation difference.

The minimal fix uppercases only string nullability in the external DESCRIBE
adapter; the system-table parser remains strict. Tests now cover actual mixed
case, uppercase/lowercase and invalid non-string/unknown values. Focused tests
passed 205 cases, followed by the final 614-case suite above. The previous
605-case primary result is diagnostic evidence, not the fixed implementation's
final result. Ray file reader/compatibility/schema and contract files are
byte-identical, so their 36/62-case results remain applicable without repetition.
Ruff, mypy, hooks, static precheck, build and Twine passed; the rebuilt wheel's
23 Python modules match the fixed source. The public documentation is unchanged.

`matrix/cleanup.json` confirms no owned resources remain. Shared base image IDs
and the unique pre-existing dangling image IDs were verified preserved.
Execution and failure records are `it-execution-plan.json`, `it-attempt-result.json`
and `describe-fix/results.json`. The user subsequently approved exactly one complete 125-case retry followed by
a cumulative self-review. `it-retry-execution-plan.json` records the repaired
source and the new `matrix-final/` evidence directory. The final retry completed:
125 passed, zero failures/errors/skips, verified from the three JUnit files.
Actual alternate metadata SQL, full native/Doris data comparisons, original raw
partition identities and both workers' hard-pinned HDFS/S3 reads passed on both
Doris targets. The required suite also passed MySQL, Flight, auto, combined
predicate/projection branches, permissions, typed empty reads and existing writes.

The complete runtime/test manifest remains identical to the 614-case unit state.
Runner fingerprint `7395830de845d43165df88ca6d655e3633b11634c1417b46657ac6a62cc27a97`
was recomputed after execution, including the pinned Hadoop source manifest.
The immutable upstream JNI source blobs were rechecked. `matrix-final/cleanup.json`
confirms no owned containers, networks, volumes or custom image tags remain;
shared base IDs and the unique pre-existing dangling image IDs were preserved.
`it-final-result.json` records the verified totals, fingerprints and worker evidence.

Cumulative self-review covered the complete PR diff against master, including
all 42 tracked changes and 3 new files, plus affected surrounding code. No
remaining blocking code issue or required validation gap was identified for the
approved experimental scope. Wider Doris deployments and older-Ray HDFS/S3
clusters are not certified by capability detection or local-file contracts.
No commit, push or remote PR update is authorized by the test approval.

## Shared-storage and master merge validation

The user approved minimal conflict resolution for PR #13, merging master
`d9e4aae91c41e0b65a54499443f5f8e248814872` into native-read head
`3e193296810ffad2282eeb2265a98fa8ac3eb99f` without committing or pushing.
The merge retains projection/predicate hooks, external structured metadata and
single-query planning. Shared `build_read_sql` now preserves the catalog in both
bound MySQL and literal Flight forms. Both parents' evidence below is preserved
as history, not asserted to be a new run on this merged state.

| Suite | Scope and reason | Result |
| --- | --- | --- |
| Focused unit diagnostics | External qualified SQL, predicate binding, trusted percent literals, projection/source identity, metadata and reader cleanup | 102 passed, 2.19s; `focused-unit.log` |
| Final unit and public contracts | Both parents' full unit/contract suites plus external predicate/projection regression | 556 passed, zero skipped, 37 upstream warnings, 31.07s; `unit-contract-final.log` |
| Static and documentation | Ruff format/lint, strict mypy, docs contracts, strict Sphinx and spelling | Passed; `docs-contract.log`, `docs-strict.log`, `docs-spelling.log` |
| Real Doris infrastructure | Changed shared SQL rendering and reader/planner combination | Not run in this minimal conflict-resolution step; prior native 94-case and upstream protocol results are historical and do not certify the merged combination |

Evidence root: `/private/tmp/rd-native-master-merge-20261021/`. No Compose,
memory-budget or CI configuration is changed by this resolution. No infrastructure
suite, extra runtime baseline, commit or push has been started for this step.

This ledger records the infrastructure and exact code state used for RFC-candidate evidence.
Reuse requires unchanged covered behavior, configuration, dependencies and infrastructure;
any source change must have an explicit impact assessment and separate evidence for changed
branches. A prior fingerprint is never represented as a run of a different code state.

## Shared-storage native read development

### Automatic native selection

The user approved replacing the unreleased `read_mode` parameter with
`auto_native_read=False`. The default reads through Doris without native
discovery. Enabling the flag selects a compatible Hive Parquet source or retains
Doris reading only for a specific not-applicable decision. Real metadata,
permission, connection and data errors remain errors; no runtime source replay.
External catalogs now use the existing typed, bounded Doris readers with one
fully qualified table query and no internal tablet discovery.

| Suite | Scope, source state and reason | Overlap / result |
| --- | --- | --- |
| Full unit and public Ray contracts | Boolean API, removed read_mode, native decision/error boundaries, shared structured-column parsing, external SQL/planning/empty schemas, existing reader contracts; base 838c198 plus automatic-selection changes | Final source: 498 passed, zero skipped, 36 upstream warnings, 13.90s; `unit-contract-final.log` |
| Doris 4.0.6 automatic-selection IT | Spark/HMS/HDFS/S3, actual native and Doris paths, ORC and layout/limit fallback, UTC/name settings, worker reads and no replay | Final approved retry: 32 passed, zero skipped, 20 upstream warnings, 48.23s; `matrix-final/native-4.0.6.xml` |
| Doris 3.0.6.2 version IT | Same automatic source decisions and metadata/reader contracts | Final approved retry: 32 passed, zero skipped, 20 upstream warnings, 49.63s; `matrix-final/native-3.0.6.2.xml` |
| Existing Doris 4.0.6 read/write regression | Internal default path plus changed shared connection/error helpers | Final approved run: 30 passed, zero skipped, 5 upstream warnings, 58.09s; `matrix-final/query-regression.xml` |

Evidence root: `/private/tmp/rd-native-auto-20261021/`. The immutable verified
Hadoop 3.3.0 source context and cached base images may be reused; the runner
records current fingerprints and image IDs, builds each runtime once, starts
Compose with --no-build and verifies scoped cleanup. No benchmark, new CPU
architecture or unrelated runtime matrix is added. Original 22-case results
below remain historical evidence, not automatic-selection verification.

The first attempt is recorded in `matrix-driver.log`, `matrix/results.json`,
`matrix/native-4.0.6.xml` and `matrix/cleanup.json`. The fix shares
`information_schema.columns` SQL and parsing between native discovery and the
Doris external-table planner; internal DESCRIBE remains unchanged. Focused tests
passed (162 cases), followed by the 498-case final unit/contract suite above.
The earlier 488-case full run predates this fix and is not final-source evidence.
The user explicitly approved the final 94-case rerun on 2026-10-21: two 32-case
version suites and the existing 30-case regression. The retry uses the fixed
source fingerprint `9615240b24b34ab1a6abd0623306ad3e4f0fd2b806a31b6860d201d798b4a3e3`.
Logs are recorded at `matrix-final/` with driver output in `matrix-final-driver.log`.
The first attempt cannot validate this changed parser; cached bases and Hadoop
source are reused. This approval covers one complete retry, not further repeats.

Final static and artifact checks passed: Ruff lint/format, strict mypy, all seven
pre-commit hooks, the new untracked test's applicable hooks, the local static PR
precheck without duplicate tests, documentation contracts, strict Sphinx,
spelling, wheel/sdist builds and Twine. Wheel imports and the default/off API were
verified with optional HMS dependencies blocked; all wheel Python modules match
the current source. The generated API contains the boolean signature and a
working native-guide anchor. The final real matrix completed successfully:
94 passed, zero failures/errors/skips, verified from the three JUnit files.
`matrix-final/cleanup.json` confirms no owned containers, networks, volumes or
custom image tags remain. The runtime/test/infrastructure fingerprint matches the
final unit/contract state and remains unchanged after the run. The two pre-existing
dangling image IDs were preserved; no global cleanup was used.

Final self-review corrected stale external-table scope statements in README,
the read guide and FAQ. Those documentation-only edits do not invalidate the
real suite; documentation contracts, strict Sphinx and spelling passed again.
The native path and actual Doris fallback are verified on both target versions,
including ORC, unsupported layouts, limits, column-name/time-zone settings,
Ray V2 guards, empty schemas, permissions and no runtime replay. Wider providers,
versions, authorization systems, architectures and performance are not certified.

### Public read mode naming (historical)

2026-10-21: the approved public modes are `doris` (default) and `native`.
The `query` name is rejected before constructing either reader; no alias is kept.
Both modes accept table identifiers, not complete SQL statements. The full
unit/contract suite passed once: 448 passed, zero skipped, 36 upstream warnings,
15.11s, including default/explicit `doris` routing and early `query` rejection.

Evidence: `/private/tmp/rd-native-mode-rename-20261021/`. AST comparison confirms
that the approved mode constants and error text are the only executable API
changes. Native dispatch and all reader, metadata, integration fixture and
infrastructure code remain equivalent. Reuse both 22-case native Doris results
and the 30-case Doris read/write result; no infrastructure suite is repeated.
Documentation contracts, strict Sphinx and spelling passed. Wheel/sdist and
Twine checks passed; the final wheel verifies the `doris` signature/default,
both Doris routing forms, `query` rejection and optional HMS isolation.
The public API reference, README, compatibility and native guides use the
same terminology, and the PR English body and Chinese review are synchronized.
Ruff, strict mypy, all seven pre-commit hooks and pre-push static layers passed.
The existing source-policy test now constructs the same forbidden module path
from its components so the local pre-push scanner cannot mistake its assertion
string for an import. Its assertion is unchanged; the final API file's eight
tests passed in 1.89s. This does not add eight cases to the 448-case suite count.

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
