# Contributing to ray-doris

Thank you for improving the Doris integration for Ray Data. Open an issue before a large API or
architecture change so that compatibility and test expectations can be agreed on first.

The project supports both Ray Data reads and writes. Reads use `read_doris()` and
`DorisDatasource`; writes use `write_doris()` or `DorisDatasink` through Ray's public
`Dataset.write_datasink()` API. Stream Load writes support `load`, `upsert`, and Merge-on-Write
`partial_update` only. They use bounded Parquet or line-delimited JSON requests and fail closed
when the Doris outcome is unknown; automatic Ray task retry is deliberately disabled for this
profile. The project does not provide exactly-once, whole-dataset atomicity, DDL, overwrite,
truncate, Stream Load 2PC, or arbitrary SQL writes.

## Local checks

Use Python 3.12 for the main development environment:

```bash
uv venv --python 3.12
uv pip install -e ".[dev,flight]"
pre-commit install --hook-type pre-commit
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/mypy
.venv/bin/python -m pytest tests/unit --cov=ray_doris --cov-report=term-missing
.venv/bin/python -m pytest tests/contract -v
.venv/bin/python -m build
.venv/bin/twine check dist/*
```

Changes to SQL, planning, schema conversion, either reader, or the write path must also pass the
Docker-backed Doris suite:

```bash
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  -u http_proxy -u https_proxy -u all_proxy \
  docker compose -f tests/integration/docker-compose.yml build fe
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  -u http_proxy -u https_proxy -u all_proxy \
  docker compose -f tests/integration/docker-compose.yml build be
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  -u http_proxy -u https_proxy -u all_proxy \
  docker compose -f tests/integration/docker-compose.yml up -d --no-build
.venv/bin/python -m pytest tests/integration -v
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  -u http_proxy -u https_proxy -u all_proxy \
  docker compose -f tests/integration/docker-compose.yml logs --no-color \
  > /tmp/ray-doris-it.log
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  -u http_proxy -u https_proxy -u all_proxy \
  docker compose -f tests/integration/docker-compose.yml down -v --rmi local
```

The same isolated suite covers reads and writes, including Stream Load metadata discovery,
Duplicate/Unique/Aggregate table models, Merge-on-Write partial updates, permissions, redirects,
response classification, readback, and Ray's public Datasink lifecycle. Keep the command output and
Compose logs; update `tests/it-ledger.md` with the source SHA, dependency versions, image digest,
topology, and result.

Do not skip infrastructure tests to produce a green result. Diagnose the service, retain the
Compose logs, and fix the root cause. Never use broad Docker cleanup commands; operate only on the
`ray-doris-it` Compose project.

Changes to tablet scheduling, Flight SQL, Ray task options, retry behavior, TLS, or distributed
execution must also pass the opt-in slow suite:

```bash
tests/slow_integration/run.sh
```

For changes limited to tablet task count and worker distribution, use the targeted profile instead
of the worker/BE failure scenarios:

```bash
RAY_DORIS_SLOW_PROFILE=core tests/slow_integration/run.sh
```

The suite requires a Docker host with at least 16 GiB of available memory. Its default topology is
one Ray head, three Ray workers, one Doris 4.0.6 FE, three Doris 4.0.6 BEs, and a production-style
TLS and Flight ingress that exposes per-BE traffic counters. The functional profile loads 10,000
rows, executes a bounded Stream Load write across the three Ray workers with Doris readback, and
runs repeated Flight reads for at least five seconds. The script automatically uses a
local `ray-cluster:2.55.1` image when present and otherwise uses the fixed public
`rayproject/ray:2.55.1-py312-cpu` base. Select a different trusted local image with
`RAY_BASE_IMAGE`. Run high-pressure profiles only on a dedicated host, for example:

```bash
RAY_DORIS_ROW_COUNT=1000000 \
RAY_DORIS_STRESS_SECONDS=300 \
RAY_DORIS_BE_MEMORY_LIMIT=4g \
tests/slow_integration/run.sh
```

Size the dedicated host for the requested container limits; the functional profile keeps each BE
at 2 GiB by default.

The slow workflow supports manual, reusable, and scheduled full runs. It is not a required check in
the default CI workflow. A successful full run emits `slow-result.json`; release verification
requires that artifact from a successful workflow run on the exact release SHA. The `core` profile
is diagnostic only and never produces release evidence.

## Documentation checks

Create an isolated documentation environment so Sphinx and runtime development dependencies do
not drift together:

```bash
uv venv .docs-venv --python 3.12
uv pip install --python .docs-venv/bin/python -r doc/requirements-doc.lock.txt
uv pip install --python .docs-venv/bin/python --no-build-isolation --no-deps -e .
.docs-venv/bin/python tools/check_docs.py
make -C doc strict SPHINXBUILD=../.docs-venv/bin/sphinx-build
make -C doc spelling SPHINXBUILD=../.docs-venv/bin/sphinx-build
```

Run `make -C doc linkcheck SPHINXBUILD=../.docs-venv/bin/sphinx-build` separately because external
sites can fail transiently. New documentation pages use MyST Markdown under `doc/source/` and
follow the Ray documentation organization and writing style. Keep examples source-aligned and do
not describe an untested configuration or extension point as supported behavior.

## Compatibility

Code must remain compatible with the declared `ray[data]>=2.49.2,<2.59` range. Python 3.9 is an
Alpha legacy compatibility target because it no longer receives upstream security fixes; it isn't
a stable production baseline. Do not import modules below `ray.data._internal`. Add a unit test for
every independently verifiable behavior, including error paths and cleanup. Unsupported Doris types
must fail closed. Datasink callback signatures and timing differ between Ray 2.49.2–2.52.x and
Ray >=2.53.0; keep that adaptation in `_compat.py`, and test empty Dataset behavior for both
generations. `Datasink.min_rows_per_write` is a batching target, not a strict physical request limit.

Source, comments, documentation, commit messages, and public GitHub content are written in English.
Every commit must include:

```text
Signed-off-by: jiangxt2 <jiangxt2@vip.qq.com>
```

## Maintainers

Maintainer access is based on sustained, constructive contributions across implementation,
testing, documentation, review, and user support. Existing maintainers nominate contributors in a
public repository discussion, document the expected responsibilities, and use repository and PyPI
roles that can be transferred without changing the package name. No release credential should be
held by only one person once a second maintainer is available.
