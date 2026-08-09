---
myst:
  html_meta:
    description: "Set up ray-doris development and documentation environments, run static and real Doris gates, and preserve public Ray API and fail-closed schema contracts."
---

(ray-doris-contributing)=

# Contributing

Read the repository `CONTRIBUTING.md` before changing runtime behavior. Open an issue before a large API or architecture change so contributors can agree on the compatibility and infrastructure gates first.

## Set up the development environment

Use Python 3.12 for the main development environment:

```bash
uv venv --python 3.12
uv pip install -e ".[dev,flight]"
pre-commit install --hook-type pre-commit
```

Run the unit and package gates:

```bash
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/mypy
.venv/bin/python -m pytest tests/unit --cov=ray_doris --cov-report=term-missing
.venv/bin/python -m build
.venv/bin/twine check dist/*
```

Runtime changes to SQL, planning, schemas, or readers also require the Doris 4.0.6 integration suite. Distributed scheduling, Flight, retry, TLS, and failover changes require `tests/slow_integration/run.sh`. See `CONTRIBUTING.md` for resource and cleanup constraints.

## Set up the documentation environment

Keep documentation dependencies separate from the runtime development environment:

```bash
uv venv .docs-venv --python 3.12
uv pip install --python .docs-venv/bin/python -r doc/requirements-doc.lock.txt
uv pip install --python .docs-venv/bin/python --no-build-isolation --no-deps -e .
```

The no-dependency editable installation is intentional. The lock file supplies every documentation, runtime, and editable-build dependency, while Sphinx autodoc imports the real public package so a mock can't hide a broken first-party import or stale API target. Regenerate the lock file after changing project dependencies.

## Validate documentation

Run the source-aligned contract check and a clean Sphinx build:

```bash
.docs-venv/bin/python tools/check_docs.py
make -C doc strict SPHINXBUILD=../.docs-venv/bin/sphinx-build
make -C doc spelling SPHINXBUILD=../.docs-venv/bin/sphinx-build
```

The contract check verifies the Ray-style navigation order, MyST page descriptions, documented public exports, importable API targets, and Python example syntax.

Run link checking separately because external sites can fail transiently:

```bash
make -C doc linkcheck SPHINXBUILD=../.docs-venv/bin/sphinx-build
```

## Write documentation

Add new pages as MyST Markdown under `doc/source/`. Give every page an `html_meta` description, use sentence-case headings, write in active voice and present tense, and address the reader as “you.”

Use Sphinx cross-references for API symbols and `literalinclude` for maintained source examples. When an example requires Doris, make it syntactically verifiable and point to the real integration test that proves the behavior. Don't claim a capability from a configuration field, protocol skeleton, or workflow alone.

## Preserve connector boundaries

Keep new runtime code on public Ray APIs. Don't import `ray.data._internal`. Unsupported Doris types must fail closed, and access or TLS failures must not become permissive fallback paths.

Source, comments, documentation, commit messages, and public GitHub content use English. Every commit requires the repository's fixed `Signed-off-by` trailer.
