---
myst:
  html_meta:
    description: "ray-doris dependency bounds, tested Python and Ray combinations, Doris 4.0.6 integration coverage, Flight requirements, and compatibility limits."
---

(ray-doris-compatibility)=

# Compatibility

The package dependency accepts `ray[data]>=2.49.2,<2.57`. Ray marks `ReadTask` as DeveloperAPI, so each new Ray minor must pass the compatibility matrix before the upper bound advances.

## Review the tested matrix

Continuous integration covers these combinations:

| Python | Ray | Verification |
| --- | --- | --- |
| 3.9 | 2.49.2 | Alpha legacy compatibility only; unit and Ray signature tests |
| 3.10 | 2.55.1 | Unit and Ray signature compatibility tests |
| 3.12 | 2.56.1 | Unit tests and required Doris 4.0.6 integration tests |

The optional distributed suite uses Python 3.12, Ray 2.55.1, and Doris 4.0.6. It runs one Ray head with no scheduling CPUs, three one-CPU Ray workers, one Doris frontend, three Doris backends, and a TLS and Flight ingress.

## Understand the Doris target

Doris 4.0.6 is the fixed real-infrastructure target for required and distributed integration tests. Those tests verify these behaviors:

- Internal-catalog table reads through MySQL and explicit Flight SQL.
- Column projection, trusted filters, empty results, and tablet planning.
- `BOOLEAN`, decimal, `LARGEINT`, JSON string, date, and microsecond timestamp conversion.
- Minimum table read privilege and distinct authentication or permission failures.
- Multiple Ray worker and Doris backend participation.
- Ray worker retry and Doris backend failure with replicated tablets.
- Native MySQL TLS and certificate-validated HTTPS through the test ingress.

The suite doesn't verify every Doris release, storage model, deployment proxy, authentication provider, or Flight TLS endpoint.

## Review Python and Flight limits

The core package requires Python 3.9 or newer. Python 3.9 reached end of life on October 31, 2025, so it is retained only as an Alpha legacy compatibility target. It isn't part of a stable or production profile. A future stable release will require a Python line that still receives upstream security fixes at its release date.

The Flight extra is conditional on Python 3.10 or newer because the supported Arrow Database Connectivity (ADBC) Flight SQL dependency doesn't install on Python 3.9. Python 3.9 can use the MySQL transport. Explicit Flight and TLS-required automatic Flight fail early when the dependency isn't available.

## Review the Ray API boundary

`ray-doris` uses Ray's documented Datasource extension interfaces and doesn't import `ray.data._internal` modules. Ray documents `ReadTask` as DeveloperAPI, not a minor-version-stable public API.

The compatibility layer requires the `ReadTask` constructor to accept a schema, and the package dependency starts at Ray 2.49.2. It passes `per_task_row_limit` only when the installed constructor supports that argument.

Datasource construction also checks the installed Ray release before any Doris network request.
It accepts final releases in `>=2.49.2,<2.57` and local rebuild suffixes such as
`2.56.1+vendor.1`. Release candidates, development builds, and post-release builds aren't
supported.

Advanced keyword support in `ray.data.read_datasource()` belongs to the installed Ray version. The convenience function exposes the cross-version arguments validated by this project: `concurrency`, `override_num_blocks`, and `ray_remote_args`.

## Review protocol coverage

MySQL is the production-candidate transport. Flight SQL and `auto` remain experimental because Doris documents Flight SQL as experimental and worker-local automatic selection can differ across heterogeneous environments.

The required integration suite uses the default frontend HTTP query-plan endpoint. The distributed suite verifies certificate-validated HTTPS through HAProxy because Doris 4.0.6 native frontend HTTPS has a Jetty WebSocket startup regression in this test topology.

Doris 4.0.6 advertises plaintext Flight `grpc` endpoints in the distributed suite. Flight stays on an isolated Compose network. The test doesn't establish a positive `grpc+tls` Doris server compatibility claim.
