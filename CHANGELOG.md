# Changelog

All notable changes to this project are documented in this file.

## Unreleased

- Bound Ray support to tested 2.49.2 through 2.56.x DeveloperAPI signatures.
- Balance tablet splits, avoid draining active MySQL results on early consumer termination, and
  align MySQL and Flight result-schema checks.
- Reject non-serializable worker options on the driver and redact filters and server details from
  representations, logs, and public errors.
- Mark Python 3.9 as Alpha legacy compatibility and Flight SQL and automatic transport selection as
  experimental.
- Validate release ancestry against the `master` default branch.

## 0.1.0a1

- Add tablet-aware Apache Doris reads through Ray Data's documented V1 Datasource extension API.
- Add bounded MySQL streaming and optional Arrow Flight SQL transport.
- Add explicit schema conversion, deterministic planning, and controlled fallback behavior.
- Add Ray compatibility, real Doris integration, package, and release verification workflows.
