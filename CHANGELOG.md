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
- Add environment-referenced credentials that resolve independently on the driver and workers
  without serializing the resolved value.
- Add a private-CA option and independent timeout for query-plan HTTPS, plus fixed redacted MySQL
  TLS setup errors.
- Move distributed worker-retry and backend-failure evidence to the minimum-privilege MySQL path.
- Bind releases to a successful full slow-suite manifest from the exact release commit and workflow
  run while keeping FE cluster HA as a deployment responsibility.

## 0.1.0a1

- Add tablet-aware Apache Doris reads through Ray Data's documented V1 Datasource extension API.
- Add bounded MySQL streaming and optional Arrow Flight SQL transport.
- Add explicit schema conversion, deterministic planning, and controlled fallback behavior.
- Add Ray compatibility, real Doris integration, package, and release verification workflows.
