# Security policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Report it privately through GitHub's
security advisory interface for this repository. Include affected versions, reproduction steps,
impact, and any suggested mitigation.

## Credential handling

A literal `password` remains in Ray's serialized datasource and task state for compatibility. The
enterprise-candidate MySQL profile instead uses `password_env`: only the environment-variable name
is serialized, and the driver and each worker resolve its value immediately before connecting.
Inject the same variable into every eligible Ray process, run only on a trusted Ray control plane
and object store, grant the Doris account only `SELECT` on required tables, and avoid placing
credentials in source code, logs, or issue reports.

The default MySQL, query-plan HTTP, and Flight URI schemes are unencrypted. Configure MySQL TLS
through `client_kwargs`, use `http_scheme="https"` with `http_ca_file` when a private CA protects
query planning, and use `flight_scheme="grpc+tls"` with certificate settings in `flight_options`.
Do not send production credentials over the default schemes outside a trusted private network.

The connector accepts one logical FE hostname. Doris and the deployment platform remain
responsible for FE membership, leader election, quorum, health checks, and load-balancer failover.
