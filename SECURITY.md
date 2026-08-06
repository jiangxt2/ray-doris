# Security policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Report it privately through GitHub's
security advisory interface for this repository. Include affected versions, reproduction steps,
impact, and any suggested mitigation.

## Credential handling

`ray-doris` sends credentials to Ray workers as part of datasource configuration. Run it only on a
trusted Ray cluster and network. Inject secrets at runtime, grant the Doris account only `SELECT`
on required tables, and avoid placing credentials in source code, logs, or issue reports.

The default MySQL, query-plan HTTP, and Flight URI schemes are unencrypted. Configure MySQL TLS
through `client_kwargs`, use `http_scheme="https"` with a trusted TLS endpoint for query planning,
and use `flight_scheme="grpc+tls"` with certificate settings in `flight_options`. Do not send
production credentials over the default schemes outside a trusted private network.
