---
myst:
  html_meta:
    description: "Secure ray-doris credentials and protect its MySQL, HTTPS query-plan, and Flight SQL network paths on a trusted Ray cluster."
---

(ray-doris-secure-connections)=

# Secure connections

The default MySQL, query-plan HTTP, Stream Load HTTP, and Flight SQL schemes don't encrypt traffic. Use them only on a trusted private network. Protect each protocol separately for production credentials.

TLS configuration doesn't change transport maturity: MySQL is the production-candidate path, while Flight SQL and `auto` remain experimental.

## Understand the credential boundary

Ray serializes datasource configuration into worker task state. A literal `password` is redacted
from `repr()` and connector logs but remains in that serialized state for compatibility.

For the enterprise-candidate MySQL profile, pass the name of an environment variable instead:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris.example.com",
    user="ray_reader",
    password_env="DORIS_PASSWORD",
)
```

Only the environment-variable name is serialized. The driver resolves its value before each
`DESCRIBE` or query-plan request, and a worker resolves it again before every split connection
attempt. Inject the same variable into the driver and every Ray worker, including replacement
workers. A missing variable fails before that process opens a network connection; an empty value
preserves Doris's empty-password behavior. Don't set both a non-empty literal `password` and
`password_env`.

Run `ray-doris` only on a trusted Ray cluster. Inject credentials at runtime, restrict access to Ray logs and object storage, and don't put secrets in source code, documentation, issue reports, or persistent Dataset references.

## Grant minimum Doris access

The reader account needs access to the frontend MySQL and HTTP ports and `SELECT_PRIV` on the target internal-catalog table. Flight reads additionally require the frontend Flight SQL port and network access to the backend Flight endpoints advertised by Doris.

`ray-doris` doesn't call administrator-only `SHOW TABLETS` as a fallback. The `_query_plan` request performs the table authorization check.

## Configure MySQL TLS

Pass PyMySQL TLS settings through `client_kwargs`:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris.example.com",
    user="ray_reader",
    password_env="DORIS_PASSWORD",
    client_kwargs={
        "ssl": {
            "ca": "/etc/doris-tls/ca.pem",
            "check_hostname": True,
        },
        "read_timeout": 300,
        "write_timeout": 30,
    },
)
```

The connector manages host, port, user, password, database, charset, cursor class, and connect timeout. It rejects attempts to replace those fields through `client_kwargs`.

## Protect query planning with HTTPS

Set `http_scheme="https"` and point `host` and `http_port` at a certificate-validated frontend endpoint or reverse proxy:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris.example.com",
    http_port=8443,
    http_scheme="https",
    http_ca_file="/etc/doris-tls/ca.pem",
    user="ray_reader",
    password_env="DORIS_PASSWORD",
    query_plan_timeout=30,
)
```

The client refuses authenticated POST redirects. Configure the final endpoint directly so a redirect can't change the request method or expose the `Authorization` header.

`http_ca_file` loads a private CA with Python's default TLS context. Certificate and hostname
verification remain enabled; there is no trust-all or hostname-bypass option. Leave it unset to use
the process default trust store. It is valid only with `http_scheme="https"`.

A TLS validation failure raises {ref}`DorisConfigurationError <ray-doris-api-configuration-error>` and never becomes a single-task planning fallback.

## Protect Stream Load writes

Write metadata discovery and Stream Load can use separate TLS settings on the same immutable
`DorisConnection`:

```python
from ray_doris import DorisConnection

connection = DorisConnection(
    host="doris-fe.example.com",
    username="ray_writer",
    password_env="DORIS_PASSWORD",
    http_port=8443,
    http_secure=True,
    http_ca_file="/etc/doris-tls/ca.pem",
    mysql_ca_file="/etc/doris-tls/ca.pem",
    redirect_hosts=("doris-be.example.com",),
    redirect_ports=(8040,),
    redirect_policy="public",
)
```

`http_ca_file` applies to the HTTPS Stream Load request and `mysql_ca_file` enables PyMySQL server
certificate verification for `SHOW CREATE TABLE` and `DESCRIBE`. Both paths keep hostname
verification enabled. A redirect must match the configured host and port allowlist and cannot
downgrade an HTTPS write to HTTP. Stream Load disables ambient HTTP proxy settings because a proxy
could observe or replay a request body.

## Define the FE availability boundary

Use one logical hostname for the query-plan and MySQL endpoints. `ray-doris` validates that
hostname and reopens connections for each planning or split attempt, but it doesn't discover FE
members or manage leader election, quorum, health checks, or endpoint failover. Provide those
properties with Doris and an external load balancer, and test that deployment independently.

## Configure Flight TLS

Set `flight_scheme="grpc+tls"` and pass the Arrow Database Connectivity (ADBC) certificate options required by your endpoint:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris.example.com",
    transport="flight",
    flight_port=8070,
    flight_scheme="grpc+tls",
    flight_options={
        "<driver-specific TLS root certificate option>": "<PEM certificate>",
        "adbc.flight.sql.rpc.timeout_seconds.query": "300",
    },
)
```

Replace the TLS option placeholder with the certificate option supported by your installed ADBC Flight SQL driver and deployment. `flight_options` can't replace the URI, username, password, or connector-managed connect timeout.

The Doris 4.0.6 distributed suite verifies plaintext Flight only inside an isolated Compose network because that release advertises plaintext `grpc` endpoints. It doesn't claim a positive native `grpc+tls` Doris server test. Validate a deployment-provided Flight TLS endpoint separately.
