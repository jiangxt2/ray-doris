---
myst:
  html_meta:
    description: "Secure ray-doris credentials and protect its MySQL, HTTPS query-plan, and Flight SQL network paths on a trusted Ray cluster."
---

(ray-doris-secure-connections)=

# Secure connections

The default MySQL, query-plan HTTP, and Flight SQL schemes don't encrypt traffic. Use them only on a trusted private network. Protect each protocol separately for production credentials.

## Understand the credential boundary

Ray serializes datasource configuration into worker task state. `repr()` and connector logs redact passwords and option values, but the real values remain in serialized state so workers can connect to Doris.

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
    password="...",
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
    user="ray_reader",
    password="...",
)
```

The client refuses authenticated POST redirects. Configure the final endpoint directly so a redirect can't change the request method or expose the `Authorization` header.

A TLS validation failure raises {ref}`DorisConfigurationError <ray-doris-api-configuration-error>` and never becomes a single-task planning fallback.

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
