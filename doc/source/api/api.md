---
myst:
  html_meta:
    description: "Generated API reference for ray-doris read_doris, DorisDatasource, and all public configuration, planning, schema, and read exceptions."
---

(ray-doris-api)=

# API reference

This page documents the curated public package surface. Signatures and docstrings come from the installed source tree rather than duplicated handwritten declarations.

The connector uses Ray's documented Datasource extension interfaces without importing `ray.data._internal`. Ray classifies `ReadTask` as DeveloperAPI, so compatibility is limited to the tested Ray minor window. MySQL is the production-candidate transport; Flight SQL and `auto` are experimental.

For the enterprise-candidate MySQL profile, use `password_env` rather than a literal password,
strict MySQL TLS in `client_kwargs`, HTTPS with `http_ca_file`, an explicit
`query_plan_timeout`, and `on_query_plan_error="error"`. The environment credential is resolved
again in every driver request and worker attempt; its value isn't stored in the datasource or
ReadTask payload.

## Read a Doris table

(ray-doris-api-read-doris)=

```{autofunction} ray_doris.read_doris
```

## Construct a datasource

(ray-doris-api-datasource)=

```{autoclass} ray_doris.DorisDatasource
:members:
```

## Handle public exceptions

All connector exceptions derive from `DorisError`. Configuration errors also derive from `ValueError`, while authentication and permission errors derive from `DorisPlanningError`.

(ray-doris-api-error)=

```{autoexception} ray_doris.DorisError
```

(ray-doris-api-configuration-error)=

```{autoexception} ray_doris.DorisConfigurationError
```

(ray-doris-api-schema-error)=

```{autoexception} ray_doris.DorisSchemaError
```

(ray-doris-api-planning-error)=

```{autoexception} ray_doris.DorisPlanningError
```

(ray-doris-api-authentication-error)=

```{autoexception} ray_doris.DorisAuthenticationError
```

(ray-doris-api-permission-error)=

```{autoexception} ray_doris.DorisPermissionError
```

(ray-doris-api-read-error)=

```{autoexception} ray_doris.DorisReadError
```
