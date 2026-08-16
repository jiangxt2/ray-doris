---
myst:
  html_meta:
    description: "Task-oriented guides for reading and writing Doris tables with Ray Data, selecting transports, tuning parallelism, mapping schemas, securing connections, and diagnosing failures."
---

(ray-doris-user-guides)=

# User guides

Start with the [Quickstart](../quickstart.md) if you haven't created a Dataset yet. These guides explain each production-facing read and write configuration boundary.

::::{grid} 1 2 2 2
:gutter: 1

:::{grid-item-card} Read data
:link: read-data
:link-type: doc

Select one internal table, project columns, apply a trusted filter, and choose planning fallback behavior.
:::

:::{grid-item-card} Configure transports
:link: configure-transports
:link-type: doc

Choose the production-candidate MySQL transport or experimental Flight SQL/automatic fallback and configure execution timeouts.
:::

:::{grid-item-card} Write data
:link: write-data
:link-type: doc

Load, upsert, and Merge-on-Write partial-update batches through the public Ray Datasink API.
:::

:::{grid-item-card} Tune parallelism
:link: tune-parallelism
:link-type: doc

Coordinate tablet grouping, task concurrency, output blocks, and batch size.
:::

:::{grid-item-card} Work with schemas
:link: work-with-schemas
:link-type: doc

Review the Doris-to-Arrow type mapping and fail-closed conversion rules.
:::

:::{grid-item-card} Secure connections
:link: secure-connections
:link-type: doc

Protect MySQL, query-plan HTTP, and Flight SQL traffic and keep credentials out of logs.
:::

:::{grid-item-card} Troubleshoot reads
:link: troubleshooting
:link-type: doc

Map public exceptions to configuration, planning, permission, schema, and worker failures.
:::
::::

```{toctree}
:maxdepth: 2

read-data
write-data
configure-transports
tune-parallelism
work-with-schemas
secure-connections
troubleshooting
```
