from __future__ import annotations

import os

import pyarrow as pa
import pytest

os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")

import ray  # noqa: E402
from ray.data.expressions import col  # noqa: E402

from ray_doris._compat import supports_projection_pushdown  # noqa: E402
from tests.projection_helpers import RecordingDorisDatasource, SQLRecorder  # noqa: E402

pytestmark = pytest.mark.contract


@pytest.fixture(scope="module", autouse=True)
def ray_runtime():
    if ray.is_initialized():
        yield
        return
    ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        yield
    finally:
        ray.shutdown()


@pytest.fixture
def table():
    return pa.Table.from_pydict(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [None, "b", "c", "d", "e"],
            "unused": [0] * 5,
            "wide": ["x" * 32] * 5,
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                ("value", pa.string()),
                ("unused", pa.int64()),
                ("wide", pa.string()),
            ]
        ),
    )


@pytest.fixture
def recorder():
    actor = SQLRecorder.remote()
    try:
        yield actor
    finally:
        ray.kill(actor)


def source(table, recorder, columns=None, projection_enabled=True):
    return RecordingDorisDatasource(
        table="db.records",
        host="fe",
        fixture_table=table,
        recorder=recorder,
        columns=columns,
        projection_enabled=projection_enabled,
    )


@pytest.mark.parametrize("explicit", [None, ["unused", "value", "id"]])
@pytest.mark.parametrize("cached", [False, True])
def test_dataset_selection_prunes_sql_and_preserves_order_and_branches(
    table, recorder, explicit, cached
):
    datasource = source(table, recorder, explicit)
    if cached:
        datasource.get_read_tasks(1)
    dataset = ray.data.read_datasource(datasource, override_num_blocks=2)
    selected = dataset.select_columns(["value", "id"])
    materialized = selected.materialize()
    assert materialized.schema().names == ["value", "id"]
    blocks = list(materialized.iter_batches(batch_format="pyarrow"))
    actual = pa.concat_tables(blocks)
    assert actual.to_pylist() == table.select(["value", "id"]).to_pylist()
    assert actual.schema.types == table.select(["value", "id"]).schema.types
    if supports_projection_pushdown():
        assert not actual.schema.field("id").nullable
    else:
        native = ray.data.from_arrow(table).select_columns(["value", "id"]).materialize()
        native_blocks = list(native.iter_batches(batch_format="pyarrow"))
        assert actual.schema == pa.concat_tables(native_blocks).schema
    records = ray.get(recorder.read.remote())
    sql = [query for stage, query, _ in records if stage == "synthetic"]
    expected = (
        "SELECT `value`, `id`"
        if supports_projection_pushdown()
        else ("SELECT *" if explicit is None else "SELECT `unused`, `value`, `id`")
    )
    assert sql and all(query.startswith(expected + " FROM") for query in sql)
    assert selected.select_columns(["id"]).take_all() == table.select(["id"]).to_pylist()
    assert dataset.select_columns(["unused"]).take_all() == table.select(["unused"]).to_pylist()
    assert dataset.take_all() == (table if explicit is None else table.select(explicit)).to_pylist()
    assert datasource.config.columns == (None if explicit is None else tuple(explicit))


def test_residual_filter_dependency_and_rename_are_preserved(table, recorder):
    dataset = ray.data.read_datasource(source(table, recorder))
    result = dataset.filter(lambda row: row["unused"] == 0).select_columns(["value", "id"])
    assert result.rename_columns({"id": "key"}).take_all() == [
        {"value": row["value"], "key": row["id"]} for row in table.to_pylist()
    ]
    queries = [sql for stage, sql, _ in ray.get(recorder.read.remote()) if stage == "synthetic"]
    assert queries and all(query.startswith("SELECT * FROM") for query in queries)


@pytest.mark.parametrize("empty_input", [False, True])
def test_empty_projection_matches_native_materialization_rows_and_schema(
    table, recorder, empty_input
):
    if empty_input:
        table = table.slice(0, 0)
    if ray.__version__.startswith("2.49."):
        # This Ray generation rejects empty selections at the public API.
        for dataset in (
            ray.data.from_arrow(table),
            ray.data.read_datasource(source(table, recorder)),
        ):
            with pytest.raises(ValueError, match="at least one column"):
                dataset.select_columns([])
        return
    # Use the same public Datasource read as the native Ray baseline. Empty
    # materialized reads can have unknown schema even without pushdown, unlike
    # from_arrow(), whose input blocks already carry their schema.
    native = (
        ray.data.read_datasource(source(table, recorder, projection_enabled=False))
        .select_columns([])
        .materialize()
    )
    dataset = ray.data.read_datasource(source(table, recorder)).select_columns([]).materialize()
    assert dataset.schema() == native.schema()
    assert dataset.count() == native.count()
    assert dataset.count() == table.num_rows
    assert dataset.take_all() == native.take_all()
    assert sum(block.num_rows for block in dataset.iter_batches(batch_format="pyarrow")) == sum(
        block.num_rows for block in native.iter_batches(batch_format="pyarrow")
    )


def test_unknown_and_duplicate_columns_keep_native_errors(table, recorder):
    for dataset in (ray.data.from_arrow(table), ray.data.read_datasource(source(table, recorder))):
        with pytest.raises((ValueError, KeyError, ray.exceptions.RayTaskError), match="missing"):
            dataset.select_columns(["missing"]).take_all()
        with pytest.raises(ValueError):
            dataset.select_columns(["id", "id"])


def test_expression_filter_retains_dependency_but_prunes_unneeded_data(table, recorder):
    dataset = ray.data.read_datasource(source(table, recorder))
    if supports_projection_pushdown():
        dataset = dataset.select_columns(["value", "id", "unused"]).filter(expr=col("unused") == 0)
    else:
        dataset = dataset.filter(lambda row: row["unused"] == 0)
    rows = dataset.select_columns(["value", "id"]).take_all()
    assert rows == table.select(["value", "id"]).to_pylist()
    queries = [sql for stage, sql, _ in ray.get(recorder.read.remote()) if stage == "synthetic"]
    assert queries
    if supports_projection_pushdown():
        assert all("`unused`" in sql and "`wide`" not in sql and "*" not in sql for sql in queries)
    else:
        assert all(sql.startswith("SELECT * FROM") for sql in queries)
