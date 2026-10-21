from __future__ import annotations

from functools import partial

import pytest
import ray
from tests.projection_helpers import RecordingDorisDatasource, SQLRecorder

from ray_doris import read_doris

pytestmark = pytest.mark.integration


@pytest.fixture
def recorder():
    actor = SQLRecorder.remote()
    try:
        yield actor
    finally:
        ray.kill(actor)


@pytest.mark.parametrize("transport", ["mysql", "flight"])
@pytest.mark.parametrize("cached", [False, True])
def test_actual_worker_sql_prunes_columns_and_preserves_cached_branches(
    doris_config, recorder, transport, cached
):
    source = RecordingDorisDatasource(
        recorder=recorder,
        **doris_config.reader_kwargs(transport=transport, filter="id >= 2", batch_size=1),
    )
    if cached:
        source.get_read_tasks(2)
    dataset = ray.data.read_datasource(source, override_num_blocks=4, concurrency=2)
    selected = dataset.select_columns(["category", "id"]).materialize()
    assert selected.schema().names == ["category", "id"]
    rows = sorted(selected.take_all(), key=lambda row: row["id"])
    assert [row["id"] for row in rows] == [2, 3, 4]
    assert rows[0]["category"] is None
    assert not next(iter(selected.iter_batches(batch_format="pyarrow"))).schema.field("id").nullable
    records = ray.get(recorder.read.remote())
    worker_sql = [sql for stage, sql, _ in records if stage == "worker"]
    assert worker_sql and all(sql.startswith("SELECT `category`, `id` FROM") for sql in worker_sql)
    assert all("WHERE id >= 2" in sql for sql in worker_sql)
    assert any(
        stage == "planner" and sql.startswith("SELECT `category`, `id` FROM")
        for stage, sql, _ in records
    )
    assert sorted(row["id"] for row in dataset.select_columns(["id"]).take_all()) == [2, 3, 4]
    assert source.config.columns is None


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_public_reader_preserves_filter_column_not_in_output(
    doris_config, recorder, transport, monkeypatch
):
    monkeypatch.setattr(
        "ray_doris._api.DorisDatasource", partial(RecordingDorisDatasource, recorder=recorder)
    )
    selected = read_doris(
        **doris_config.reader_kwargs(
            transport=transport, columns=["id", "category"], filter="id >= 3"
        )
    ).select_columns(["category"])
    assert sorted(row["category"] for row in selected.take_all()) == ["alpha", "beta"]
    queries = [sql for stage, sql, _ in ray.get(recorder.read.remote()) if stage == "worker"]
    assert queries and all(sql.startswith("SELECT `category` FROM") for sql in queries)
    assert all("WHERE id >= 3" in sql for sql in queries)


@pytest.mark.parametrize("transport", ["mysql", "flight"])
@pytest.mark.parametrize("empty", [False, True])
def test_real_empty_projection_matches_native_rows_and_schema(
    doris_config, recorder, transport, empty
):
    source = RecordingDorisDatasource(
        recorder=recorder,
        **doris_config.reader_kwargs(
            transport=transport,
            columns=["id"],
            filter="1 = 0" if empty else "id >= 2",
            batch_size=1,
        ),
    )
    native = ray.data.read_datasource(
        RecordingDorisDatasource(
            recorder=recorder,
            projection_enabled=False,
            **doris_config.reader_kwargs(
                transport=transport,
                columns=["id"],
                filter="1 = 0" if empty else "id >= 2",
                batch_size=1,
            ),
        )
    )
    expected = native.select_columns([]).materialize()
    actual = ray.data.read_datasource(source).select_columns([]).materialize()
    assert actual.count() == expected.count()
    assert actual.count() == (0 if empty else 3)
    assert actual.schema() == expected.schema()
    assert actual.take_all() == expected.take_all()


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_projection_preserves_single_task_fallback(doris_config, recorder, transport):
    predicate = (
        f"id IN (SELECT id FROM `{doris_config.database}`.`{doris_config.table}` WHERE id >= 3)"
    )
    source = RecordingDorisDatasource(
        recorder=recorder,
        **doris_config.reader_kwargs(transport=transport, filter=predicate),
    )
    rows = ray.data.read_datasource(source).select_columns(["id"]).take_all()
    assert sorted(row["id"] for row in rows) == [3, 4]
    queries = [sql for stage, sql, _ in ray.get(recorder.read.remote()) if stage == "worker"]
    assert len(queries) == 1
    assert queries[0].startswith("SELECT `id` FROM")
    assert " TABLET(" not in queries[0]
