from __future__ import annotations

import os
import pickle
from typing import Any, Iterable, Mapping, Optional

import pyarrow as pa
import pytest

os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")

import ray  # noqa: E402
from ray.data.datasource import Datasink, WriteResult  # noqa: E402

from ray_doris import (  # noqa: E402
    DorisConnection,
    DorisDatasink,
    DorisTable,
    DorisWriteOptions,
)
from ray_doris._compat import datasink_compatibility  # noqa: E402

pytestmark = pytest.mark.contract


class _RecordingSink(Datasink[Mapping[str, Any]]):
    """Small public Datasink used to assert Ray's write result envelope."""

    def __init__(self, min_rows: Optional[int] = None) -> None:
        self.starts: list[Optional[pa.Schema]] = []
        self.bundle_rows: list[int] = []
        self.completed: Optional[WriteResult[Mapping[str, Any]]] = None
        self._min_rows = min_rows

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return self._min_rows

    def on_write_start(self, schema: Optional[pa.Schema] = None) -> None:
        self.starts.append(schema)

    def write(self, blocks: Iterable[Any], ctx: Any) -> Mapping[str, Any]:
        del ctx
        rows = 0
        for block in blocks:
            assert isinstance(block, pa.Table)
            rows += block.num_rows
        self.bundle_rows.append(rows)
        return {
            "status": "success",
            "request_statuses": ("Success",),
            "batches": 1,
            "attempted_rows": rows,
            "loaded_rows": rows,
            "filtered_rows": 0,
            "uploaded_bytes": rows * 10,
        }

    def on_write_complete(self, write_result: WriteResult[Mapping[str, Any]]) -> None:
        self.completed = write_result


@pytest.fixture(scope="module", autouse=True)
def ray_runtime() -> Iterable[None]:
    if ray.is_initialized():
        yield
        return
    ray.init(address="local", include_dashboard=False, num_cpus=2)
    try:
        yield
    finally:
        ray.shutdown()


def test_doris_datasink_public_shape_is_serializable_and_has_batch_target() -> None:
    sink = DorisDatasink(
        DorisConnection(host="fe.example", password_env="DORIS_PASSWORD"),
        DorisTable("analytics", "events"),
        DorisWriteOptions(operation="partial_update"),
    )
    restored = pickle.loads(pickle.dumps(sink))
    assert restored.get_name() == "Apache Doris Stream Load"
    assert restored.min_rows_per_write == DorisWriteOptions().batch_rows
    assert "DORIS_PASSWORD" not in repr(restored.connection)


def test_public_write_datasink_preserves_single_task_summary_and_ray_accounting() -> None:
    sink = _RecordingSink(min_rows=2)
    dataset = ray.data.from_arrow(pa.table({"id": pa.array([1, 2, 3, 4, 5])}))
    dataset.write_datasink(sink, ray_remote_args={"max_retries": 0})
    compatibility = datasink_compatibility()
    assert len(sink.starts) == 1
    assert (sink.starts[0] is not None) == compatibility.on_write_start_has_schema
    assert sink.completed is not None
    assert sink.completed.num_rows == 5
    assert sink.completed.size_bytes >= 0
    assert len(sink.completed.write_returns) == 1
    assert sink.completed.write_returns[0]["attempted_rows"] == 5
    assert sink.completed.write_returns[0]["batches"] == 1


def test_public_write_datasink_empty_dataset_skips_non_file_start_on_new_ray() -> None:
    sink = _RecordingSink(min_rows=2)
    empty = ray.data.range(0)
    empty.write_datasink(sink, ray_remote_args={"max_retries": 0})
    compatibility = datasink_compatibility()
    if compatibility.on_write_start_on_empty_dataset:
        assert sink.starts == [None]
    else:
        assert sink.starts == []
    assert sink.completed is not None
    assert sink.completed.write_returns == []
