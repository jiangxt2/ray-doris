from typing import Iterable
from unittest.mock import Mock

import pyarrow as pa
import pytest
from ray.data.block import BlockMetadata

from ray_doris import _compat
from ray_doris._errors import DorisConfigurationError


def metadata() -> BlockMetadata:
    return BlockMetadata(num_rows=None, size_bytes=None, exec_stats=None, input_files=None)


def read_fn() -> Iterable[pa.Table]:
    return [pa.table({"id": [1]})]


@pytest.mark.parametrize(
    "version",
    ["2.49.2", "2.57.0", "2.58.0", "2.58.99", "2.58.0+vendor.1"],
)
def test_supported_ray_version_window(monkeypatch, version) -> None:
    monkeypatch.setattr(_compat, "_RAY_VERSION", version)
    _compat.ensure_supported_ray_version()


@pytest.mark.parametrize(
    "version",
    [
        "2.49.1",
        "2.49.2rc1",
        "2.58.0.dev0",
        "2.58.0.post1",
        "2.58.0garbage",
        "2.59.0",
        "3.0.0",
        "unknown",
    ],
)
def test_unsupported_ray_version_fails_fast(monkeypatch, version) -> None:
    monkeypatch.setattr(_compat, "_RAY_VERSION", version)
    with pytest.raises(DorisConfigurationError, match=r">=2\.49\.2,<2\.59"):
        _compat.ensure_supported_ray_version()


def test_installed_read_task_matches_supported_contract() -> None:
    expected_metadata = metadata()
    expected_schema = pa.schema([("id", pa.int64())])

    task = _compat.make_read_task(read_fn, expected_metadata, expected_schema, None)

    assert task.metadata is expected_metadata
    assert task.schema == expected_schema
    blocks = list(task.read_fn())
    assert len(blocks) == 1
    assert isinstance(blocks[0], pa.Table)
    assert blocks[0].equals(pa.table({"id": [1]}))


def test_make_read_task_supports_ray_249_signature_fixture(monkeypatch) -> None:
    constructor = Mock()

    def read_task(read_fn, metadata, schema=None):
        constructor(read_fn=read_fn, metadata=metadata, schema=schema)
        return "task"

    monkeypatch.setattr(_compat, "ReadTask", read_task)
    assert _compat.make_read_task(read_fn, metadata(), pa.schema([]), None) == "task"
    constructor.assert_called_once()


def test_make_read_task_supports_current_signature_fixture(monkeypatch) -> None:
    constructor = Mock()

    def read_task(read_fn, metadata, schema=None, per_task_row_limit=None):
        constructor(
            read_fn=read_fn,
            metadata=metadata,
            schema=schema,
            per_task_row_limit=per_task_row_limit,
        )
        return "task"

    monkeypatch.setattr(_compat, "ReadTask", read_task)
    assert _compat.make_read_task(read_fn, metadata(), pa.schema([]), 10) == "task"
    assert constructor.call_args.kwargs["per_task_row_limit"] == 10


def test_make_read_task_rejects_signature_without_schema(monkeypatch) -> None:
    def read_task(read_fn, metadata):
        return None

    monkeypatch.setattr(_compat, "ReadTask", read_task)
    with pytest.raises(DorisConfigurationError, match=r"ReadTask.*>=2\.49\.2,<2\.59"):
        _compat.make_read_task(read_fn, metadata(), pa.schema([]), None)


def test_make_read_task_rejects_non_null_limit_on_old_ray(monkeypatch) -> None:
    def read_task(read_fn, metadata, schema=None):
        return None

    monkeypatch.setattr(_compat, "ReadTask", read_task)
    with pytest.raises(DorisConfigurationError, match="per_task_row_limit"):
        _compat.make_read_task(read_fn, metadata(), pa.schema([]), 1)


def test_make_read_task_does_not_retry_internal_type_error(monkeypatch) -> None:
    calls = 0

    def read_task(read_fn, metadata, schema=None):
        nonlocal calls
        calls += 1
        raise TypeError("constructor bug")

    monkeypatch.setattr(_compat, "ReadTask", read_task)
    with pytest.raises(TypeError, match="constructor bug"):
        _compat.make_read_task(read_fn, metadata(), pa.schema([]), None)
    assert calls == 1
