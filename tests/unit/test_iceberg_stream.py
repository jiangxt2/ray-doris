from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pytest
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from ray_doris._errors import DorisPermissionError, DorisReadError, _UnsafeIcebergFallbackError
from ray_doris._iceberg_stream import _stream_iceberg_or_doris, stream_iceberg_with_fallback
from ray_doris._models import DorisReadConfig
from ray_doris._sql import parse_table


@pytest.fixture
def context():
    config = DorisReadConfig.from_options(
        host="fe", user="reader", catalog="lake", table=parse_table("db.t")
    )
    return replace(config, snapshot_id=123), pa.schema([("id", pa.int64())])


def native_blocks(*blocks, error=None):
    yield from blocks
    if error is not None:
        raise error


def identity_ok():
    pass


@pytest.mark.parametrize("failure", ["catalog", "native"])
def test_failure_before_first_block_reads_pinned_doris(context, monkeypatch, caplog, failure):
    config, schema = context
    dataset = Mock()
    dataset.iter_batches.return_value = native_blocks(error=DorisPermissionError("secret"))
    fallback = Mock(return_value=iter([pa.table({"id": [7]})]))
    monkeypatch.setattr("ray_doris._iceberg_stream.read_split", fallback)
    check = Mock(side_effect=OSError("secret")) if failure == "catalog" else identity_ok
    blocks = _stream_iceberg_or_doris(dataset, config, schema, check)
    assert [block.to_pylist() for block in blocks] == [[{"id": 7}]]
    assert fallback.call_count == 1
    assert fallback.call_args.args[0].snapshot_id == 123
    assert "secret" not in caplog.text
    if failure == "catalog":
        dataset.iter_batches.assert_not_called()


def test_failure_after_first_block_does_not_replay(context, monkeypatch):
    config, schema = context
    dataset = Mock()
    dataset.iter_batches.return_value = native_blocks(
        pa.table({"id": [1]}), error=OSError("secret")
    )
    fallback = Mock()
    monkeypatch.setattr("ray_doris._iceberg_stream.read_split", fallback)
    blocks = _stream_iceberg_or_doris(dataset, config, schema, identity_ok)
    assert next(blocks).to_pylist() == [{"id": 1}]
    with pytest.raises(DorisReadError, match="disable auto_native_read") as error:
        next(blocks)
    assert "secret" not in str(error.value)
    fallback.assert_not_called()


def test_doris_fallback_failure_is_not_retried(context, monkeypatch):
    config, schema = context
    dataset = Mock()
    dataset.iter_batches.return_value = native_blocks(error=OSError("native"))
    fallback = Mock(side_effect=DorisPermissionError("Doris denied"))
    monkeypatch.setattr("ray_doris._iceberg_stream.read_split", fallback)
    with pytest.raises(DorisPermissionError, match="Doris denied"):
        list(_stream_iceberg_or_doris(dataset, config, schema, identity_ok))
    fallback.assert_called_once()


def test_identity_conflict_fails_before_native_or_doris(context, monkeypatch):
    config, schema = context
    dataset = Mock()
    fallback = Mock()
    monkeypatch.setattr("ray_doris._iceberg_stream.read_split", fallback)
    check = Mock(side_effect=_UnsafeIcebergFallbackError("identity changed"))
    with pytest.raises(_UnsafeIcebergFallbackError, match="identity changed"):
        list(_stream_iceberg_or_doris(dataset, config, schema, check))
    dataset.iter_batches.assert_not_called()
    fallback.assert_not_called()


def test_coordinator_is_single_and_does_not_retry(context, monkeypatch):
    config, schema = context
    safe_context = SimpleNamespace(
        max_errored_blocks=0, retried_map_errors=False, raise_original_map_exception=False
    )
    result = Mock(context=safe_context)
    read = Mock(return_value=result)
    monkeypatch.setattr("ray_doris._iceberg_stream.ray.data.read_datasource", read)
    runtime_env = {"env_vars": {"EXAMPLE_PASSWORD": "runtime-value"}}
    strategy = NodeAffinitySchedulingStrategy(node_id="a" * 56, soft=False)
    assert (
        stream_iceberg_with_fallback(
            Mock(context=safe_context), config, 123, schema, identity_ok, runtime_env, strategy
        )
        is result
    )
    source = read.call_args.args[0]
    assert source._config.snapshot_id == 123
    assert len(source.get_read_tasks(4)) == 1
    assert read.call_args.kwargs["ray_remote_args"]["max_retries"] == 0
    assert read.call_args.kwargs["ray_remote_args"]["num_cpus"] == 0
    assert read.call_args.kwargs["ray_remote_args"]["runtime_env"] == runtime_env
    assert read.call_args.kwargs["ray_remote_args"]["scheduling_strategy"] is strategy
