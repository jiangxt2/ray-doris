"""Exercise the Iceberg stream coordinator with real Ray task execution."""

import tempfile
import time
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest
import ray
import ray.data
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from ray_doris._errors import _UnsafeIcebergFallbackError
from ray_doris._iceberg_stream import stream_iceberg_with_fallback
from ray_doris._models import DorisReadConfig
from ray_doris._sql import parse_table

pytestmark = pytest.mark.contract


@pytest.fixture(scope="module", autouse=True)
def local_ray():
    with tempfile.TemporaryDirectory(prefix="rdis-", dir="/tmp") as directory:
        marker = Path(directory) / "fallback.log"
        runtime_env = {
            "worker_process_setup_hook": "tests.iceberg_integration.retry_hook.record_fallback",
            "env_vars": {
                "ICEBERG_FALLBACK_MARKER": str(marker),
                "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
            },
        }
        ray.init(num_cpus=4, include_dashboard=False, _temp_dir=directory, runtime_env=runtime_env)
        context = ray.data.DataContext.get_current()
        previous = context.target_max_block_size
        context.target_max_block_size = 1024
        yield marker
        context.target_max_block_size = previous
        ray.shutdown()


@pytest.fixture
def config():
    return DorisReadConfig.from_options(
        host="127.0.0.1",
        mysql_port=1,
        connect_timeout=1,
        user="reader",
        catalog="lake",
        table=parse_table("db.t"),
    )


def fail_before(_batch):
    raise RuntimeError("native failure")


def fail_identity(_batch):
    raise _UnsafeIcebergFallbackError("table identity changed")


@ray.remote(num_cpus=0)
class OutputGate:
    def __init__(self):
        self.released = False

    def release(self):
        self.released = True

    def is_released(self):
        return self.released


def fail_after(batch, gate):
    yield batch
    deadline = time.monotonic() + 20
    while not ray.get(gate.is_released.remote()):
        if time.monotonic() >= deadline:
            raise RuntimeError("timed out waiting for first output")
        time.sleep(0.05)
    raise RuntimeError("sensitive native failure")


def identity_ok():
    pass


def identity_changed():
    raise _UnsafeIcebergFallbackError("table identity changed")


def wrap(dataset, config, check_identity=identity_ok, runtime_env=None, scheduling_strategy=None):
    return stream_iceberg_with_fallback(
        dataset,
        config,
        123,
        pa.schema([("id", pa.int64())]),
        check_identity,
        runtime_env,
        scheduling_strategy,
    )


def test_nested_dataset_remains_lazy_and_streams(config):
    dataset = wrap(ray.data.range(20, override_num_blocks=2), config)
    assert sorted(row["id"] for row in dataset.take_all()) == list(range(20))


@pytest.mark.parametrize(
    "failure,expected", [(fail_before, "Doris"), (fail_identity, "table identity changed")]
)
def test_preoutput_failures_select_safe_path(config, failure, expected):
    native = ray.data.range(1).map_batches(failure, max_retries=0)
    with pytest.raises(Exception, match=expected):
        wrap(native, config).take_all()


def test_postoutput_failure_reports_disable_without_doris_replay(config, local_ray):
    before = local_ray.read_text().splitlines() if local_ray.exists() else []
    gate = OutputGate.remote()
    native = ray.data.range(2048, override_num_blocks=1).map_batches(
        fail_after, batch_size=2048, batch_format="pyarrow", fn_kwargs={"gate": gate}, max_retries=0
    )
    batches = iter(wrap(native, config).iter_batches(batch_size=None, prefetch_batches=0))
    assert len(next(batches)["id"]) > 0
    ray.get(gate.release.remote())
    with pytest.raises(Exception, match="disable auto_native_read") as error:
        list(batches)
    assert "sensitive native failure" not in str(error.value)
    assert (local_ray.read_text().splitlines() if local_ray.exists() else []) == before


def test_context_change_after_construction_uses_doris(config):
    dataset = wrap(ray.data.range(1), config)
    dataset.context.retried_map_errors = True
    dataset.context.max_map_retries = 0
    # The configured Doris endpoint refuses connections. Seeing its error proves
    # the coordinator did not return the native row after the policy changed.
    with pytest.raises(Exception, match="Doris"):
        dataset.take_all()


def test_coordinator_rechecks_identity_before_output(config, local_ray):
    before = local_ray.read_text().splitlines() if local_ray.exists() else []
    with pytest.raises(Exception, match="table identity changed"):
        wrap(ray.data.range(1), config, identity_changed).take_all()
    assert (local_ray.read_text().splitlines() if local_ray.exists() else []) == before


def test_task_runtime_environment_reaches_doris_fallback(config, monkeypatch, local_ray):
    variable = "RAY_DORIS_CONTRACT_TASK_PASSWORD"
    monkeypatch.delenv(variable, raising=False)
    config = replace(config, password_env=variable)
    native = ray.data.range(1).map_batches(fail_before, max_retries=0)
    before = local_ray.read_text().splitlines() if local_ray.exists() else []
    runtime_env = {"env_vars": {variable: "test-only"}}
    strategy = NodeAffinitySchedulingStrategy(ray.get_runtime_context().get_node_id(), soft=False)
    with pytest.raises(Exception, match="MySQL error 2003"):
        wrap(native, config, runtime_env=runtime_env, scheduling_strategy=strategy).take_all()
    entries = local_ray.read_text().splitlines()[len(before) :]
    assert [entry.split(" ", 1)[0] for entry in entries] == ["123"]
