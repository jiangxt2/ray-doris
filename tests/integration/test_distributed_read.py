import pytest

from ray_doris import read_doris

pytestmark = pytest.mark.integration


def test_public_entrypoint_executes_distributed_ray_read(doris_config) -> None:
    dataset = read_doris(
        **doris_config.reader_kwargs(tablet_size=1),
        concurrency=2,
        override_num_blocks=4,
        ray_remote_args={"num_cpus": 0.25},
    )
    assert sorted(row["id"] for row in dataset.select_columns(["id"]).take_all()) == [1, 2, 3, 4]
    assert set(dataset.unique("category")) == {"alpha", "beta", None}
