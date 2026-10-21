"""Public Ray interface negotiation without version-specific assumptions."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pyarrow.fs import LocalFileSystem
from ray.data.datasource.partitioning import Partitioning

from ray_doris import _compat
from ray_doris._errors import _NativeReadNotApplicableError


def legacy_reader(
    paths,
    *,
    filesystem=None,
    columns=None,
    partitioning=None,
    file_extensions=None,
    concurrency=None,
    override_num_blocks=None,
    ray_remote_args=None,
    **arrow_kwargs,
):
    pass


def modern_reader(
    paths,
    *,
    filesystem=None,
    columns=None,
    partitioning=None,
    file_extensions=None,
    concurrency=None,
    override_num_blocks=None,
    ray_remote_args=None,
    ignore_missing_paths=False,
    **arrow_kwargs,
):
    pass


@pytest.mark.parametrize(
    "reader,v2,kwargs",
    [(legacy_reader, False, {}), (modern_reader, True, {"ignore_missing_paths": False})],
)
def test_reader_profile_and_kwargs_are_discovered(monkeypatch, reader, v2, kwargs):
    monkeypatch.setattr(_compat.ray.data, "read_parquet", reader)
    context = SimpleNamespace(use_datasource_v2=True) if v2 else SimpleNamespace()
    monkeypatch.setattr(_compat.ray.data.DataContext, "get_current", Mock(return_value=context))
    _compat.ensure_native_ray_version()
    assert _compat.native_uses_datasource_v2() is v2
    filesystem = LocalFileSystem()
    options = _compat.native_parquet_options(
        base_dir="/data/p=parent", filesystem=filesystem, partitioning=None
    )
    partitioning = options.pop("partitioning")
    if v2:
        assert partitioning is None
    else:
        assert partitioning.field_names == [] and partitioning.field_types == {}
        assert partitioning.base_dir == "/data/p=parent" and partitioning.filesystem is filesystem
    assert options == kwargs


@pytest.mark.parametrize("reader", [legacy_reader, modern_reader])
def test_explicit_partitioning_survives_reader_adaptation(monkeypatch, reader):
    monkeypatch.setattr(_compat.ray.data, "read_parquet", reader)
    filesystem = LocalFileSystem()
    partitioning = Partitioning(
        "hive", base_dir="/data", field_names=["p"], field_types={"p": int}, filesystem=filesystem
    )
    options = _compat.native_parquet_options(
        base_dir="/data", filesystem=filesystem, partitioning=partitioning
    )
    assert options["partitioning"] is partitioning


def test_variadic_kwargs_are_not_proof_of_public_parameter_support(monkeypatch):
    monkeypatch.setattr(_compat.ray.data, "read_parquet", lambda *args, **kwargs: None)
    with pytest.raises(_NativeReadNotApplicableError, match="parameters"):
        _compat.ensure_native_ray_version()


@pytest.mark.parametrize(
    "context", [SimpleNamespace(), SimpleNamespace(use_datasource_v2="unknown")]
)
def test_unknown_reader_selection_is_not_guessed(monkeypatch, context):
    monkeypatch.setattr(_compat.ray.data, "read_parquet", modern_reader)
    monkeypatch.setattr(_compat.ray.data.DataContext, "get_current", Mock(return_value=context))
    with pytest.raises(_NativeReadNotApplicableError):
        _compat.native_uses_datasource_v2()


def test_missing_typed_partitioning_capability_is_not_silently_dropped(monkeypatch):
    monkeypatch.setattr(_compat, "Partitioning", lambda style, base_dir: None)
    with pytest.raises(_NativeReadNotApplicableError, match="partitioning"):
        _compat.ensure_native_partitioning()
