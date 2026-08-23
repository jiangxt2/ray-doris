from importlib.metadata import requires, version
from unittest.mock import Mock

import pytest

from ray_doris import __version__, read_doris


def test_read_doris_uses_public_ray_entrypoint(monkeypatch) -> None:
    read_datasource = Mock(return_value="dataset")
    monkeypatch.setattr("ray_doris._api.ray.data.read_datasource", read_datasource)
    result = read_doris(
        table="db.table",
        host="fe",
        http_scheme="https",
        http_ca_file="ca.pem",
        flight_scheme="grpc+tls",
        connect_timeout=3.5,
        query_plan_timeout=4.5,
        password_env="RAY_DORIS_PASSWORD",
        concurrency=2,
        override_num_blocks=4,
        ray_remote_args={"num_cpus": 0.25},
    )
    assert result == "dataset"
    datasource = read_datasource.call_args.args[0]
    assert datasource.config.http_scheme == "https"
    assert datasource.config.flight_scheme == "grpc+tls"
    assert datasource.config.connect_timeout == 3.5
    assert datasource.config.query_plan_timeout == 4.5
    assert datasource.config.http_ca_file == "ca.pem"
    assert datasource.config.password_env == "RAY_DORIS_PASSWORD"
    assert read_datasource.call_args.kwargs == {
        "concurrency": 2,
        "override_num_blocks": 4,
        "ray_remote_args": {"num_cpus": 0.25},
    }


def test_read_doris_unknown_parameter_fails_fast() -> None:
    with pytest.raises(TypeError):
        read_doris(table="db.table", host="fe", overrid_num_blocks=4)


def test_package_does_not_import_ray_internal_modules() -> None:
    from pathlib import Path

    source = Path(__file__).parents[2] / "src" / "ray_doris"
    for path in source.glob("*.py"):
        assert "ray.data._internal" not in path.read_text()


def test_package_version_matches_distribution_metadata() -> None:
    assert __version__ == version("ray-doris")


def test_distribution_metadata_bounds_supported_ray_window() -> None:
    package_requirements = requires("ray-doris") or []
    assert "ray[data]<2.57,>=2.49.2" in package_requirements


def test_distribution_metadata_declares_typing_extensions() -> None:
    package_requirements = requires("ray-doris") or []
    assert "typing-extensions<5,>=4.12" in package_requirements
