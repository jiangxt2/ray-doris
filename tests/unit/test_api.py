from importlib.metadata import requires, version
from pathlib import Path
from unittest.mock import Mock

import pytest

from ray_doris import DorisConfigurationError, __version__, read_doris


@pytest.mark.parametrize("mode_options", [{}, {"read_mode": "doris"}], ids=["default", "doris"])
def test_read_doris_uses_public_ray_entrypoint(monkeypatch, mode_options) -> None:
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
        **mode_options,
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


def test_read_doris_rejects_query_mode_before_constructing_reader(monkeypatch) -> None:
    datasource = Mock(side_effect=AssertionError("Doris reader constructed"))
    native_import = Mock(side_effect=AssertionError("native reader imported"))
    monkeypatch.setattr("ray_doris._api.DorisDatasource", datasource)
    monkeypatch.setattr("ray_doris._api.import_module", native_import)
    with pytest.raises(DorisConfigurationError, match="read_mode must be 'doris' or 'native'"):
        read_doris(table="db.table", host="unused", read_mode="query")
    datasource.assert_not_called()
    native_import.assert_not_called()


def test_read_doris_unknown_parameter_fails_fast() -> None:
    with pytest.raises(TypeError):
        read_doris(table="db.table", host="fe", overrid_num_blocks=4)


def test_package_does_not_import_ray_internal_modules() -> None:
    source = Path(__file__).parents[2] / "src" / "ray_doris"
    private_module = ".".join(("ray", "data", "_internal"))
    for path in source.glob("*.py"):
        assert private_module not in path.read_text()


def test_package_version_matches_distribution_metadata() -> None:
    assert __version__ == version("ray-doris")


def test_distribution_metadata_bounds_supported_ray_window() -> None:
    package_requirements = requires("ray-doris") or []
    assert "ray[data]<2.60,>=2.49.2" in package_requirements


def test_distribution_metadata_declares_typing_extensions() -> None:
    package_requirements = requires("ray-doris") or []
    assert "typing-extensions<5,>=4.12" in package_requirements
