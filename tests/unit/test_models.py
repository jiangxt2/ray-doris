import math

import pytest
from ray import cloudpickle

from ray_doris._errors import DorisConfigurationError
from ray_doris._models import DorisReadConfig
from ray_doris._sql import parse_table


def make_config(**kwargs: object) -> DorisReadConfig:
    values = {"table": parse_table("db.table"), "host": "fe", "user": "reader"}
    values.update(kwargs)
    return DorisReadConfig.from_options(**values)


def test_config_defaults() -> None:
    config = make_config()
    assert config.transport == "mysql"
    assert config.connect_timeout == 10.0


def test_config_repr_redacts_password_and_option_values() -> None:
    config = make_config(
        password="super-secret",
        client_options={"ssl_key": "mysql-secret"},
        flight_options={"adbc.flight.sql.client_option.token": "flight-secret"},
    )
    rendered = repr(config)
    assert "super-secret" not in rendered
    assert "mysql-secret" not in rendered
    assert "flight-secret" not in rendered
    assert "<redacted>" in rendered


def test_config_is_cloudpickle_serializable_and_freezes_options() -> None:
    options = {"read_timeout": 30, "ssl": {"ca": "original.pem"}}
    config = make_config(client_options=options)
    options["read_timeout"] = 99
    options["ssl"]["ca"] = "changed.pem"
    returned = config.mysql_options()
    returned["ssl"]["ca"] = "returned.pem"
    restored = cloudpickle.loads(cloudpickle.dumps(config))
    assert restored == config
    assert restored.mysql_options()["read_timeout"] == 30
    assert restored.mysql_options()["ssl"]["ca"] == "original.pem"


@pytest.mark.parametrize(
    "timeout",
    [math.nan, math.inf, -math.inf, 31_536_001, 10**1000],
)
def test_config_rejects_invalid_connect_timeout(timeout: float) -> None:
    with pytest.raises(DorisConfigurationError) as exc_info:
        make_config(connect_timeout=timeout)
    assert str(exc_info.value) == (
        "connect_timeout must be finite, positive, and at most 31536000 seconds"
    )


@pytest.mark.parametrize("value", ["30", 0, math.nan, math.inf])
def test_config_rejects_invalid_mysql_execution_timeout(value: object) -> None:
    with pytest.raises(DorisConfigurationError, match="read_timeout"):
        make_config(client_options={"read_timeout": value})


def test_config_rejects_non_mapping_options() -> None:
    with pytest.raises(DorisConfigurationError, match="must be a mapping"):
        make_config(client_options=[("read_timeout", 30)])


@pytest.mark.parametrize(
    "kwargs",
    [
        {"host": ""},
        {"mysql_port": 0},
        {"http_port": 65536},
        {"tablet_size": 0},
        {"batch_size": 0},
        {"connect_timeout": 0},
        {"transport": "unknown"},
        {"http_scheme": "ftp"},
        {"flight_scheme": "https"},
        {"on_query_plan_error": "ignore"},
        {"batch_size": "10"},
        {"client_options": {"password": "override"}},
        {"client_options": {"connect_timeout": 1}},
        {"client_options": {"passwd": "override"}},
        {"flight_options": {"username": "override"}},
        {"flight_options": {"adbc.flight.sql.rpc.timeout_seconds.connect": "1"}},
        {"flight_options": {"token": 123}},
    ],
)
def test_config_rejects_invalid_values(kwargs: object) -> None:
    with pytest.raises(DorisConfigurationError):
        make_config(**kwargs)
