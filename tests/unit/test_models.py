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
    options = {"read_timeout": 30}
    config = make_config(client_options=options)
    options["read_timeout"] = 99
    restored = cloudpickle.loads(cloudpickle.dumps(config))
    assert restored == config
    assert restored.mysql_options()["read_timeout"] == 30


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
        {"flight_options": {"username": "override"}},
        {"flight_options": {"token": 123}},
    ],
)
def test_config_rejects_invalid_values(kwargs: object) -> None:
    with pytest.raises(DorisConfigurationError):
        make_config(**kwargs)
