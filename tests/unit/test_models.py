import math
import traceback

import pytest
from ray import cloudpickle

from ray_doris._errors import DorisConfigurationError
from ray_doris._models import DorisReadConfig
from ray_doris._sql import parse_table


def make_config(**kwargs: object) -> DorisReadConfig:
    values = {"table": parse_table("db.table"), "host": "fe", "user": "reader"}
    values.update(kwargs)
    return DorisReadConfig.from_options(**values)


def _assert_redacted_exception(exception: BaseException, caplog, sentinel: str) -> None:
    rendered_traceback = "".join(
        traceback.format_exception(type(exception), exception, exception.__traceback__)
    )
    assert sentinel not in str(exception)
    assert sentinel not in rendered_traceback
    assert sentinel not in caplog.text
    assert exception.__cause__ is None


def test_config_defaults() -> None:
    config = make_config()
    assert config.transport == "mysql"
    assert config.connect_timeout == 10.0
    assert config.effective_query_plan_timeout == 10.0
    assert config.password_env is None
    assert config.http_ca_file is None


def test_config_repr_redacts_password_and_option_values() -> None:
    config = make_config(
        password="super-secret",
        filter="tenant_id = 'sensitive-filter-value'",
        client_options={"ssl_key": "mysql-secret"},
        flight_options={"adbc.flight.sql.client_option.token": "flight-secret"},
    )
    rendered = repr(config)
    assert "super-secret" not in rendered
    assert "mysql-secret" not in rendered
    assert "flight-secret" not in rendered
    assert "sensitive-filter-value" not in rendered
    assert "filter=<redacted>" in rendered
    assert "<redacted>" in rendered


def test_config_repr_redacts_environment_name_and_http_ca_path() -> None:
    rendered = repr(
        make_config(
            password_env="RAY_DORIS_PASSWORD_SECRET_NAME",
            http_scheme="https",
            http_ca_file="/private/tls/sensitive-ca-name.pem",
        )
    )
    assert "RAY_DORIS_PASSWORD_SECRET_NAME" not in rendered
    assert "sensitive-ca-name.pem" not in rendered
    assert "password_env=<configured>" in rendered
    assert "http_ca_file=<configured>" in rendered


def test_config_repr_distinguishes_missing_filter_without_exposing_values() -> None:
    assert "filter=None" in repr(make_config())


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


def test_config_resolves_environment_password_without_serializing_value(monkeypatch) -> None:
    secret = "resolved-password-secret-sentinel"
    monkeypatch.setenv("RAY_DORIS_TEST_PASSWORD", secret)
    config = make_config(password_env="RAY_DORIS_TEST_PASSWORD")
    assert config.resolve_password() == secret
    assert secret.encode() not in cloudpickle.dumps(config)


def test_config_environment_password_preserves_empty_value(monkeypatch) -> None:
    monkeypatch.setenv("RAY_DORIS_TEST_PASSWORD", "")
    assert make_config(password_env="RAY_DORIS_TEST_PASSWORD").resolve_password() == ""


def test_config_missing_environment_password_is_redacted(monkeypatch, caplog) -> None:
    sentinel = "RAY_DORIS_MISSING_PASSWORD_SENTINEL"
    monkeypatch.delenv(sentinel, raising=False)
    with pytest.raises(
        DorisConfigurationError, match="environment variable is unavailable"
    ) as captured:
        make_config(password_env=sentinel).resolve_password()
    _assert_redacted_exception(captured.value, caplog, sentinel)


def test_config_query_plan_timeout_overrides_legacy_default() -> None:
    config = make_config(connect_timeout=3.0, query_plan_timeout=7.5)
    assert config.effective_query_plan_timeout == 7.5


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


@pytest.mark.parametrize("timeout", [True, "30", 0, math.nan, math.inf, 31_536_001])
def test_config_rejects_invalid_query_plan_timeout(timeout: object) -> None:
    with pytest.raises(DorisConfigurationError, match="query_plan_timeout"):
        make_config(query_plan_timeout=timeout)


@pytest.mark.parametrize("name", ["", "1PASSWORD", "HAS SPACE", "HAS-DASH", b"PASSWORD"])
def test_config_rejects_invalid_password_environment_name(name: object) -> None:
    with pytest.raises(DorisConfigurationError, match="password_env"):
        make_config(password_env=name)


def test_config_rejects_literal_and_environment_password_together() -> None:
    with pytest.raises(DorisConfigurationError, match="mutually exclusive"):
        make_config(password="literal", password_env="RAY_DORIS_PASSWORD")


def test_config_requires_https_for_http_ca_file() -> None:
    with pytest.raises(DorisConfigurationError, match="requires http_scheme='https'"):
        make_config(http_ca_file="ca.pem")


@pytest.mark.parametrize("value", ["30", 0, math.nan, math.inf])
def test_config_rejects_invalid_mysql_execution_timeout(value: object) -> None:
    with pytest.raises(DorisConfigurationError, match="read_timeout"):
        make_config(client_options={"read_timeout": value})


def test_config_rejects_non_mapping_options() -> None:
    with pytest.raises(DorisConfigurationError, match="must be a mapping"):
        make_config(client_options=[("read_timeout", 30)])


def test_config_copy_failure_does_not_expose_underlying_value() -> None:
    class NotCopyable:
        def __deepcopy__(self, memo):
            raise RuntimeError("copy-secret-sentinel")

    with pytest.raises(DorisConfigurationError, match="values must be copyable") as captured:
        make_config(client_options={"program_name": NotCopyable()})
    assert "copy-secret-sentinel" not in str(captured.value)
    assert captured.value.__cause__ is None


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
        {"http_scheme": "https", "http_ca_file": ""},
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
