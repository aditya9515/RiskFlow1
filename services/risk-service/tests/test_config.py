import pytest

from risk_service.config import ConfigurationError, Settings


def test_defaults_are_valid() -> None:
    settings = Settings.from_env({})

    assert settings.kafka_brokers == ("localhost:9092",)
    assert settings.redis_url == "redis://localhost:6379/0"
    assert settings.review_threshold == 40
    assert settings.block_threshold == 70


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("KAFKA_BROKERS", "  ", "at least one broker"),
        ("RISK_POLL_TIMEOUT", "nope", "must be a number"),
        ("VELOCITY_WINDOW_SECONDS", "0", "must be positive"),
        ("BLOCK_THRESHOLD", "101", "must not exceed 100"),
    ],
)
def test_invalid_values_are_rejected(name: str, value: str, message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        Settings.from_env({name: value})


def test_threshold_order_is_validated() -> None:
    with pytest.raises(ConfigurationError, match="BLOCK_THRESHOLD must exceed"):
        Settings.from_env({"REVIEW_THRESHOLD": "70", "BLOCK_THRESHOLD": "40"})
