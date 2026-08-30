import pytest

from streaming_analytics.config import Settings


def test_settings_load_safe_defaults() -> None:
    settings = Settings.from_env({"KAFKA_BROKERS": "kafka:19092"})

    assert settings.payments_topic == "payments.created"
    assert settings.decisions_topic == "risk.decisions"
    assert settings.starting_offsets == "earliest"
    assert settings.trigger_interval == "5 seconds"
    assert settings.max_offsets_per_trigger == 10000


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({}, "KAFKA_BROKERS is required"),
        ({"KAFKA_BROKERS": "bad broker"}, "must not contain whitespace"),
        (
            {
                "KAFKA_BROKERS": "kafka:19092",
                "PAYMENTS_TOPIC": "same",
                "RISK_DECISIONS_TOPIC": "same",
            },
            "must differ",
        ),
        (
            {"KAFKA_BROKERS": "kafka:19092", "SPARK_OUTPUT_ROOT": "relative"},
            "absolute normalized",
        ),
        (
            {"KAFKA_BROKERS": "kafka:19092", "SPARK_STARTING_OFFSETS": "middle"},
            "earliest or latest",
        ),
        (
            {"KAFKA_BROKERS": "kafka:19092", "SPARK_TRIGGER_INTERVAL": "immediately"},
            "positive integer",
        ),
        (
            {"KAFKA_BROKERS": "kafka:19092", "SPARK_MAX_OFFSETS_PER_TRIGGER": "0"},
            "greater than zero",
        ),
    ],
)
def test_settings_reject_invalid_configuration(environment: dict[str, str], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Settings.from_env(environment)
