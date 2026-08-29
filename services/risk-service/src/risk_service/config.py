from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from os import environ


class ConfigurationError(ValueError):
    """Raised when environment configuration is invalid."""


@dataclass(frozen=True)
class Settings:
    kafka_brokers: tuple[str, ...]
    payments_topic: str
    decisions_topic: str
    invalid_events_topic: str
    consumer_group: str
    redis_url: str
    redis_key_prefix: str
    poll_timeout_seconds: float
    publish_timeout_seconds: float
    retry_backoff_seconds: float
    velocity_window_seconds: int
    event_cache_ttl_seconds: int
    feature_state_ttl_seconds: int
    high_amount_minor: int
    extreme_amount_minor: int
    review_threshold: int
    block_threshold: int
    risky_merchant_ids: frozenset[str]
    high_risk_countries: frozenset[str]
    log_level: str

    @classmethod
    def from_env(cls, values: Mapping[str, str] | None = None) -> Settings:
        source = environ if values is None else values

        brokers = tuple(
            item.strip()
            for item in source.get("KAFKA_BROKERS", "localhost:9092").split(",")
            if item.strip()
        )
        if not brokers:
            raise ConfigurationError("KAFKA_BROKERS must contain at least one broker")

        settings = cls(
            kafka_brokers=brokers,
            payments_topic=_text(source, "PAYMENTS_TOPIC", "payments.created"),
            decisions_topic=_text(source, "RISK_DECISIONS_TOPIC", "risk.decisions"),
            invalid_events_topic=_text(source, "RISK_INVALID_EVENTS_TOPIC", "risk.invalid-events"),
            consumer_group=_text(source, "RISK_CONSUMER_GROUP", "riskflow-deterministic-rules-v1"),
            redis_url=_text(source, "REDIS_URL", "redis://localhost:6379/0"),
            redis_key_prefix=_text(source, "REDIS_KEY_PREFIX", "riskflow:risk:v1"),
            poll_timeout_seconds=_positive_float(source, "RISK_POLL_TIMEOUT", 1.0),
            publish_timeout_seconds=_positive_float(source, "RISK_PUBLISH_TIMEOUT", 5.0),
            retry_backoff_seconds=_positive_float(source, "RISK_RETRY_BACKOFF", 1.0),
            velocity_window_seconds=_positive_int(source, "VELOCITY_WINDOW_SECONDS", 300),
            event_cache_ttl_seconds=_positive_int(source, "EVENT_CACHE_TTL_SECONDS", 2592000),
            feature_state_ttl_seconds=_positive_int(source, "FEATURE_STATE_TTL_SECONDS", 2592000),
            high_amount_minor=_positive_int(source, "HIGH_AMOUNT_MINOR", 100000),
            extreme_amount_minor=_positive_int(source, "EXTREME_AMOUNT_MINOR", 500000),
            review_threshold=_bounded_score(source, "REVIEW_THRESHOLD", 40),
            block_threshold=_bounded_score(source, "BLOCK_THRESHOLD", 70),
            risky_merchant_ids=_csv_set(source.get("RISKY_MERCHANT_IDS", "")),
            high_risk_countries=frozenset(
                item.upper() for item in _csv_set(source.get("HIGH_RISK_COUNTRIES", ""))
            ),
            log_level=_text(source, "LOG_LEVEL", "info").upper(),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.extreme_amount_minor <= self.high_amount_minor:
            raise ConfigurationError("EXTREME_AMOUNT_MINOR must exceed HIGH_AMOUNT_MINOR")
        if self.block_threshold <= self.review_threshold:
            raise ConfigurationError("BLOCK_THRESHOLD must exceed REVIEW_THRESHOLD")
        if self.event_cache_ttl_seconds < self.velocity_window_seconds:
            raise ConfigurationError(
                "EVENT_CACHE_TTL_SECONDS must be at least VELOCITY_WINDOW_SECONDS"
            )


def _text(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _positive_float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = values.get(name, str(default))
    try:
        parsed = float(raw)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a number") from error
    if parsed <= 0:
        raise ConfigurationError(f"{name} must be positive")
    return parsed


def _positive_int(values: Mapping[str, str], name: str, default: int) -> int:
    raw = values.get(name, str(default))
    try:
        parsed = int(raw)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error
    if parsed <= 0:
        raise ConfigurationError(f"{name} must be positive")
    return parsed


def _bounded_score(values: Mapping[str, str], name: str, default: int) -> int:
    score = _positive_int(values, name, default)
    if score > 100:
        raise ConfigurationError(f"{name} must not exceed 100")
    return score


def _csv_set(raw: str) -> frozenset[str]:
    return frozenset(item.strip() for item in raw.split(",") if item.strip())
