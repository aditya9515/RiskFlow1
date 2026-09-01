from __future__ import annotations

import logging
import signal
import threading

from redis import Redis

from risk_service.config import ConfigurationError, Settings
from risk_service.features import RedisFeatureStore
from risk_service.kafka_worker import RiskWorker, SynchronousKafkaProducer
from risk_service.logging_json import configure_logging
from risk_service.model import (
    ModelArtifactError,
    RiskModel,
    UnavailableRiskModel,
    XGBoostRiskModel,
)
from risk_service.processor import DecisionProcessor
from risk_service.rules import RuleEngine


def main() -> int:
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        configure_logging("INFO")
        logging.getLogger("risk_service").error("invalid configuration: %s", error)
        return 1

    configure_logging(settings.log_level)
    logger = logging.getLogger("risk_service")
    stop_event = threading.Event()

    def request_shutdown(_signum: int, _frame: object) -> None:
        logger.info("shutdown requested")
        stop_event.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    client = Redis.from_url(settings.redis_url, decode_responses=False)
    feature_store = RedisFeatureStore(
        client=client,
        key_prefix=settings.redis_key_prefix,
        velocity_window_seconds=settings.velocity_window_seconds,
        event_cache_ttl_seconds=settings.event_cache_ttl_seconds,
        feature_state_ttl_seconds=settings.feature_state_ttl_seconds,
    )
    producer = SynchronousKafkaProducer(settings)
    try:
        feature_store.ping()
        risk_model = load_risk_model(settings, logger)
        processor = DecisionProcessor(
            feature_store,
            RuleEngine(settings),
            risk_model,
            fallback_review_score=settings.review_threshold,
        )
        worker = RiskWorker.build(settings, producer, processor)
        worker.run(stop_event)
    except Exception:
        logger.exception("risk service stopped unexpectedly")
        return 1
    finally:
        producer.close()
        feature_store.close()
    return 0


def load_risk_model(settings: Settings, logger: logging.Logger) -> RiskModel:
    try:
        risk_model = XGBoostRiskModel(settings.model_path, settings.model_metadata_path)
    except ModelArtifactError:
        logger.exception("risk model unavailable; manual-review fallback enabled")
        return UnavailableRiskModel()

    logger.info(
        "risk model loaded",
        extra={
            "model_version": risk_model.model_version,
            "model_review_threshold": risk_model.review_threshold,
        },
    )
    return risk_model


if __name__ == "__main__":
    raise SystemExit(main())
