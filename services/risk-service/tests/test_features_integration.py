from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from uuid import uuid4

import pytest
from conftest import with_event_id
from redis import Redis

from risk_service.config import Settings
from risk_service.features import RedisFeatureStore
from risk_service.model import ModelScore, ModelScoringError
from risk_service.models import PaymentCreatedEnvelope
from risk_service.processor import MODEL_FALLBACK_VERSION, DecisionProcessor
from risk_service.rules import RuleEngine


class FailingRiskModel:
    def score(self, _payment: object, _features: object) -> ModelScore:
        raise ModelScoringError("model unavailable during inference")


class RecoveredRiskModel:
    def score(self, _payment: object, _features: object) -> ModelScore:
        return ModelScore(
            probability=0.01,
            risk_score=1,
            review_threshold=0.05,
            model_version="xgb-recovered-test-v1",
        )


@pytest.mark.integration
def test_redis_observation_is_atomic_and_idempotent(
    payment_event: PaymentCreatedEnvelope, decision_time
) -> None:
    redis_url = os.getenv("TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TEST_REDIS_URL is not set")

    key_prefix = f"riskflow:test:{uuid4()}"
    client = Redis.from_url(redis_url, decode_responses=False)
    store = RedisFeatureStore(client, key_prefix, 300, 3600, 3600)
    try:
        first = store.observe(payment_event, decision_time)
        replay = store.observe(payment_event, decision_time + timedelta(seconds=20))

        assert replay == first
        assert first.velocity_5m == 1
        assert first.new_device is True
        assert first.cross_border is False

        second_event = with_event_id(payment_event, 2).model_copy(
            update={
                "occurred_at": payment_event.occurred_at + timedelta(seconds=30),
                "payload": with_event_id(payment_event, 2).payload.model_copy(
                    update={"device_id": "device-1", "country": "GB"}
                ),
            }
        )
        second = store.observe(second_event, decision_time + timedelta(seconds=30))

        assert second.velocity_5m == 2
        assert second.new_device is False
        assert second.cross_border is True
        assert second.baseline_country == "IN"
        assert client.zcard(f"{key_prefix}:customer:customer-1:velocity") == 2
    finally:
        for key in client.scan_iter(f"{key_prefix}:*"):
            client.delete(key)
        store.close()


@pytest.mark.integration
def test_concurrent_duplicate_observations_update_velocity_once(
    payment_event: PaymentCreatedEnvelope, decision_time
) -> None:
    redis_url = os.getenv("TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TEST_REDIS_URL is not set")

    key_prefix = f"riskflow:test:{uuid4()}"
    client = Redis.from_url(redis_url, decode_responses=False)
    store = RedisFeatureStore(client, key_prefix, 300, 3600, 3600)
    try:
        with ThreadPoolExecutor(max_workers=16) as executor:
            snapshots = list(
                executor.map(
                    lambda _attempt: store.observe(payment_event, decision_time),
                    range(32),
                )
            )

        assert all(snapshot == snapshots[0] for snapshot in snapshots)
        assert snapshots[0].velocity_5m == 1
        velocity_key = f"{key_prefix}:customer:customer-1:velocity"
        assert client.zcard(velocity_key) == 1
    finally:
        for key in client.scan_iter(f"{key_prefix}:*"):
            client.delete(key)
        store.close()


@pytest.mark.integration
def test_cached_fallback_decision_is_stable_after_worker_restart_and_model_recovery(
    settings: Settings, payment_event: PaymentCreatedEnvelope, decision_time
) -> None:
    redis_url = os.getenv("TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TEST_REDIS_URL is not set")

    key_prefix = f"riskflow:test:{uuid4()}"
    client = Redis.from_url(redis_url, decode_responses=False)
    store = RedisFeatureStore(client, key_prefix, 300, 3600, 3600)
    try:
        unavailable_processor = DecisionProcessor(
            store,
            RuleEngine(settings),
            FailingRiskModel(),
            fallback_review_score=settings.review_threshold,
            now=lambda: decision_time,
        )
        first = unavailable_processor.process(payment_event)

        recovered_processor = DecisionProcessor(
            store,
            RuleEngine(settings),
            RecoveredRiskModel(),
            fallback_review_score=settings.review_threshold,
            now=lambda: decision_time + timedelta(seconds=30),
        )
        replay = recovered_processor.process(payment_event)

        assert replay == first
        assert replay.payload.model_version == MODEL_FALLBACK_VERSION
        assert client.zcard(f"{key_prefix}:customer:customer-1:velocity") == 1
    finally:
        for key in client.scan_iter(f"{key_prefix}:*"):
            client.delete(key)
        store.close()
