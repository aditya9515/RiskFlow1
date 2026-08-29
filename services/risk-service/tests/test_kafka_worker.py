from __future__ import annotations

import json
import threading
from dataclasses import replace
from datetime import datetime
from typing import Any

from risk_service.config import Settings
from risk_service.kafka_worker import RiskWorker
from risk_service.models import FeatureSnapshot, PaymentCreatedEnvelope
from risk_service.processor import DecisionProcessor
from risk_service.rules import RuleEngine


class FakeMessage:
    def __init__(self, value: bytes) -> None:
        self._value = value

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return "payments.created"

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return 12

    def timestamp(self) -> tuple[int, int]:
        return (1, 1788076800000)


class FakeConsumer:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.commits = 0
        self.seeks = 0

    def commit(self, **_kwargs: Any) -> list[object]:
        self.calls.append("commit")
        self.commits += 1
        return [object()]

    def seek(self, _partition: object) -> None:
        self.calls.append("seek")
        self.seeks += 1

    def subscribe(self, _topics: list[str]) -> None:
        self.calls.append("subscribe")

    def close(self) -> None:
        self.calls.append("close")


class FakeProducer:
    def __init__(self, calls: list[str], fail: bool = False) -> None:
        self.calls = calls
        self.fail = fail
        self.records: list[tuple[str, str, bytes]] = []

    def publish(
        self,
        topic: str,
        key: str,
        value: bytes,
        _headers: list[tuple[str, str]],
    ) -> None:
        self.calls.append("publish")
        if self.fail:
            raise ConnectionError("broker unavailable")
        self.records.append((topic, key, value))


class FixedFeatureStore:
    def __init__(self, decision_time: datetime) -> None:
        self._snapshot = FeatureSnapshot(
            velocity_5m=1,
            new_device=False,
            cross_border=False,
            baseline_country="IN",
            decision_at=decision_time,
        )

    def observe(self, _event: PaymentCreatedEnvelope, _decision_at: datetime) -> FeatureSnapshot:
        return self._snapshot


def build_worker(
    settings: Settings,
    decision_time: datetime,
    consumer: FakeConsumer,
    producer: FakeProducer,
) -> RiskWorker:
    processor = DecisionProcessor(
        FixedFeatureStore(decision_time),
        RuleEngine(settings),
        now=lambda: decision_time,
    )
    return RiskWorker(settings, consumer, producer, processor)


def test_valid_decision_is_published_before_offset_commit(
    settings: Settings,
    payment_event: PaymentCreatedEnvelope,
    decision_time: datetime,
) -> None:
    calls: list[str] = []
    consumer = FakeConsumer(calls)
    producer = FakeProducer(calls)
    worker = build_worker(settings, decision_time, consumer, producer)

    worker._handle(FakeMessage(payment_event.model_dump_json().encode()), threading.Event())

    assert calls == ["publish", "commit"]
    assert producer.records[0][0] == "risk.decisions"
    output = json.loads(producer.records[0][2])
    assert output["payload"]["source_event_id"] == str(payment_event.event_id)


def test_invalid_event_is_quarantined_before_offset_commit(
    settings: Settings, decision_time: datetime
) -> None:
    calls: list[str] = []
    consumer = FakeConsumer(calls)
    producer = FakeProducer(calls)
    worker = build_worker(settings, decision_time, consumer, producer)

    worker._handle(FakeMessage(b'{"not":"a payment event"}'), threading.Event())

    assert calls == ["publish", "commit"]
    assert producer.records[0][0] == "risk.invalid-events"
    output = json.loads(producer.records[0][2])
    assert output["event_type"] == "risk.input.rejected"
    assert output["aggregate_id"]
    assert output["trace_id"]
    assert "not" not in output["payload"]
    assert output["payload"]["error_message"] == (
        "event does not match the payments.created schema"
    )


def test_publish_failure_seeks_to_same_offset_without_commit(
    settings: Settings,
    payment_event: PaymentCreatedEnvelope,
    decision_time: datetime,
) -> None:
    settings = replace(settings, retry_backoff_seconds=0.001)
    calls: list[str] = []
    consumer = FakeConsumer(calls)
    producer = FakeProducer(calls, fail=True)
    worker = build_worker(settings, decision_time, consumer, producer)

    worker._handle(FakeMessage(payment_event.model_dump_json().encode()), threading.Event())

    assert calls == ["publish", "seek"]
    assert consumer.commits == 0
    assert consumer.seeks == 1


def test_already_cancelled_worker_subscribes_then_closes_cleanly(
    settings: Settings, decision_time: datetime
) -> None:
    calls: list[str] = []
    consumer = FakeConsumer(calls)
    producer = FakeProducer(calls)
    worker = build_worker(settings, decision_time, consumer, producer)
    stop_event = threading.Event()
    stop_event.set()

    worker.run(stop_event)

    assert calls == ["subscribe", "close"]
