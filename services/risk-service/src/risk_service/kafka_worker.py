from __future__ import annotations

import hashlib
import logging
import threading
from datetime import UTC, datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from pydantic import ValidationError

from risk_service.config import Settings
from risk_service.models import InvalidEventEnvelope, InvalidEventPayload, parse_payment_created
from risk_service.processor import DecisionProcessor


class RecordProducer(Protocol):
    def publish(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: list[tuple[str, str]],
    ) -> None: ...


class SynchronousKafkaProducer:
    def __init__(self, settings: Settings) -> None:
        self._timeout = settings.publish_timeout_seconds
        self._producer = Producer(
            {
                "bootstrap.servers": ",".join(settings.kafka_brokers),
                "client.id": "riskflow-risk-service",
                "acks": "all",
                "enable.idempotence": True,
                "message.timeout.ms": int(settings.publish_timeout_seconds * 1000),
            }
        )

    def publish(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: list[tuple[str, str]],
    ) -> None:
        delivery_error: list[KafkaError] = []

        def on_delivery(error: KafkaError | None, _message: Message) -> None:
            if error is not None:
                delivery_error.append(error)

        self._producer.produce(
            topic=topic,
            key=key.encode(),
            value=value,
            headers=headers,
            on_delivery=on_delivery,
        )
        remaining = self._producer.flush(self._timeout)
        if remaining:
            raise TimeoutError(f"Kafka did not acknowledge {remaining} record(s) before timeout")
        if delivery_error:
            raise KafkaException(delivery_error[0])

    def close(self) -> None:
        self._producer.flush(self._timeout)


class RiskWorker:
    def __init__(
        self,
        settings: Settings,
        consumer: Consumer,
        producer: RecordProducer,
        processor: DecisionProcessor,
    ) -> None:
        self._settings = settings
        self._consumer = consumer
        self._producer = producer
        self._processor = processor
        self._logger = logging.getLogger("risk_service.worker")

    @classmethod
    def build(
        cls,
        settings: Settings,
        producer: RecordProducer,
        processor: DecisionProcessor,
    ) -> RiskWorker:
        consumer = Consumer(
            {
                "bootstrap.servers": ",".join(settings.kafka_brokers),
                "group.id": settings.consumer_group,
                "client.id": "riskflow-risk-service",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "allow.auto.create.topics": False,
            }
        )
        return cls(settings, consumer, producer, processor)

    def run(self, stop_event: threading.Event) -> None:
        self._consumer.subscribe([self._settings.payments_topic])
        self._logger.info(
            "risk worker started",
            extra={"topic": self._settings.payments_topic},
        )
        try:
            while not stop_event.is_set():
                message = self._consumer.poll(self._settings.poll_timeout_seconds)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                self._handle(message, stop_event)
        finally:
            self._consumer.close()
            self._logger.info("risk worker stopped")

    def _handle(self, message: Message, stop_event: threading.Event) -> None:
        try:
            event = parse_payment_created(message.value())
        except (ValidationError, ValueError, UnicodeDecodeError):
            try:
                self._publish_invalid(message)
                self._commit(message)
            except Exception:
                self._retry(message, stop_event, event_id=None)
            return

        try:
            decision = self._processor.process(event)
            self._producer.publish(
                self._settings.decisions_topic,
                str(decision.aggregate_id),
                decision.model_dump_json().encode(),
                [
                    ("event_type", decision.event_type),
                    ("schema_version", str(decision.schema_version)),
                    ("source_event_id", str(event.event_id)),
                ],
            )
            self._commit(message)
            self._logger.info(
                "risk decision published",
                extra={
                    "event_id": str(decision.event_id),
                    "payment_id": str(decision.aggregate_id),
                    "decision": decision.payload.decision,
                    "risk_score": decision.payload.risk_score,
                    "topic": self._settings.decisions_topic,
                    "partition": message.partition(),
                    "offset": message.offset(),
                },
            )
        except Exception:
            self._retry(message, stop_event, event_id=str(event.event_id))

    def _publish_invalid(self, message: Message) -> None:
        raw_value = message.value() or b""
        record_hash = hashlib.sha256(raw_value).hexdigest()
        source = f"{message.topic()}:{message.partition()}:{message.offset()}:{record_hash}"
        event_id = uuid5(NAMESPACE_URL, f"riskflow:risk.input.rejected:{source}")
        aggregate_id = uuid5(NAMESPACE_URL, f"riskflow:invalid.aggregate:{source}")
        trace_id = uuid5(NAMESPACE_URL, f"riskflow:invalid.trace:{source}")
        _timestamp_type, timestamp_ms = message.timestamp()
        occurred_at = (
            datetime.fromtimestamp(timestamp_ms / 1000, UTC)
            if timestamp_ms is not None
            else datetime.fromtimestamp(0, UTC)
        )
        rejected = InvalidEventEnvelope(
            event_id=event_id,
            event_type="risk.input.rejected",
            aggregate_id=aggregate_id,
            schema_version=1,
            occurred_at=occurred_at,
            trace_id=trace_id,
            payload=InvalidEventPayload(
                source_topic=message.topic(),
                source_partition=message.partition(),
                source_offset=message.offset(),
                error_code="invalid_event",
                error_message="event does not match the payments.created schema",
                record_sha256=record_hash,
            ),
        )
        self._producer.publish(
            self._settings.invalid_events_topic,
            str(event_id),
            rejected.model_dump_json().encode(),
            [("event_type", rejected.event_type), ("schema_version", "1")],
        )
        self._logger.warning(
            "invalid payment event quarantined",
            extra={
                "event_id": str(event_id),
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            },
        )

    def _commit(self, message: Message) -> None:
        committed = self._consumer.commit(message=message, asynchronous=False)
        if not committed:
            raise KafkaException("Kafka returned no committed offsets")

    def _retry(
        self,
        message: Message,
        stop_event: threading.Event,
        event_id: str | None,
    ) -> None:
        self._logger.exception(
            "risk event processing failed; offset will be retried",
            extra={
                "event_id": event_id,
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            },
        )
        self._consumer.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
        stop_event.wait(self._settings.retry_backoff_seconds)
