from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming_analytics.transforms import build_streams

SOURCE_SCHEMA = StructType(
    [
        StructField("source_topic", StringType(), False),
        StructField("source_partition", IntegerType(), False),
        StructField("source_offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("raw_value", StringType(), False),
    ]
)


def test_valid_contracts_are_flattened_with_broker_coordinates(spark: SparkSession) -> None:
    source = _source(
        spark, [("payments.created", 0, _payment_event()), ("risk.decisions", 1, _decision_event())]
    )

    streams = build_streams(source, "payments.created", "risk.decisions")
    payment = streams.payments.first()
    decision = streams.decisions.first()

    assert streams.payments.count() == 1
    assert streams.decisions.count() == 1
    assert streams.quarantine.count() == 0
    assert payment.payment_id == "10000000-0000-4000-8000-000000000001"
    assert payment.amount_minor == 125000
    assert payment.source_partition == 0
    assert str(payment.event_date) == "2026-08-30"
    assert decision.decision == "REVIEW"
    assert decision.reason_codes == ["HIGH_AMOUNT", "NEW_DEVICE"]
    assert decision.source_partition == 1


def test_malformed_and_contract_invalid_records_are_quarantined(
    spark: SparkSession,
) -> None:
    invalid_payment = _payment_event()
    invalid_payment["schema_version"] = 9
    invalid_payment["payload"]["currency"] = "usd"
    invalid_decision = _decision_event()
    invalid_decision["payload"]["decision_id"] = "not-a-uuid"
    source = _source(
        spark,
        [
            ("payments.created", 0, "{not-json"),
            ("payments.created", 1, invalid_payment),
            ("risk.decisions", 2, invalid_decision),
            ("unexpected.topic", 3, {"value": "ignored"}),
        ],
    )

    streams = build_streams(source, "payments.created", "risk.decisions")
    quarantined = {row.source_offset: set(row.error_codes) for row in streams.quarantine.collect()}

    assert streams.payments.count() == 0
    assert streams.decisions.count() == 0
    assert len(quarantined) == 4
    assert "malformed_json" in quarantined[0]
    assert quarantined[1] >= {"invalid_schema_version", "invalid_currency"}
    assert "invalid_decision_id" in quarantined[2]
    assert quarantined[3] == {"unsupported_topic"}


def test_missing_required_fields_cannot_enter_curated_output(spark: SparkSession) -> None:
    payment = _payment_event()
    del payment["payload"]["customer_id"]
    decision = _decision_event()
    decision["payload"]["reason_codes"] = []
    source = _source(
        spark,
        [("payments.created", 0, payment), ("risk.decisions", 1, decision)],
    )

    streams = build_streams(source, "payments.created", "risk.decisions")
    codes = {row.source_offset: set(row.error_codes) for row in streams.quarantine.collect()}

    assert streams.payments.count() == 0
    assert streams.decisions.count() == 0
    assert {"invalid_customer_id"} <= codes[0]
    assert {"missing_reason_codes"} <= codes[1]


def _source(spark: SparkSession, records: list[tuple[str, int, dict[str, object] | str]]):
    timestamp = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)
    rows = [
        Row(
            source_topic=topic,
            source_partition=partition,
            source_offset=offset,
            kafka_timestamp=timestamp,
            raw_value=value if isinstance(value, str) else json.dumps(value),
        )
        for offset, (topic, partition, value) in enumerate(records)
    ]
    return spark.createDataFrame(rows, SOURCE_SCHEMA)


def _payment_event() -> dict[str, object]:
    return {
        "event_id": "50000000-0000-4000-8000-000000000001",
        "event_type": "payments.created",
        "aggregate_id": "10000000-0000-4000-8000-000000000001",
        "schema_version": 1,
        "occurred_at": "2026-08-30T12:00:00Z",
        "trace_id": "60000000-0000-4000-8000-000000000001",
        "payload": {
            "payment_id": "10000000-0000-4000-8000-000000000001",
            "customer_id": "customer-1",
            "merchant_id": "merchant-1",
            "device_id": "device-1",
            "amount_minor": 125000,
            "currency": "USD",
            "country": "IN",
            "status": "PENDING_RISK",
            "created_at": "2026-08-30T12:00:00Z",
        },
    }


def _decision_event() -> dict[str, object]:
    return {
        "event_id": "70000000-0000-4000-8000-000000000001",
        "event_type": "risk.decision.completed",
        "aggregate_id": "10000000-0000-4000-8000-000000000001",
        "schema_version": 2,
        "occurred_at": "2026-08-30T12:00:05Z",
        "trace_id": "60000000-0000-4000-8000-000000000001",
        "payload": {
            "decision_id": "70000000-0000-4000-8000-000000000001",
            "payment_id": "10000000-0000-4000-8000-000000000001",
            "source_event_id": "50000000-0000-4000-8000-000000000001",
            "decision": "REVIEW",
            "risk_score": 55,
            "rule_score": 55,
            "model_score": 42,
            "model_probability": 0.42,
            "model_review_threshold": 0.4,
            "reason_codes": ["HIGH_AMOUNT", "NEW_DEVICE"],
            "rule_version": "rules-v1",
            "model_version": "xgb-synthetic-v1",
            "decision_at": "2026-08-30T12:00:05Z",
            "features": {
                "velocity_5m": 1,
                "new_device": True,
                "cross_border": False,
                "baseline_country": "IN",
                "decision_at": "2026-08-30T12:00:05Z",
            },
        },
    }
