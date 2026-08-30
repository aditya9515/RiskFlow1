from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from streaming_analytics.schemas import DECISION_ENVELOPE_SCHEMA, PAYMENT_ENVELOPE_SCHEMA

UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
UTC_TIMESTAMP_PATTERN = r".*(Z|[+-][0-9]{2}:[0-9]{2})$"
REASON_CODE_PATTERN = r"^[A-Z0-9_]{1,100}$"


@dataclass(frozen=True)
class Streams:
    payments: DataFrame
    decisions: DataFrame
    quarantine: DataFrame


def normalize_kafka_source(kafka: DataFrame) -> DataFrame:
    """Keep broker coordinates with each value so every lake row is traceable."""
    return kafka.select(
        F.col("topic").cast("string").alias("source_topic"),
        F.col("partition").cast("int").alias("source_partition"),
        F.col("offset").cast("long").alias("source_offset"),
        F.col("timestamp").cast("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_value"),
    )


def build_streams(source: DataFrame, payments_topic: str, decisions_topic: str) -> Streams:
    payments_parsed = _parse_topic(source, payments_topic, PAYMENT_ENVELOPE_SCHEMA)
    payment_errors = _payment_errors()
    payments_validated = payments_parsed.withColumn("validation_errors", payment_errors)

    decisions_parsed = _parse_topic(source, decisions_topic, DECISION_ENVELOPE_SCHEMA)
    decision_errors = _decision_errors()
    decisions_validated = decisions_parsed.withColumn("validation_errors", decision_errors)

    payments = payments_validated.filter(F.size("validation_errors") == 0).select(
        F.lower("event.event_id").alias("event_id"),
        F.col("event.event_type").alias("event_type"),
        F.lower("event.aggregate_id").alias("aggregate_id"),
        F.col("event.schema_version").alias("schema_version"),
        F.col("event.occurred_at").alias("occurred_at"),
        F.lower("event.trace_id").alias("trace_id"),
        F.lower("event.payload.payment_id").alias("payment_id"),
        F.trim("event.payload.customer_id").alias("customer_id"),
        F.trim("event.payload.merchant_id").alias("merchant_id"),
        F.trim("event.payload.device_id").alias("device_id"),
        F.col("event.payload.amount_minor").alias("amount_minor"),
        F.col("event.payload.currency").alias("currency"),
        F.col("event.payload.country").alias("country"),
        F.col("event.payload.status").alias("status"),
        F.col("event.payload.created_at").alias("created_at"),
        "source_topic",
        "source_partition",
        "source_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("ingested_at"),
        F.to_date("event.occurred_at").alias("event_date"),
    )

    decisions = decisions_validated.filter(F.size("validation_errors") == 0).select(
        F.lower("event.event_id").alias("event_id"),
        F.col("event.event_type").alias("event_type"),
        F.lower("event.aggregate_id").alias("aggregate_id"),
        F.col("event.schema_version").alias("schema_version"),
        F.col("event.occurred_at").alias("occurred_at"),
        F.lower("event.trace_id").alias("trace_id"),
        F.lower("event.payload.decision_id").alias("decision_id"),
        F.lower("event.payload.payment_id").alias("payment_id"),
        F.lower("event.payload.source_event_id").alias("source_event_id"),
        F.col("event.payload.decision").alias("decision"),
        F.col("event.payload.risk_score").alias("risk_score"),
        F.col("event.payload.rule_score").alias("rule_score"),
        F.col("event.payload.model_score").alias("model_score"),
        F.col("event.payload.model_probability").alias("model_probability"),
        F.col("event.payload.model_review_threshold").alias("model_review_threshold"),
        F.col("event.payload.reason_codes").alias("reason_codes"),
        F.trim("event.payload.rule_version").alias("rule_version"),
        F.trim("event.payload.model_version").alias("model_version"),
        F.col("event.payload.decision_at").alias("decision_at"),
        F.col("event.payload.features.velocity_5m").alias("velocity_5m"),
        F.col("event.payload.features.new_device").alias("new_device"),
        F.col("event.payload.features.cross_border").alias("cross_border"),
        F.col("event.payload.features.baseline_country").alias("baseline_country"),
        "source_topic",
        "source_partition",
        "source_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("ingested_at"),
        F.to_date("event.occurred_at").alias("event_date"),
    )

    invalid_payments = _quarantine_projection(
        payments_validated.filter(F.size("validation_errors") > 0)
    )
    invalid_decisions = _quarantine_projection(
        decisions_validated.filter(F.size("validation_errors") > 0)
    )
    unknown_topics = _quarantine_projection(
        source.filter(~F.col("source_topic").isin(payments_topic, decisions_topic)).withColumn(
            "validation_errors", F.array(F.lit("unsupported_topic"))
        )
    )
    quarantine = invalid_payments.unionByName(invalid_decisions).unionByName(unknown_topics)

    return Streams(payments=payments, decisions=decisions, quarantine=quarantine)


def _parse_topic(source: DataFrame, topic: str, schema: object) -> DataFrame:
    return source.filter(F.col("source_topic") == topic).withColumn(
        "event",
        F.from_json(
            "raw_value",
            schema,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )


def _payment_errors() -> Column:
    conditions = [
        _error(
            F.col("event").isNull() | F.col("event._corrupt_record").isNotNull(), "malformed_json"
        ),
        _error(
            F.col("event.event_type").isNull() | (F.col("event.event_type") != "payments.created"),
            "invalid_event_type",
        ),
        _error(
            F.col("event.schema_version").isNull() | (F.col("event.schema_version") != 1),
            "invalid_schema_version",
        ),
        _required_uuid("event.event_id", "invalid_event_id"),
        _required_uuid("event.aggregate_id", "invalid_aggregate_id"),
        _required_uuid("event.trace_id", "invalid_trace_id"),
        _error(F.col("event.occurred_at").isNull(), "missing_occurred_at"),
        _error(
            ~_raw_timestamp("$.occurred_at").rlike(UTC_TIMESTAMP_PATTERN), "occurred_at_not_utc"
        ),
        _required_uuid("event.payload.payment_id", "invalid_payment_id"),
        _required_text("event.payload.customer_id", "invalid_customer_id"),
        _required_text("event.payload.merchant_id", "invalid_merchant_id"),
        _required_text("event.payload.device_id", "invalid_device_id"),
        _error(
            F.col("event.payload.amount_minor").isNull()
            | (F.col("event.payload.amount_minor") <= 0),
            "invalid_amount_minor",
        ),
        _error(
            F.col("event.payload.currency").isNull()
            | ~F.col("event.payload.currency").rlike(r"^[A-Z]{3}$"),
            "invalid_currency",
        ),
        _error(
            F.col("event.payload.country").isNull()
            | ~F.col("event.payload.country").rlike(r"^[A-Z]{2}$"),
            "invalid_country",
        ),
        _error(
            F.col("event.payload.status").isNull()
            | (F.col("event.payload.status") != "PENDING_RISK"),
            "invalid_payment_status",
        ),
        _error(F.col("event.payload.created_at").isNull(), "missing_created_at"),
        _error(
            ~_raw_timestamp("$.payload.created_at").rlike(UTC_TIMESTAMP_PATTERN),
            "created_at_not_utc",
        ),
        _error(
            F.lower("event.aggregate_id") != F.lower("event.payload.payment_id"),
            "aggregate_payment_mismatch",
        ),
    ]
    return F.array_compact(F.array(*conditions))


def _decision_errors() -> Column:
    probability = F.col("event.payload.model_probability")
    threshold = F.col("event.payload.model_review_threshold")
    reason_codes = F.col("event.payload.reason_codes")
    conditions = [
        _error(
            F.col("event").isNull() | F.col("event._corrupt_record").isNotNull(), "malformed_json"
        ),
        _error(
            F.col("event.event_type").isNull()
            | (F.col("event.event_type") != "risk.decision.completed"),
            "invalid_event_type",
        ),
        _error(
            F.col("event.schema_version").isNull() | (F.col("event.schema_version") != 2),
            "invalid_schema_version",
        ),
        _required_uuid("event.event_id", "invalid_event_id"),
        _required_uuid("event.aggregate_id", "invalid_aggregate_id"),
        _required_uuid("event.trace_id", "invalid_trace_id"),
        _error(F.col("event.occurred_at").isNull(), "missing_occurred_at"),
        _error(
            ~_raw_timestamp("$.occurred_at").rlike(UTC_TIMESTAMP_PATTERN), "occurred_at_not_utc"
        ),
        _required_uuid("event.payload.decision_id", "invalid_decision_id"),
        _required_uuid("event.payload.payment_id", "invalid_payment_id"),
        _required_uuid("event.payload.source_event_id", "invalid_source_event_id"),
        _error(
            F.col("event.payload.decision").isNull()
            | ~F.col("event.payload.decision").isin("ALLOW", "REVIEW", "BLOCK"),
            "invalid_decision",
        ),
        _score_error("event.payload.risk_score", "invalid_risk_score"),
        _score_error("event.payload.rule_score", "invalid_rule_score"),
        _score_error("event.payload.model_score", "invalid_model_score"),
        _error(
            probability.isNull() | F.isnan(probability) | (probability < 0) | (probability > 1),
            "invalid_model_probability",
        ),
        _error(
            threshold.isNull() | F.isnan(threshold) | (threshold <= 0) | (threshold >= 1),
            "invalid_model_threshold",
        ),
        _error(reason_codes.isNull() | (F.size(reason_codes) == 0), "missing_reason_codes"),
        _error(
            F.exists(
                reason_codes, lambda value: value.isNull() | ~value.rlike(REASON_CODE_PATTERN)
            ),
            "invalid_reason_code",
        ),
        _required_text("event.payload.rule_version", "invalid_rule_version", 255),
        _required_text("event.payload.model_version", "invalid_model_version", 255),
        _error(F.col("event.payload.decision_at").isNull(), "missing_decision_at"),
        _error(
            ~_raw_timestamp("$.payload.decision_at").rlike(UTC_TIMESTAMP_PATTERN),
            "decision_at_not_utc",
        ),
        _error(
            F.col("event.payload.features.velocity_5m").isNull()
            | (F.col("event.payload.features.velocity_5m") < 1),
            "invalid_velocity_5m",
        ),
        _error(F.col("event.payload.features.new_device").isNull(), "missing_new_device"),
        _error(F.col("event.payload.features.cross_border").isNull(), "missing_cross_border"),
        _error(
            F.col("event.payload.features.baseline_country").isNull()
            | ~F.col("event.payload.features.baseline_country").rlike(r"^[A-Z]{2}$"),
            "invalid_baseline_country",
        ),
        _error(F.col("event.payload.features.decision_at").isNull(), "missing_feature_decision_at"),
        _error(
            F.lower("event.event_id") != F.lower("event.payload.decision_id"),
            "event_decision_mismatch",
        ),
        _error(
            F.lower("event.aggregate_id") != F.lower("event.payload.payment_id"),
            "aggregate_payment_mismatch",
        ),
        _error(
            F.col("event.occurred_at") != F.col("event.payload.decision_at"),
            "occurred_decision_time_mismatch",
        ),
        _error(
            F.col("event.payload.decision_at") != F.col("event.payload.features.decision_at"),
            "feature_decision_time_mismatch",
        ),
    ]
    return F.array_compact(F.array(*conditions))


def _quarantine_projection(frame: DataFrame) -> DataFrame:
    return frame.select(
        "source_topic",
        "source_partition",
        "source_offset",
        "kafka_timestamp",
        "raw_value",
        F.col("validation_errors").alias("error_codes"),
        F.current_timestamp().alias("quarantined_at"),
        F.to_date("kafka_timestamp").alias("event_date"),
    )


def _required_uuid(path: str, code: str) -> Column:
    value = F.col(path)
    return _error(value.isNull() | ~value.rlike(UUID_PATTERN), code)


def _required_text(path: str, code: str, maximum: int = 255) -> Column:
    value = F.trim(F.col(path))
    return _error(value.isNull() | (F.length(value) == 0) | (F.length(value) > maximum), code)


def _score_error(path: str, code: str) -> Column:
    value = F.col(path)
    return _error(value.isNull() | (value < 0) | (value > 100), code)


def _raw_timestamp(path: str) -> Column:
    return F.get_json_object("raw_value", path)


def _error(condition: Column, code: str) -> Column:
    return F.when(condition, F.lit(code))
