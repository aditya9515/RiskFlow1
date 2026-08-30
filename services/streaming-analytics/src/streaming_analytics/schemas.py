from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PAYMENT_PAYLOAD_SCHEMA = StructType(
    [
        StructField("payment_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("device_id", StringType()),
        StructField("amount_minor", LongType()),
        StructField("currency", StringType()),
        StructField("country", StringType()),
        StructField("status", StringType()),
        StructField("created_at", TimestampType()),
    ]
)

FEATURE_SCHEMA = StructType(
    [
        StructField("velocity_5m", IntegerType()),
        StructField("new_device", BooleanType()),
        StructField("cross_border", BooleanType()),
        StructField("baseline_country", StringType()),
        StructField("decision_at", TimestampType()),
    ]
)

DECISION_PAYLOAD_SCHEMA = StructType(
    [
        StructField("decision_id", StringType()),
        StructField("payment_id", StringType()),
        StructField("source_event_id", StringType()),
        StructField("decision", StringType()),
        StructField("risk_score", IntegerType()),
        StructField("rule_score", IntegerType()),
        StructField("model_score", IntegerType()),
        StructField("model_probability", DoubleType()),
        StructField("model_review_threshold", DoubleType()),
        StructField("reason_codes", ArrayType(StringType(), containsNull=False)),
        StructField("rule_version", StringType()),
        StructField("model_version", StringType()),
        StructField("decision_at", TimestampType()),
        StructField("features", FEATURE_SCHEMA),
    ]
)


def envelope_schema(payload: StructType) -> StructType:
    return StructType(
        [
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("aggregate_id", StringType()),
            StructField("schema_version", IntegerType()),
            StructField("occurred_at", TimestampType()),
            StructField("trace_id", StringType()),
            StructField("payload", payload),
            StructField("_corrupt_record", StringType()),
        ]
    )


PAYMENT_ENVELOPE_SCHEMA = envelope_schema(PAYMENT_PAYLOAD_SCHEMA)
DECISION_ENVELOPE_SCHEMA = envelope_schema(DECISION_PAYLOAD_SCHEMA)
