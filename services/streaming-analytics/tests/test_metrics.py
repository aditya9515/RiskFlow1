from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from streaming_analytics.metrics import (
    aggregate_operational_batch,
    build_operational_observations,
    start_operational_query,
    write_operational_batch,
)


def test_observations_cover_required_operational_families(spark: SparkSession) -> None:
    observations = _observations(spark)
    family_counts = {
        row.metric_family: row["count"]
        for row in observations.groupBy("metric_family").count().collect()
    }

    assert family_counts == {
        "decision_rate": 2,
        "ingestion_latency": 3,
        "payment_volume": 3,
        "quarantine_error": 2,
        "risk_score_distribution": 4,
    }
    latencies = {
        row.dimension_value: (row.latency_ms_count, row.latency_ms_sum)
        for row in aggregate_operational_batch(observations)
        .filter(F.col("metric_family") == "ingestion_latency")
        .collect()
    }
    assert latencies == {
        "payments.created": (1, 250),
        "risk.decisions": (2, 1200),
    }


def test_batch_aggregates_are_mergeable_and_typed(spark: SparkSession) -> None:
    rows = {
        (
            row.metric_family,
            row.dimension_type,
            row.dimension_value,
            row.outcome,
            row.metric_bucket,
        ): row
        for row in aggregate_operational_batch(_observations(spark)).collect()
    }

    overall_payment = rows[("payment_volume", "overall", "ALL", None, None)]
    assert overall_payment.event_count == 1
    assert overall_payment.amount_minor_sum == 125000

    merchant_payment = rows[("payment_volume", "merchant", "merchant-1", None, None)]
    country_payment = rows[("payment_volume", "country", "IN", None, None)]
    assert (merchant_payment.event_count, merchant_payment.amount_minor_sum) == (
        1,
        125000,
    )
    assert (country_payment.event_count, country_payment.amount_minor_sum) == (
        1,
        125000,
    )

    review_rate = rows[("decision_rate", "overall", "ALL", "REVIEW", None)]
    allow_rate = rows[("decision_rate", "overall", "ALL", "ALLOW", None)]
    assert review_rate.denominator_count == 2
    assert review_rate.rate == pytest.approx(0.5)
    assert allow_rate.denominator_count == 2
    assert allow_rate.rate == pytest.approx(0.5)

    score_bucket = rows[("risk_score_distribution", "overall", "ALL", None, "50-59")]
    assert score_bucket.event_count == 1
    assert score_bucket.risk_score_avg == pytest.approx(55.0)

    malformed = rows[
        ("quarantine_error", "source_topic", "payments.created", None, "malformed_json")
    ]
    assert malformed.event_count == 1


def test_retried_batch_overwrites_its_deterministic_partition(
    spark: SparkSession, tmp_path: Path
) -> None:
    output = tmp_path / "retry-output"
    observations = _observations(spark)

    write_operational_batch(observations, 7, str(output))
    write_operational_batch(observations, 7, str(output))

    written = spark.read.parquet(str(output))
    assert written.agg(F.sum("event_count")).first()[0] == observations.count()
    assert written.select("batch_id").distinct().collect()[0].batch_id == 7


def test_operational_checkpoint_restart_only_adds_new_batches(
    spark: SparkSession, tmp_path: Path
) -> None:
    source = tmp_path / "source"
    output = tmp_path / "output"
    checkpoint = tmp_path / "checkpoint"
    observations = _observations(spark)

    observations.write.mode("append").parquet(str(source))
    _run_available_now(spark, observations.schema, source, output, checkpoint)
    first = spark.read.parquet(str(output))
    first_total = first.agg(F.sum("event_count")).first()[0]
    assert first_total == observations.count()
    assert first.select("batch_id").distinct().count() == 1

    (
        observations.filter(F.col("metric_family") == "decision_rate")
        .limit(1)
        .withColumn("source_record_id", F.lit("new-decision-observation"))
        .write.mode("append")
        .parquet(str(source))
    )
    _run_available_now(spark, observations.schema, source, output, checkpoint)
    second = spark.read.parquet(str(output))
    assert second.agg(F.sum("event_count")).first()[0] == first_total + 1
    assert second.select("batch_id").distinct().count() == 2

    _run_available_now(spark, observations.schema, source, output, checkpoint)
    third = spark.read.parquet(str(output))
    assert third.agg(F.sum("event_count")).first()[0] == first_total + 1
    assert third.select("batch_id").distinct().count() == 2


def _run_available_now(
    spark: SparkSession,
    schema: object,
    source: Path,
    output: Path,
    checkpoint: Path,
) -> None:
    observations = spark.readStream.schema(schema).parquet(str(source))
    query = start_operational_query(
        observations,
        output_path=str(output),
        checkpoint_path=str(checkpoint),
        available_now=True,
    )
    assert query.awaitTermination(30)
    assert query.exception() is None


def _observations(spark: SparkSession) -> DataFrame:
    day = date(2026, 8, 30)
    base = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)
    payments = spark.createDataFrame(
        [
            Row(
                event_id="50000000-0000-4000-8000-000000000001",
                event_date=day,
                merchant_id="merchant-1",
                country="IN",
                amount_minor=125000,
                source_topic="payments.created",
                kafka_timestamp=base + timedelta(milliseconds=100),
                ingested_at=base + timedelta(milliseconds=350),
            )
        ]
    )
    decisions = spark.createDataFrame(
        [
            Row(
                event_id="70000000-0000-4000-8000-000000000001",
                event_date=day,
                decision="REVIEW",
                risk_score=55,
                source_topic="risk.decisions",
                kafka_timestamp=base + timedelta(seconds=1, milliseconds=100),
                ingested_at=base + timedelta(seconds=1, milliseconds=600),
            ),
            Row(
                event_id="70000000-0000-4000-8000-000000000002",
                event_date=day,
                decision="ALLOW",
                risk_score=20,
                source_topic="risk.decisions",
                kafka_timestamp=base + timedelta(seconds=2, milliseconds=100),
                ingested_at=base + timedelta(seconds=2, milliseconds=800),
            ),
        ]
    )
    quarantine = spark.createDataFrame(
        [
            Row(
                event_date=day,
                source_topic="payments.created",
                source_partition=0,
                source_offset=10,
                error_codes=["malformed_json", "invalid_event_type"],
            )
        ]
    )
    return build_operational_observations(payments, decisions, quarantine)
