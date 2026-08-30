from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery


def build_operational_observations(
    payments: DataFrame,
    decisions: DataFrame,
    quarantine: DataFrame,
) -> DataFrame:
    """Convert valid and quarantined events into one mergeable metric input schema."""
    payment_dimensions = F.array(
        F.struct(F.lit("overall").alias("type"), F.lit("ALL").alias("value")),
        F.struct(F.lit("merchant").alias("type"), F.col("merchant_id").alias("value")),
        F.struct(F.lit("country").alias("type"), F.col("country").alias("value")),
    )
    payment_volume = payments.withColumn("dimension", F.explode(payment_dimensions)).select(
        F.col("event_id").alias("source_record_id"),
        "event_date",
        F.lit("payment_volume").alias("metric_family"),
        F.col("dimension.type").alias("dimension_type"),
        F.col("dimension.value").alias("dimension_value"),
        _null_string("outcome"),
        _null_string("metric_bucket"),
        F.lit(1).cast("long").alias("event_count_value"),
        F.col("amount_minor").cast("long").alias("amount_minor_value"),
        _null_long("risk_score_value"),
        _null_long("latency_ms_value"),
    )

    decision_rate = decisions.select(
        F.col("event_id").alias("source_record_id"),
        "event_date",
        F.lit("decision_rate").alias("metric_family"),
        F.lit("overall").alias("dimension_type"),
        F.lit("ALL").alias("dimension_value"),
        F.col("decision").alias("outcome"),
        _null_string("metric_bucket"),
        F.lit(1).cast("long").alias("event_count_value"),
        _null_long("amount_minor_value"),
        _null_long("risk_score_value"),
        _null_long("latency_ms_value"),
    )

    score_lower_bound = F.least(
        F.floor(F.col("risk_score") / F.lit(10)) * F.lit(10), F.lit(90)
    ).cast("int")
    score_upper_bound = F.when(score_lower_bound == 90, F.lit(100)).otherwise(score_lower_bound + 9)
    score_dimensions = F.array(
        F.struct(F.lit("overall").alias("type"), F.lit("ALL").alias("value")),
        F.struct(F.lit("decision").alias("type"), F.col("decision").alias("value")),
    )
    risk_scores = decisions.withColumn("dimension", F.explode(score_dimensions)).select(
        F.col("event_id").alias("source_record_id"),
        "event_date",
        F.lit("risk_score_distribution").alias("metric_family"),
        F.col("dimension.type").alias("dimension_type"),
        F.col("dimension.value").alias("dimension_value"),
        _null_string("outcome"),
        F.format_string("%02d-%02d", score_lower_bound, score_upper_bound).alias("metric_bucket"),
        F.lit(1).cast("long").alias("event_count_value"),
        _null_long("amount_minor_value"),
        F.col("risk_score").cast("long").alias("risk_score_value"),
        _null_long("latency_ms_value"),
    )

    payment_latency = _ingestion_latency_observations(payments)
    decision_latency = _ingestion_latency_observations(decisions)

    quarantine_errors = quarantine.withColumn("error_code", F.explode("error_codes")).select(
        F.concat_ws(
            ":",
            "source_topic",
            F.col("source_partition").cast("string"),
            F.col("source_offset").cast("string"),
            "error_code",
        ).alias("source_record_id"),
        "event_date",
        F.lit("quarantine_error").alias("metric_family"),
        F.lit("source_topic").alias("dimension_type"),
        F.col("source_topic").alias("dimension_value"),
        _null_string("outcome"),
        F.col("error_code").alias("metric_bucket"),
        F.lit(1).cast("long").alias("event_count_value"),
        _null_long("amount_minor_value"),
        _null_long("risk_score_value"),
        _null_long("latency_ms_value"),
    )

    return (
        payment_volume.unionByName(decision_rate)
        .unionByName(risk_scores)
        .unionByName(payment_latency)
        .unionByName(decision_latency)
        .unionByName(quarantine_errors)
    )


def aggregate_operational_batch(observations: DataFrame) -> DataFrame:
    """Create additive daily deltas that remain mathematically mergeable across batches."""
    dimensions = [
        "event_date",
        "metric_family",
        "dimension_type",
        "dimension_value",
        "outcome",
        "metric_bucket",
    ]
    aggregated = observations.groupBy(*dimensions).agg(
        F.sum("event_count_value").cast("long").alias("event_count"),
        F.sum("amount_minor_value").cast("long").alias("amount_minor_sum"),
        F.sum("risk_score_value").cast("long").alias("risk_score_sum"),
        F.count("risk_score_value").cast("long").alias("risk_score_count"),
        F.min("risk_score_value").cast("int").alias("risk_score_min"),
        F.max("risk_score_value").cast("int").alias("risk_score_max"),
        F.sum("latency_ms_value").cast("long").alias("latency_ms_sum"),
        F.count("latency_ms_value").cast("long").alias("latency_ms_count"),
        F.min("latency_ms_value").cast("long").alias("latency_ms_min"),
        F.max("latency_ms_value").cast("long").alias("latency_ms_max"),
    )
    decision_partition = Window.partitionBy(
        "event_date", "metric_family", "dimension_type", "dimension_value"
    )
    return (
        aggregated.withColumn(
            "denominator_count",
            F.when(
                F.col("metric_family") == "decision_rate",
                F.sum("event_count").over(decision_partition).cast("long"),
            ).cast("long"),
        )
        .withColumn(
            "rate",
            F.when(
                F.col("denominator_count") > 0,
                F.col("event_count") / F.col("denominator_count"),
            ).cast("double"),
        )
        .withColumn(
            "risk_score_avg",
            F.when(
                F.col("risk_score_count") > 0,
                F.col("risk_score_sum") / F.col("risk_score_count"),
            ).cast("double"),
        )
        .withColumn(
            "latency_ms_avg",
            F.when(
                F.col("latency_ms_count") > 0,
                F.col("latency_ms_sum") / F.col("latency_ms_count"),
            ).cast("double"),
        )
        .withColumn("calculated_at", F.current_timestamp())
    )


def write_operational_batch(observations: DataFrame, batch_id: int, output_path: str) -> None:
    """Overwrite one deterministic batch partition so a foreachBatch retry is idempotent."""
    batch_path = Path(output_path) / f"batch_id={batch_id:020d}"
    (
        aggregate_operational_batch(observations)
        .write.mode("overwrite")
        .partitionBy("event_date", "metric_family")
        .parquet(str(batch_path))
    )


def start_operational_query(
    observations: DataFrame,
    *,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str | None = None,
    available_now: bool = False,
) -> StreamingQuery:
    def write_batch(frame: DataFrame, batch_id: int) -> None:
        write_operational_batch(frame, batch_id, output_path)

    writer = (
        observations.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .queryName("riskflow_operational_metrics")
        .foreachBatch(write_batch)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    elif trigger_interval is not None:
        writer = writer.trigger(processingTime=trigger_interval)
    else:
        raise ValueError("trigger_interval is required unless available_now is enabled")
    return writer.start()


def _ingestion_latency_observations(frame: DataFrame) -> DataFrame:
    latency_ms = F.greatest(
        F.lit(0), F.unix_millis("ingested_at") - F.unix_millis("kafka_timestamp")
    ).cast("long")
    return frame.select(
        F.col("event_id").alias("source_record_id"),
        "event_date",
        F.lit("ingestion_latency").alias("metric_family"),
        F.lit("source_topic").alias("dimension_type"),
        F.col("source_topic").alias("dimension_value"),
        _null_string("outcome"),
        _null_string("metric_bucket"),
        F.lit(1).cast("long").alias("event_count_value"),
        _null_long("amount_minor_value"),
        _null_long("risk_score_value"),
        latency_ms.alias("latency_ms_value"),
    )


def _null_string(name: str):
    return F.lit(None).cast("string").alias(name)


def _null_long(name: str):
    return F.lit(None).cast("long").alias(name)
