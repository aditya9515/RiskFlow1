from __future__ import annotations

import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> int:
    if len(sys.argv) != 2:
        raise ValueError("usage: inspect_output.py OUTPUT_ROOT")
    root = Path(sys.argv[1])
    spark = (
        SparkSession.builder.appName("RiskFlowStreamingOutputInspection")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    result: dict[str, object] = {}
    try:
        for name in ("payments", "risk_decisions"):
            path = root / name
            if not path.exists():
                result[name] = {"rows": 0, "distinct_event_ids": 0}
                continue
            frame = spark.read.parquet(str(path))
            row = frame.agg(
                F.count("*").alias("rows"),
                F.countDistinct("event_id").alias("distinct_event_ids"),
            ).first()
            result[name] = row.asDict()

        quarantine_path = root / "quarantine"
        if quarantine_path.exists():
            quarantine = spark.read.parquet(str(quarantine_path))
            error_counts = {
                row["error_code"]: row["count"]
                for row in quarantine.select(F.explode("error_codes").alias("error_code"))
                .groupBy("error_code")
                .count()
                .collect()
            }
            result["quarantine"] = {
                "rows": quarantine.count(),
                "error_counts": error_counts,
            }
        else:
            result["quarantine"] = {"rows": 0, "error_counts": {}}

        metrics_path = root / "operational_metrics"
        if metrics_path.exists():
            metrics = spark.read.parquet(str(metrics_path))
            overall_payments = (
                metrics.filter(
                    (F.col("metric_family") == "payment_volume")
                    & (F.col("dimension_type") == "overall")
                )
                .agg(
                    F.sum("event_count").alias("payment_count"),
                    F.sum("amount_minor_sum").alias("amount_minor_sum"),
                )
                .first()
            )
            payment_breakdowns = {
                dimension: {
                    row["dimension_value"]: {
                        "payment_count": row["payment_count"],
                        "amount_minor_sum": row["amount_minor_sum"],
                    }
                    for row in metrics.filter(
                        (F.col("metric_family") == "payment_volume")
                        & (F.col("dimension_type") == dimension)
                    )
                    .groupBy("dimension_value")
                    .agg(
                        F.sum("event_count").alias("payment_count"),
                        F.sum("amount_minor_sum").alias("amount_minor_sum"),
                    )
                    .collect()
                }
                for dimension in ("merchant", "country")
            }
            decision_counts = {
                row["outcome"]: row["count"]
                for row in metrics.filter(F.col("metric_family") == "decision_rate")
                .groupBy("outcome")
                .agg(F.sum("event_count").alias("count"))
                .collect()
            }
            decision_total = sum(decision_counts.values())
            decision_rates = {
                outcome: count / decision_total for outcome, count in decision_counts.items()
            }
            score_buckets = {
                row["metric_bucket"]: row["count"]
                for row in metrics.filter(
                    (F.col("metric_family") == "risk_score_distribution")
                    & (F.col("dimension_type") == "overall")
                )
                .groupBy("metric_bucket")
                .agg(F.sum("event_count").alias("count"))
                .collect()
            }
            latency_by_topic = {
                row["dimension_value"]: {
                    "count": row["count"],
                    "average_ms": row["latency_ms_sum"] / row["count"],
                    "minimum_ms": row["minimum_ms"],
                    "maximum_ms": row["maximum_ms"],
                }
                for row in metrics.filter(F.col("metric_family") == "ingestion_latency")
                .groupBy("dimension_value")
                .agg(
                    F.sum("latency_ms_count").alias("count"),
                    F.sum("latency_ms_sum").alias("latency_ms_sum"),
                    F.min("latency_ms_min").alias("minimum_ms"),
                    F.max("latency_ms_max").alias("maximum_ms"),
                )
                .collect()
            }
            quarantine_metric_counts = {
                row["metric_bucket"]: row["count"]
                for row in metrics.filter(F.col("metric_family") == "quarantine_error")
                .groupBy("metric_bucket")
                .agg(F.sum("event_count").alias("count"))
                .collect()
            }
            result["operational_metrics"] = {
                "rows": metrics.count(),
                "batches": metrics.select("batch_id").distinct().count(),
                "payment_count": overall_payments["payment_count"] or 0,
                "amount_minor_sum": overall_payments["amount_minor_sum"] or 0,
                "payments_by_merchant": payment_breakdowns["merchant"],
                "payments_by_country": payment_breakdowns["country"],
                "decision_counts": decision_counts,
                "decision_rates": decision_rates,
                "risk_score_buckets": score_buckets,
                "ingestion_latency_by_topic": latency_by_topic,
                "quarantine_error_counts": quarantine_metric_counts,
            }
        else:
            result["operational_metrics"] = {
                "rows": 0,
                "batches": 0,
                "payment_count": 0,
                "amount_minor_sum": 0,
                "payments_by_merchant": {},
                "payments_by_country": {},
                "decision_counts": {},
                "decision_rates": {},
                "risk_score_buckets": {},
                "ingestion_latency_by_topic": {},
                "quarantine_error_counts": {},
            }
        print(json.dumps(result, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
