from __future__ import annotations

import logging
import signal
import sys
from pathlib import PurePosixPath
from threading import Event

from pyspark.sql import SparkSession

from streaming_analytics.config import Settings
from streaming_analytics.metrics import build_operational_observations, start_operational_query
from streaming_analytics.sink import start_parquet_query
from streaming_analytics.transforms import build_streams, normalize_kafka_source


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format='{"level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
    )
    logger = logging.getLogger("streaming-analytics")
    try:
        settings = Settings.from_env()
    except ValueError as error:
        logger.error("invalid configuration: %s", error)
        return 1

    spark = _spark_session()
    spark.sparkContext.setLogLevel("WARN")
    stop_requested = Event()
    _register_shutdown(stop_requested)

    queries = []
    try:
        kafka = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_brokers)
            .option("subscribe", f"{settings.payments_topic},{settings.decisions_topic}")
            .option("startingOffsets", settings.starting_offsets)
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
            .load()
        )
        streams = build_streams(
            normalize_kafka_source(kafka), settings.payments_topic, settings.decisions_topic
        )
        payments = streams.payments.withWatermark(
            "occurred_at", "7 days"
        ).dropDuplicatesWithinWatermark(["event_id"])
        decisions = streams.decisions.withWatermark(
            "occurred_at", "7 days"
        ).dropDuplicatesWithinWatermark(["event_id"])
        operational_observations = build_operational_observations(
            payments, decisions, streams.quarantine
        )

        queries.extend(
            [
                start_parquet_query(
                    payments,
                    output_path=_path(settings.output_root, "payments"),
                    checkpoint_path=_path(settings.checkpoint_root, "payments"),
                    query_name="riskflow_payments_curated",
                    partition_columns=["event_date"],
                    trigger_interval=settings.trigger_interval,
                ),
                start_parquet_query(
                    decisions,
                    output_path=_path(settings.output_root, "risk_decisions"),
                    checkpoint_path=_path(settings.checkpoint_root, "risk_decisions"),
                    query_name="riskflow_decisions_curated",
                    partition_columns=["event_date", "decision"],
                    trigger_interval=settings.trigger_interval,
                ),
                start_parquet_query(
                    streams.quarantine,
                    output_path=_path(settings.output_root, "quarantine"),
                    checkpoint_path=_path(settings.checkpoint_root, "quarantine"),
                    query_name="riskflow_events_quarantine",
                    partition_columns=["event_date", "source_topic"],
                    trigger_interval=settings.trigger_interval,
                ),
                start_operational_query(
                    operational_observations,
                    output_path=_path(settings.output_root, "operational_metrics"),
                    checkpoint_path=_path(settings.checkpoint_root, "operational_metrics"),
                    trigger_interval=settings.trigger_interval,
                ),
            ]
        )
        logger.info("started %d checkpointed streaming queries", len(queries))
        while not stop_requested.wait(1):
            terminated = [query for query in queries if not query.isActive]
            if terminated:
                for query in terminated:
                    logger.error("query %s terminated: %s", query.name, query.exception())
                return 1
        return 0
    except Exception:
        logger.exception("streaming analytics failed")
        return 1
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()
        logger.info("streaming analytics stopped")


def _spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("RiskFlowStreamingAnalytics")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def _register_shutdown(stop_requested: Event) -> None:
    def request_stop(_signum: int, _frame: object) -> None:
        stop_requested.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)


def _path(root: str, child: str) -> str:
    return str(PurePosixPath(root) / child)


if __name__ == "__main__":
    sys.exit(main())
