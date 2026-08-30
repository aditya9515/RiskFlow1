from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType

from streaming_analytics.sink import start_parquet_query

RECOVERY_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_date", DateType(), False),
    ]
)


def test_checkpoint_restart_processes_only_new_source_files(
    spark: SparkSession, tmp_path: Path
) -> None:
    source_path = tmp_path / "source"
    output_path = tmp_path / "output"
    checkpoint_path = tmp_path / "checkpoint"

    _append_input(spark, source_path, "event-1")
    _run_available_now(spark, source_path, output_path, checkpoint_path)
    assert _output_ids(spark, output_path) == {"event-1"}

    _append_input(spark, source_path, "event-2")
    _run_available_now(spark, source_path, output_path, checkpoint_path)
    assert _output_ids(spark, output_path) == {"event-1", "event-2"}

    # A second restart with no new input must not duplicate committed files.
    _run_available_now(spark, source_path, output_path, checkpoint_path)
    assert _output_ids(spark, output_path) == {"event-1", "event-2"}
    assert (output_path / "event_date=2026-08-30").is_dir()


def _append_input(spark: SparkSession, path: Path, event_id: str) -> None:
    spark.createDataFrame(
        [Row(event_id=event_id, event_date=date(2026, 8, 30))], RECOVERY_SCHEMA
    ).write.mode("append").parquet(str(path))


def _run_available_now(spark: SparkSession, source: Path, output: Path, checkpoint: Path) -> None:
    frame = spark.readStream.schema(RECOVERY_SCHEMA).parquet(str(source))
    query = start_parquet_query(
        frame,
        output_path=str(output),
        checkpoint_path=str(checkpoint),
        query_name="checkpoint_recovery_test",
        partition_columns=["event_date"],
        available_now=True,
    )
    assert query.awaitTermination(30)
    assert query.exception() is None


def _output_ids(spark: SparkSession, output: Path) -> set[str]:
    return {row.event_id for row in spark.read.parquet(str(output)).select("event_id").collect()}
