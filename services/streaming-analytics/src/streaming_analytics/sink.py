from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def start_parquet_query(
    frame: DataFrame,
    *,
    output_path: str,
    checkpoint_path: str,
    query_name: str,
    partition_columns: Sequence[str],
    trigger_interval: str | None = None,
    available_now: bool = False,
) -> StreamingQuery:
    writer = (
        frame.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .queryName(query_name)
        .partitionBy(*partition_columns)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    elif trigger_interval is not None:
        writer = writer.trigger(processingTime=trigger_interval)
    else:
        raise ValueError("trigger_interval is required unless available_now is enabled")
    return writer.start()
