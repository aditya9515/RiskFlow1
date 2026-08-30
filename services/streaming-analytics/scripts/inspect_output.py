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
        print(json.dumps(result, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
