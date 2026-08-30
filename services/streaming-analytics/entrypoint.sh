#!/usr/bin/env bash
set -euo pipefail

KAFKA_JARS="$(find /opt/spark/.ivy2/jars -type f -name '*.jar' -print | sort | paste -sd, -)"
if [[ -z "${KAFKA_JARS}" ]]; then
    echo "Kafka connector jars are missing from the image" >&2
    exit 1
fi

export PYSPARK_SUBMIT_ARGS="--master ${SPARK_MASTER:-local[2]} \
--jars ${KAFKA_JARS} \
--conf spark.ui.enabled=false \
--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
--conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
pyspark-shell"

exec python3 /opt/riskflow/src/streaming_analytics/main.py
