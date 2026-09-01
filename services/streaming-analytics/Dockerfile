FROM apache/spark:4.1.3-python3 AS runtime

USER root

RUN mkdir -p /opt/riskflow/scripts /opt/spark/.ivy2 /var/lib/riskflow/streaming && \
    chown -R spark:spark /opt/riskflow /opt/spark/.ivy2 /var/lib/riskflow

USER spark

COPY --chown=spark:spark services/streaming-analytics/scripts/verify_runtime.py /opt/riskflow/scripts/verify_runtime.py

RUN /opt/spark/bin/spark-submit \
    --master local[1] \
    --conf spark.ui.enabled=false \
    --conf spark.jars.ivy=/opt/spark/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 \
    /opt/riskflow/scripts/verify_runtime.py

COPY --chown=spark:spark services/streaming-analytics/ /opt/riskflow/

ENV PYTHONPATH=/opt/riskflow/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYTHONUNBUFFERED=1

USER root
RUN chmod 0555 /opt/riskflow/entrypoint.sh
USER spark

ENTRYPOINT ["/opt/riskflow/entrypoint.sh"]

FROM runtime AS test

USER root
RUN python3 -m pip install --no-cache-dir coverage==7.15.4 pytest==9.1.1 ruff==0.16.5
USER spark

ENTRYPOINT []
CMD ["bash", "-lc", "python3 -m ruff format --check /opt/riskflow && python3 -m ruff check /opt/riskflow && python3 -m coverage run --branch --source=/opt/riskflow/src/streaming_analytics -m pytest -q /opt/riskflow/tests && python3 -m coverage report --show-missing"]
