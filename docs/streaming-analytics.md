# Streaming analytics

Checkpoint 5A established the lake-ingestion boundary. Checkpoint 5B adds restart-safe, mergeable operational aggregates over the same validated streams. Spark Structured Streaming reads the existing payment and risk-decision contracts, validates them, stores append-only curated Parquet data, quarantines invalid records, and writes daily aggregate deltas for operational reporting.

## Runtime

- Apache Spark `4.1.3` with the official `apache/spark:4.1.3-python3` image.
- Scala `2.13` Kafka connector `spark-sql-kafka-0-10_2.13:4.1.3`.
- Connector and transitive JARs are resolved while the image is built, so container startup does not depend on Maven Central.
- Java `21` and Python `3.10` come from the pinned Spark image.

The Docker image build runs a real one-row Spark job. This catches an unusable Java/Python/Spark combination before the runtime image is accepted.

## Input contracts

One Kafka source subscribes to both configured topics while retaining `topic`, `partition`, `offset`, and broker timestamp for lineage.

`payments.created` must use envelope schema version `1`. Its payload contains the payment ID, customer, merchant, device, positive integer minor-unit amount, uppercase three-letter currency, uppercase two-letter country, `PENDING_RISK` status, and UTC creation time.

`risk.decision.completed` must use envelope schema version `2`. Its payload contains decision/payment/source IDs, `ALLOW`, `REVIEW`, or `BLOCK`, bounded rule/model/final scores, probability and threshold, reason codes, rule/model versions, decision time, and the online feature snapshot.

Validation also checks UUID shape, required text, UTC offsets, event type, version, matching aggregate/payment IDs, matching decision/event IDs, and consistent decision timestamps. Parsing uses explicit Spark `StructType` definitions rather than schema inference.

## Output layout

The default root is `/var/lib/riskflow/streaming/data` in the named `streaming_data` volume:

```text
data/
  payments/event_date=YYYY-MM-DD/*.parquet
  risk_decisions/event_date=YYYY-MM-DD/decision=ALLOW|REVIEW|BLOCK/*.parquet
  quarantine/event_date=YYYY-MM-DD/source_topic=<topic>/*.parquet
  operational_metrics/
    batch_id=<20-digit-id>/event_date=YYYY-MM-DD/metric_family=<family>/*.parquet
```

Curated rows include source Kafka coordinates and `ingested_at`. Quarantine rows retain the raw value, source coordinates, quarantine time, and all applicable typed error codes. Keeping malformed records separate prevents one bad event from blocking useful analytics while preserving evidence for diagnosis.

The operational dataset contains these metric families:

| Family | Dimensions | Stored values |
| --- | --- | --- |
| `payment_volume` | overall, merchant, country | payment count and integer minor-unit amount sum |
| `decision_rate` | decision outcome | outcome count and batch denominator/rate |
| `risk_score_distribution` | overall and decision outcome | ten-point score bucket plus score count/sum/min/max/average |
| `ingestion_latency` | source Kafka topic | latency count/sum/min/max/average in milliseconds |
| `quarantine_error` | source topic and error code | invalid-record error count |

Each row is a daily delta from one Spark micro-batch. Counts, amount sums, score sums/counts, and latency sums/counts can be added across batches. Global rates and averages must be recomputed from their combined numerators and denominators; averaging per-batch rates or averages would be mathematically wrong. The inspection script follows this rule.

Ingestion latency is measured from the Kafka record timestamp to the moment the curated record entered this Spark job. Replaying historical topic data therefore produces intentionally large latency values. Use this metric operationally only after the job has caught up; it is not end-to-end payment-decision latency.

## Checkpoints and recovery

The four sinks have independent checkpoint directories:

```text
checkpoints/
  payments/
  risk_decisions/
  quarantine/
  operational_metrics/
```

Spark commits input offsets and file-sink metadata in these directories. `SPARK_STARTING_OFFSETS` only controls a query with no checkpoint; after that, the checkpoint is authoritative. Restarting the same job with the same output and checkpoint roots resumes after its committed offsets.

Do not reuse one checkpoint with a different query or output path, and do not delete a checkpoint while keeping its Parquet files. Either mistake can create gaps or duplicates. Checkpoints and data must be backed up and restored as one logical unit.

Curated streams apply a seven-day event-time watermark and deduplicate `event_id` within that state window. This protects normal retry/replay traffic without retaining unbounded state. A duplicate older than the watermark can be written again, so long-term consumers should still treat `event_id` as the durable identity.

Kafka delivery and the file sink should be understood as recoverable, checkpoint-driven processing—not a blanket exactly-once claim across every external system. The curated restart test starts a file stream three times against one checkpoint, adds input between starts, and proves the third start does not duplicate already committed rows.

Operational metrics use `foreachBatch`. Every batch writes to a deterministic `batch_id` directory with overwrite mode, so retrying the same batch ID replaces its prior result instead of adding another copy. The dedicated checkpoint prevents committed Kafka offsets from being processed again after a normal restart. An empty micro-batch may leave an empty batch directory; readers count only rows with a `batch_id` value.

Plain Parquet does not provide a multi-file transaction. A host or storage failure during overwrite can still leave a partial batch. Production object storage should use an atomic table format such as Delta Lake or Iceberg, or publish a completion manifest after verification. This local checkpoint keeps the implementation reliable enough for the current Compose deployment without making a stronger guarantee than the storage layer provides.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BROKERS` | required | Kafka bootstrap servers |
| `PAYMENTS_TOPIC` | `payments.created` | Payment topic |
| `RISK_DECISIONS_TOPIC` | `risk.decisions` | Decision topic |
| `SPARK_MASTER` | `local[2]` | Local Spark execution setting passed by the entrypoint |
| `SPARK_OUTPUT_ROOT` | `/var/lib/riskflow/streaming/data` | Curated/quarantine data root |
| `SPARK_CHECKPOINT_ROOT` | `/var/lib/riskflow/streaming/checkpoints` | Durable query state root |
| `SPARK_STARTING_OFFSETS` | `earliest` | Initial offsets when no checkpoint exists |
| `SPARK_TRIGGER_INTERVAL` | `5 seconds` | Micro-batch trigger interval |
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | `10000` | Bound on Kafka records per trigger |

Output and checkpoint roots must be distinct, absolute, normalized container paths. Topic names, offset mode, trigger syntax, and positive record bounds are validated before the Kafka stream starts.

## Verification

Build and run the isolated test image from the repository root:

```powershell
docker build --target test `
    -t riskflow-streaming-analytics-test `
    -f services/streaming-analytics/Dockerfile .
docker run --rm riskflow-streaming-analytics-test
```

Inspect the live default output:

```powershell
docker compose run --rm --no-deps `
    --entrypoint /opt/spark/bin/spark-submit `
    streaming-analytics `
    --master 'local[1]' `
    --conf spark.ui.enabled=false `
    /opt/riskflow/scripts/inspect_output.py `
    /var/lib/riskflow/streaming/data
```

The inspection result reports row and distinct `event_id` counts for curated datasets; quarantine row/error counts; overall, merchant, and country payment totals; decision counts/rates; score buckets; per-topic ingestion latency; and aggregated quarantine error counts. Equal row and distinct-ID counts are the expected result for curated data within the configured deduplication window.
