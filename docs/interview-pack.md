# RiskFlow Goldman Sachs interview pack

## Accuracy boundary

Use only claims supported by the repository and [measured results](benchmarks.md). RiskFlow is a verified local Docker Compose system, not a public deployment or production banking platform. The XGBoost results come from reproducible synthetic data and must never be presented as real fraud-detection performance.

## 30-second introduction

“RiskFlow is a real-time payment-risk and decisioning platform I built with Go, PostgreSQL, Kafka, Python, Redis, XGBoost, Spark, and Next.js. The Go API accepts payments idempotently and commits each payment with an outbox event in one transaction. Kafka then drives explainable rules and ML scoring, while another transactional consumer stores the decision, audit evidence, and manual-review work. I focused on financial-system reliability: safe retries, reconciliation, graceful dependency failures, and measured behavior rather than an oversized architecture.”

## 90-second explanation

“RiskFlow starts with a Go payment API. A caller supplies an idempotency key, and the API validates and normalizes the payment before hashing the normalized fields with SHA-256. PostgreSQL’s unique constraint handles concurrent retries. A new payment and its `payments.created` outbox event commit in one transaction, so there cannot be a payment without its durable publication intent.

“A separate Go worker claims outbox rows with `FOR UPDATE SKIP LOCKED`, publishes them to Kafka, and marks them published only after broker acknowledgement. The Python risk worker validates the event, atomically maintains five-minute velocity and device/country features in Redis, runs readable rules, and scores a versioned XGBoost artifact. It publishes `ALLOW`, `REVIEW`, or `BLOCK` with scores, reasons, versions, and timestamps.

“A Go decision consumer stores the immutable decision, payment status change, audit event, ingestion receipt, and optional manual-review row in one PostgreSQL transaction before committing the Kafka offset. Manual review is role-controlled and uses optimistic locking. Reconciliation detects missing or inconsistent evidence, Spark writes checkpointed analytical Parquet data, and a Next.js dashboard exposes operational state. I verified 179 conservatively counted tests and locally measured median throughput of 145.279 accepted payments per second across three bounded runs, with a median run-level p95 acceptance latency of 35.569 milliseconds.”

## Complete architecture explanation

### 1. Payment acceptance

The public Go API exposes health/readiness, create/get payment, manual review, dashboard, and metrics endpoints. Money is stored as integer minor units. Request identifiers are trimmed; currency and country are uppercased; unknown JSON fields are rejected. Each request has bounded database and server timeouts plus a UUIDv4 correlation ID.

For `POST /v1/payments`, the API hashes the validated normalized fields in fixed order. PostgreSQL tries to insert the payment under a unique idempotency key. A winning request inserts the payment and outbox event in one transaction and returns `201`; a matching loser reads the original row and returns `200`; a different fingerprint returns typed `409`. Invalid requests never reach the transaction.

### 2. Transactional outbox and Kafka

The API does not publish directly to Kafka inside the HTTP request. Instead, the database commit leaves durable publication work in `outbox_events`. A separate publisher holds a row lock while publishing, allowing multiple workers to skip each other’s locked rows. It persists retry counts, next-attempt time, last error, publication time, or terminal quarantine time.

Kafka delivery is at least once. If Kafka acknowledges a message and the worker crashes before PostgreSQL records `published_at`, the row is published again after restart. RiskFlow accepts that transport duplicate and makes downstream effects idempotent.

### 3. Risk decisioning

The Python worker consumes `payments.created`, validates the schema, and uses one Redis Lua operation to deduplicate the source event while updating five-minute velocity, seen devices, and baseline country. Rules assign readable reason codes and may return `ALLOW`, `REVIEW`, or `BLOCK`. The pinned XGBoost model uses the same four-feature preprocessing contract as training and may escalate an otherwise allowed payment to review; it cannot directly block.

The output decision ID is deterministic from the input event ID and model version. The complete decision is cached before publication, so a redelivery cannot change velocity twice or produce a different answer after a restart. Malformed input is published to `risk.invalid-events`; its offset advances only after quarantine succeeds.

### 4. Decision persistence and controls

The Go decision consumer validates schema-v2 decisions and uses one PostgreSQL transaction for the decision, payment state, system audit event, ingestion receipt, and conditional manual-review work. It advances the Kafka offset only after the transaction commits. A replay creates another immutable transport receipt but not another decision, audit event, or review item.

Reviewers authenticate through server-side token mappings. An auditor can read pending work; a reviewer can approve or reject. The request includes the review version it observed. PostgreSQL updates only that version, so two reviewers cannot silently overwrite each other.

### 5. Analytics, operations, and recovery

The read-only dashboard API takes a consistent PostgreSQL snapshot for transactional totals, failures, review backlog, reason codes, and recent decisions. Next.js keeps its bearer token on the server. Spark consumes payment and decision topics, validates both schemas, separates curated and malformed records, writes partitioned Parquet, and uses independent checkpoints for restart recovery.

Prometheus-compatible metrics cover request latency, outbox backlog/retries, decision processing, lag, rejected records, and pending review. The reconciler reports missing, duplicate, late, or inconsistent transactional evidence. PostgreSQL remains the operational source of truth; Redis and Parquet are not used to reconstruct an HTTP payment response.

## Five important design decisions

### 1. Fingerprint normalized fields, not raw JSON

Raw JSON can differ in whitespace, key order, or lowercase codes while representing the same payment. RiskFlow validates and normalizes first, then hashes fields in fixed order. This makes legitimate retries stable while still detecting meaningful conflicts.

### 2. Use a transactional outbox

Publishing to Kafka from inside the request cannot be atomic with a normal PostgreSQL commit. The outbox reduces the problem to one local transaction: either the payment and publication intent both exist, or neither does. A worker then retries publication independently.

### 3. Claim at-least-once delivery with idempotent effects

Calling the platform “exactly once” would hide acknowledgement gaps between PostgreSQL, Kafka, Redis, and files. Stable IDs, unique constraints, replay caches, and ingestion receipts make retries safe and observable instead.

### 4. Fail conservatively when decision dependencies are uncertain

Redis failure pauses processing because fabricated velocity data could incorrectly allow a payment. Model failure still runs deterministic rules: an explicit rules block wins, while every other uncertain case goes to manual review with `ML_UNAVAILABLE`.

### 5. Treat control evidence as data, not logs

Decisions, ingestion receipts, and audit events are immutable PostgreSQL rows. Manual review uses actor identity, reason, timestamp, and optimistic versioning. Reconciliation independently checks whether the expected evidence exists and agrees.

## Five failure stories

These are real implementation or injected-failure stories. Be clear which kind each one is.

### 1. Readiness returned 404

- **Symptom:** `/healthz` worked but `/readyz` was not registered.
- **Root cause:** liveness existed without a separate dependency-aware readiness route.
- **Resolution:** added `/readyz` using an injected `Pinger`, a one-second database deadline, and typed `503 database_unavailable`; added success/failure handler tests.
- **Evidence:** stopping PostgreSQL produced typed `503` in 1,248 ms while `/healthz` stayed `200`, then readiness recovered without restarting the API.

### 2. Decision consumer did not recover cleanly after Kafka returned

- **Symptom:** the container health listener stayed alive, but a long-lived broker poll could remain stuck after an outage.
- **Root cause:** liveness of the process did not prove progress of the consumer loop; polling lacked a strict deadline.
- **Resolution:** added `DECISION_POLL_TIMEOUT`, re-entered bounded polls, and tested broker recovery.
- **Evidence:** a fresh outage retained the record and later reduced consumer lag to zero without restarting the consumer.

### 3. Kafka redelivery produced a duplicate transport receipt

- **Symptom:** deliberately republishing one decision changed ingestion receipts from one to two.
- **Root cause:** at-least-once transport permits redelivery around offset/acknowledgement boundaries.
- **Resolution:** deterministic decision identity and database uniqueness classify the second record as `REPLAYED`.
- **Evidence:** decision and audit counts both remained exactly one.

### 4. Redis was unavailable during risk processing

- **Symptom:** the payment reached Kafka but stayed `PENDING_RISK` with no decision.
- **Root cause:** online velocity/device/country state could not be read or updated safely.
- **Resolution:** retained the Kafka offset, retried with bounded delay, and used an atomic Lua script once Redis returned.
- **Evidence:** recovery produced one `ALLOWED` decision without restarting the risk worker.

### 5. The ML artifact was intentionally made unavailable

- **Symptom:** model loading/scoring could not produce a trustworthy probability.
- **Root cause:** missing or incompatible model evidence must not silently become a normal score.
- **Resolution:** retained deterministic rule blocks and routed every other uncertain payment to review with explicit fallback version and reason.
- **Evidence:** the injected case became `REVIEW`, score `40`, reason codes `NEW_DEVICE,ML_UNAVAILABLE`, and model version `ml-unavailable-review-v1`.

## Transaction and outbox explanation

The key distinction is local atomicity versus distributed delivery. PostgreSQL can atomically insert the payment and outbox row because both are in one database transaction. It cannot atomically commit that same transaction with Kafka in this design.

After commit, a worker selects one due event with `FOR UPDATE SKIP LOCKED`. The row lock prevents another worker from claiming it concurrently. The worker publishes while holding the claim and marks `published_at` only after Kafka acknowledges. Failure stores the next retry and error; invalid event structure is quarantined. A crash in the acknowledgement gap may publish twice, which is why consumers still deduplicate.

Short interview answer: “The outbox prevents lost publication intent, not duplicate delivery. PostgreSQL gives atomic payment plus intent; stable event identity makes subsequent at-least-once publication safe.”

## Idempotency explanation

An idempotency key identifies one intended operation, but the key alone cannot tell whether a caller accidentally reused it for a different payment. RiskFlow therefore stores both the key and a SHA-256 fingerprint of the normalized business fields.

The database unique constraint is the concurrency authority. With many simultaneous calls, one insert wins. Losing inserts load the winner inside their transaction: matching fingerprint means replay and returns the original ID; different fingerprint means conflict. The concurrent test sent 64 identical requests at concurrency 32 and produced one `201`, 63 `200` responses, one payment, one outbox event, and one decision.

## Reconciliation, audit, and operational-risk connection

Audit answers “what happened, who or what caused it, and when?” Reconciliation answers “does the expected chain of records exist and agree?” They solve different control problems.

RiskFlow stores automated decisions and system audit events immutably. Manual actions add reviewer identity, reason code, timestamps, and version-controlled state transitions. Ingestion receipts preserve the Kafka topic/partition/offset and whether a record was applied, replayed, or rejected.

The reconciler looks for breaks such as a payment missing an outbox event, a published payment missing a decision after the grace period, a status inconsistent with its decision, or review state inconsistent with payment state. This connects directly to operational risk: failures become detectable exceptions with evidence and ownership rather than silent data loss. It supports investigation and remediation, but it is not a claim of regulatory compliance.

## SQL interview questions

### 1. Find unpublished, non-quarantined outbox events

```sql
SELECT id, aggregate_id, delivery_attempts, next_attempt_at, last_error
FROM outbox_events
WHERE published_at IS NULL
  AND dead_lettered_at IS NULL
ORDER BY next_attempt_at, created_at, id;
```

The partial pending index supports this access path and avoids scanning completed history.

### 2. Find payments that do not have exactly one outbox event

```sql
SELECT p.id, COUNT(o.id) AS outbox_count
FROM payments p
LEFT JOIN outbox_events o ON o.aggregate_id = p.id
GROUP BY p.id
HAVING COUNT(o.id) <> 1;
```

The `LEFT JOIN` is essential because an inner join would hide payments with zero outbox rows.

### 3. Find published payments missing a decision after 30 seconds

```sql
SELECT p.id, p.created_at, o.published_at
FROM payments p
JOIN outbox_events o ON o.aggregate_id = p.id
LEFT JOIN payment_decisions d ON d.payment_id = p.id
WHERE o.published_at IS NOT NULL
  AND d.payment_id IS NULL
  AND p.created_at < clock_timestamp() - INTERVAL '30 seconds';
```

The grace period prevents in-flight asynchronous work from becoming a false exception.

### 4. Find payment statuses inconsistent with automated decisions

```sql
SELECT p.id, p.status, d.decision
FROM payments p
JOIN payment_decisions d ON d.payment_id = p.id
WHERE (d.decision = 'ALLOW'  AND p.status <> 'ALLOWED')
   OR (d.decision = 'REVIEW' AND p.status <> 'REVIEW')
   OR (d.decision = 'BLOCK'  AND p.status <> 'BLOCKED');
```

For resolved manual reviews, the real reconciler must also account for the later reviewer transition; a simple query is not the whole control.

### 5. Return the newest automated decision per payment

```sql
SELECT payment_id, decision_id, decision, risk_score, decision_at
FROM (
    SELECT d.*,
           ROW_NUMBER() OVER (
               PARTITION BY payment_id
               ORDER BY decision_at DESC, decision_id DESC
           ) AS row_number
    FROM payment_decisions d
) ranked
WHERE row_number = 1;
```

The deterministic secondary ordering handles equal timestamps.

### 6. Measure pending-review age and identify an SLA breach

```sql
SELECT payment_id,
       version,
       clock_timestamp() - enqueued_at AS age
FROM manual_review_queue
WHERE status = 'PENDING'
  AND enqueued_at < clock_timestamp() - INTERVAL '15 minutes'
ORDER BY enqueued_at;
```

An index restricted to `status = 'PENDING'` supports the queue order.

### 7. Calculate decision distribution by day

```sql
SELECT decision_at::date AS decision_date,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE decision = 'ALLOW') AS allowed,
       COUNT(*) FILTER (WHERE decision = 'REVIEW') AS reviewed,
       COUNT(*) FILTER (WHERE decision = 'BLOCK') AS blocked
FROM payment_decisions
GROUP BY decision_at::date
ORDER BY decision_date;
```

Store the component counts; derive rates from totals rather than averaging already-rounded percentages.

### 8. Why use `FOR UPDATE SKIP LOCKED`?

`FOR UPDATE` claims the selected row until the transaction ends. `SKIP LOCKED` lets another publisher continue to a different event rather than wait. This supports concurrent workers, although holding a database transaction across broker publication is a throughput tradeoff that should be measured.

## Distributed-systems questions

### Why not claim exactly-once processing?

PostgreSQL, Kafka, Redis, and the file sink do not share one atomic commit. An acknowledgement may succeed immediately before a process crash prevents local state or offset commit. RiskFlow uses at-least-once transport and idempotent business effects.

### What happens if the API commits but crashes before replying?

The client may not know whether the operation succeeded. It retries with the same idempotency key and normalized request, receiving the original payment ID as `200` rather than creating another payment.

### What happens if Kafka acknowledges but `published_at` is not committed?

The outbox row remains eligible and is published again. The consumer recognizes the stable event identity and reuses the cached decision; PostgreSQL does not duplicate domain history.

### Where is ordering required?

Kafka keys payment events by payment ID, preserving per-payment partition order. Customer-velocity ordering is harder because different payment IDs for one customer can occupy different partitions. Scaling risk consumers therefore requires an explicit customer-key/feature-ordering strategy rather than simply adding replicas.

### How is backpressure visible?

Kafka lag, pending outbox count, processing latency, and pending review backlog reveal different queues. The 750-payment burst showed fast API/outbox work but a payment-to-persisted-decision p95 of about 21.4 seconds because the single risk worker accumulated work.

### Retry versus quarantine?

Transient dependency failures are retried with bounded delay and retained offsets. Structurally invalid or permanently conflicting records are stored or published as rejection evidence so one poison message cannot block the partition forever.

### Why is reconciliation still necessary if operations are transactional?

Transactions protect only one database boundary. They do not prove end-to-end completeness across time and systems, nor do they detect every operational mistake. Reconciliation independently checks expected relationships and states after a grace period.

### What is the availability choice when Redis fails?

RiskFlow chooses consistency of decision features over immediate availability: it pauses risk processing and retains the Kafka offset. The HTTP acceptance path remains available because the payment and outbox can still commit.

## Go questions

### Why Go for the API and workers?

Go provides a small deployment artifact, strong standard HTTP/concurrency support, explicit error handling, and efficient long-running workers. Python remains the better boundary for XGBoost and data-science tooling.

### How are interfaces used?

Handlers depend on narrow service interfaces, readiness depends on `Pinger`, and workers depend on small store/publisher/consumer interfaces. Tests inject fakes without mocking every concrete implementation.

### Why pass `context.Context` first?

It carries request cancellation, deadlines, and the correlation ID across call boundaries. Database and broker operations can stop when the request or application is shutting down.

### How does graceful shutdown work?

The root context is cancelled by the operating-system signal path. The HTTP server calls `Shutdown` under a bounded timeout and closes forcibly only if graceful completion fails. Workers check cancellation and avoid marking unfinished work complete.

### Why use `errors.Is` and `errors.As`?

Wrapped errors retain their category. HTTP handlers can map domain validation, not-found, conflict, and timeout errors to stable typed responses without string matching.

### How is shared-memory concurrency controlled?

The critical business concurrency is delegated to PostgreSQL unique constraints and row locks. Go goroutines manage server/work-loop concurrency, but they are not treated as a cross-instance lock.

### Why disallow unknown JSON fields?

Silently accepting misspelled fields can turn a caller error into incorrect payment data. Strict decoding fails early with a typed request error.

### Why structured logging and bounded metric labels?

JSON fields are searchable and correlation IDs join events across components. Metric labels never include unbounded payment/customer/request IDs, preventing cardinality growth that could overwhelm Prometheus.

## Kafka questions

### Why use the payment ID as the Kafka key?

Kafka hashes the key to a partition, preserving order for events belonging to one payment and distributing different payments across partitions.

### When are offsets committed?

The risk worker commits after it publishes a decision or quarantine event. The decision consumer commits after PostgreSQL applies or stores rejection evidence. Failure before commit causes redelivery.

### What does a consumer group provide?

Within a group, Kafka assigns each partition to at most one active consumer. This distributes partitions and preserves ordered processing within each partition, but it does not make downstream writes idempotent.

### What is consumer lag?

Lag is the difference between the partition’s latest offset and the group’s committed position. It is a queue-depth indicator, not by itself an end-to-end latency guarantee.

### What happens during a rebalance?

Partitions can move between consumers. In-flight work must finish or remain uncommitted before ownership changes. The worker exposes bounded polling and explicit rebalance allowance so durable offsets remain authoritative.

### Why three partitions?

Three partitions allow limited parallelism in the local demonstration and show keyed ordering. It is not a capacity recommendation; partition count in production would follow throughput, ordering, retention, and consumer-parallelism measurements.

### Why not rely only on Kafka retries for malformed events?

A deterministic schema error will never recover through repeated polling. Quarantine records preserve evidence and allow the partition to progress after durable handling.

### Would Kafka transactions solve the whole problem?

They can atomically coordinate certain Kafka reads/writes and offsets, but they do not automatically include PostgreSQL, Redis, or Parquet. Cross-system idempotency and reconciliation would still be required.

## ML and metric questions

### Precision

Of the payments predicted for review, precision is the fraction that are synthetic positives. Low precision means more manual-review workload.

### Recall

Of all synthetic positives, recall is the fraction selected for review. Low recall means more costly false negatives under the project’s fictional cost model.

### F1

F1 is the harmonic mean of precision and recall. It is useful for balance but does not encode the project’s asymmetric false-negative and review costs.

### PR-AUC versus ROC-AUC

ROC-AUC measures ranking across true-positive and false-positive rates and can look optimistic with rare positives. PR-AUC focuses on precision and recall and is usually more informative for an imbalanced review problem.

### Why not use accuracy?

With a 3.628% synthetic positive rate, predicting everything as low risk would have high accuracy while finding nothing useful. Class-specific and ranking metrics expose that failure.

### How was the threshold chosen?

The model was trained on the training split, early-stopped and thresholded using validation data, then evaluated once on the untouched test split. Threshold candidates minimized fictional cost: false negative `500`, manual review `25`.

### Why can the model not directly block?

Synthetic labels do not justify an irreversible automated block. Rules retain block authority, while the model can only add human review.

### What do the measured metrics mean?

At threshold `0.05`, synthetic test precision was `0.133333`, recall `0.506887`, F1 `0.211130`, PR-AUC `0.198821`, and ROC-AUC `0.764561`. They validate the reproducible pipeline and tradeoff calculation, not banking effectiveness.

### What is training-serving skew?

It occurs when online features differ from training preprocessing. RiskFlow uses one named four-feature contract and verifies feature ordering and artifact metadata before consumption.

### What would production ML require next?

Governed representative data, privacy and bias review, temporal validation, probability calibration, drift monitoring, model registry/promotion controls, champion-challenger or shadow evaluation, and formal model-risk approval.

## Twenty likely follow-ups with model answers

### 1. What part did you personally build?

“I implemented and verified the full local repository: the Go API/workers and PostgreSQL controls, Python rules/model pipeline and Redis features, Spark job, Next.js dashboard, Compose/CI, failure tests, benchmarks, and documentation. I would separate that from claiming production operation or institutional data experience.”

### 2. Why make risk asynchronous?

“Payment acceptance should not fail merely because Kafka or scoring is briefly unavailable. The database transaction durably accepts the payment as `PENDING_RISK`; the outbox guarantees later publication. The tradeoff is eventual final status, which the API and dashboard expose clearly.”

### 3. Why not call Kafka directly from the API?

“A Kafka publish could succeed while the database fails, or the database could commit while publication fails. The outbox makes payment plus publication intent one PostgreSQL transaction and moves unreliable network work to a retryable worker.”

### 4. What prevents concurrent duplicate payments?

“Not an in-memory mutex. PostgreSQL’s unique idempotency key is shared by every API instance. One insert wins; losers compare the persisted normalized fingerprint and either replay or conflict.”

### 5. Why SHA-256 if this is not a security signature?

“I need a stable compact equality fingerprint, not authentication. SHA-256 makes accidental collisions negligible, while the deterministic field encoding and normalization are the more important parts of correctness.”

### 6. What if the API times out after committing?

“The response truth is uncertain to the client, so the error tells it to retry with the same idempotency key. The retry returns the already committed payment rather than duplicating it.”

### 7. What is the weakest reliability point today?

“The single local risk worker is the clearest measured bottleneck. Under 750 accepted payments, payment-to-persisted-decision p95 rose to about 21.4 seconds even though outbox publication p95 stayed under one second.”

### 8. How would you scale the risk layer?

“First I would choose an ordering key aligned with customer-level features or move feature updates behind a serialization boundary. Then I would add partitions/consumers and remeasure lag and decision latency. Simply adding replicas could reorder customer velocity observations.”

### 9. Why is PostgreSQL the source of truth instead of Kafka?

“The product needs current transactional payment/review state, constraints, and audit queries. Kafka is the durable event transport and replay source; Parquet is analytical. Each has a deliberately different responsibility.”

### 10. How do you know a retry did not change the decision?

“The source event ID keys the feature snapshot and complete decision in Redis, and the decision ID is deterministic. PostgreSQL also has unique decision identity. A replay returns recorded evidence rather than rescoring against newer state.”

### 11. Why pause on Redis failure instead of defaulting velocity to zero?

“Zero would look like trusted low velocity and could incorrectly allow a payment. Retaining the Kafka offset preserves the ability to compute the real feature after recovery; availability is sacrificed at that boundary for safer decisions.”

### 12. Why review when ML fails?

“The system distinguishes policy certainty from model uncertainty. Clear deterministic block rules still block; everything else that lacks a trustworthy model result moves to a human queue with an explicit `ML_UNAVAILABLE` reason.”

### 13. How do you prevent two reviewers overwriting each other?

“Each queue row has a version. The update includes the expected version and pending status. Only one transaction changes it; a stale request receives typed `409` with the current state.”

### 14. Are audit rows enough for compliance?

“No. They demonstrate technical evidence and control design, not certification. Real use would also need retention governance, access reviews, privacy, segregation of duties, tamper-resistant backups, monitoring, and regulatory/legal assessment.”

### 15. What did the failure testing teach you?

“Process liveness and processing progress are different. The Kafka consumer’s health endpoint stayed alive while a broker poll could remain stuck. A bounded poll timeout fixed recovery and gave me a concrete operational metric to watch.”

### 16. How did you test idempotency under concurrency?

“I sent 64 identical requests at concurrency 32. The observed result was one `201`, 63 `200` replays, one returned payment ID, and direct database counts of one payment, one outbox event, and one decision.”

### 17. What do the performance numbers actually measure?

“They measure local HTTP acceptance through the PostgreSQL payment/outbox commit, with no TLS or network distance. They do not measure production capacity or full risk-decision completion. I report the asynchronous delay separately.”

### 18. Why use synthetic ML data?

“I did not have legitimate governed transaction labels, so synthetic data was the honest way to demonstrate reproducible training, artifact versioning, threshold selection, and evaluation. I explicitly avoid treating its metrics as evidence of real fraud performance.”

### 19. What did you intentionally leave out?

“I left out a double-entry ledger, maker-checker workflow, Airflow, MLflow, and public deployment because the reliable payment/event/control path was higher value. I would add technologies only when there is a tested operational need.”

### 20. What would you do first before production?

“I would start with a threat model and managed secrets/TLS/identity, then HA database/Kafka topology and backups, then representative workload and failure testing. For ML I would require governed data and formal validation. The local architecture is a design demonstration, not production readiness.”

## Final reminders

- Lead with correctness and operational controls, then technologies.
- Say “payment acceptance” when quoting HTTP latency.
- Say “local bounded measurement” when quoting throughput.
- Say “synthetic test set” before any model metric.
- Explain the observed bottleneck openly.
- Never say deployed, exactly once, bank-grade, compliant, or production-ready without new evidence.
