# Reliability and failure behavior

RiskFlow uses at-least-once event delivery with idempotent database effects. It does not claim one distributed exactly-once transaction across PostgreSQL, Kafka, Redis, and the file lake. Each boundary instead has a deliberate retry, deduplication, or quarantine rule.

## Failure matrix

| Scenario | Immediate behavior | Recovery and safety boundary |
| --- | --- | --- |
| PostgreSQL unavailable | API `/healthz` stays `200`; `/readyz` returns typed `503` within its bounded ping deadline. Payment writes fail without a partial payment/outbox transaction. PostgreSQL-backed metric gauges are omitted and collection success becomes zero. | The API and workers remain live. Readiness and database work recover after PostgreSQL returns. Already committed outbox and decision records remain durable. |
| Kafka unavailable | A valid HTTP payment can still commit its payment and outbox rows. The outbox publisher leaves `published_at` empty, records the failed attempt, and retries with persisted bounded exponential backoff. Decision-consumer polls are bounded by `DECISION_POLL_TIMEOUT`, so a broker outage cannot trap the worker in one unbounded fetch. | Publication and consumption resume after Kafka returns. A crash around broker acknowledgement can duplicate an event, so consumers deduplicate it. |
| Redis unavailable | The risk worker does not fabricate velocity/device/country features and does not commit the input Kafka offset. It seeks the same record and retries after bounded delay. | Processing resumes after Redis returns. The atomic Redis observation script ensures one source event increments velocity once. |
| Duplicate HTTP request | The same idempotency key plus the same normalized fingerprint replays the original payment as `200`; conflicting reuse is typed `409`. | PostgreSQL uniqueness and the payment transaction yield one payment and one outbox row, including under concurrent calls. |
| Duplicate payment Kafka event | Redis returns the cached feature snapshot and complete decision for the source event ID. | The replay cannot increment velocity twice or change its earlier decision during the configured cache lifetime. The decision consumer also deduplicates the deterministic decision ID. |
| Consumer crash after publish, before offset commit | Kafka may redeliver the input because its offset was not committed. | The cached decision is republished identically. PostgreSQL records the replay receipt without duplicating decision history, audit events, or review work. |
| Malformed payment event | The risk worker publishes a hashed rejection envelope to `risk.invalid-events`, then commits the bad input offset. If quarantine publication fails, it retains the input offset. | A poison record cannot silently disappear or permanently block its partition. Raw input fields are not copied into the rejection payload. |
| Model artifact or inference unavailable | Deterministic rules still execute. A rules `BLOCK` wins; every other payment becomes `REVIEW` with `ML_UNAVAILABLE` and `ml-unavailable-review-v1`. Model score/probability are zero because no inference occurred; the final score is raised to the policy review threshold. | The complete fallback decision is cached before Kafka publication, so replay after a restart or model recovery returns the original result. Human review is the conservative fallback. |

## Why the system is not called exactly once

PostgreSQL cannot atomically commit the same transaction as Kafka and Redis in this design. For example, Kafka can acknowledge a publish immediately before a process crash prevents a database or consumer-offset commit. Retrying is therefore necessary and can repeat a message.

RiskFlow makes those repeats harmless at the domain layer: stable event IDs, normalized HTTP fingerprints, Redis event caches, PostgreSQL unique constraints, immutable histories, and ingestion receipts all recognize work already completed. This is at-least-once transport with idempotent effects.

## Reproducible verification

Run the automated reliability tests while the Compose Redis port is available:

```powershell
Push-Location services/risk-service
$env:TEST_REDIS_URL = "redis://localhost:16379/15"
& .\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
Remove-Item Env:TEST_REDIS_URL
Pop-Location
```

The Redis integration suite uses a separate logical database and unique key prefix. It removes only those test-prefixed keys.

For controlled dependency checks, stop one service at a time and always restore it before moving to the next scenario:

```powershell
docker compose stop postgres
# Check /readyz, /healthz, and /metrics; then restore PostgreSQL.
docker compose start postgres

docker compose stop kafka
# Create a payment and inspect its unpublished outbox retry state; then restore Kafka.
docker compose start kafka

docker compose stop redis
# Publish a payment event and verify its input offset is retained; then restore Redis.
docker compose start redis
```

Use unique payment and idempotency identifiers for every run. Do not delete persisted evidence from the main development database to make counts look clean; compare before/after counts for the specific identifiers instead.

## Checkpoint 7B local evidence

Measured on 1 September 2026 against the local Docker Compose stack. These are functional reliability observations, not production availability or performance benchmarks.

| Check | Measured result |
| --- | --- |
| PostgreSQL outage | `/readyz` returned typed `503 database_unavailable` in `1,248 ms`; `/healthz` remained `200`; readiness recovered to `200` without restarting the API. |
| Kafka outage | HTTP payment creation returned `201`; its outbox row remained unpublished and recorded a failed attempt. After broker recovery, the row reached three total delivery attempts, `published_at` was set, the payment became `ALLOWED`, and exactly one decision existed without restarting either worker. |
| Redis outage | The payment event reached Kafka while the database remained `PENDING_RISK` with zero decisions. Redis recovery produced `ALLOWED` with exactly one decision without restarting the risk worker. |
| Decision-consumer restart | With the consumer stopped, the outbox published while the payment remained `PENDING_RISK` with zero decisions. Restarting the consumer produced `ALLOWED` with exactly one decision. |
| Duplicate HTTP | First request `201`, normalized replay `200` with the same payment ID, conflicting key reuse typed `409 idempotency_conflict`; direct counts were one payment and one outbox event. |
| Duplicate Kafka | Republish changed ingestion receipts from `1` to `2` and replay receipts from `0` to `1`; payment decisions and audit events both remained `1`. |
| Malformed Kafka input | The invalid-topic aggregate end offset advanced from `5` to `6`, exactly one quarantine record, and the risk worker remained running. |
| Missing model artifact | The payment became `REVIEW` with final score `40`, rule score `15`, model score/probability `0`, model version `ml-unavailable-review-v1`, and reasons `NEW_DEVICE,ML_UNAVAILABLE`. The normal `xgb-synthetic-v1` model then reloaded. |

The first Kafka outage run exposed a consumer recovery defect: after a broker error, a long-lived poll could remain stuck while the separate health listener kept the container alive. `DECISION_POLL_TIMEOUT` now bounds every fetch. A fresh outage test then recovered the retained lag to zero without restarting the consumer.
