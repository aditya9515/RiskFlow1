# RiskFlow demo guide

## Prepare once

Run from the repository root with Docker Desktop available:

```powershell
docker compose up -d --build
docker compose ps
Invoke-RestMethod http://localhost:8080/readyz
Invoke-RestMethod http://localhost:3000/healthz
```

Open `http://localhost:3000` and keep a PowerShell terminal visible. Use a unique run ID so the audience sees only records from the current demonstration:

```powershell
$demoRun = "interview-$([DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))"
```

Do not reset Docker volumes immediately before an interview. A warm, already verified stack is safer than rebuilding dependencies during the demonstration.

## Two-minute recruiter demo

### 0:00–0:20 — problem

“RiskFlow is a real-time payment-risk platform that takes a payment from safe acceptance to an explainable decision. It focuses on financial-system essentials: duplicate protection, durable events, audit evidence, graceful failure, and measured behavior.”

### 0:20–0:45 — architecture

Show the [system diagram](architecture.md).

“The Go API stores a validated payment and outbox event in one PostgreSQL transaction. A worker publishes to Kafka; a Python service combines rules, Redis velocity features, and versioned XGBoost scoring. A Go consumer atomically persists the decision, status, audit evidence, and manual-review work.”

### 0:45–1:20 — live outcomes

```powershell
& .\scripts\generate-demo-payments.ps1 -RunId $demoRun
```

“This creates allow, review, and block scenarios, then waits for their asynchronous final states. Repeating the same run is safe: its idempotency keys return the original payment IDs.”

Refresh the dashboard and point to payment totals, allow/review/block distribution, the pending manual-review count, recent reason codes, model version, and processing/reconciliation indicators.

### 1:20–1:45 — reliability

“Kafka outages do not lose accepted payments; unpublished events stay in PostgreSQL. HTTP retries and Kafka redelivery are deduplicated. Redis failure pauses risk processing rather than fabricating velocity data, while a missing ML artifact routes uncertainty to review.”

### 1:45–2:00 — evidence

“The project has 179 conservatively counted tests across four services. On my documented local setup, three bounded samples had median throughput of 145.279 requests per second with a median run-level p95 of 35.569 milliseconds for payment acceptance. Those are local measurements, not production claims.”

## Five-minute technical demo

### 0:00–0:40 — contract and transaction

Show `POST /v1/payments` in [API examples](api-examples.md) and the payment sequence in [architecture](architecture.md).

“Money is an integer number of minor units. The API normalizes validated fields and hashes them in deterministic order. A unique idempotency key plus that fingerprint distinguishes a legitimate replay from conflicting key reuse. The payment and `payments.created` outbox row commit together.”

### 0:40–1:20 — prove idempotency

```powershell
& .\scripts\generate-demo-payments.ps1 -RunId $demoRun
& .\scripts\generate-demo-payments.ps1 -RunId $demoRun
```

Point out that the first output reports three creations and the second reports three replays with the same payment IDs.

“PostgreSQL uniqueness is the concurrency authority. Application pre-checks alone would race; the database transaction decides the winner.”

### 1:20–2:05 — asynchronous reliability

Show the outbox and risk portions of the architecture.

“The outbox publisher uses `FOR UPDATE SKIP LOCKED`, so multiple workers can claim different rows without double-working the same locked row. It marks publication only after Kafka acknowledgement and persists retry timing. This is at-least-once delivery: a crash in the acknowledgement gap may redeliver, so downstream state is keyed by stable event identity.”

### 2:05–2:50 — decisioning and fallback

Show recent dashboard decisions and reason codes.

“The risk worker validates the event, atomically observes Redis velocity/device/country state, runs explainable rules, and scores a versioned XGBoost artifact trained only on reproducible synthetic data. Rules can block. ML can escalate to review. If ML is missing, rules still block clear risk and all other uncertain cases move to review. Redis failure pauses consumption because using made-up online features would be unsafe.”

### 2:50–3:35 — persistence, review, and audit

Show the [database diagram](database.md), then the dashboard review count.

“The decision consumer commits five related effects together: the immutable decision, payment status, system audit event, optional review item, and Kafka ingestion receipt. Manual review uses role-controlled bearer credentials and optimistic locking. A stale reviewer receives `409` instead of overwriting the first decision.”

Optionally approve the newly generated review using the command in [API examples](api-examples.md).

### 3:35–4:15 — controls and recovery

```powershell
docker compose run --rm decision-reconciler
```

“The reconciliation job checks for accepted payments missing outbox or decision evidence, duplicate or inconsistent records, and review-state breaks. Invalid Kafka data is quarantined with source coordinates. Spark maintains independent checkpoints and writes curated and malformed records to separate Parquet paths.”

The development database deliberately retains operational evidence. A reconciliation exception from an earlier duplicate-delivery or outage exercise should be explained with its recorded detail, not deleted just to make the demo show zero.

### 4:15–4:45 — observability and measurements

Show `http://localhost:8080/metrics` or [benchmarks](benchmarks.md).

“Metrics cover bounded HTTP routes, request latency, outbox backlog and retries, consumer lag, rejected records, pending reviews, and processing latency. The measured burst also exposed a real bottleneck: one risk worker queued decisions while the API continued accepting payments. I documented it rather than presenting the HTTP rate as end-to-end capacity.”

### 4:45–5:00 — close

“The core lesson is that correctness comes from explicit boundaries: database transactions for local atomicity, an outbox across PostgreSQL and Kafka, stable identities for retries, immutable evidence for controls, and conservative fallback behavior when dependencies fail.”

## Fast verification before presenting

```powershell
docker compose ps
Invoke-RestMethod http://localhost:8080/healthz
Invoke-RestMethod http://localhost:8080/readyz
Invoke-RestMethod http://localhost:3000/healthz
docker exec riskflow1-redis redis-cli ping
docker exec riskflow1-kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:19092 --list
```

Expected: application containers are running/healthy, health and readiness are `200`, Redis returns `PONG`, and Kafka lists `payments.created`, `risk.decisions`, and `risk.invalid-events`.

## Safe fallback if a live view is slow

- Keep the checked-in [desktop screenshot](screenshots/operational-dashboard.png) and [mobile screenshot](screenshots/operational-dashboard-mobile.png) available.
- Use the generator's JSON output as the primary proof; it does not depend on the browser rendering.
- Do not claim an outcome that has not appeared. If a record remains `PENDING_RISK`, inspect worker health and explain the asynchronous boundary.
- Do not perform an unpracticed dependency outage during a short recruiter demo. The measured recovery evidence is already documented in [reliability](reliability.md).

## Claims to avoid

- “Deployed” unless a live URL has actually been tested.
- “Exactly once” across PostgreSQL, Kafka, Redis, and Spark.
- “Production fraud accuracy” for the synthetic model.
- A throughput or latency number without stating the local environment and measured scope.
- Compliance certification, bank-grade security, or high availability that the local Compose setup does not implement.
