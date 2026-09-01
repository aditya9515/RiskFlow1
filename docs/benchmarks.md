# Reproducible measurements

## Scope and honesty boundary

These are local engineering measurements from 1 September 2026. They are not production capacity, an availability claim, or banking-model performance. The HTTP measurements used one laptop, one Docker Desktop stack, one API instance, one outbox publisher, one risk worker, and one decision consumer. The dashboard and Spark streaming service remained active during the load samples.

The client called `http://localhost:8080` without TLS or network distance. `POST /v1/payments` latency ends after the PostgreSQL payment/outbox transaction commits; it does not include the asynchronous risk decision. End-to-end decision delay is reported separately.

## Environment

| Component | Measured environment |
| --- | --- |
| Host | Windows 11 Home Single Language `10.0.26200` |
| Processor | Intel Core i5-11400H, 6 cores / 12 logical processors |
| Host memory | 15.73 GiB |
| Docker Desktop engine | `29.6.2`, 12 CPUs, 7.62 GiB memory visible to Docker |
| PowerShell | `7.6.4` |
| Go | `1.27.0` locally; CI uses supported `1.26.x` |
| Python | `3.13.14` locally |
| Node.js | `24.14.1` locally |
| PostgreSQL / Kafka / Redis | PostgreSQL 17 / Kafka 4.3.1 / Redis 8 Compose images |

## Test count and coverage

Coverage is intentionally reported per service. Combining different languages and coverage engines into one percentage would be misleading.

| Service and scope | Tests | Statement coverage | Branch coverage |
| --- | ---: | ---: | ---: |
| Go payment API, unit tests | — | 51.4% | Not measured by Go cover |
| Go payment API, unit + PostgreSQL integration | 107 top-level tests | 66.7% | Not measured by Go cover |
| Python risk service, including live Redis integration | 47 | 83% combined line/branch coverage | Included in combined result |
| Next.js dashboard | 9 | 80.39% | 82.05% |
| Spark streaming analytics | 16 | 68% combined line/branch coverage | Included in combined result |

The conservative total is **179 tests**: Go top-level test functions plus the cases reported by Pytest and Vitest. Go subtests are not added again, so this does not inflate the total by counting both a parent and its children.

Reproduce coverage:

```powershell
Push-Location services/payment-api
New-Item -ItemType Directory -Force build/coverage | Out-Null
go test -coverprofile build/coverage/go-unit.out ./...
go tool cover -func build/coverage/go-unit.out | Select-Object -Last 1

# TEST_DATABASE_URL must point to a separately migrated disposable database.
$env:TEST_DATABASE_URL = "postgres://riskflow:local_password@localhost:55432/riskflow_coverage?sslmode=disable"
go test -tags=integration -count=1 -p=1 `
    -coverprofile build/coverage/go-integration.out ./...
go tool cover -func build/coverage/go-integration.out | Select-Object -Last 1
Remove-Item Env:TEST_DATABASE_URL
Pop-Location

Push-Location services/risk-service
$env:TEST_REDIS_URL = "redis://localhost:16379/15"
& .\.venv\Scripts\python.exe -m coverage erase
& .\.venv\Scripts\python.exe -m coverage run `
    --branch --source=risk_service -m pytest -q -p no:cacheprovider
& .\.venv\Scripts\python.exe -m coverage report --show-missing
Remove-Item Env:TEST_REDIS_URL
Pop-Location

Push-Location services/dashboard
npm run test:coverage
Pop-Location

docker build --target test `
    --tag riskflow-streaming-analytics-coverage `
    --file services/streaming-analytics/Dockerfile .
docker run --rm riskflow-streaming-analytics-coverage
```

The Go integration database must be created separately, migrated through `migrate/migrate:v4.19.1`, and removed after the run. Tests may truncate their target database and must never receive the normal development database URL.

## Payment acceptance load

Each run created 250 unique, valid payments at concurrency 25. The runner uses one shared `HttpClient`, records each request independently, applies nearest-rank percentiles, and fails unless every response is `201` with a distinct payment ID.

The harness intentionally creates durable, prefixed benchmark records and never deletes operational data. Use a disposable Compose database when retained evidence is not wanted.

```powershell
& .\scripts\measure-payment-api.ps1 `
    -Requests 250 `
    -Concurrency 25 `
    -Mode Unique `
    -RunId bench-payment-20260901-r1
```

| Run | Requests | Failed | Duration | Requests/s | Mean | p50 | p95 | p99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| r1 | 250 | 0 | 1.645977 s | 151.886 | 16.318 ms | 13.220 ms | 37.091 ms | 79.596 ms | 79.765 ms |
| r2 | 250 | 0 | 1.720827 s | 145.279 | 17.332 ms | 14.798 ms | 35.569 ms | 63.703 ms | 64.044 ms |
| r3 | 250 | 0 | 1.755324 s | 142.424 | 15.165 ms | 12.709 ms | 33.778 ms | 52.508 ms | 52.707 ms |

Across the three bounded samples, median throughput was **145.279 requests/s**. The median run-level latency values were **p50 13.220 ms**, **p95 35.569 ms**, and **p99 63.703 ms**. All 750 requests succeeded and produced 750 distinct payments.

## Idempotency under concurrent retries

The same normalized request and idempotency key were sent 64 times at concurrency 32:

```powershell
& .\scripts\measure-payment-api.ps1 `
    -Requests 64 `
    -Concurrency 32 `
    -Mode IdenticalReplay `
    -RunId bench-idempotency-20260901
```

Measured results:

- one `201 Created`;
- 63 `200 OK` replays;
- one unique payment ID returned;
- direct PostgreSQL counts: one payment, one outbox event, one decision;
- **zero duplicate payment, outbox, or decision rows**.

The run completed in `0.656827 s` at `97.438 requests/s`. Retry latency was p50 `4.829 ms`, p95 `106.364 ms`, and p99 `106.902 ms`.

## Event-processing delay

All 750 load-test payments reached a published outbox event and a persisted risk decision. There were zero pending outbox rows when measured.

| Stage from payment `created_at` | p50 | p95 | p99 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| Outbox published | 759.126 ms | 969.266 ms | 975.893 ms | 984.746 ms |
| Risk decision persisted | 11,995.560 ms | 21,446.287 ms | 22,092.527 ms | 22,202.985 ms |

Reproduce the stored-timestamp calculation in PostgreSQL, changing the prefix to the selected run IDs:

```sql
WITH measured AS (
    SELECT
        EXTRACT(EPOCH FROM (o.published_at - p.created_at)) * 1000 AS publish_ms,
        EXTRACT(EPOCH FROM (d.recorded_at - p.created_at)) * 1000 AS decision_ms
    FROM payments p
    JOIN outbox_events o ON o.aggregate_id = p.id
    JOIN payment_decisions d ON d.payment_id = p.id
    WHERE p.idempotency_key LIKE 'bench-payment-20260901-r%'
)
SELECT
    COUNT(*) AS completed_payments,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY publish_ms) AS publish_p50_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY publish_ms) AS publish_p95_ms,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY publish_ms) AS publish_p99_ms,
    MAX(publish_ms) AS publish_max_ms,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY decision_ms) AS decision_p50_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY decision_ms) AS decision_p95_ms,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY decision_ms) AS decision_p99_ms,
    MAX(decision_ms) AS decision_max_ms
FROM measured;
```

The burst result exposes a real current bottleneck: HTTP acceptance and outbox publication remain quick, while the single Python risk worker builds a queue during a 750-payment burst. This is useful capacity evidence, not a result to hide. Increasing consumer parallelism safely would require partition-aware feature ordering and another measured run.

An idle-stack end-to-end functional check observed `PENDING_RISK → ALLOWED` in `151.093 ms` with a 100 ms polling interval:

```powershell
& .\scripts\verify-payment-e2e.ps1 `
    -RunId e2e-checkpoint8a-20260901 `
    -TimeoutSeconds 30 `
    -PollIntervalMilliseconds 100
```

Because the client polls, that `151.093 ms` is observed completion time with up to 100 ms measurement granularity. The PostgreSQL burst percentiles above use stored timestamps and are the more precise queue-delay measurement.

## Synthetic ML evaluation

The versioned XGBoost artifact retains its previously measured fictional test-set results: precision `0.133333`, recall `0.506887`, F1 `0.211130`, PR-AUC `0.198821`, and ROC-AUC `0.764561`. These are synthetic-data pipeline measurements, not evidence of real fraud-detection quality. Dataset generation, threshold cost assumptions, the confusion matrix, and the exact training command are documented in [the model report](ml-model.md).
