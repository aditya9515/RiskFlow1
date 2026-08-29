# RiskFlow

RiskFlow is a real-time payment risk and ML decisioning platform. The payment core provides validated, idempotent payment creation backed by a PostgreSQL transaction and transactional outbox. A Python worker consumes those events, maintains online Redis features, combines deterministic rules with a versioned XGBoost model, and publishes explainable risk decisions.

## Requirements

- Docker Desktop with Linux containers
- Go 1.26 or newer for local development
- Python 3.13 for direct risk-service development
- PowerShell 7 for the documented Windows commands

## Local configuration

Docker Compose reads `.env`; the Go and Python applications do not load it directly. Environment variables are the applications' only configuration source.

```powershell
Copy-Item .env.example .env
```

Use a URL-safe local PostgreSQL password because Compose constructs database URLs from the individual PostgreSQL variables. Never commit `.env`.

For a direct local Go run, set the database URL explicitly:

```powershell
$env:DATABASE_URL = "postgres://riskflow:your_local_password@localhost:5432/riskflow?sslmode=disable"
$env:HTTP_ADDR = ":8080"
Push-Location services/payment-api
go run ./cmd/api
Pop-Location
```

## Start the stack

From the repository root:

```powershell
docker compose up -d --build
docker compose ps
```

The one-shot `migrate` container runs all pending migrations before the payment API starts. The API never applies migrations itself.

## Health checks

```powershell
Invoke-RestMethod http://localhost:8080/healthz
Invoke-RestMethod http://localhost:8080/readyz
```

- `/healthz` reports whether the API process is alive and never queries PostgreSQL.
- `/readyz` reports whether PostgreSQL can answer within the configured readiness timeout.

## Create a payment

Amounts use integer minor units, so `1250` means USD 12.50 rather than a floating-point value.

```powershell
$headers = @{
    "Idempotency-Key" = "checkout-2026-0001"
    "Content-Type" = "application/json"
}
$body = @{
    customer_id = "customer-1"
    merchant_id = "merchant-1"
    device_id = "device-1"
    amount_minor = 1250
    currency = "USD"
    country = "IN"
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/payments -Headers $headers -Body $body
```

The first request returns `201 Created`. Repeating the normalized payment with the same key returns `200 OK` and the original payment ID. Reusing the key for different payment fields returns a typed `409 Conflict`.

The request fingerprint is SHA-256 over validated, normalized fields in a fixed order; insignificant JSON whitespace, object-key order, and lowercase currency/country codes do not change it.

## Retrieve a payment

Use the UUID returned by the create endpoint:

```powershell
$paymentID = "10000000-0000-4000-8000-000000000001"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/v1/payments/$paymentID"
```

A stored payment returns `200 OK`. A valid UUID with no corresponding payment returns the typed error `payment_not_found` with `404 Not Found`. A malformed UUID returns `invalid_payment_id` with `400 Bad Request` without querying PostgreSQL.

## Outbox publisher

The `outbox-publisher` is a separate Go process. It claims one unpublished row with PostgreSQL `FOR UPDATE SKIP LOCKED`, publishes a versioned envelope to the `payments.created` Kafka topic, and sets `published_at` only after Kafka acknowledges the record.

The Kafka record key is the payment ID, which keeps events for one payment on the same partition. Delivery is at least once: a crash after Kafka accepts a record but before PostgreSQL commits can cause the same `event_id` to be delivered again. Consumers must therefore store or otherwise deduplicate processed event IDs.

The envelope contains `event_id`, `event_type`, `aggregate_id`, `schema_version`, `occurred_at`, `trace_id`, and the domain `payload` object. Kafka failures leave the outbox row unpublished and are retried with exponential backoff capped by `OUTBOX_RETRY_MAX_BACKOFF`.

Delivery state is stored on each outbox row. `delivery_attempts`, `last_attempt_at`, `last_error`, and `next_attempt_at` make failures observable and ensure retry timing survives a worker restart. Transient broker failures are retried indefinitely rather than discarded. Structurally invalid or unsupported events are quarantined with `dead_lettered_at` so they cannot block valid events behind them.

Useful operational queries:

```sql
SELECT COUNT(*) AS pending_events
FROM outbox_events
WHERE published_at IS NULL AND dead_lettered_at IS NULL;

SELECT id, event_type, delivery_attempts, last_error, dead_lettered_at
FROM outbox_events
WHERE dead_lettered_at IS NOT NULL
ORDER BY dead_lettered_at DESC;
```

PostgreSQL row locks allow multiple publisher processes to share the backlog safely:

```powershell
docker compose up -d --scale outbox-publisher=2
```

## Rules and ML risk worker

The Python `risk-service` validates `payments.created` envelopes before using them. It atomically records each source `event_id` and updates customer velocity, seen-device, and first-observed-country features in Redis. A replay returns the cached feature snapshot instead of incrementing velocity twice.

Rules produce `ALLOW`, `REVIEW`, or `BLOCK`, a score from 0 to 100, readable reason codes, `rules-v1`, and a UTC decision timestamp. Current signals are:

- high or extreme amount;
- five-minute customer velocity;
- a newly observed device;
- a country different from the customer's first observed country;
- explicitly configured merchant IDs and countries.

`RISKY_MERCHANT_IDS` and `HIGH_RISK_COUNTRIES` default to empty because the project has no authoritative risk list. Failure-history rules are intentionally deferred until a trustworthy failure/decision history exists. The first observed country is an online heuristic, not verified customer residency.

The second layer loads `xgb-synthetic-v1`. It scores the same amount, velocity, device, and cross-border features used during training. A model score above its validation-selected threshold may escalate `ALLOW` to `REVIEW`; only rules can directly `BLOCK`. Each schema-v2 decision includes the final score, rule score, model score and probability, threshold, reason codes, rule version, and model version.

The model was trained on 50,000 reproducible fictional payments with a 60/20/20 train/validation/test split. Its measured synthetic test results are precision `0.133333`, recall `0.506887`, F1 `0.211130`, PR-AUC `0.198821`, and ROC-AUC `0.764561`. These are synthetic-data engineering results, not real banking performance. See [the model documentation](docs/ml-model.md) for the cost assumptions, confusion matrix, artifact hashes, reproduction command, and limitations.

The decision is published to `risk.decisions` before the input Kafka offset is committed. A crash in between can publish a duplicate, so each output uses a deterministic decision `event_id` derived from the input `event_id`; downstream consumers must deduplicate it. Invalid event contracts are published to `risk.invalid-events` before their offsets are committed, preventing a poison record from blocking its partition without silently discarding it.

Run Python verification directly:

```powershell
Push-Location services/risk-service
python -m venv .venv
& .\.venv\Scripts\python.exe -m pip install ".[dev]"
& .\.venv\Scripts\python.exe -m ruff format --check .
& .\.venv\Scripts\python.exe -m ruff check .
$env:TEST_REDIS_URL = "redis://localhost:6379/15"
& .\.venv\Scripts\python.exe -m pytest -q
Pop-Location
```

Reproduce the versioned model from `services/risk-service`:

```powershell
& .\.venv\Scripts\python.exe -m risk_service.training `
    --output-dir artifacts `
    --samples 50000 `
    --seed 20260830
```

## Go verification

```powershell
Push-Location services/payment-api
go vet ./...
go mod tidy
go test ./...
$env:TEST_DATABASE_URL = "postgres://riskflow:your_local_password@localhost:5432/riskflow?sslmode=disable"
go test -tags=integration -count=1 -p=1 ./...
go build ./...
Pop-Location
```

## Migrations

Migrations use `golang-migrate` v4.19.1 and live in `database/migrations` as matching `up` and `down` files. The migration container records the applied version in PostgreSQL's `schema_migrations` table:

```powershell
docker exec riskflow1-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT version, dirty FROM schema_migrations;"'
```
