# RiskFlow

RiskFlow is a real-time payment risk and ML decisioning platform. The payment core provides validated, idempotent payment creation backed by a PostgreSQL transaction and transactional outbox.

## Requirements

- Docker Desktop with Linux containers
- Go 1.26 or newer for local development
- PowerShell 7 for the documented Windows commands

## Local configuration

Docker Compose reads `.env`; the Go application does not. Environment variables are the application's only configuration source.

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
