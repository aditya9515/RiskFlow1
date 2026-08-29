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

## Go verification

```powershell
Push-Location services/payment-api
go vet ./...
go mod tidy
go test ./...
$env:TEST_DATABASE_URL = "postgres://riskflow:your_local_password@localhost:5432/riskflow?sslmode=disable"
go test -tags=integration -count=1 ./...
go build ./...
Pop-Location
```

## Migrations

Migrations use `golang-migrate` v4.19.1 and live in `database/migrations` as matching `up` and `down` files. The migration container records the applied version in PostgreSQL's `schema_migrations` table:

```powershell
docker exec riskflow1-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT version, dirty FROM schema_migrations;"'
```
