# RiskFlow

RiskFlow is a real-time payment risk and ML decisioning platform. Checkpoint 1A establishes the reliable Go service foundation, PostgreSQL migrations, Kafka and Redis infrastructure, and independently meaningful liveness and readiness checks.

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

## Go verification

```powershell
Push-Location services/payment-api
go vet ./...
go mod tidy
go test ./...
go build ./...
Pop-Location
```

## Migrations

Migrations use `golang-migrate` v4.19.1 and live in `database/migrations` as matching `up` and `down` files. The migration container records the applied version in PostgreSQL's `schema_migrations` table:

```powershell
docker exec riskflow1-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT version, dirty FROM schema_migrations;"'
```
