# API examples

These examples target PowerShell 7 and the local Docker Compose API at `http://localhost:8080`. Identifiers and timestamps in example responses are illustrative; commands create and retrieve real local records.

## Health and readiness

```powershell
Invoke-RestMethod http://localhost:8080/healthz
# { "status": "ok" }

Invoke-RestMethod http://localhost:8080/readyz
# { "status": "ready" }
```

`/healthz` answers only whether the process is alive. `/readyz` makes a bounded PostgreSQL ping and returns typed `503` when the database cannot answer.

## Create a payment

Amounts are integer minor units: `1250` means USD 12.50.

```powershell
$idempotencyKey = "api-example-$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())"
$headers = @{ "Idempotency-Key" = $idempotencyKey }
$body = @{
    customer_id = "customer-api-example"
    merchant_id = "merchant-api-example"
    device_id = "device-api-example"
    amount_minor = 1250
    currency = "usd"
    country = "in"
} | ConvertTo-Json

$created = Invoke-WebRequest `
    -Method Post `
    -Uri http://localhost:8080/v1/payments `
    -Headers $headers `
    -ContentType "application/json" `
    -Body $body `
    -SkipHttpErrorCheck

$created.StatusCode
$payment = $created.Content | ConvertFrom-Json
$payment
```

The first response is `201 Created`. Stored currency and country values are normalized to uppercase:

```json
{
  "id": "11111111-2222-4333-8444-555555555555",
  "customer_id": "customer-api-example",
  "merchant_id": "merchant-api-example",
  "device_id": "device-api-example",
  "amount_minor": 1250,
  "currency": "USD",
  "country": "IN",
  "status": "PENDING_RISK",
  "created_at": "2026-09-01T12:00:00Z",
  "updated_at": "2026-09-01T12:00:00Z"
}
```

## Idempotent replay and conflict

Repeating the same normalized request returns `200 OK` and the original payment ID:

```powershell
$replay = Invoke-WebRequest `
    -Method Post `
    -Uri http://localhost:8080/v1/payments `
    -Headers $headers `
    -ContentType "application/json" `
    -Body $body `
    -SkipHttpErrorCheck

$replay.StatusCode
($replay.Content | ConvertFrom-Json).id -eq $payment.id
# 200
# True
```

Changing a normalized field while reusing the key returns typed `409 Conflict`:

```powershell
$conflictingBody = $body | ConvertFrom-Json
$conflictingBody.amount_minor = 9999

$conflict = Invoke-WebRequest `
    -Method Post `
    -Uri http://localhost:8080/v1/payments `
    -Headers $headers `
    -ContentType "application/json" `
    -Body ($conflictingBody | ConvertTo-Json) `
    -SkipHttpErrorCheck

$conflict.StatusCode
$conflict.Content | ConvertFrom-Json
```

```json
{
  "error": {
    "code": "idempotency_conflict",
    "message": "Idempotency-Key was already used with a different payment request"
  }
}
```

## Typed validation failure

Invalid requests create no payment or outbox row:

```powershell
$invalid = Invoke-WebRequest `
    -Method Post `
    -Uri http://localhost:8080/v1/payments `
    -Headers @{ "Idempotency-Key" = "invalid-api-example" } `
    -ContentType "application/json" `
    -Body (@{
        customer_id = ""
        merchant_id = "merchant-api-example"
        device_id = "device-api-example"
        amount_minor = 0
        currency = "US"
        country = "IND"
    } | ConvertTo-Json) `
    -SkipHttpErrorCheck

$invalid.StatusCode
$invalid.Content | ConvertFrom-Json -AsHashtable
```

The response is `400` with stable field errors:

```json
{
  "error": {
    "code": "validation_error",
    "message": "payment request validation failed",
    "fields": {
      "amount_minor": "must be greater than zero",
      "country": "must be a two-letter country code",
      "currency": "must be a three-letter currency code",
      "customer_id": "is required"
    }
  }
}
```

## Retrieve the final payment

```powershell
do {
    Start-Sleep -Milliseconds 100
    $stored = Invoke-RestMethod "http://localhost:8080/v1/payments/$($payment.id)"
} while ($stored.status -eq "PENDING_RISK")

$stored
```

The asynchronous final state is `ALLOWED`, `REVIEW`, or `BLOCKED`. A malformed UUID returns `400 invalid_payment_id`; a valid UUID that does not exist returns `404 payment_not_found`.

## Generate one payment of each outcome

The checked-in generator creates deterministic ALLOWED, REVIEW, and BLOCKED examples with the default `.env.example` thresholds and pinned model:

```powershell
& .\scripts\generate-demo-payments.ps1
```

Reusing its reported `run_id` is safe: the three original idempotency keys replay rather than create duplicates.

## List and resolve manual review

Read the reviewer token from your local secret configuration; do not place the token in source control or PowerShell history shared with others.

```powershell
$reviewToken = Read-Host "Local risk_reviewer token" -MaskInput
$authHeaders = @{ Authorization = "Bearer $reviewToken" }

$queue = Invoke-RestMethod `
    -Method Get `
    -Uri "http://localhost:8080/v1/reviews?limit=50" `
    -Headers $authHeaders

$item = $queue.reviews | Select-Object -First 1
$item

$resolution = @{
    expected_version = $item.version
    reason_code = "CUSTOMER_VERIFIED"
} | ConvertTo-Json

Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/v1/reviews/$($item.payment_id)/approve" `
    -Headers $authHeaders `
    -ContentType "application/json" `
    -Body $resolution
```

The API takes reviewer identity from the bearer-token mapping. A stale `expected_version` returns a typed `409` and cannot overwrite the winning reviewer.

## Read the operational snapshot

```powershell
$auditorToken = Read-Host "Local risk_auditor token" -MaskInput

$snapshot = Invoke-RestMethod `
    -Method Get `
    -Uri "http://localhost:8080/v1/dashboard?recent_limit=20" `
    -Headers @{ Authorization = "Bearer $auditorToken" }

$snapshot.payments
$snapshot.decisions
$snapshot.processing
$snapshot.reconciliation
```

This response is marked `Cache-Control: no-store`. The token is server-side configuration for the Next.js dashboard and is never a `NEXT_PUBLIC_` value.

## Reconciliation and metrics

```powershell
docker compose run --rm decision-reconciler

Invoke-WebRequest http://localhost:8080/metrics |
    Select-Object -ExpandProperty Content
```

The reconciler returns JSON evidence for missing, duplicate, or inconsistent records. Metrics use bounded labels and do not expose customer, payment, event, or request IDs.
