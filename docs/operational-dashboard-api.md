# Operational dashboard API

Checkpoint 6A establishes the read-only PostgreSQL contract for the operational dashboard. The Next.js interface is intentionally deferred to Checkpoint 6B so it can consume a tested, stable response instead of embedding database assumptions in the browser.

## Endpoint and access

`GET /v1/dashboard?recent_limit=20` returns one operational snapshot. `recent_limit` defaults to `20` and must be between `1` and `100`.

The endpoint requires the same bearer credentials as manual-review operations. Both `risk_reviewer` and `risk_auditor` can read it. Tokens are loaded from `REVIEW_AUTH_CREDENTIALS_JSON`, hashed after startup, and never returned by the API. Responses set `Cache-Control: no-store` because they contain customer and payment operations data.

Typed failures are:

- `400 validation_error` for an invalid `recent_limit`;
- `401 unauthorized` for missing or invalid bearer credentials;
- `403 forbidden` for an unsupported role;
- `504 request_timeout` when the bounded database work exceeds `DASHBOARD_REQUEST_TIMEOUT`;
- `500 internal_error` for an unexpected reporting failure.

## Response sections

- `payments` contains total payment count, total integer minor-unit amount, and counts for every valid payment status.
- `decisions` contains total automated decisions, `ALLOW`/`REVIEW`/`BLOCK` counts, average persisted risk score, and the rule/model versions from the most recent decision.
- `manual_review.pending` is the current pending review backlog.
- `processing` distinguishes unpublished outbox backlog, the retrying subset, terminal dead-lettered outbox rows, and rejected decision Kafka records.
- `reconciliation` contains the configured grace period plus exception counts grouped by control code. It deliberately omits detailed exception text from this frequently refreshed endpoint.
- `recent_decisions` contains newest-first payment and decision evidence, including amount, country, status, score, reason codes, versions, and decision timestamp.

`outbox_retrying` is a subset of `outbox_pending`, not an additional backlog. A payment amount is always represented as `amount_minor`; the API never converts it to floating point.

## Query consistency and sources

Payment, decision, review, processing, and recent-decision queries run inside one PostgreSQL `REPEATABLE READ`, read-only transaction. They therefore see a consistent committed database snapshot even while workers continue writing.

The reconciliation count runs immediately afterward using the same control rules as the full `decision-reconciler` report. It returns grouped counts rather than materializing every exception. A concurrent commit between these two read boundaries can appear in reconciliation before it appears in the other totals; the next dashboard refresh naturally converges.

PostgreSQL is the operational system of record for this API. Spark's Parquet aggregates remain a separate analytical/recovery output and are not read by the request path. This keeps dashboard availability independent of the Spark job and avoids presenting eventually written lake files as transactional state.

Migration `000005_dashboard_reporting_indexes` adds the global `(decision_at DESC, decision_id DESC)` index used by the bounded recent-decision query.

## PowerShell verification

```powershell
$auditorToken = "your_local_auditor_token"
$headers = @{ Authorization = "Bearer $auditorToken" }

Invoke-RestMethod -Method Get `
    -Uri "http://localhost:8080/v1/dashboard?recent_limit=20" `
    -Headers $headers
```

Compare core totals directly with PostgreSQL:

```sql
SELECT count(*) AS payment_count, COALESCE(sum(amount_minor), 0) AS amount_minor_total
FROM payments;

SELECT decision, count(*)
FROM payment_decisions
GROUP BY decision
ORDER BY decision;

SELECT count(*) AS pending_reviews
FROM manual_review_queue
WHERE status = 'PENDING';
```

During Checkpoint 6A verification, three API-created payments produced one persisted `ALLOW`, one `REVIEW`, and one `BLOCK`. The endpoint and direct SQL both reported `3` payments, `730000` total minor units, one decision of each type, one pending review, no processing failures, and zero reconciliation exceptions. These are local functional-test records, not load-test or production performance results.
