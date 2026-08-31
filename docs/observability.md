# Observability and metrics

Checkpoint 7A adds bounded, Prometheus-compatible instrumentation to the three Go processes. It does not add a Prometheus server or claim production alerting; each process exposes a scrape endpoint that a future collector can poll.

## Endpoints

| Process | Endpoint | Compose exposure |
| --- | --- | --- |
| `payment-api` | `GET :8080/metrics` | host port `${HTTP_PORT}` |
| `outbox-publisher` | `GET :9091/metrics` | Compose network only |
| `risk-decision-consumer` | `GET :9092/metrics` | Compose network only |

Both workers also expose `GET /healthz` on their metrics listeners. This liveness route proves that the process and listener are running; individual PostgreSQL and Kafka failures are visible through retries and operation results rather than hidden behind liveness.

PowerShell verification from the repository root:

```powershell
(Invoke-WebRequest http://localhost:8080/metrics).Content

$outboxContainer = docker compose ps -q outbox-publisher | Select-Object -First 1
docker exec $outboxContainer wget -qO- http://127.0.0.1:9091/metrics

$decisionContainer = docker compose ps -q risk-decision-consumer
docker exec $decisionContainer wget -qO- http://127.0.0.1:9092/metrics
```

## Correlation and logs

Every API response includes `X-Request-ID`. The middleware preserves a syntactically valid UUIDv4 supplied in that header or generates a cryptographically random UUIDv4. The same value is attached to the request context, written to the structured completion log, and used as the outbox event `trace_id` for a new payment.

Request logs include `request_id`, HTTP method, normalized route, status code, and elapsed milliseconds. Domain logs continue to contain event and aggregate identifiers where needed for diagnosis. Secrets, bearer tokens, request bodies, and raw database URLs are not logged.

## Metric contracts

API and PostgreSQL state:

- `riskflow_http_requests_total{method,route,status_code}`
- `riskflow_http_request_duration_seconds{method,route}`
- `riskflow_http_in_flight_requests`
- `riskflow_outbox_events{state}` where state is `pending`, `retrying`, or `dead_lettered`; live backlog is `pending + retrying`
- `riskflow_decision_events_rejected`
- `riskflow_manual_reviews_pending`
- `riskflow_postgres_metrics_collection_success`
- `riskflow_postgres_metrics_collection_duration_seconds`

Outbox publisher:

- `riskflow_outbox_publish_attempts_total{result}` where result is `success`, `retryable_error`, or `quarantined`
- `riskflow_outbox_publish_duration_seconds{result}`
- `riskflow_outbox_retries_total{stage}`

Decision persistence consumer:

- `riskflow_decision_records_total{disposition}` where disposition is `applied`, `replayed`, or `rejected`
- `riskflow_decision_processing_duration_seconds{disposition}`
- `riskflow_decision_end_to_end_latency_seconds{disposition}`
- `riskflow_decision_retries_total{stage}`
- `riskflow_kafka_consumer_lag_records{topic,partition}`

Each process also exposes standard Go runtime/process series plus `riskflow_build_info{service,version}`. The current development binary reports version `dev`; release-version injection is deferred until a real release pipeline exists.

## Failure semantics

The API operational collector executes one count-only PostgreSQL query per scrape with `METRICS_COLLECTION_TIMEOUT` (default `1s`). On failure it exports collection success as `0`, omits queue gauges instead of presenting stale values, logs a structured warning, and still returns the scrape response. API `/healthz` remains independent of PostgreSQL; `/readyz` remains the typed database readiness contract.

Kafka lag is an approximate high-watermark distance captured when a decision record is fetched. It is not a broker-wide consumer-group lag audit and can momentarily differ during rebalances. Topic and partition are bounded by configured Kafka topology. No metric label accepts payment IDs, event IDs, trace IDs, customer IDs, error text, URLs, or raw untrusted values.
