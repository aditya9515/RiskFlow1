# RiskFlow resume material

## Project heading

**RiskFlow — Real-Time Payment Risk & ML Decisioning Platform**

Go, PostgreSQL, Kafka, Python, XGBoost, Redis, Spark Structured Streaming, Next.js, Docker

## Recommended resume bullets

- Engineered an idempotent Go payment API using normalized SHA-256 request fingerprints and a PostgreSQL transactional outbox; a 64-request test at concurrency 32 produced exactly one payment, one outbox event, and one risk decision.
- Built an at-least-once Kafka risk pipeline with durable `SKIP LOCKED` outbox retries, Redis-backed velocity/replay features, explainable rules plus versioned XGBoost scoring, immutable audit history, optimistic manual review, and reconciliation controls.
- Verified the platform with 179 conservatively counted Go/Python/TypeScript/Spark tests; across three bounded local 250-request runs, measured median payment-acceptance throughput of 145.279 requests/s with median run-level p95 latency of 35.569 ms and zero failed requests.

## Compact two-bullet version

- Built a Go/PostgreSQL/Kafka payment-risk platform with normalized idempotency, transactional outbox delivery, explainable Python/XGBoost decisions, Redis online features, and immutable review/audit controls.
- Verified 179 tests and concurrent retry safety; measured 145.279 requests/s median local payment-acceptance throughput and 35.569 ms median run-level p95 latency across three bounded runs.

## One-line portfolio description

Reliable local payment-risk platform demonstrating idempotent acceptance, transactional event delivery, explainable rules/ML decisions, manual-review controls, reconciliation, streaming analytics, and measured failure recovery.

## Evidence behind each claim

| Resume claim | Repository evidence |
| --- | --- |
| Normalized SHA-256 idempotency | Payment validation/fingerprint service, unique `payments.idempotency_key`, and concurrent integration/load tests. |
| Transactional outbox | Migration `000001`, Go payment repository transaction, and outbox publisher. |
| Safe multiple publishers | PostgreSQL `FOR UPDATE SKIP LOCKED` claim query and simultaneous-worker tests. |
| Explainable rules and XGBoost | Versioned `rules-v1` decisions, `xgb-synthetic-v1` artifact/metadata, and explicit reason codes. |
| Immutable audit and review controls | Migrations `000003`/`000004`, immutable triggers, role mapping, and optimistic queue version. |
| 179 tests | Conservative language-specific count in [benchmarks](benchmarks.md); Go subtests are not counted again. |
| 145.279 requests/s and 35.569 ms p95 | Median of three checked-in, bounded local payment-acceptance samples in [benchmarks](benchmarks.md). |
| Zero failed requests | All 750 unique requests in the three measured samples returned `201` and distinct IDs. |

## Claims intentionally excluded

- No public or cloud deployment has been verified.
- No production availability, security, compliance, or scalability claim has been measured.
- Synthetic XGBoost metrics are not included because they do not represent real banking performance.
- “Exactly once” is excluded; the implemented contract is at-least-once delivery with idempotent business effects.
- The local burst exposed a single-risk-worker queue bottleneck, so HTTP acceptance throughput must not be described as end-to-end decision capacity.

## Interview-safe metric wording

Use: “Across three bounded local runs of 250 unique requests at concurrency 25, median payment-acceptance throughput was 145.279 requests per second, and the median run-level p95 was 35.569 milliseconds.”

Avoid: “RiskFlow handles 145 requests per second in production.”

Use: “The synthetic model test demonstrated the training and threshold-selection pipeline.”

Avoid: “The model detects banking fraud with 76% accuracy.”
