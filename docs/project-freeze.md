# RiskFlow project freeze

## Freeze status

RiskFlow reached its interview-ready local release boundary on 1 September 2026. The repository is frozen after the final release gates and GitHub Actions succeed. Future feature work should be a deliberate post-interview decision, not an unplanned change before a demonstration.

This is a verified local Docker Compose system. It is not described as a production deployment, a highly available service, or evidence of real-world fraud-model performance.

## Final live rehearsal

The final rehearsal used run ID `freeze-20260901-170934` against the running Compose stack.

| Check | Measured result |
|---|---:|
| First three-payment run | `979.279 ms`; 3 created |
| Identical replay | `39.060 ms`; 3 replayed |
| Original payment IDs preserved | yes |
| Final outcomes | 1 allowed, 1 review, 1 blocked |
| Payment rows for the run | 3 |
| Outbox rows for the run | 3 |
| Persisted decisions for the run | 3 |
| Pending review rows for the run | 1 |
| Payment API health/readiness | HTTP `200` / `200` |
| Dashboard health/page | HTTP `200` / `200` |
| Redis | `PONG` |
| Kafka application-topic consumer lag | 0 for both risk and persistence groups |
| Applied migration | version 5, not dirty |

Repeating the demo did not create duplicate payment, outbox, or decision rows. The dashboard displayed the generated run. The reconciler completed successfully and reported one intentionally retained `DUPLICATE_DELIVERY` exception from an earlier reliability exercise; that evidence is explained during the technical demo rather than deleted.

The Spark output inspection completed successfully with 840 distinct payment event IDs, 840 distinct risk-decision event IDs, and 47 quarantined test records. These cumulative development-volume counts are not load-test results. Large historical ingestion-latency values include stopped-stack time and are not presented as service latency.

## Interview claim audit

- The two-minute recruiter narration was shortened during rehearsal to leave time for the command, dashboard refresh, and natural pauses.
- The five-minute technical narration leaves more than two minutes for commands, navigation, and follow-up explanation at a deliberate speaking pace.
- The interview pack contains a short-first introduction, architecture and control explanations, failure stories, question banks, and 20 focused interviewer follow-ups.
- Resume statements use the test counts, coverage, load samples, event-delay measurements, and synthetic-model metrics recorded in [benchmarks](benchmarks.md).
- Every model claim is labeled synthetic, every performance claim is scoped to the measured local environment, and no deployment claim is made.
- Answer content was audited for evidence and consistency. Spoken delivery was not assigned a fabricated score; it still depends on the candidate rehearsing aloud.

## Known limits to state plainly

- Compose is single-host development infrastructure, not high availability.
- Delivery across PostgreSQL and Kafka is at least once; consumers deduplicate stable event identities.
- The XGBoost artifact was trained on reproducible synthetic data and does not demonstrate banking fraud accuracy.
- Redis unavailability pauses risk consumption; a missing ML artifact moves otherwise uncertain payments to `REVIEW`.
- The dashboard is an operator demo and needs identity-aware access control before any real deployment.
- The stored reconciliation exception is deliberate operational evidence, not a hidden clean-up item.

## Interview-day runbook

1. Start Docker Desktop early and use the existing warm volumes.
2. Copy `.env.example` to the ignored `.env` only if local configuration is missing; set the dashboard token locally and never commit it.
3. Run the fast checks in [demo](demo.md), then open `http://localhost:3000`.
4. Generate a unique run ID and rehearse the two commands once before joining the interview.
5. Keep the checked-in desktop/mobile screenshots and structured generator output as fallbacks.
6. Do not upgrade dependencies, recreate volumes, retrain the model, or perform an unpracticed outage demonstration immediately before the interview.

## Frozen evidence set

- [Demo scripts and fallback](demo.md)
- [Measured tests and benchmarks](benchmarks.md)
- [Reliability and recovery evidence](reliability.md)
- [Goldman Sachs interview pack](interview-pack.md)
- [Evidence-backed resume bullets](resume.md)
- [Architecture](architecture.md) and [database controls](database.md)
