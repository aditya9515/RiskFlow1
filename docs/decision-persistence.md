# Risk decision persistence and reconciliation

## Transaction boundary

The Go `risk-decision-consumer` reads one `risk.decisions` schema-v2 record at a time with Kafka auto-commit disabled. For a first valid delivery, one PostgreSQL transaction:

1. locks the payment row;
2. inserts immutable decision history;
3. changes `PENDING_RISK` to `ALLOWED`, `REVIEW`, or `BLOCKED`;
4. writes an immutable system audit event;
5. creates a `PENDING` manual-review entry only for `REVIEW`;
6. records the Kafka topic, partition, offset, and record digest;
7. commits.

The worker commits the Kafka offset only after that transaction succeeds. A crash after the database commit but before the offset commit causes a replay. The deterministic decision ID and event fingerprint turn that replay into a receipt without repeating the domain changes.

This is at-least-once consumption with exactly-once database effects, not a claim that Kafka and PostgreSQL share one exactly-once transaction.

New consumer groups default to `DECISION_AUTO_OFFSET_RESET=earliest` so retained decisions are recovered. `latest` is available only for an intentional operational cutover where historical topic data must be skipped.

## Stored controls

- `payment_decisions` is immutable automated decision history with scores, reasons, versions, timestamps, and the exact feature snapshot.
- `audit_events` is an immutable record of who or what changed payment state and why.
- `manual_review_queue` contains review work and its optimistic version. Role-controlled approval or rejection records the reviewer, reason, timestamp, terminal queue status, and matching payment state atomically.
- `decision_ingestion_records` records every accepted, replayed, or rejected Kafka coordinate. Rejected values are retained for investigation instead of being silently discarded.

Reusing a decision ID with different normalized content, reusing one source event for another decision, or applying an automated decision to a payment that is no longer pending is quarantined and reported.

## Exception report

Run the read-only one-shot reconciliation job from the repository root:

```powershell
docker compose run --rm decision-reconciler
```

It emits JSON and checks for:

- a published payment that remains without a decision beyond the grace period;
- multiple decisions for one payment;
- duplicate Kafka deliveries of one decision;
- payment status that disagrees with its decision;
- missing or unexpected manual-review queue entries;
- rejected decision records.

`DUPLICATE_DELIVERY` is evidence that at-least-once delivery occurred; it is not data corruption when the decision, audit, and queue row counts remain one.

## Useful inspection queries

```sql
SELECT decision, count(*)
FROM payment_decisions
GROUP BY decision
ORDER BY decision;

SELECT disposition, count(*)
FROM decision_ingestion_records
GROUP BY disposition
ORDER BY disposition;

SELECT payment_id, decision_id, enqueued_at
FROM manual_review_queue
WHERE status = 'PENDING'
ORDER BY enqueued_at;
```
