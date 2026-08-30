# Manual review controls

## Trust boundary

`REVIEW_AUTH_CREDENTIALS_JSON` is required when the payment API starts. It is a JSON array whose entries bind one opaque bearer token to one reviewer identity and role:

```json
[
  {
    "reviewer_id": "reviewer-operations-1",
    "role": "risk_reviewer",
    "token": "replace_with_a_random_secret_of_at_least_32_characters"
  }
]
```

Environment variables are the source of truth. Go does not read `.env`; Docker Compose may read `.env` and pass the credential JSON explicitly. `.env` is ignored by Git and credentials must never be committed.

At startup, the API validates the entire credential set and fails closed for missing entries, short tokens, unsupported roles, duplicate identities, or duplicate tokens. The authenticator retains SHA-256 token digests and compares presented tokens in constant time. The token mapping supplies the audit identity; headers such as `X-Reviewer-ID` are ignored.

This static local credential mechanism keeps the checkpoint small and testable. A deployed system should replace it with an identity provider, short-lived signed access tokens, centralized rotation/revocation, and transport security. It should not expose this local HTTP configuration directly to an untrusted network.

## Roles

| Role | List pending reviews | Approve | Reject |
| --- | ---: | ---: | ---: |
| `risk_auditor` | Yes | No | No |
| `risk_reviewer` | Yes | Yes | Yes |

Missing or invalid credentials return typed `401 Unauthorized` and `WWW-Authenticate: Bearer`. An authenticated auditor attempting an action receives typed `403 Forbidden`.

## Endpoints

`GET /v1/reviews?limit=50` lists oldest pending items first. `limit` defaults to 50 and must be between 1 and 100. Each item includes payment data, automated decision score and reasons, rule/model versions, queue version, and timestamps.

`POST /v1/reviews/{payment_id}/approve` and `POST /v1/reviews/{payment_id}/reject` accept:

```json
{
  "expected_version": 1,
  "reason_code": "CUSTOMER_VERIFIED"
}
```

Reason codes are normalized to uppercase and contain only letters, digits, and underscores. They are required and limited to 100 characters. Approval produces queue status `APPROVED` and payment status `ALLOWED`; rejection produces queue status `REJECTED` and payment status `BLOCKED`.

Typed conflicts are:

- `review_version_conflict`: the item is still pending but its version differs;
- `review_already_resolved`: another reviewer already completed the item;
- `review_state_conflict`: the queue and payment states disagree.

Version conflicts include `current_status` and `current_version` so a client can refresh instead of retrying blindly.

## Atomic state transition

One PostgreSQL transaction:

1. conditionally updates the pending queue row only when its version matches;
2. changes the payment from `REVIEW` to `ALLOWED` or `BLOCKED`;
3. inserts `MANUAL_REVIEW_APPROVED` or `MANUAL_REVIEW_REJECTED` in immutable `audit_events` with actor type `USER`, token-bound reviewer identity, reason code, old/new states, role, and versions;
4. commits all three changes.

If the payment update or audit insert fails, PostgreSQL rolls back the queue change too. Concurrent reviewers contend on the same queue row: exactly one conditional update succeeds, while all later actions receive a conflict.

## Useful inspection queries

```sql
SELECT payment_id, status, version, enqueued_at
FROM manual_review_queue
WHERE status = 'PENDING'
ORDER BY enqueued_at, payment_id;

SELECT payment_id, status, version, reviewer_id, resolution_reason, resolved_at
FROM manual_review_queue
WHERE status IN ('APPROVED', 'REJECTED')
ORDER BY resolved_at DESC;

SELECT aggregate_id AS payment_id, event_type, actor_id, occurred_at, details
FROM audit_events
WHERE event_type IN ('MANUAL_REVIEW_APPROVED', 'MANUAL_REVIEW_REJECTED')
ORDER BY occurred_at DESC;
```
