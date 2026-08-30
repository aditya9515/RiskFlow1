package decision

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ReconciliationException is one evidence-backed control break.
type ReconciliationException struct {
	Code       string `json:"code"`
	PaymentID  string `json:"payment_id,omitempty"`
	DecisionID string `json:"decision_id,omitempty"`
	Detail     string `json:"detail"`
}

// ReconciliationReport is a point-in-time exception report.
type ReconciliationReport struct {
	GeneratedAt    time.Time                 `json:"generated_at"`
	GracePeriod    string                    `json:"grace_period"`
	ExceptionCount int                       `json:"exception_count"`
	Exceptions     []ReconciliationException `json:"exceptions"`
}

// Reconciler compares payment, decision, receipt, and review-queue state.
type Reconciler struct {
	pool        *pgxpool.Pool
	gracePeriod time.Duration
	now         func() time.Time
}

func NewReconciler(pool *pgxpool.Pool, gracePeriod time.Duration) (*Reconciler, error) {
	if pool == nil {
		return nil, fmt.Errorf("reconciliation PostgreSQL pool is required")
	}
	if gracePeriod <= 0 {
		return nil, fmt.Errorf("reconciliation grace period must be positive")
	}
	return &Reconciler{pool: pool, gracePeriod: gracePeriod, now: time.Now}, nil
}

// Run returns exceptions without modifying operational data.
func (r *Reconciler) Run(ctx context.Context) (ReconciliationReport, error) {
	generatedAt := r.now().UTC()
	cutoff := generatedAt.Add(-r.gracePeriod)
	rows, err := r.pool.Query(ctx, reconciliationSQL, cutoff)
	if err != nil {
		return ReconciliationReport{}, fmt.Errorf("query decision reconciliation exceptions: %w", err)
	}
	defer rows.Close()

	exceptions := make([]ReconciliationException, 0)
	for rows.Next() {
		var item ReconciliationException
		if err := rows.Scan(&item.Code, &item.PaymentID, &item.DecisionID, &item.Detail); err != nil {
			return ReconciliationReport{}, fmt.Errorf("scan decision reconciliation exception: %w", err)
		}
		exceptions = append(exceptions, item)
	}
	if err := rows.Err(); err != nil {
		return ReconciliationReport{}, fmt.Errorf("iterate decision reconciliation exceptions: %w", err)
	}

	return ReconciliationReport{
		GeneratedAt:    generatedAt,
		GracePeriod:    r.gracePeriod.String(),
		ExceptionCount: len(exceptions),
		Exceptions:     exceptions,
	}, nil
}

const reconciliationSQL = `
WITH exceptions AS (
    SELECT
        'MISSING_DECISION'::text AS code,
        p.id::text AS payment_id,
        ''::text AS decision_id,
        'published payment remains PENDING_RISK beyond the grace period'::text AS detail
    FROM payments p
    WHERE p.status = 'PENDING_RISK'
      AND p.created_at <= $1
      AND EXISTS (
          SELECT 1
          FROM outbox_events o
          WHERE o.aggregate_id = p.id
            AND o.event_type = 'payments.created'
            AND o.published_at IS NOT NULL
      )
      AND NOT EXISTS (SELECT 1 FROM payment_decisions d WHERE d.payment_id = p.id)

    UNION ALL

    SELECT
        'DUPLICATE_DECISION',
        d.payment_id::text,
        '',
        format('%s automated decisions persisted for one payment', count(*))
    FROM payment_decisions d
    GROUP BY d.payment_id
    HAVING count(*) > 1

    UNION ALL

    SELECT
        'DUPLICATE_DELIVERY',
        d.payment_id::text,
        d.decision_id::text,
        format('%s Kafka records carried the same decision event', count(*))
    FROM decision_ingestion_records i
    JOIN payment_decisions d ON d.decision_id = i.event_id
    WHERE i.disposition IN ('APPLIED', 'REPLAYED')
    GROUP BY d.payment_id, d.decision_id
    HAVING count(*) > 1

    UNION ALL

    SELECT
        'PAYMENT_STATUS_MISMATCH',
        p.id::text,
        d.decision_id::text,
        format('payment status %s does not match decision %s', p.status, d.decision)
    FROM payments p
    JOIN payment_decisions d ON d.payment_id = p.id
    LEFT JOIN manual_review_queue q ON q.decision_id = d.decision_id
    WHERE p.status <> CASE
        WHEN d.decision = 'REVIEW' AND q.status = 'APPROVED' THEN 'ALLOWED'
        WHEN d.decision = 'REVIEW' AND q.status = 'REJECTED' THEN 'BLOCKED'
        WHEN d.decision = 'REVIEW' THEN 'REVIEW'
        WHEN d.decision = 'ALLOW' THEN 'ALLOWED'
        WHEN d.decision = 'BLOCK' THEN 'BLOCKED'
    END

    UNION ALL

    SELECT
        'REVIEW_QUEUE_MISSING',
        d.payment_id::text,
        d.decision_id::text,
        'REVIEW decision has no manual-review queue entry'
    FROM payment_decisions d
    LEFT JOIN manual_review_queue q ON q.decision_id = d.decision_id
    WHERE d.decision = 'REVIEW' AND q.decision_id IS NULL

    UNION ALL

    SELECT
        'UNEXPECTED_REVIEW_QUEUE',
        q.payment_id::text,
        q.decision_id::text,
        format('manual-review entry points to a %s decision', d.decision)
    FROM manual_review_queue q
    JOIN payment_decisions d ON d.decision_id = q.decision_id
    WHERE d.decision <> 'REVIEW'

    UNION ALL

    SELECT
        'REJECTED_DECISION_EVENT',
        '',
        '',
        format('%s[%s] offset %s rejected: %s', source_topic, source_partition, source_offset, error_message)
    FROM decision_ingestion_records
    WHERE disposition = 'REJECTED'
)
SELECT code, payment_id, decision_id, detail
FROM exceptions
ORDER BY code, payment_id, decision_id, detail`
