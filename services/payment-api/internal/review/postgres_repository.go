package review

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository persists review actions as one atomic state transition.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) ListPending(ctx context.Context, limit int) ([]Item, error) {
	rows, err := r.pool.Query(ctx, selectPendingReviewsSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending manual reviews: %w", err)
	}
	defer rows.Close()

	items := make([]Item, 0)
	for rows.Next() {
		item, scanErr := scanItem(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan pending manual review: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending manual reviews: %w", err)
	}
	return items, nil
}

func (r *PostgresRepository) Resolve(ctx context.Context, command ResolveCommand) (Item, error) {
	reviewStatus, paymentStatus, eventType, err := actionState(command.Action)
	if err != nil {
		return Item{}, err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Item{}, fmt.Errorf("begin manual review transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var decisionID string
	err = tx.QueryRow(ctx, resolveReviewSQL,
		command.PaymentID,
		reviewStatus,
		command.ExpectedVersion,
		command.ResolvedAt,
		command.Principal.ReviewerID,
		command.ReasonCode,
	).Scan(&decisionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Item{}, currentConflict(ctx, tx, command.PaymentID)
	}
	if err != nil {
		return Item{}, fmt.Errorf("resolve manual review queue item: %w", err)
	}

	result, err := tx.Exec(ctx, updatePaymentStatusSQL, command.PaymentID, paymentStatus, command.ResolvedAt)
	if err != nil {
		return Item{}, fmt.Errorf("update reviewed payment status: %w", err)
	}
	if result.RowsAffected() != 1 {
		return Item{}, ErrReviewStateConflict
	}

	details, err := json.Marshal(map[string]any{
		"action":                  command.Action,
		"previous_payment_status": "REVIEW",
		"new_payment_status":      paymentStatus,
		"previous_review_status":  StatusPending,
		"new_review_status":       reviewStatus,
		"expected_version":        command.ExpectedVersion,
		"new_version":             command.ExpectedVersion + 1,
		"reason_code":             command.ReasonCode,
		"reviewer_role":           command.Principal.Role,
	})
	if err != nil {
		return Item{}, fmt.Errorf("encode manual review audit details: %w", err)
	}
	if _, err := tx.Exec(ctx, insertManualAuditSQL,
		command.AuditID,
		command.PaymentID,
		eventType,
		command.Principal.ReviewerID,
		decisionID,
		command.ResolvedAt,
		details,
	); err != nil {
		return Item{}, fmt.Errorf("insert manual review audit event: %w", err)
	}

	item, err := scanItem(tx.QueryRow(ctx, selectReviewByPaymentIDSQL, command.PaymentID))
	if err != nil {
		return Item{}, fmt.Errorf("load resolved manual review: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Item{}, fmt.Errorf("commit manual review transaction: %w", err)
	}
	return item, nil
}

func actionState(action string) (reviewStatus, paymentStatus, eventType string, err error) {
	switch action {
	case ActionApprove:
		return StatusApproved, "ALLOWED", "MANUAL_REVIEW_APPROVED", nil
	case ActionReject:
		return StatusRejected, "BLOCKED", "MANUAL_REVIEW_REJECTED", nil
	default:
		return "", "", "", fmt.Errorf("unsupported manual review action %q", action)
	}
}

func currentConflict(ctx context.Context, tx pgx.Tx, paymentID string) error {
	var status string
	var version int
	err := tx.QueryRow(ctx, `SELECT status, version FROM manual_review_queue WHERE payment_id = $1`, paymentID).Scan(&status, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrReviewNotFound
	}
	if err != nil {
		return fmt.Errorf("load current manual review state: %w", err)
	}
	cause := ErrReviewVersionConflict
	if status != StatusPending {
		cause = ErrReviewAlreadyResolved
	}
	return &ConflictError{Cause: cause, CurrentStatus: status, CurrentVersion: version}
}

type rowScanner interface {
	Scan(...any) error
}

func scanItem(row rowScanner) (Item, error) {
	var item Item
	err := row.Scan(
		&item.PaymentID,
		&item.DecisionID,
		&item.CustomerID,
		&item.MerchantID,
		&item.AmountMinor,
		&item.Currency,
		&item.Country,
		&item.PaymentStatus,
		&item.ReviewStatus,
		&item.Version,
		&item.RiskScore,
		&item.ReasonCodes,
		&item.RuleVersion,
		&item.ModelVersion,
		&item.EnqueuedAt,
		&item.ResolvedAt,
		&item.ReviewerID,
		&item.ResolutionReason,
	)
	item.Currency = strings.TrimSpace(item.Currency)
	item.Country = strings.TrimSpace(item.Country)
	item.EnqueuedAt = item.EnqueuedAt.UTC()
	if item.ResolvedAt != nil {
		resolvedAt := item.ResolvedAt.UTC()
		item.ResolvedAt = &resolvedAt
	}
	return item, err
}

const reviewItemColumns = `
    q.payment_id::text,
    q.decision_id::text,
    p.customer_id,
    p.merchant_id,
    p.amount_minor,
    p.currency::text,
    p.country::text,
    p.status,
    q.status,
    q.version,
    d.risk_score,
    d.reason_codes,
    d.rule_version,
    d.model_version,
    q.enqueued_at,
    q.resolved_at,
    COALESCE(q.reviewer_id, ''),
    COALESCE(q.resolution_reason, '')`

const reviewItemJoins = `
FROM manual_review_queue q
JOIN payments p ON p.id = q.payment_id
JOIN payment_decisions d ON d.decision_id = q.decision_id`

var selectPendingReviewsSQL = `SELECT` + reviewItemColumns + reviewItemJoins + `
WHERE q.status = 'PENDING'
ORDER BY q.enqueued_at, q.payment_id
LIMIT $1`

var selectReviewByPaymentIDSQL = `SELECT` + reviewItemColumns + reviewItemJoins + `
WHERE q.payment_id = $1`

const resolveReviewSQL = `
UPDATE manual_review_queue
SET status = $2,
    version = version + 1,
    resolved_at = $4,
    reviewer_id = $5,
    resolution_reason = $6
WHERE payment_id = $1
  AND status = 'PENDING'
  AND version = $3
RETURNING decision_id::text`

const updatePaymentStatusSQL = `
UPDATE payments
SET status = $2, updated_at = $3
WHERE id = $1 AND status = 'REVIEW'`

const insertManualAuditSQL = `
INSERT INTO audit_events (
    id, aggregate_type, aggregate_id, event_type, actor_type, actor_id,
    decision_id, occurred_at, details
) VALUES ($1, 'PAYMENT', $2, $3, 'USER', $4, $5, $6, $7)`
