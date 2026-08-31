package dashboard

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository reads a consistent operational snapshot without changing state.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) Load(ctx context.Context, recentLimit int) (Snapshot, error) {
	if recentLimit < 1 || recentLimit > 100 {
		return Snapshot{}, fmt.Errorf("recent decision limit must be between 1 and 100")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Snapshot{}, fmt.Errorf("begin dashboard read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var snapshot Snapshot
	if err := scanPaymentSummary(tx.QueryRow(ctx, paymentSummarySQL), &snapshot.Payments); err != nil {
		return Snapshot{}, fmt.Errorf("load dashboard payment summary: %w", err)
	}
	if err := scanDecisionSummary(tx.QueryRow(ctx, decisionSummarySQL), &snapshot.Decisions); err != nil {
		return Snapshot{}, fmt.Errorf("load dashboard decision summary: %w", err)
	}
	if err := loadLatestVersion(ctx, tx, &snapshot.Decisions); err != nil {
		return Snapshot{}, err
	}
	if err := tx.QueryRow(ctx, manualReviewSummarySQL).Scan(&snapshot.ManualReview.Pending); err != nil {
		return Snapshot{}, fmt.Errorf("load dashboard manual-review summary: %w", err)
	}
	if err := scanProcessingSummary(tx.QueryRow(ctx, processingSummarySQL), &snapshot.Processing); err != nil {
		return Snapshot{}, fmt.Errorf("load dashboard processing summary: %w", err)
	}
	recent, err := loadRecentDecisions(ctx, tx, recentLimit)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.RecentDecisions = recent

	if err := tx.Commit(ctx); err != nil {
		return Snapshot{}, fmt.Errorf("commit dashboard read transaction: %w", err)
	}
	return snapshot, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanPaymentSummary(row rowScanner, summary *PaymentSummary) error {
	return row.Scan(
		&summary.Total,
		&summary.AmountMinorTotal,
		&summary.ByStatus.PendingRisk,
		&summary.ByStatus.Allowed,
		&summary.ByStatus.Review,
		&summary.ByStatus.Blocked,
		&summary.ByStatus.Failed,
	)
}

func scanDecisionSummary(row rowScanner, summary *DecisionSummary) error {
	return row.Scan(
		&summary.Total,
		&summary.ByOutcome.Allow,
		&summary.ByOutcome.Review,
		&summary.ByOutcome.Block,
		&summary.AverageRiskScore,
	)
}

func scanProcessingSummary(row rowScanner, summary *ProcessingSummary) error {
	return row.Scan(
		&summary.OutboxPending,
		&summary.OutboxRetrying,
		&summary.OutboxDeadLettered,
		&summary.DecisionEventsRejected,
	)
}

func loadLatestVersion(ctx context.Context, tx pgx.Tx, summary *DecisionSummary) error {
	err := tx.QueryRow(ctx, latestVersionSQL).Scan(
		&summary.LatestRuleVersion,
		&summary.LatestModelVersion,
		&summary.LatestDecisionAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load dashboard latest model version: %w", err)
	}
	if summary.LatestDecisionAt != nil {
		value := summary.LatestDecisionAt.UTC()
		summary.LatestDecisionAt = &value
	}
	return nil
}

func loadRecentDecisions(ctx context.Context, tx pgx.Tx, limit int) ([]RecentDecision, error) {
	rows, err := tx.Query(ctx, recentDecisionsSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("query recent dashboard decisions: %w", err)
	}
	defer rows.Close()

	items := make([]RecentDecision, 0, limit)
	for rows.Next() {
		var item RecentDecision
		if err := rows.Scan(
			&item.DecisionID,
			&item.PaymentID,
			&item.CustomerID,
			&item.MerchantID,
			&item.AmountMinor,
			&item.Currency,
			&item.Country,
			&item.PaymentStatus,
			&item.Decision,
			&item.RiskScore,
			&item.ReasonCodes,
			&item.RuleVersion,
			&item.ModelVersion,
			&item.DecisionAt,
		); err != nil {
			return nil, fmt.Errorf("scan recent dashboard decision: %w", err)
		}
		item.Currency = strings.TrimSpace(item.Currency)
		item.Country = strings.TrimSpace(item.Country)
		item.DecisionAt = item.DecisionAt.UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent dashboard decisions: %w", err)
	}
	return items, nil
}

const paymentSummarySQL = `
SELECT
    count(*),
    COALESCE(sum(amount_minor), 0),
    count(*) FILTER (WHERE status = 'PENDING_RISK'),
    count(*) FILTER (WHERE status = 'ALLOWED'),
    count(*) FILTER (WHERE status = 'REVIEW'),
    count(*) FILTER (WHERE status = 'BLOCKED'),
    count(*) FILTER (WHERE status = 'FAILED')
FROM payments`

const decisionSummarySQL = `
SELECT
    count(*),
    count(*) FILTER (WHERE decision = 'ALLOW'),
    count(*) FILTER (WHERE decision = 'REVIEW'),
    count(*) FILTER (WHERE decision = 'BLOCK'),
    COALESCE(avg(risk_score), 0)::double precision
FROM payment_decisions`

const latestVersionSQL = `
SELECT rule_version, model_version, decision_at
FROM payment_decisions
ORDER BY decision_at DESC, decision_id DESC
LIMIT 1`

const manualReviewSummarySQL = `
SELECT count(*)
FROM manual_review_queue
WHERE status = 'PENDING'`

const processingSummarySQL = `
SELECT
    (SELECT count(*) FROM outbox_events
     WHERE published_at IS NULL AND dead_lettered_at IS NULL),
    (SELECT count(*) FROM outbox_events
     WHERE published_at IS NULL AND dead_lettered_at IS NULL AND delivery_attempts > 0),
    (SELECT count(*) FROM outbox_events WHERE dead_lettered_at IS NOT NULL),
    (SELECT count(*) FROM decision_ingestion_records WHERE disposition = 'REJECTED')`

const recentDecisionsSQL = `
SELECT
    d.decision_id::text,
    d.payment_id::text,
    p.customer_id,
    p.merchant_id,
    p.amount_minor,
    p.currency::text,
    p.country::text,
    p.status,
    d.decision,
    d.risk_score,
    d.reason_codes,
    d.rule_version,
    d.model_version,
    d.decision_at
FROM payment_decisions d
JOIN payments p ON p.id = d.payment_id
ORDER BY d.decision_at DESC, d.decision_id DESC
LIMIT $1`
