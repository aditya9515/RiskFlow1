//go:build integration

package dashboard

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/decision"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDashboardSnapshotHandlesEmptyDatabase(t *testing.T) {
	pool := dashboardIntegrationPool(t)
	reconciler, err := decision.NewReconciler(pool, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(NewPostgresRepository(pool), reconciler)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Load(context.Background(), 20)
	if err != nil {
		t.Fatalf("load empty dashboard snapshot: %v", err)
	}
	if snapshot.Payments.Total != 0 || snapshot.Payments.AmountMinorTotal != 0 ||
		snapshot.Decisions.Total != 0 || snapshot.Decisions.AverageRiskScore != 0 {
		t.Fatalf("empty summary = payments:%+v decisions:%+v", snapshot.Payments, snapshot.Decisions)
	}
	if snapshot.RecentDecisions == nil || len(snapshot.RecentDecisions) != 0 {
		t.Fatalf("empty recent decisions = %#v", snapshot.RecentDecisions)
	}
	if snapshot.Reconciliation.ByCode == nil || snapshot.Reconciliation.ExceptionCount != 0 {
		t.Fatalf("empty reconciliation = %+v", snapshot.Reconciliation)
	}
}

func TestDashboardSnapshotMatchesPostgresEvidence(t *testing.T) {
	pool := dashboardIntegrationPool(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	seedDashboardEvidence(t, pool, now)

	reconciler, err := decision.NewReconciler(pool, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(NewPostgresRepository(pool), reconciler)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Load(context.Background(), 2)
	if err != nil {
		t.Fatalf("load dashboard snapshot: %v", err)
	}

	if snapshot.Payments.Total != 5 || snapshot.Payments.AmountMinorTotal != 15000 {
		t.Fatalf("payment totals = %+v", snapshot.Payments)
	}
	if snapshot.Payments.ByStatus.PendingRisk != 1 || snapshot.Payments.ByStatus.Allowed != 1 ||
		snapshot.Payments.ByStatus.Review != 1 || snapshot.Payments.ByStatus.Blocked != 1 ||
		snapshot.Payments.ByStatus.Failed != 1 {
		t.Fatalf("payment statuses = %+v", snapshot.Payments.ByStatus)
	}
	if snapshot.Decisions.Total != 3 || snapshot.Decisions.ByOutcome.Allow != 1 ||
		snapshot.Decisions.ByOutcome.Review != 1 || snapshot.Decisions.ByOutcome.Block != 1 ||
		snapshot.Decisions.AverageRiskScore != 50 {
		t.Fatalf("decision summary = %+v", snapshot.Decisions)
	}
	if snapshot.Decisions.LatestModelVersion != "model-v2" || snapshot.Decisions.LatestRuleVersion != "rules-v2" {
		t.Fatalf("latest versions = %q/%q", snapshot.Decisions.LatestRuleVersion, snapshot.Decisions.LatestModelVersion)
	}
	if snapshot.ManualReview.Pending != 1 {
		t.Fatalf("manual-review summary = %+v", snapshot.ManualReview)
	}
	if snapshot.Processing.OutboxPending != 1 || snapshot.Processing.OutboxRetrying != 1 ||
		snapshot.Processing.OutboxDeadLettered != 1 || snapshot.Processing.DecisionEventsRejected != 1 {
		t.Fatalf("processing summary = %+v", snapshot.Processing)
	}
	if len(snapshot.RecentDecisions) != 2 || snapshot.RecentDecisions[0].PaymentID != dashboardPayment3 ||
		snapshot.RecentDecisions[1].PaymentID != dashboardPayment2 {
		t.Fatalf("recent decisions = %+v", snapshot.RecentDecisions)
	}
	if snapshot.Reconciliation.ExceptionCount != 3 ||
		snapshot.Reconciliation.ByCode["MISSING_DECISION"] != 1 ||
		snapshot.Reconciliation.ByCode["DUPLICATE_DELIVERY"] != 1 ||
		snapshot.Reconciliation.ByCode["REJECTED_DECISION_EVENT"] != 1 {
		t.Fatalf("reconciliation = %+v", snapshot.Reconciliation)
	}
}

const (
	dashboardPayment1 = "41000000-0000-4000-8000-000000000001"
	dashboardPayment2 = "41000000-0000-4000-8000-000000000002"
	dashboardPayment3 = "41000000-0000-4000-8000-000000000003"
)

func dashboardIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("create dashboard integration pool: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatalf("ping dashboard integration database: %v", err)
	}
	truncateDashboardTables(t, pool)
	t.Cleanup(func() {
		truncateDashboardTables(t, pool)
		pool.Close()
	})
	return pool
}

func truncateDashboardTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
        TRUNCATE TABLE manual_review_queue, audit_events, decision_ingestion_records,
            payment_decisions, outbox_events, payments CASCADE`)
	if err != nil {
		t.Fatalf("truncate dashboard integration tables: %v", err)
	}
}

func seedDashboardEvidence(t *testing.T, pool *pgxpool.Pool, now time.Time) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin dashboard seed transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	statements := []string{`
INSERT INTO payments (
    id, idempotency_key, request_fingerprint, customer_id, merchant_id, device_id,
    amount_minor, currency, country, status, created_at, updated_at
) VALUES
	('41000000-0000-4000-8000-000000000001', 'dashboard-key-1', repeat('a', 64), 'customer-1', 'merchant-1', 'device-1', 1000, 'INR', 'IN', 'ALLOWED', $1::timestamptz - interval '5 minutes', $1),
	('41000000-0000-4000-8000-000000000002', 'dashboard-key-2', repeat('b', 64), 'customer-2', 'merchant-2', 'device-2', 2000, 'INR', 'IN', 'REVIEW', $1::timestamptz - interval '4 minutes', $1),
	('41000000-0000-4000-8000-000000000003', 'dashboard-key-3', repeat('c', 64), 'customer-3', 'merchant-3', 'device-3', 3000, 'USD', 'US', 'BLOCKED', $1::timestamptz - interval '3 minutes', $1),
	('41000000-0000-4000-8000-000000000004', 'dashboard-key-4', repeat('d', 64), 'customer-4', 'merchant-4', 'device-4', 4000, 'GBP', 'GB', 'FAILED', $1::timestamptz - interval '2 minutes', $1),
	('41000000-0000-4000-8000-000000000005', 'dashboard-key-5', repeat('e', 64), 'customer-5', 'merchant-5', 'device-5', 5000, 'EUR', 'DE', 'PENDING_RISK', $1::timestamptz - interval '10 minutes', $1)`,
		`
INSERT INTO outbox_events (
    id, aggregate_id, event_type, schema_version, occurred_at, trace_id, payload,
    delivery_attempts, next_attempt_at, last_attempt_at, last_error, published_at, dead_lettered_at
) VALUES
	('42000000-0000-4000-8000-000000000001', '41000000-0000-4000-8000-000000000001', 'payments.created', 1, $1::timestamptz - interval '5 minutes', '45000000-0000-4000-8000-000000000001', '{}'::jsonb, 1, $1, $1, NULL, $1, NULL),
	('42000000-0000-4000-8000-000000000002', '41000000-0000-4000-8000-000000000002', 'payments.created', 1, $1::timestamptz - interval '4 minutes', '45000000-0000-4000-8000-000000000002', '{}'::jsonb, 1, $1, $1, NULL, $1, NULL),
	('42000000-0000-4000-8000-000000000003', '41000000-0000-4000-8000-000000000003', 'payments.created', 1, $1::timestamptz - interval '3 minutes', '45000000-0000-4000-8000-000000000003', '{}'::jsonb, 2, $1, $1, 'temporary broker failure', NULL, NULL),
	('42000000-0000-4000-8000-000000000004', '41000000-0000-4000-8000-000000000004', 'payments.created', 1, $1::timestamptz - interval '2 minutes', '45000000-0000-4000-8000-000000000004', '{}'::jsonb, 1, $1, $1, 'unsupported event', NULL, $1),
	('42000000-0000-4000-8000-000000000005', '41000000-0000-4000-8000-000000000005', 'payments.created', 1, $1::timestamptz - interval '10 minutes', '45000000-0000-4000-8000-000000000005', '{}'::jsonb, 1, $1, $1, NULL, $1::timestamptz - interval '9 minutes', NULL)`,
		`
INSERT INTO payment_decisions (
    decision_id, payment_id, source_event_id, trace_id, schema_version, decision,
    risk_score, rule_score, model_score, model_probability, model_review_threshold,
    reason_codes, rule_version, model_version, velocity_5m, new_device, cross_border,
    baseline_country, decision_at, event_fingerprint
) VALUES
	('43000000-0000-4000-8000-000000000001', '41000000-0000-4000-8000-000000000001', '44000000-0000-4000-8000-000000000001', '45000000-0000-4000-8000-000000000001', 2, 'ALLOW', 10, 10, 10, 0.10, 0.50, ARRAY['NO_RISK_SIGNALS'], 'rules-v1', 'model-v1', 1, false, false, 'IN', $1::timestamptz - interval '3 minutes', repeat('1', 64)),
	('43000000-0000-4000-8000-000000000002', '41000000-0000-4000-8000-000000000002', '44000000-0000-4000-8000-000000000002', '45000000-0000-4000-8000-000000000002', 2, 'REVIEW', 50, 50, 50, 0.50, 0.50, ARRAY['HIGH_AMOUNT'], 'rules-v1', 'model-v1', 2, true, false, 'IN', $1::timestamptz - interval '2 minutes', repeat('2', 64)),
	('43000000-0000-4000-8000-000000000003', '41000000-0000-4000-8000-000000000003', '44000000-0000-4000-8000-000000000003', '45000000-0000-4000-8000-000000000003', 2, 'BLOCK', 90, 90, 90, 0.90, 0.50, ARRAY['EXTREME_AMOUNT'], 'rules-v2', 'model-v2', 3, true, true, 'US', $1::timestamptz - interval '1 minute', repeat('3', 64))`,
		`
INSERT INTO manual_review_queue (payment_id, decision_id, enqueued_at)
VALUES ('41000000-0000-4000-8000-000000000002', '43000000-0000-4000-8000-000000000002', $1::timestamptz - interval '2 minutes')`,
		`
INSERT INTO decision_ingestion_records (
	source_topic, source_partition, source_offset, record_sha256, event_id,
	payment_id, disposition, error_code, error_message, rejected_value, recorded_at
) VALUES
	('risk.decisions', 0, 1, repeat('4', 64), '43000000-0000-4000-8000-000000000001', '41000000-0000-4000-8000-000000000001', 'APPLIED', NULL, NULL, NULL, $1),
	('risk.decisions', 0, 2, repeat('5', 64), '43000000-0000-4000-8000-000000000001', '41000000-0000-4000-8000-000000000001', 'REPLAYED', NULL, NULL, NULL, $1),
	('risk.decisions', 0, 3, repeat('6', 64), NULL, NULL, 'REJECTED', 'invalid_event', 'invalid decision event', decode('00', 'hex'), $1)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement, now); err != nil {
			t.Fatalf("seed dashboard integration evidence: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit dashboard seed transaction: %v", err)
	}
}
