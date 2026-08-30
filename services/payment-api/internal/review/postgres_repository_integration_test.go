//go:build integration

package review

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/database"
)

func TestPostgresListPendingReviewEvidence(t *testing.T) {
	repository, pool := newReviewIntegrationRepository(t)
	seedReview(t, pool, integrationReviewPaymentID, integrationReviewDecisionID)

	items, err := repository.ListPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	item := items[0]
	if item.PaymentID != integrationReviewPaymentID || item.DecisionID != integrationReviewDecisionID || item.ReviewStatus != StatusPending || item.Version != 1 {
		t.Fatalf("item identity/state = %+v", item)
	}
	if item.RiskScore != 55 || len(item.ReasonCodes) != 2 || item.ReasonCodes[0] != "HIGH_AMOUNT" {
		t.Fatalf("item evidence = %+v", item)
	}
}

func TestPostgresReviewActionsAreAtomicAndAudited(t *testing.T) {
	tests := []struct {
		name          string
		action        string
		wantReview    string
		wantPayment   string
		wantEventType string
	}{
		{name: "approve", action: ActionApprove, wantReview: StatusApproved, wantPayment: "ALLOWED", wantEventType: "MANUAL_REVIEW_APPROVED"},
		{name: "reject", action: ActionReject, wantReview: StatusRejected, wantPayment: "BLOCKED", wantEventType: "MANUAL_REVIEW_REJECTED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository, pool := newReviewIntegrationRepository(t)
			seedReview(t, pool, integrationReviewPaymentID, integrationReviewDecisionID)
			resolvedAt := time.Date(2026, time.August, 30, 15, 0, 0, 0, time.UTC)
			item, err := repository.Resolve(context.Background(), ResolveCommand{
				PaymentID: integrationReviewPaymentID, Action: test.action, ExpectedVersion: 1,
				ReasonCode: "CUSTOMER_CONTACTED", Principal: Principal{ReviewerID: "reviewer-1", Role: RoleRiskReviewer},
				AuditID: integrationReviewManualAuditID, ResolvedAt: resolvedAt,
			})
			if err != nil {
				t.Fatal(err)
			}
			if item.ReviewStatus != test.wantReview || item.PaymentStatus != test.wantPayment || item.Version != 2 || item.ReviewerID != "reviewer-1" || item.ResolutionReason != "CUSTOMER_CONTACTED" {
				t.Fatalf("resolved item = %+v", item)
			}

			var reviewStatus, paymentStatus, eventType, actorType, actorID, reason string
			var version int
			var auditCount int
			if err := pool.QueryRow(context.Background(), `
                    SELECT q.status, q.version, q.resolution_reason, p.status,
                           a.event_type, a.actor_type, a.actor_id,
                           (SELECT count(*) FROM audit_events WHERE event_type LIKE 'MANUAL_REVIEW_%')
                    FROM manual_review_queue q
                    JOIN payments p ON p.id = q.payment_id
                    JOIN audit_events a ON a.decision_id = q.decision_id AND a.event_type LIKE 'MANUAL_REVIEW_%'
                    WHERE q.payment_id = $1`, integrationReviewPaymentID,
			).Scan(&reviewStatus, &version, &reason, &paymentStatus, &eventType, &actorType, &actorID, &auditCount); err != nil {
				t.Fatal(err)
			}
			if reviewStatus != test.wantReview || paymentStatus != test.wantPayment || eventType != test.wantEventType || version != 2 || reason != "CUSTOMER_CONTACTED" || actorType != "USER" || actorID != "reviewer-1" || auditCount != 1 {
				t.Fatalf("stored state = review:%s v%d reason:%s payment:%s event:%s actor:%s/%s count:%d", reviewStatus, version, reason, paymentStatus, eventType, actorType, actorID, auditCount)
			}
			if _, err := pool.Exec(context.Background(), `UPDATE audit_events SET actor_id = 'changed' WHERE id = $1`, integrationReviewManualAuditID); err == nil {
				t.Fatal("immutable manual review audit accepted an update")
			}
		})
	}
}

func TestPostgresPaymentStateConflictRollsBackQueue(t *testing.T) {
	repository, pool := newReviewIntegrationRepository(t)
	seedReview(t, pool, integrationReviewPaymentID, integrationReviewDecisionID)
	if _, err := pool.Exec(context.Background(), `UPDATE payments SET status = 'ALLOWED' WHERE id = $1`, integrationReviewPaymentID); err != nil {
		t.Fatal(err)
	}
	_, err := repository.Resolve(context.Background(), ResolveCommand{
		PaymentID: integrationReviewPaymentID, Action: ActionReject, ExpectedVersion: 1,
		ReasonCode: "RISK_CONFIRMED", Principal: Principal{ReviewerID: "reviewer-1", Role: RoleRiskReviewer},
		AuditID: integrationReviewManualAuditID, ResolvedAt: time.Now().UTC(),
	})
	if !errors.Is(err, ErrReviewStateConflict) {
		t.Fatalf("error = %v, want ErrReviewStateConflict", err)
	}
	var status string
	var version, manualAudits int
	if err := pool.QueryRow(context.Background(), `
        SELECT status, version,
               (SELECT count(*) FROM audit_events WHERE event_type LIKE 'MANUAL_REVIEW_%')
        FROM manual_review_queue WHERE payment_id = $1`, integrationReviewPaymentID,
	).Scan(&status, &version, &manualAudits); err != nil {
		t.Fatal(err)
	}
	if status != StatusPending || version != 1 || manualAudits != 0 {
		t.Fatalf("queue state after conflict = %s/%d, audits=%d", status, version, manualAudits)
	}
}

func TestPostgresReviewRejectsStaleAndRepeatedActions(t *testing.T) {
	repository, pool := newReviewIntegrationRepository(t)
	seedReview(t, pool, integrationReviewPaymentID, integrationReviewDecisionID)
	base := ResolveCommand{
		PaymentID: integrationReviewPaymentID, Action: ActionApprove, ExpectedVersion: 2,
		ReasonCode: "VERIFIED", Principal: Principal{ReviewerID: "reviewer-1", Role: RoleRiskReviewer},
		AuditID: integrationReviewManualAuditID, ResolvedAt: time.Now().UTC(),
	}
	_, err := repository.Resolve(context.Background(), base)
	var conflict *ConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, ErrReviewVersionConflict) || conflict.CurrentVersion != 1 || conflict.CurrentStatus != StatusPending {
		t.Fatalf("stale error = %v", err)
	}

	base.ExpectedVersion = 1
	if _, err := repository.Resolve(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	base.AuditID = "90000000-0000-4000-8000-000000000002"
	if _, err := repository.Resolve(context.Background(), base); !errors.Is(err, ErrReviewAlreadyResolved) {
		t.Fatalf("repeated error = %v, want ErrReviewAlreadyResolved", err)
	}
}

func TestPostgresConcurrentReviewActionsHaveOneWinner(t *testing.T) {
	repository, pool := newReviewIntegrationRepository(t)
	seedReview(t, pool, integrationReviewPaymentID, integrationReviewDecisionID)

	const workers = 32
	start := make(chan struct{})
	errorsFound := make(chan error, workers)
	var successes atomic.Int32
	var conflicts atomic.Int32
	var waitGroup sync.WaitGroup
	for index := 0; index < workers; index++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			<-start
			action := ActionApprove
			if worker%2 == 1 {
				action = ActionReject
			}
			_, err := repository.Resolve(context.Background(), ResolveCommand{
				PaymentID: integrationReviewPaymentID, Action: action, ExpectedVersion: 1,
				ReasonCode: "CONCURRENT_CHECK", Principal: Principal{ReviewerID: fmt.Sprintf("reviewer-%d", worker), Role: RoleRiskReviewer},
				AuditID: fmt.Sprintf("90000000-0000-4000-8000-%012d", worker+1), ResolvedAt: time.Now().UTC(),
			})
			if err == nil {
				successes.Add(1)
			} else if errors.Is(err, ErrReviewAlreadyResolved) {
				conflicts.Add(1)
			} else {
				errorsFound <- err
			}
		}(index)
	}
	close(start)
	waitGroup.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("concurrent resolution: %v", err)
	}
	if successes.Load() != 1 || conflicts.Load() != workers-1 {
		t.Fatalf("success/conflict = %d/%d", successes.Load(), conflicts.Load())
	}

	var reviewStatus, paymentStatus string
	var version, manualAudits int
	if err := pool.QueryRow(context.Background(), `
        SELECT q.status, q.version, p.status,
               (SELECT count(*) FROM audit_events WHERE event_type LIKE 'MANUAL_REVIEW_%')
        FROM manual_review_queue q JOIN payments p ON p.id = q.payment_id
        WHERE q.payment_id = $1`, integrationReviewPaymentID,
	).Scan(&reviewStatus, &version, &paymentStatus, &manualAudits); err != nil {
		t.Fatal(err)
	}
	if version != 2 || manualAudits != 1 || !((reviewStatus == StatusApproved && paymentStatus == "ALLOWED") || (reviewStatus == StatusRejected && paymentStatus == "BLOCKED")) {
		t.Fatalf("final review/version/payment/audits = %s/%d/%s/%d", reviewStatus, version, paymentStatus, manualAudits)
	}
}

func TestPostgresManualAuditFailureRollsBackAllState(t *testing.T) {
	repository, pool := newReviewIntegrationRepository(t)
	seedReview(t, pool, integrationReviewPaymentID, integrationReviewDecisionID)

	const constraintName = "test_force_manual_review_audit_failure"
	if _, err := pool.Exec(context.Background(), `ALTER TABLE audit_events ADD CONSTRAINT `+constraintName+` CHECK (event_type NOT LIKE 'MANUAL_REVIEW_%') NOT VALID`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS `+constraintName)
	})

	_, err := repository.Resolve(context.Background(), ResolveCommand{
		PaymentID: integrationReviewPaymentID, Action: ActionReject, ExpectedVersion: 1,
		ReasonCode: "RISK_CONFIRMED", Principal: Principal{ReviewerID: "reviewer-1", Role: RoleRiskReviewer},
		AuditID: integrationReviewManualAuditID, ResolvedAt: time.Now().UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), "audit") {
		t.Fatalf("error = %v, want audit failure", err)
	}

	var reviewStatus, paymentStatus string
	var version, manualAudits int
	if err := pool.QueryRow(context.Background(), `
        SELECT q.status, q.version, p.status,
               (SELECT count(*) FROM audit_events WHERE event_type LIKE 'MANUAL_REVIEW_%')
        FROM manual_review_queue q JOIN payments p ON p.id = q.payment_id
        WHERE q.payment_id = $1`, integrationReviewPaymentID,
	).Scan(&reviewStatus, &version, &paymentStatus, &manualAudits); err != nil {
		t.Fatal(err)
	}
	if reviewStatus != StatusPending || version != 1 || paymentStatus != "REVIEW" || manualAudits != 0 {
		t.Fatalf("state after rollback = %s/%d/%s/%d", reviewStatus, version, paymentStatus, manualAudits)
	}
}

const (
	integrationReviewPaymentID     = "30000000-0000-4000-8000-000000000001"
	integrationReviewDecisionID    = "70000000-0000-4000-8000-000000000001"
	integrationReviewManualAuditID = "90000000-0000-4000-8000-000000000001"
)

func newReviewIntegrationRepository(t *testing.T) (*PostgresRepository, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := database.NewPostgresPool(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	truncateReviewTables(t, pool)
	t.Cleanup(func() { truncateReviewTables(t, pool) })
	return NewPostgresRepository(pool), pool
}

func truncateReviewTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
        TRUNCATE TABLE manual_review_queue, audit_events, decision_ingestion_records,
            payment_decisions, outbox_events, payments`); err != nil {
		t.Fatalf("truncate review tables: %v", err)
	}
}

func seedReview(t *testing.T, pool *pgxpool.Pool, paymentID, decisionID string) {
	t.Helper()
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(context.Background(), `
        INSERT INTO payments (
            id, idempotency_key, request_fingerprint, customer_id, merchant_id,
            device_id, amount_minor, currency, country, status, created_at, updated_at
        ) VALUES ($1, 'review-key', $2, 'customer-1', 'merchant-1', 'device-1',
            125000, 'USD', 'IN', 'REVIEW', $3, $3)`,
		paymentID, strings.Repeat("a", 64), now,
	); err != nil {
		t.Fatalf("seed review payment: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
        INSERT INTO payment_decisions (
            decision_id, payment_id, source_event_id, trace_id, schema_version,
            decision, risk_score, rule_score, model_score, model_probability,
            model_review_threshold, reason_codes, rule_version, model_version,
            velocity_5m, new_device, cross_border, baseline_country,
            decision_at, event_fingerprint
        ) VALUES ($4, $1, '71000000-0000-4000-8000-000000000001',
            '72000000-0000-4000-8000-000000000001', 2, 'REVIEW', 55, 45, 62,
            0.62, 0.40, ARRAY['HIGH_AMOUNT','NEW_DEVICE'], 'rules-v1', 'model-v1',
            2, true, false, 'IN', $3, $2)`,
		paymentID, strings.Repeat("a", 64), now, decisionID,
	); err != nil {
		t.Fatalf("seed review decision: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
        INSERT INTO audit_events (
            id, aggregate_type, aggregate_id, event_type, actor_type, actor_id,
            decision_id, occurred_at, details
        ) VALUES ('73000000-0000-4000-8000-000000000001', 'PAYMENT', $1,
            'RISK_DECISION_RECORDED', 'SERVICE', 'risk-decision-consumer', $2, $3, '{}')`,
		paymentID, decisionID, now,
	); err != nil {
		t.Fatalf("seed review audit: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
        INSERT INTO manual_review_queue (payment_id, decision_id, enqueued_at)
		VALUES ($1, $2, $3)`,
		paymentID, decisionID, now,
	); err != nil {
		t.Fatalf("seed review queue: %v", err)
	}
}
