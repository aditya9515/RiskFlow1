//go:build integration

package decision

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/database"
)

func TestPostgresDecisionApplyAndReplayAreAtomicAndIdempotent(t *testing.T) {
	store, pool := newDecisionIntegrationStore(t)
	seedDecisionPayment(t, pool, integrationDecisionPaymentID, "PENDING_RISK", time.Now().UTC(), true)
	event := integrationDecisionEvent(t)

	first, err := store.Apply(context.Background(), event, integrationSourceRecord(10))
	if err != nil || !first.Applied || first.Replayed {
		t.Fatalf("first apply result/error = %+v/%v", first, err)
	}
	second, err := store.Apply(context.Background(), event, integrationSourceRecord(11))
	if err != nil || second.Applied || !second.Replayed {
		t.Fatalf("replay result/error = %+v/%v", second, err)
	}

	assertDecisionCounts(t, pool, 1, 1, 1, 2)
	var status, decision, baselineCountry string
	var riskScore, velocity int
	if err := pool.QueryRow(context.Background(), `
        SELECT p.status, d.decision, d.risk_score, d.velocity_5m, d.baseline_country::text
        FROM payments p
        JOIN payment_decisions d ON d.payment_id = p.id
        WHERE p.id = $1`, integrationDecisionPaymentID,
	).Scan(&status, &decision, &riskScore, &velocity, &baselineCountry); err != nil {
		t.Fatalf("query applied decision: %v", err)
	}
	if status != "REVIEW" || decision != DecisionReview || riskScore != 55 || velocity != 3 || baselineCountry != "IN" {
		t.Fatalf("stored status/decision/risk/velocity/country = %q/%q/%d/%d/%q", status, decision, riskScore, velocity, baselineCountry)
	}

	var applied, replayed int
	if err := pool.QueryRow(context.Background(), `
        SELECT
            count(*) FILTER (WHERE disposition = 'APPLIED'),
            count(*) FILTER (WHERE disposition = 'REPLAYED')
        FROM decision_ingestion_records`,
	).Scan(&applied, &replayed); err != nil {
		t.Fatalf("query ingestion dispositions: %v", err)
	}
	if applied != 1 || replayed != 1 {
		t.Fatalf("applied/replayed receipts = %d/%d", applied, replayed)
	}
}

func TestPostgresConcurrentDecisionReplayCreatesOneDomainChange(t *testing.T) {
	store, pool := newDecisionIntegrationStore(t)
	seedDecisionPayment(t, pool, integrationDecisionPaymentID, "PENDING_RISK", time.Now().UTC(), true)
	event := integrationDecisionEvent(t)

	const workers = 32
	start := make(chan struct{})
	results := make(chan ApplyResult, workers)
	errorsFound := make(chan error, workers)
	var waitGroup sync.WaitGroup
	for index := 0; index < workers; index++ {
		waitGroup.Add(1)
		go func(offset int64) {
			defer waitGroup.Done()
			<-start
			result, err := store.Apply(context.Background(), event, integrationSourceRecord(offset))
			if err != nil {
				errorsFound <- err
				return
			}
			results <- result
		}(int64(100 + index))
	}
	close(start)
	waitGroup.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("concurrent apply: %v", err)
	}
	var applied, replayed int32
	for result := range results {
		if result.Applied {
			atomic.AddInt32(&applied, 1)
		}
		if result.Replayed {
			atomic.AddInt32(&replayed, 1)
		}
	}
	if applied != 1 || replayed != workers-1 {
		t.Fatalf("applied/replayed = %d/%d", applied, replayed)
	}
	assertDecisionCounts(t, pool, 1, 1, 1, workers)
}

func TestPostgresDecisionStatusMapping(t *testing.T) {
	tests := []struct {
		decision string
		status   string
		queue    int
	}{
		{decision: DecisionAllow, status: "ALLOWED", queue: 0},
		{decision: DecisionReview, status: "REVIEW", queue: 1},
		{decision: DecisionBlock, status: "BLOCKED", queue: 0},
	}
	for index, test := range tests {
		t.Run(test.decision, func(t *testing.T) {
			store, pool := newDecisionIntegrationStore(t)
			seedDecisionPayment(t, pool, integrationDecisionPaymentID, "PENDING_RISK", time.Now().UTC(), true)
			event := integrationDecisionEvent(t)
			event.Payload.Decision = test.decision
			if _, err := store.Apply(context.Background(), event, integrationSourceRecord(int64(200+index))); err != nil {
				t.Fatalf("apply %s: %v", test.decision, err)
			}
			var status string
			if err := pool.QueryRow(context.Background(), `SELECT status FROM payments WHERE id = $1`, integrationDecisionPaymentID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != test.status {
				t.Fatalf("status = %q, want %q", status, test.status)
			}
			assertDecisionCounts(t, pool, 1, 1, test.queue, 1)
		})
	}
}

func TestPostgresAuditFailureRollsBackEntireDecision(t *testing.T) {
	store, pool := newDecisionIntegrationStore(t)
	seedDecisionPayment(t, pool, integrationDecisionPaymentID, "PENDING_RISK", time.Now().UTC(), true)

	const constraintName = "test_force_decision_audit_failure"
	if _, err := pool.Exec(context.Background(), `
        ALTER TABLE audit_events
        ADD CONSTRAINT `+constraintName+` CHECK (event_type <> 'RISK_DECISION_RECORDED') NOT VALID`); err != nil {
		t.Fatalf("add audit failure constraint: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS `+constraintName)
	})

	if _, err := store.Apply(context.Background(), integrationDecisionEvent(t), integrationSourceRecord(300)); err == nil || !strings.Contains(err.Error(), "audit") {
		t.Fatalf("error = %v, want audit insertion failure", err)
	}
	assertDecisionCounts(t, pool, 0, 0, 0, 0)
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM payments WHERE id = $1`, integrationDecisionPaymentID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING_RISK" {
		t.Fatalf("status after rollback = %q", status)
	}
}

func TestPostgresDecisionConflictCannotOverwriteHistory(t *testing.T) {
	store, pool := newDecisionIntegrationStore(t)
	seedDecisionPayment(t, pool, integrationDecisionPaymentID, "PENDING_RISK", time.Now().UTC(), true)
	event := integrationDecisionEvent(t)
	if _, err := store.Apply(context.Background(), event, integrationSourceRecord(400)); err != nil {
		t.Fatal(err)
	}
	event.Payload.ModelScore = 99
	if _, err := store.Apply(context.Background(), event, integrationSourceRecord(401)); !errors.Is(err, ErrDecisionConflict) {
		t.Fatalf("error = %v, want ErrDecisionConflict", err)
	}
	assertDecisionCounts(t, pool, 1, 1, 1, 1)
}

func TestPostgresRejectIsDurableIdempotentAndImmutable(t *testing.T) {
	store, pool := newDecisionIntegrationStore(t)
	record := SourceRecord{Topic: "risk.decisions", Partition: 2, Offset: 500, Value: []byte(`{"bad":true}`)}
	rejection := errors.New("schema mismatch")
	if err := store.Reject(context.Background(), record, "invalid_event", rejection); err != nil {
		t.Fatal(err)
	}
	if err := store.Reject(context.Background(), record, "invalid_event", rejection); err != nil {
		t.Fatalf("rejected replay: %v", err)
	}
	var count int
	var disposition, code string
	var raw []byte
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM decision_ingestion_records`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT disposition, error_code, rejected_value
		FROM decision_ingestion_records`,
	).Scan(&disposition, &code, &raw); err != nil {
		t.Fatal(err)
	}
	if count != 1 || disposition != "REJECTED" || code != "invalid_event" || string(raw) != string(record.Value) {
		t.Fatalf("rejected count/disposition/code/raw = %d/%q/%q/%q", count, disposition, code, raw)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE decision_ingestion_records SET error_code = 'changed'`); err == nil {
		t.Fatal("immutable ingestion record accepted an update")
	}
}

func TestPostgresDecisionAndAuditRowsAreImmutable(t *testing.T) {
	store, pool := newDecisionIntegrationStore(t)
	seedDecisionPayment(t, pool, integrationDecisionPaymentID, "PENDING_RISK", time.Now().UTC(), true)
	if _, err := store.Apply(context.Background(), integrationDecisionEvent(t), integrationSourceRecord(600)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE payment_decisions SET risk_score = 1`); err == nil {
		t.Fatal("immutable decision accepted an update")
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM audit_events`); err == nil {
		t.Fatal("immutable audit event accepted a delete")
	}
	assertDecisionCounts(t, pool, 1, 1, 1, 1)
}

func TestDecisionReconciliationReportsMissingDuplicateAndInconsistentState(t *testing.T) {
	store, pool := newDecisionIntegrationStore(t)
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)

	seedDecisionPayment(t, pool, integrationDecisionPaymentID, "PENDING_RISK", now.Add(-time.Minute), true)
	event := integrationDecisionEvent(t)
	if _, err := store.Apply(context.Background(), event, integrationSourceRecord(700)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(context.Background(), event, integrationSourceRecord(701)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE payments SET status = 'ALLOWED' WHERE id = $1`, integrationDecisionPaymentID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM manual_review_queue WHERE payment_id = $1`, integrationDecisionPaymentID); err != nil {
		t.Fatal(err)
	}

	missingPaymentID := "20000000-0000-4000-8000-000000000002"
	seedDecisionPayment(t, pool, missingPaymentID, "PENDING_RISK", now.Add(-time.Minute), true)
	if err := store.Reject(context.Background(), SourceRecord{
		Topic: "risk.decisions", Partition: 0, Offset: 702, Value: []byte(`{"bad":true}`),
	}, "invalid_event", errors.New("schema mismatch")); err != nil {
		t.Fatal(err)
	}

	reconciler, err := NewReconciler(pool, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.now = func() time.Time { return now }
	report, err := reconciler.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantCodes := map[string]bool{
		"DUPLICATE_DELIVERY":      false,
		"MISSING_DECISION":        false,
		"PAYMENT_STATUS_MISMATCH": false,
		"REJECTED_DECISION_EVENT": false,
		"REVIEW_QUEUE_MISSING":    false,
	}
	for _, exception := range report.Exceptions {
		if _, ok := wantCodes[exception.Code]; ok {
			wantCodes[exception.Code] = true
		}
	}
	for code, found := range wantCodes {
		if !found {
			t.Errorf("report did not contain %s: %+v", code, report.Exceptions)
		}
	}
}

const integrationDecisionPaymentID = "20000000-0000-4000-8000-000000000001"

func newDecisionIntegrationStore(t *testing.T) (*PostgresStore, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := database.NewPostgresPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create integration pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping integration database: %v", err)
	}
	t.Cleanup(pool.Close)
	truncateDecisionTables(t, pool)
	t.Cleanup(func() { truncateDecisionTables(t, pool) })
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	return store, pool
}

func truncateDecisionTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
        TRUNCATE TABLE manual_review_queue, audit_events, decision_ingestion_records,
            payment_decisions, outbox_events, payments`); err != nil {
		t.Fatalf("truncate decision integration tables: %v", err)
	}
}

func seedDecisionPayment(t *testing.T, pool *pgxpool.Pool, paymentID, status string, createdAt time.Time, published bool) {
	t.Helper()
	fingerprint := strings.Repeat("a", 64)
	if _, err := pool.Exec(context.Background(), `
        INSERT INTO payments (
            id, idempotency_key, request_fingerprint, customer_id, merchant_id,
            device_id, amount_minor, currency, country, status, created_at, updated_at
        ) VALUES ($1, $2, $3, 'customer-1', 'merchant-1', 'device-1',
            100000, 'USD', 'IN', $4, $5, $5)`,
		paymentID, "decision-key-"+paymentID, fingerprint, status, createdAt,
	); err != nil {
		t.Fatalf("seed decision payment: %v", err)
	}
	publishedAt := any(nil)
	if published {
		publishedAt = createdAt
	}
	outboxID := strings.Replace(paymentID, "20000000", "50000000", 1)
	if _, err := pool.Exec(context.Background(), `
        INSERT INTO outbox_events (
            id, aggregate_id, event_type, schema_version, occurred_at,
            trace_id, payload, published_at
        ) VALUES ($1, $2, 'payments.created', 1, $3,
            '60000000-0000-4000-8000-000000000001', '{"payment_id":"placeholder"}', $4)`,
		outboxID, paymentID, createdAt, publishedAt,
	); err != nil {
		t.Fatalf("seed decision outbox: %v", err)
	}
}

func integrationDecisionEvent(t *testing.T) Event {
	t.Helper()
	event, err := ParseEvent(validEventValue())
	if err != nil {
		t.Fatalf("parse integration decision: %v", err)
	}
	return event
}

func integrationSourceRecord(offset int64) SourceRecord {
	return SourceRecord{Topic: "risk.decisions", Partition: 0, Offset: offset, Value: validEventValue()}
}

func assertDecisionCounts(t *testing.T, pool *pgxpool.Pool, decisions, audits, reviews, receipts int) {
	t.Helper()
	var gotDecisions, gotAudits, gotReviews, gotReceipts int
	if err := pool.QueryRow(context.Background(), `
        SELECT
            (SELECT count(*) FROM payment_decisions),
            (SELECT count(*) FROM audit_events),
            (SELECT count(*) FROM manual_review_queue),
            (SELECT count(*) FROM decision_ingestion_records)`,
	).Scan(&gotDecisions, &gotAudits, &gotReviews, &gotReceipts); err != nil {
		t.Fatalf("query decision counts: %v", err)
	}
	if gotDecisions != decisions || gotAudits != audits || gotReviews != reviews || gotReceipts != receipts {
		t.Fatalf("decision/audit/review/receipt counts = %d/%d/%d/%d, want %d/%d/%d/%d",
			gotDecisions, gotAudits, gotReviews, gotReceipts, decisions, audits, reviews, receipts)
	}
}
