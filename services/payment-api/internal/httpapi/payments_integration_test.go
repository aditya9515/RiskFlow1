//go:build integration

package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/database"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/payment"
)

func TestPostgresPaymentIdempotencyLifecycle(t *testing.T) {
	handler, pool := newIntegrationHandler(t)

	firstBody := `{
        "customer_id":" customer-1 ",
        "merchant_id":"merchant-1",
        "device_id":"device-1 ",
        "amount_minor":1250,
        "currency":"usd",
        "country":"in"
    }`
	first := performPaymentRequest(handler, "lifecycle-key", firstBody)
	if first.err != nil {
		t.Fatalf("first request: %v", first.err)
	}
	if first.status != http.StatusCreated {
		t.Fatalf("first status = %d, want 201; body=%s", first.status, first.rawBody)
	}

	replayBody := `{
        "country":"IN",
        "currency":"USD",
        "amount_minor":1250,
        "device_id":"device-1",
        "merchant_id":"merchant-1",
        "customer_id":"customer-1"
    }`
	replay := performPaymentRequest(handler, "lifecycle-key", replayBody)
	if replay.err != nil {
		t.Fatalf("replay request: %v", replay.err)
	}
	if replay.status != http.StatusOK {
		t.Fatalf("replay status = %d, want 200; body=%s", replay.status, replay.rawBody)
	}
	if replay.payment.ID != first.payment.ID {
		t.Fatalf("replay payment ID = %q, want original %q", replay.payment.ID, first.payment.ID)
	}

	conflictBody := `{
        "customer_id":"customer-1",
        "merchant_id":"merchant-1",
        "device_id":"device-1",
        "amount_minor":1251,
        "currency":"USD",
        "country":"IN"
    }`
	conflict := performPaymentRequest(handler, "lifecycle-key", conflictBody)
	if conflict.err != nil {
		t.Fatalf("conflicting request: %v", conflict.err)
	}
	if conflict.status != http.StatusConflict || conflict.apiError.Code != "idempotency_conflict" {
		t.Fatalf("conflict status/code = %d/%q, want 409/idempotency_conflict; body=%s", conflict.status, conflict.apiError.Code, conflict.rawBody)
	}

	assertRowCounts(t, pool, 1, 1)

	var customerID, currency, country, fingerprint string
	if err := pool.QueryRow(context.Background(), `
        SELECT customer_id, currency::text, country::text, request_fingerprint
        FROM payments WHERE id = $1`, first.payment.ID,
	).Scan(&customerID, &currency, &country, &fingerprint); err != nil {
		t.Fatalf("query stored payment: %v", err)
	}
	if customerID != "customer-1" || currency != "USD" || country != "IN" || len(fingerprint) != 64 {
		t.Fatalf("stored normalized values are incorrect: customer=%q currency=%q country=%q fingerprint=%q", customerID, currency, country, fingerprint)
	}

	var eventType, aggregateID, payloadPaymentID string
	var schemaVersion int
	if err := pool.QueryRow(context.Background(), `
        SELECT event_type, aggregate_id::text, schema_version, payload->>'payment_id'
        FROM outbox_events`,
	).Scan(&eventType, &aggregateID, &schemaVersion, &payloadPaymentID); err != nil {
		t.Fatalf("query outbox event: %v", err)
	}
	if eventType != payment.PaymentCreated || aggregateID != first.payment.ID || payloadPaymentID != first.payment.ID || schemaVersion != 1 {
		t.Fatalf("unexpected outbox event: type=%q aggregate=%q payload_payment=%q schema=%d", eventType, aggregateID, payloadPaymentID, schemaVersion)
	}
}

func TestPostgresConcurrentIdenticalRequestsCreateOnePaymentAndEvent(t *testing.T) {
	handler, pool := newIntegrationHandler(t)

	const workers = 32
	results := make(chan requestResult, workers)
	start := make(chan struct{})
	var waitGroup sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			results <- performPaymentRequest(handler, "concurrent-key", validPaymentJSON)
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)

	created := 0
	replayed := 0
	paymentIDs := make(map[string]struct{})
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent request failed: %v", result.err)
		}
		switch result.status {
		case http.StatusCreated:
			created++
		case http.StatusOK:
			replayed++
		default:
			t.Fatalf("concurrent status = %d, body=%s", result.status, result.rawBody)
		}
		paymentIDs[result.payment.ID] = struct{}{}
	}

	if created != 1 || replayed != workers-1 {
		t.Fatalf("created/replayed = %d/%d, want 1/%d", created, replayed, workers-1)
	}
	if len(paymentIDs) != 1 {
		t.Fatalf("unique payment IDs = %d, want 1", len(paymentIDs))
	}
	assertRowCounts(t, pool, 1, 1)
}

func TestPostgresInvalidRequestCreatesNoRows(t *testing.T) {
	handler, pool := newIntegrationHandler(t)

	result := performPaymentRequest(handler, "invalid-key", `{
        "customer_id":"customer-1",
        "merchant_id":"merchant-1",
        "device_id":"device-1",
        "amount_minor":0,
        "currency":"US",
        "country":"IN"
    }`)
	if result.err != nil {
		t.Fatalf("invalid request: %v", result.err)
	}
	if result.status != http.StatusBadRequest || result.apiError.Code != "validation_error" {
		t.Fatalf("status/code = %d/%q, want 400/validation_error; body=%s", result.status, result.apiError.Code, result.rawBody)
	}
	assertRowCounts(t, pool, 0, 0)
}

func TestPostgresOutboxFailureRollsBackPayment(t *testing.T) {
	handler, pool := newIntegrationHandler(t)
	ctx := context.Background()

	const constraintName = "test_force_outbox_failure"
	if _, err := pool.Exec(ctx, `ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS `+constraintName); err != nil {
		t.Fatalf("drop stale test constraint: %v", err)
	}
	if _, err := pool.Exec(ctx, `
        ALTER TABLE outbox_events
        ADD CONSTRAINT `+constraintName+` CHECK (event_type <> 'payments.created') NOT VALID`); err != nil {
		t.Fatalf("add outbox failure constraint: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS `+constraintName)
	})

	result := performPaymentRequest(handler, "rollback-key", validPaymentJSON)
	if result.err != nil {
		t.Fatalf("rollback request: %v", result.err)
	}
	if result.status != http.StatusInternalServerError || result.apiError.Code != "internal_error" {
		t.Fatalf("status/code = %d/%q, want 500/internal_error; body=%s", result.status, result.apiError.Code, result.rawBody)
	}
	assertRowCounts(t, pool, 0, 0)
}

func newIntegrationHandler(t *testing.T) (http.Handler, *pgxpool.Pool) {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := database.NewPostgresPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create integration database pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping integration database: %v", err)
	}
	t.Cleanup(pool.Close)

	truncatePayments(t, pool)
	t.Cleanup(func() { truncatePayments(t, pool) })

	repository := payment.NewPostgresRepository(pool)
	service := payment.NewService(repository)
	handler := NewHandler(pool, service, time.Second, 3*time.Second, discardLogger())
	return handler, pool
}

func truncatePayments(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE outbox_events, payments`); err != nil {
		t.Fatalf("truncate integration tables: %v", err)
	}
}

func assertRowCounts(t *testing.T, pool *pgxpool.Pool, wantPayments, wantEvents int) {
	t.Helper()

	var paymentsCount, eventsCount int
	if err := pool.QueryRow(context.Background(), `
        SELECT
            (SELECT COUNT(*) FROM payments),
            (SELECT COUNT(*) FROM outbox_events)`,
	).Scan(&paymentsCount, &eventsCount); err != nil {
		t.Fatalf("query row counts: %v", err)
	}
	if paymentsCount != wantPayments || eventsCount != wantEvents {
		t.Fatalf("payment/outbox counts = %d/%d, want %d/%d", paymentsCount, eventsCount, wantPayments, wantEvents)
	}
}

type requestResult struct {
	status   int
	payment  paymentResponse
	apiError APIError
	rawBody  string
	err      error
}

func performPaymentRequest(handler http.Handler, idempotencyKey, body string) requestResult {
	request := httptest.NewRequest(http.MethodPost, "/v1/payments", bytes.NewBufferString(body))
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	result := requestResult{status: recorder.Code, rawBody: recorder.Body.String()}
	if recorder.Code == http.StatusCreated || recorder.Code == http.StatusOK {
		result.err = json.NewDecoder(bytes.NewReader(recorder.Body.Bytes())).Decode(&result.payment)
		return result
	}

	var response errorResponse
	if err := json.NewDecoder(bytes.NewReader(recorder.Body.Bytes())).Decode(&response); err != nil {
		result.err = fmt.Errorf("decode HTTP %d error response: %w", recorder.Code, err)
		return result
	}
	result.apiError = response.Error
	return result
}
