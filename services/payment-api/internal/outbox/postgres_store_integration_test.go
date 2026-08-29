//go:build integration

package outbox

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/database"
)

func TestPostgresStoreMarksOnlyAfterPublishSucceeds(t *testing.T) {
	store, pool := newOutboxIntegrationStore(t)
	seedOutboxEvent(t, pool)

	processed, err := store.ProcessNext(context.Background(), func(_ context.Context, event Event) error {
		if event.ID != integrationEventID || event.AggregateID != integrationPaymentID {
			t.Fatalf("unexpected claimed event: %+v", event)
		}
		if event.OccurredAt.Location() != time.UTC {
			t.Fatalf("occurred_at location = %s, want UTC", event.OccurredAt.Location())
		}
		if _, err := marshalEnvelope(event); err != nil {
			t.Fatalf("marshal claimed event: %v", err)
		}
		if published := eventPublished(t, pool); published {
			t.Fatal("event was marked published before broker acknowledgement")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("process event: %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if !eventPublished(t, pool) {
		t.Fatal("event remains unpublished after successful callback")
	}
	state := eventDeliveryState(t, pool)
	if state.attempts != 1 || state.lastAttemptAt == nil {
		t.Fatalf("attempts/last_attempt_at = %d/%v, want 1/non-nil", state.attempts, state.lastAttemptAt)
	}
}

func TestPostgresStorePublishFailureLeavesEventUnpublished(t *testing.T) {
	store, pool := newOutboxIntegrationStore(t)
	seedOutboxEvent(t, pool)

	processed, err := store.ProcessNext(context.Background(), func(context.Context, Event) error {
		return errors.New("Kafka unavailable")
	})
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if err == nil || !strings.Contains(err.Error(), "Kafka unavailable") {
		t.Fatalf("error = %v, want Kafka unavailable", err)
	}
	if eventPublished(t, pool) {
		t.Fatal("failed event was marked published")
	}
	state := eventDeliveryState(t, pool)
	if state.attempts != 1 || state.lastAttemptAt == nil || state.nextAttemptAt.IsZero() {
		t.Fatalf("failure delivery state = %+v", state)
	}
	if state.deadLetteredAt != nil || !strings.Contains(state.lastError, "Kafka unavailable") {
		t.Fatalf("failure quarantine/error state = %+v", state)
	}

	called := false
	processed, err = store.ProcessNext(context.Background(), func(context.Context, Event) error {
		called = true
		return nil
	})
	if err != nil || processed || called {
		t.Fatalf("delayed event processed/error/callback = %v/%v/%v", processed, err, called)
	}

	if _, err := pool.Exec(context.Background(), `
        UPDATE outbox_events SET next_attempt_at = clock_timestamp() WHERE id = $1`, integrationEventID); err != nil {
		t.Fatalf("make failed event due: %v", err)
	}
	processed, err = store.ProcessNext(context.Background(), func(context.Context, Event) error { return nil })
	if err != nil || !processed {
		t.Fatalf("retry processed/error = %v/%v", processed, err)
	}
	state = eventDeliveryState(t, pool)
	if !state.published || state.attempts != 2 || !strings.Contains(state.lastError, "Kafka unavailable") {
		t.Fatalf("recovered delivery state = %+v", state)
	}
}

func TestPostgresStoreSkipLockedPreventsTwoWorkersPublishingSameEvent(t *testing.T) {
	store, pool := newOutboxIntegrationStore(t)
	seedOutboxEvent(t, pool)

	claimed := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan processResult, 1)
	var publishCalls atomic.Int32

	go func() {
		processed, err := store.ProcessNext(context.Background(), func(context.Context, Event) error {
			publishCalls.Add(1)
			close(claimed)
			<-release
			return nil
		})
		firstDone <- processResult{processed: processed, err: err}
	}()

	select {
	case <-claimed:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("first worker did not claim event")
	}

	secondProcessed, secondErr := store.ProcessNext(context.Background(), func(context.Context, Event) error {
		publishCalls.Add(1)
		return nil
	})
	if secondErr != nil {
		close(release)
		t.Fatalf("second worker: %v", secondErr)
	}
	if secondProcessed {
		close(release)
		t.Fatal("second worker processed the locked event")
	}

	close(release)
	first := <-firstDone
	if first.err != nil || !first.processed {
		t.Fatalf("first worker processed/error = %v/%v", first.processed, first.err)
	}
	if publishCalls.Load() != 1 {
		t.Fatalf("publish calls = %d, want 1", publishCalls.Load())
	}
	if !eventPublished(t, pool) {
		t.Fatal("event was not marked published")
	}
}

func TestPostgresStoreQuarantinesPoisonEventWithoutPublishing(t *testing.T) {
	store, pool := newOutboxIntegrationStore(t)
	seedOutboxEvent(t, pool)
	if _, err := pool.Exec(context.Background(), `
        UPDATE outbox_events SET event_type = 'unsupported.event' WHERE id = $1`, integrationEventID); err != nil {
		t.Fatalf("poison integration event: %v", err)
	}

	publisher := &capturingPublisher{}
	worker, err := NewWorker(store, publisher, validWorkerConfig(), discardOutboxLogger())
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	processed, err := store.ProcessNext(context.Background(), worker.publish)
	if !processed || err == nil || !strings.Contains(err.Error(), "unsupported event_type") {
		t.Fatalf("poison processed/error = %v/%v", processed, err)
	}
	if publisher.calls != 0 {
		t.Fatalf("publisher calls = %d, want 0", publisher.calls)
	}

	state := eventDeliveryState(t, pool)
	if state.published || state.deadLetteredAt == nil || state.attempts != 1 {
		t.Fatalf("poison delivery state = %+v", state)
	}
	processed, err = store.ProcessNext(context.Background(), func(context.Context, Event) error {
		t.Fatal("quarantined event was selected again")
		return nil
	})
	if err != nil || processed {
		t.Fatalf("quarantined event processed/error = %v/%v", processed, err)
	}
}

func TestPostgresStoreAcknowledgedEventCanBeDeliveredAgainWhenDatabaseMarkFails(t *testing.T) {
	store, pool := newOutboxIntegrationStore(t)
	seedOutboxEvent(t, pool)

	ctx, cancel := context.WithCancel(context.Background())
	publishCalls := 0
	processed, err := store.ProcessNext(ctx, func(context.Context, Event) error {
		publishCalls++
		cancel()
		return nil
	})
	if !processed || err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled mark processed/error = %v/%v", processed, err)
	}
	if eventPublished(t, pool) {
		t.Fatal("event was marked published after cancelled database operation")
	}

	processed, err = store.ProcessNext(context.Background(), func(context.Context, Event) error {
		publishCalls++
		return nil
	})
	if !processed || err != nil {
		t.Fatalf("redelivery processed/error = %v/%v", processed, err)
	}
	if publishCalls != 2 {
		t.Fatalf("publish calls = %d, want 2", publishCalls)
	}
	if !eventPublished(t, pool) {
		t.Fatal("redelivered event was not marked published")
	}
}

type processResult struct {
	processed bool
	err       error
}

const (
	integrationPaymentID = "10000000-0000-4000-8000-000000000010"
	integrationEventID   = "20000000-0000-4000-8000-000000000010"
)

func newOutboxIntegrationStore(t *testing.T) (*PostgresStore, *pgxpool.Pool) {
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

	truncateOutboxTables(t, pool)
	t.Cleanup(func() { truncateOutboxTables(t, pool) })
	store, err := NewPostgresStore(pool, 5*time.Second, 20*time.Second)
	if err != nil {
		pool.Close()
		t.Fatalf("create PostgreSQL outbox store: %v", err)
	}
	return store, pool
}

func truncateOutboxTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE outbox_events, payments`); err != nil {
		t.Fatalf("truncate outbox integration tables: %v", err)
	}
}

func seedOutboxEvent(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, `
        INSERT INTO payments (
            id, idempotency_key, request_fingerprint, customer_id, merchant_id,
            device_id, amount_minor, currency, country, status
        ) VALUES ($1, 'outbox-integration-key', $2, 'customer-1', 'merchant-1',
            'device-1', 1250, 'USD', 'IN', 'PENDING_RISK')`,
		integrationPaymentID, strings.Repeat("a", 64),
	); err != nil {
		t.Fatalf("insert integration payment: %v", err)
	}

	if _, err := pool.Exec(ctx, `
        INSERT INTO outbox_events (
            id, aggregate_id, event_type, schema_version, occurred_at, trace_id, payload
        ) VALUES ($1, $2, 'payments.created', 1, $3, $4, $5)`,
		integrationEventID,
		integrationPaymentID,
		time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC),
		"30000000-0000-4000-8000-000000000010",
		`{"payment_id":"10000000-0000-4000-8000-000000000010","amount_minor":1250}`,
	); err != nil {
		t.Fatalf("insert integration outbox event: %v", err)
	}
}

func eventPublished(t *testing.T, pool *pgxpool.Pool) bool {
	t.Helper()
	var published bool
	if err := pool.QueryRow(context.Background(), `
        SELECT published_at IS NOT NULL
        FROM outbox_events
        WHERE id = $1`, integrationEventID,
	).Scan(&published); err != nil {
		t.Fatalf("query event publication state: %v", err)
	}
	return published
}

type deliveryState struct {
	published      bool
	attempts       int
	nextAttemptAt  time.Time
	lastAttemptAt  *time.Time
	lastError      string
	deadLetteredAt *time.Time
}

func eventDeliveryState(t *testing.T, pool *pgxpool.Pool) deliveryState {
	t.Helper()
	var state deliveryState
	if err := pool.QueryRow(context.Background(), `
        SELECT
            published_at IS NOT NULL,
            delivery_attempts,
            next_attempt_at,
            last_attempt_at,
            COALESCE(last_error, ''),
            dead_lettered_at
        FROM outbox_events
        WHERE id = $1`, integrationEventID,
	).Scan(
		&state.published,
		&state.attempts,
		&state.nextAttemptAt,
		&state.lastAttemptAt,
		&state.lastError,
		&state.deadLetteredAt,
	); err != nil {
		t.Fatalf("query event delivery state: %v", err)
	}
	return state
}
