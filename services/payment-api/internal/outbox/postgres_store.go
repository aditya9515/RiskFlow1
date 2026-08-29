package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store claims and completes durable outbox work.
type Store interface {
	ProcessNext(context.Context, func(context.Context, Event) error) (bool, error)
}

// PostgresStore coordinates publishers through row-level locks.
type PostgresStore struct {
	pool     *pgxpool.Pool
	retryMin time.Duration
	retryMax time.Duration
}

// NewPostgresStore creates a PostgreSQL-backed outbox store.
func NewPostgresStore(pool *pgxpool.Pool, retryMin, retryMax time.Duration) (*PostgresStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("PostgreSQL pool is required")
	}
	if retryMin <= 0 || retryMax < retryMin {
		return nil, fmt.Errorf("outbox retry timing is invalid")
	}
	return &PostgresStore{pool: pool, retryMin: retryMin, retryMax: retryMax}, nil
}

// ProcessNext locks one due event with SKIP LOCKED and invokes publish while
// the lock is held. Success marks it published; failure persists retry or
// quarantine state before releasing the lock.
func (s *PostgresStore) ProcessNext(ctx context.Context, publish func(context.Context, Event) error) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin outbox transaction: %w", err)
	}
	defer rollback(tx)

	event, err := scanNextEvent(tx.QueryRow(ctx, selectNextEventSQL))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim outbox event: %w", err)
	}

	if publishErr := publish(ctx, event); publishErr != nil {
		if err := s.recordFailure(tx, event, publishErr); err != nil {
			return true, errors.Join(
				fmt.Errorf("publish outbox event %s: %w", event.ID, publishErr),
				err,
			)
		}
		return true, fmt.Errorf("publish outbox event %s: %w", event.ID, publishErr)
	}

	commandTag, err := tx.Exec(ctx, markEventPublishedSQL, event.ID)
	if err != nil {
		return true, fmt.Errorf("mark outbox event %s published: %w", event.ID, err)
	}
	if commandTag.RowsAffected() != 1 {
		return true, fmt.Errorf("mark outbox event %s published: expected one updated row", event.ID)
	}

	if err := tx.Commit(ctx); err != nil {
		return true, fmt.Errorf("commit outbox event %s: %w", event.ID, err)
	}

	return true, nil
}

func (s *PostgresStore) recordFailure(tx pgx.Tx, event Event, publishErr error) error {
	failedAttempt := event.DeliveryAttempts + 1
	delay := deliveryBackoff(failedAttempt, s.retryMin, s.retryMax)
	nextAttemptAt := time.Now().UTC().Add(delay)
	permanent := isPermanent(publishErr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	commandTag, err := tx.Exec(ctx, recordEventFailureSQL,
		event.ID,
		nextAttemptAt,
		publishErr.Error(),
		permanent,
	)
	if err != nil {
		return fmt.Errorf("record outbox event %s failure: %w", event.ID, err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("record outbox event %s failure: expected one updated row", event.ID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox event %s failure: %w", event.ID, err)
	}
	return nil
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func scanNextEvent(row interface{ Scan(...any) error }) (Event, error) {
	var event Event
	var payload []byte
	err := row.Scan(
		&event.ID,
		&event.EventType,
		&event.AggregateID,
		&event.SchemaVersion,
		&event.OccurredAt,
		&event.TraceID,
		&payload,
		&event.DeliveryAttempts,
	)
	event.OccurredAt = event.OccurredAt.UTC()
	event.Payload = payload
	return event, err
}

const selectNextEventSQL = `
SELECT
    id::text,
    event_type,
    aggregate_id::text,
    schema_version,
    occurred_at,
    trace_id::text,
    payload,
    delivery_attempts
FROM outbox_events
WHERE published_at IS NULL
  AND dead_lettered_at IS NULL
  AND next_attempt_at <= clock_timestamp()
ORDER BY next_attempt_at, created_at, id
FOR UPDATE SKIP LOCKED
LIMIT 1`

const markEventPublishedSQL = `
UPDATE outbox_events
SET published_at = clock_timestamp(),
    last_attempt_at = clock_timestamp(),
    delivery_attempts = delivery_attempts + 1
WHERE id = $1
  AND published_at IS NULL
  AND dead_lettered_at IS NULL`

const recordEventFailureSQL = `
UPDATE outbox_events
SET delivery_attempts = delivery_attempts + 1,
    last_attempt_at = clock_timestamp(),
    next_attempt_at = $2,
    last_error = LEFT($3, 2000),
    dead_lettered_at = CASE WHEN $4 THEN clock_timestamp() ELSE NULL END
WHERE id = $1
  AND published_at IS NULL
  AND dead_lettered_at IS NULL`

func deliveryBackoff(attempt int, minimum, maximum time.Duration) time.Duration {
	delay := minimum
	for currentAttempt := 1; currentAttempt < attempt && delay < maximum; currentAttempt++ {
		delay = nextBackoff(delay, maximum)
	}
	return delay
}
