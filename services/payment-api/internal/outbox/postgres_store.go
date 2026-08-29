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
	pool *pgxpool.Pool
}

// NewPostgresStore creates a PostgreSQL-backed outbox store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// ProcessNext locks one unpublished event with SKIP LOCKED, invokes publish
// while the lock is held, and marks the event only after publish succeeds.
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

	if err := publish(ctx, event); err != nil {
		return true, fmt.Errorf("publish outbox event %s: %w", event.ID, err)
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
    payload
FROM outbox_events
WHERE published_at IS NULL
ORDER BY created_at, id
FOR UPDATE SKIP LOCKED
LIMIT 1`

const markEventPublishedSQL = `
UPDATE outbox_events
SET published_at = clock_timestamp()
WHERE id = $1 AND published_at IS NULL`
