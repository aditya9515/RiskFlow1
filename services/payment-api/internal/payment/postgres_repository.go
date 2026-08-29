package payment

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository stores payments using PostgreSQL transactions.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository creates a PostgreSQL-backed payment repository.
func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// Create inserts the payment and outbox event together. PostgreSQL's unique
// idempotency key is the concurrency arbiter across all API instances.
func (r *PostgresRepository) Create(ctx context.Context, candidate Payment, event OutboxEvent) (CreateResult, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CreateResult{}, fmt.Errorf("begin payment transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	stored, err := scanPayment(tx.QueryRow(ctx, insertPaymentSQL,
		candidate.ID,
		candidate.IdempotencyKey,
		candidate.RequestFingerprint,
		candidate.CustomerID,
		candidate.MerchantID,
		candidate.DeviceID,
		candidate.AmountMinor,
		candidate.Currency,
		candidate.Country,
		candidate.Status,
		candidate.CreatedAt,
		candidate.UpdatedAt,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		existing, selectErr := scanPayment(tx.QueryRow(ctx, selectPaymentByIdempotencyKeySQL, candidate.IdempotencyKey))
		if selectErr != nil {
			return CreateResult{}, fmt.Errorf("load idempotent payment: %w", selectErr)
		}
		if existing.RequestFingerprint != candidate.RequestFingerprint {
			return CreateResult{}, ErrIdempotencyConflict
		}

		return CreateResult{Payment: existing, Replayed: true}, nil
	}
	if err != nil {
		return CreateResult{}, fmt.Errorf("insert payment: %w", err)
	}

	if _, err := tx.Exec(ctx, insertOutboxEventSQL,
		event.ID,
		event.AggregateID,
		event.EventType,
		event.SchemaVersion,
		event.OccurredAt,
		event.TraceID,
		event.Payload,
	); err != nil {
		return CreateResult{}, fmt.Errorf("insert payment outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return CreateResult{}, fmt.Errorf("commit payment transaction: %w", err)
	}

	return CreateResult{Payment: stored, Replayed: false}, nil
}

// GetByID loads a payment without changing it or its outbox event.
func (r *PostgresRepository) GetByID(ctx context.Context, id string) (Payment, error) {
	stored, err := scanPayment(r.pool.QueryRow(ctx, selectPaymentByIDSQL, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrPaymentNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("load payment by ID: %w", err)
	}

	return stored, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanPayment(row rowScanner) (Payment, error) {
	var stored Payment
	err := row.Scan(
		&stored.ID,
		&stored.IdempotencyKey,
		&stored.RequestFingerprint,
		&stored.CustomerID,
		&stored.MerchantID,
		&stored.DeviceID,
		&stored.AmountMinor,
		&stored.Currency,
		&stored.Country,
		&stored.Status,
		&stored.CreatedAt,
		&stored.UpdatedAt,
	)
	stored.Currency = strings.TrimSpace(stored.Currency)
	stored.Country = strings.TrimSpace(stored.Country)
	stored.CreatedAt = stored.CreatedAt.UTC()
	stored.UpdatedAt = stored.UpdatedAt.UTC()
	return stored, err
}

const insertPaymentSQL = `
INSERT INTO payments (
    id, idempotency_key, request_fingerprint, customer_id, merchant_id,
    device_id, amount_minor, currency, country, status, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING
    id::text, idempotency_key, request_fingerprint, customer_id, merchant_id,
    device_id, amount_minor, currency::text, country::text, status, created_at, updated_at`

const selectPaymentByIdempotencyKeySQL = `
SELECT
    id::text, idempotency_key, request_fingerprint, customer_id, merchant_id,
    device_id, amount_minor, currency::text, country::text, status, created_at, updated_at
FROM payments
WHERE idempotency_key = $1`

const selectPaymentByIDSQL = `
SELECT
    id::text, idempotency_key, request_fingerprint, customer_id, merchant_id,
    device_id, amount_minor, currency::text, country::text, status, created_at, updated_at
FROM payments
WHERE id = $1`

const insertOutboxEventSQL = `
INSERT INTO outbox_events (
    id, aggregate_id, event_type, schema_version, occurred_at, trace_id, payload
) VALUES ($1, $2, $3, $4, $5, $6, $7)`
