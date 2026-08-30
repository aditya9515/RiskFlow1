package decision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	dispositionApplied  = "APPLIED"
	dispositionReplayed = "REPLAYED"
	dispositionRejected = "REJECTED"
)

// PostgresStore makes the decision, payment state, audit event, review queue,
// and Kafka ingestion receipt one atomic database change.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, errors.New("decision PostgreSQL pool is required")
	}
	return &PostgresStore{pool: pool}, nil
}

// Apply persists a valid decision. A replay creates only a new ingestion
// receipt; it never duplicates decision, audit, or review rows.
func (s *PostgresStore) Apply(ctx context.Context, event Event, record SourceRecord) (ApplyResult, error) {
	if err := validateSourceRecord(record); err != nil {
		return ApplyResult{}, err
	}
	eventFingerprint, err := Fingerprint(event)
	if err != nil {
		return ApplyResult{}, err
	}
	recordDigest := recordSHA256(record.Value)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("begin decision transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	disposition, found, err := existingReceipt(ctx, tx, record, recordDigest)
	if err != nil {
		return ApplyResult{}, err
	}
	if found {
		if disposition == dispositionRejected {
			return ApplyResult{}, fmt.Errorf("%w: source record was already rejected", ErrSourceRecordConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return ApplyResult{}, fmt.Errorf("commit replayed ingestion receipt: %w", err)
		}
		return ApplyResult{Replayed: true}, nil
	}

	var paymentStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM payments WHERE id = $1 FOR UPDATE`, event.Payload.PaymentID).Scan(&paymentStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return ApplyResult{}, ErrPaymentNotFound
	}
	if err != nil {
		return ApplyResult{}, fmt.Errorf("lock decision payment: %w", err)
	}

	var storedFingerprint string
	err = tx.QueryRow(ctx, `
        SELECT event_fingerprint
        FROM payment_decisions
        WHERE decision_id = $1`, event.Payload.DecisionID,
	).Scan(&storedFingerprint)
	if err == nil {
		if storedFingerprint != eventFingerprint {
			return ApplyResult{}, ErrDecisionConflict
		}
		if err := insertReceipt(ctx, tx, record, recordDigest, event, dispositionReplayed); err != nil {
			return ApplyResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ApplyResult{}, fmt.Errorf("commit decision replay receipt: %w", err)
		}
		return ApplyResult{Replayed: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ApplyResult{}, fmt.Errorf("load existing decision: %w", err)
	}

	var sourceDecisionID string
	err = tx.QueryRow(ctx, `
        SELECT decision_id::text
        FROM payment_decisions
        WHERE source_event_id = $1`, event.Payload.SourceEventID,
	).Scan(&sourceDecisionID)
	if err == nil {
		return ApplyResult{}, fmt.Errorf("%w: source event already belongs to decision %s", ErrDecisionConflict, sourceDecisionID)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ApplyResult{}, fmt.Errorf("check source event decision: %w", err)
	}

	if paymentStatus != "PENDING_RISK" {
		return ApplyResult{}, fmt.Errorf("%w: current status is %s", ErrPaymentStateConflict, paymentStatus)
	}

	_, err = tx.Exec(ctx, `
        INSERT INTO payment_decisions (
            decision_id, payment_id, source_event_id, trace_id, schema_version,
            decision, risk_score, rule_score, model_score, model_probability,
            model_review_threshold, reason_codes, rule_version, model_version,
            velocity_5m, new_device, cross_border, baseline_country,
            decision_at, event_fingerprint
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9, $10,
            $11, $12, $13, $14,
            $15, $16, $17, $18, $19, $20
        )`,
		event.Payload.DecisionID,
		event.Payload.PaymentID,
		event.Payload.SourceEventID,
		event.TraceID,
		event.SchemaVersion,
		event.Payload.Decision,
		event.Payload.RiskScore,
		event.Payload.RuleScore,
		event.Payload.ModelScore,
		event.Payload.ModelProbability,
		event.Payload.ModelReviewThreshold,
		event.Payload.ReasonCodes,
		event.Payload.RuleVersion,
		event.Payload.ModelVersion,
		event.Payload.Features.Velocity5m,
		event.Payload.Features.NewDevice,
		event.Payload.Features.CrossBorder,
		event.Payload.Features.BaselineCountry,
		event.Payload.DecisionAt,
		eventFingerprint,
	)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("insert payment decision: %w", err)
	}

	status, err := paymentStatusForDecision(event.Payload.Decision)
	if err != nil {
		return ApplyResult{}, err
	}
	command, err := tx.Exec(ctx, `
        UPDATE payments
        SET status = $2, updated_at = clock_timestamp()
        WHERE id = $1 AND status = 'PENDING_RISK'`, event.Payload.PaymentID, status)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("update payment decision status: %w", err)
	}
	if command.RowsAffected() != 1 {
		return ApplyResult{}, ErrPaymentStateConflict
	}

	details, err := json.Marshal(map[string]any{
		"decision":        event.Payload.Decision,
		"model_version":   event.Payload.ModelVersion,
		"reason_codes":    event.Payload.ReasonCodes,
		"risk_score":      event.Payload.RiskScore,
		"rule_version":    event.Payload.RuleVersion,
		"source_event_id": event.Payload.SourceEventID,
		"trace_id":        event.TraceID,
	})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("encode decision audit details: %w", err)
	}
	_, err = tx.Exec(ctx, `
        INSERT INTO audit_events (
            id, aggregate_type, aggregate_id, event_type, actor_type,
            actor_id, decision_id, occurred_at, details
        ) VALUES ($1, 'payment', $2, 'RISK_DECISION_RECORDED', 'SYSTEM',
            'risk-decision-consumer', $1, $3, $4)`,
		event.Payload.DecisionID, event.Payload.PaymentID, event.Payload.DecisionAt, details,
	)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("insert decision audit event: %w", err)
	}

	if event.Payload.Decision == DecisionReview {
		_, err = tx.Exec(ctx, `
            INSERT INTO manual_review_queue (payment_id, decision_id, enqueued_at)
            VALUES ($1, $2, $3)`,
			event.Payload.PaymentID, event.Payload.DecisionID, event.Payload.DecisionAt,
		)
		if err != nil {
			return ApplyResult{}, fmt.Errorf("enqueue manual review: %w", err)
		}
	}

	if err := insertReceipt(ctx, tx, record, recordDigest, event, dispositionApplied); err != nil {
		return ApplyResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ApplyResult{}, fmt.Errorf("commit payment decision: %w", err)
	}
	return ApplyResult{Applied: true}, nil
}

// Reject durably records a malformed Kafka record before its offset is committed.
func (s *PostgresStore) Reject(ctx context.Context, record SourceRecord, code string, rejection error) error {
	if err := validateSourceRecord(record); err != nil {
		return err
	}
	if !validRejectionCode(code) {
		return errors.New("unsupported decision rejection code")
	}
	if rejection == nil {
		return errors.New("decision rejection reason is required")
	}

	recordDigest := recordSHA256(record.Value)
	errorMessageRunes := []rune(strings.ToValidUTF8(rejection.Error(), "?"))
	if len(errorMessageRunes) > 2000 {
		errorMessageRunes = errorMessageRunes[:2000]
	}
	errorMessage := string(errorMessageRunes)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rejected decision transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	disposition, found, err := existingReceipt(ctx, tx, record, recordDigest)
	if err != nil {
		return err
	}
	if found && disposition != dispositionRejected {
		return fmt.Errorf("%w: source record was already accepted", ErrSourceRecordConflict)
	}
	if !found {
		_, err = tx.Exec(ctx, `
            INSERT INTO decision_ingestion_records (
                source_topic, source_partition, source_offset, record_sha256,
                disposition, error_code, error_message, rejected_value
            ) VALUES ($1, $2, $3, $4, 'REJECTED', $5, $6, $7)`,
			record.Topic, record.Partition, record.Offset, recordDigest, code, errorMessage, record.Value,
		)
		if err != nil {
			return fmt.Errorf("insert rejected decision record: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rejected decision record: %w", err)
	}
	return nil
}

func existingReceipt(ctx context.Context, tx pgx.Tx, record SourceRecord, digest string) (string, bool, error) {
	var storedDigest, disposition string
	err := tx.QueryRow(ctx, `
        SELECT record_sha256, disposition
        FROM decision_ingestion_records
        WHERE source_topic = $1 AND source_partition = $2 AND source_offset = $3`,
		record.Topic, record.Partition, record.Offset,
	).Scan(&storedDigest, &disposition)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("load decision ingestion receipt: %w", err)
	}
	if storedDigest != digest {
		return "", false, fmt.Errorf("%w: coordinate already contains a different value", ErrSourceRecordConflict)
	}
	if disposition != dispositionApplied && disposition != dispositionReplayed && disposition != dispositionRejected {
		return "", false, fmt.Errorf("%w: unsupported stored disposition %q", ErrSourceRecordConflict, disposition)
	}
	return disposition, true, nil
}

func insertReceipt(
	ctx context.Context,
	tx pgx.Tx,
	record SourceRecord,
	digest string,
	event Event,
	disposition string,
) error {
	command, err := tx.Exec(ctx, `
        INSERT INTO decision_ingestion_records (
            source_topic, source_partition, source_offset, record_sha256,
            event_id, payment_id, disposition
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING`,
		record.Topic, record.Partition, record.Offset, digest,
		event.EventID, event.Payload.PaymentID, disposition,
	)
	if err != nil {
		return fmt.Errorf("insert decision ingestion receipt: %w", err)
	}
	if command.RowsAffected() == 1 {
		return nil
	}

	storedDisposition, found, err := existingReceipt(ctx, tx, record, digest)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("decision ingestion receipt conflict was not visible")
	}
	if storedDisposition == dispositionRejected {
		return fmt.Errorf("%w: source record was already rejected", ErrSourceRecordConflict)
	}
	return nil
}

func validateSourceRecord(record SourceRecord) error {
	topic := strings.TrimSpace(record.Topic)
	if topic == "" || len(topic) > 249 {
		return errors.New("Kafka source topic must contain 1-249 characters")
	}
	if record.Partition < 0 {
		return errors.New("Kafka source partition must be non-negative")
	}
	if record.Offset < 0 {
		return errors.New("Kafka source offset must be non-negative")
	}
	return nil
}

func recordSHA256(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func paymentStatusForDecision(value string) (string, error) {
	switch value {
	case DecisionAllow:
		return "ALLOWED", nil
	case DecisionReview:
		return "REVIEW", nil
	case DecisionBlock:
		return "BLOCKED", nil
	default:
		return "", fmt.Errorf("%w: unsupported decision %q", ErrInvalidEvent, value)
	}
}

func validRejectionCode(value string) bool {
	switch value {
	case "invalid_event", "payment_not_found", "decision_conflict", "payment_state_conflict":
		return true
	default:
		return false
	}
}
