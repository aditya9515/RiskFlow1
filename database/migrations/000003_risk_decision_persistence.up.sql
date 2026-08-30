BEGIN;

CREATE TABLE payment_decisions (
    decision_id UUID PRIMARY KEY,
    payment_id UUID NOT NULL REFERENCES payments(id),
    source_event_id UUID NOT NULL UNIQUE,
    trace_id UUID NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 2),
    decision VARCHAR(16) NOT NULL CHECK (decision IN ('ALLOW', 'REVIEW', 'BLOCK')),
    risk_score INTEGER NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    rule_score INTEGER NOT NULL CHECK (rule_score BETWEEN 0 AND 100),
    model_score INTEGER NOT NULL CHECK (model_score BETWEEN 0 AND 100),
    model_probability DOUBLE PRECISION NOT NULL CHECK (model_probability BETWEEN 0 AND 1),
    model_review_threshold DOUBLE PRECISION NOT NULL
        CHECK (model_review_threshold > 0 AND model_review_threshold < 1),
    reason_codes TEXT[] NOT NULL CHECK (cardinality(reason_codes) > 0),
    rule_version VARCHAR(255) NOT NULL CHECK (length(btrim(rule_version)) > 0),
    model_version VARCHAR(255) NOT NULL CHECK (length(btrim(model_version)) > 0),
    velocity_5m INTEGER NOT NULL CHECK (velocity_5m >= 1),
    new_device BOOLEAN NOT NULL,
    cross_border BOOLEAN NOT NULL,
    baseline_country CHAR(2) NOT NULL CHECK (baseline_country ~ '^[A-Z]{2}$'),
    decision_at TIMESTAMPTZ NOT NULL,
    event_fingerprint CHAR(64) NOT NULL
        CHECK (event_fingerprint ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_payment_decisions_payment_decision_at
    ON payment_decisions (payment_id, decision_at DESC);

CREATE TABLE decision_ingestion_records (
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    record_sha256 CHAR(64) NOT NULL CHECK (record_sha256 ~ '^[0-9a-f]{64}$'),
    event_id UUID,
    payment_id UUID REFERENCES payments(id),
    disposition VARCHAR(16) NOT NULL
        CHECK (disposition IN ('APPLIED', 'REPLAYED', 'REJECTED')),
    error_code VARCHAR(100),
    error_message TEXT,
    rejected_value BYTEA,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_topic, source_partition, source_offset),
    CONSTRAINT decision_ingestion_record_shape CHECK (
        (
            disposition IN ('APPLIED', 'REPLAYED')
            AND event_id IS NOT NULL
            AND payment_id IS NOT NULL
            AND error_code IS NULL
            AND error_message IS NULL
            AND rejected_value IS NULL
        ) OR (
            disposition = 'REJECTED'
            AND event_id IS NULL
            AND payment_id IS NULL
            AND error_code IS NOT NULL
            AND error_message IS NOT NULL
            AND rejected_value IS NOT NULL
        )
    )
);

CREATE INDEX idx_decision_ingestion_event_id
    ON decision_ingestion_records (event_id)
    WHERE event_id IS NOT NULL;

CREATE INDEX idx_decision_ingestion_rejected
    ON decision_ingestion_records (recorded_at DESC)
    WHERE disposition = 'REJECTED';

CREATE TABLE audit_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    actor_type VARCHAR(32) NOT NULL,
    actor_id VARCHAR(255) NOT NULL,
    decision_id UUID NOT NULL REFERENCES payment_decisions(decision_id),
    occurred_at TIMESTAMPTZ NOT NULL,
    details JSONB NOT NULL CHECK (jsonb_typeof(details) = 'object'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (event_type, decision_id)
);

CREATE INDEX idx_audit_events_aggregate_occurred_at
    ON audit_events (aggregate_type, aggregate_id, occurred_at DESC);

CREATE TABLE manual_review_queue (
    payment_id UUID PRIMARY KEY REFERENCES payments(id),
    decision_id UUID NOT NULL UNIQUE REFERENCES payment_decisions(decision_id),
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED')),
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    resolved_at TIMESTAMPTZ,
    reviewer_id VARCHAR(255),
    resolution_reason TEXT,
    CONSTRAINT manual_review_resolution_shape CHECK (
        (status = 'PENDING' AND resolved_at IS NULL AND reviewer_id IS NULL)
        OR
        (status IN ('APPROVED', 'REJECTED') AND resolved_at IS NOT NULL AND reviewer_id IS NOT NULL)
    )
);

CREATE INDEX idx_manual_review_queue_pending
    ON manual_review_queue (enqueued_at, payment_id)
    WHERE status = 'PENDING';

CREATE FUNCTION reject_immutable_row_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is immutable', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER payment_decisions_are_immutable
BEFORE UPDATE OR DELETE ON payment_decisions
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_mutation();

CREATE TRIGGER decision_ingestion_records_are_immutable
BEFORE UPDATE OR DELETE ON decision_ingestion_records
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_mutation();

CREATE TRIGGER audit_events_are_immutable
BEFORE UPDATE OR DELETE ON audit_events
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_mutation();

COMMIT;
