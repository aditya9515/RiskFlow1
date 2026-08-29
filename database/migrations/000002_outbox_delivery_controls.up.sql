BEGIN;

DROP INDEX idx_outbox_events_unpublished;

ALTER TABLE outbox_events
    ADD COLUMN delivery_attempts INTEGER NOT NULL DEFAULT 0
        CHECK (delivery_attempts >= 0),
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN last_attempt_at TIMESTAMPTZ,
    ADD COLUMN last_error TEXT,
    ADD COLUMN dead_lettered_at TIMESTAMPTZ,
    ADD CONSTRAINT outbox_events_one_terminal_state
        CHECK (published_at IS NULL OR dead_lettered_at IS NULL);

CREATE INDEX idx_outbox_events_pending
    ON outbox_events (next_attempt_at, created_at, id)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;

COMMIT;
