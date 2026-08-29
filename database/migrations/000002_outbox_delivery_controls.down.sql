BEGIN;

DROP INDEX idx_outbox_events_pending;

ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_one_terminal_state,
    DROP COLUMN dead_lettered_at,
    DROP COLUMN last_error,
    DROP COLUMN last_attempt_at,
    DROP COLUMN next_attempt_at,
    DROP COLUMN delivery_attempts;

CREATE INDEX idx_outbox_events_unpublished
    ON outbox_events (created_at, id)
    WHERE published_at IS NULL;

COMMIT;
