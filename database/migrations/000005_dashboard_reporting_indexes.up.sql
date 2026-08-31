BEGIN;

CREATE INDEX idx_payment_decisions_recent
    ON payment_decisions (decision_at DESC, decision_id DESC);

COMMIT;
