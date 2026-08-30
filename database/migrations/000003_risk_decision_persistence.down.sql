BEGIN;

DROP TABLE IF EXISTS manual_review_queue;
DROP TABLE IF EXISTS audit_events;
DROP TABLE IF EXISTS decision_ingestion_records;
DROP TABLE IF EXISTS payment_decisions;
DROP FUNCTION IF EXISTS reject_immutable_row_mutation();

COMMIT;
