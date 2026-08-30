BEGIN;

ALTER TABLE manual_review_queue
    DROP CONSTRAINT manual_review_resolution_shape,
    ADD CONSTRAINT manual_review_resolution_shape CHECK (
        (
            status = 'PENDING'
            AND resolved_at IS NULL
            AND reviewer_id IS NULL
            AND resolution_reason IS NULL
        ) OR (
            status IN ('APPROVED', 'REJECTED')
            AND resolved_at IS NOT NULL
            AND length(btrim(reviewer_id)) > 0
            AND length(btrim(resolution_reason)) > 0
        )
    );

COMMIT;
