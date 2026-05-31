-- Tighten schema constraints to match the PDF Definition of Done.
-- Runs on first init of the postgres data volume (alphabetical, after 002).

\c rico

-- 1. screens_embeddings must reference an existing screen ----------------
ALTER TABLE screens_embeddings
    ADD CONSTRAINT screens_embeddings_screen_fk
    FOREIGN KEY (screen_id) REFERENCES screens_metadata(screen_id)
    ON DELETE CASCADE;

-- 2. screens_review_queue too --------------------------------------------
ALTER TABLE screens_review_queue
    ADD CONSTRAINT screens_review_queue_screen_fk
    FOREIGN KEY (screen_id) REFERENCES screens_metadata(screen_id)
    ON DELETE CASCADE;

-- 3. screens_eval should be tied to a run --------------------------------
ALTER TABLE screens_eval
    ADD COLUMN IF NOT EXISTS run_id UUID NOT NULL REFERENCES pipeline_runs(run_id);

-- 4. PDF DoD: every row has non-null run_id + source_fingerprint ---------
ALTER TABLE screens_metadata     ALTER COLUMN run_id             SET NOT NULL;
ALTER TABLE screens_embeddings   ALTER COLUMN run_id             SET NOT NULL;
ALTER TABLE screens_review_queue ALTER COLUMN run_id             SET NOT NULL;
ALTER TABLE screens_metadata     ALTER COLUMN source_fingerprint SET NOT NULL;
ALTER TABLE screens_embeddings   ALTER COLUMN source_fingerprint SET NOT NULL;
