-- Revision 0166 (down_revision 0165), written by hand.
-- The post_sync fan-out records its outcome on its own ledger row, in the transaction that commits its work, so the
-- outcome outlives the River job row: post_sync_outcome is the fan-out outcome token ('published' or 'no_repositories')
-- and post_sync_committed_at the time of the first commit that wrote it. A row with both NULL has no committed fan-out on
-- record; every row written before this revision reads that way, and no row is backfilled. Both columns are nullable
-- with no default, so each ALTER changes catalog metadata only (no table rewrite). The domain role's UPDATE on the two
-- columns is a runtime grant (dho migrate river), not part of this file.

ALTER TABLE public.sync_run_post_dispatches
    ADD COLUMN post_sync_outcome text NULL,
    ADD COLUMN post_sync_committed_at timestamp with time zone NULL,
    ADD CONSTRAINT ck_sync_run_post_dispatches_post_sync_outcome
        CHECK (post_sync_outcome IS NULL OR post_sync_outcome IN ('published', 'no_repositories')),
    ADD CONSTRAINT ck_sync_run_post_dispatches_post_sync_outcome_pair
        CHECK ((post_sync_outcome IS NULL) = (post_sync_committed_at IS NULL));
