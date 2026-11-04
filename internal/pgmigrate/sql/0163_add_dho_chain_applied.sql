-- Revision 0163 (down_revision 0162), written by hand.
-- dho_chain_applied is the applied-revision ledger of `dho migrate postgres` (CHAOS-11660). alembic_version holds only
-- the application head (and the cutover head 0066), so it cannot show whether a chain revision below the head ran. The
-- migrator writes one row per chain revision in the same transaction as the revision: source 'apply' with the sha256 of
-- the file it ran. When this revision is applied, the chain revisions alembic_version already recorded get a 'seed' row
-- with no sha. A chain revision at or below the recorded head with no row refuses the walk (chain_revision_unapplied);
-- an 'apply' row whose sha differs from the build's file refuses it too (chain_revision_sha_mismatch).
-- The migrator role owns it and only the migrator reads or writes it: no application role is granted anything.
-- Expand-only: a new table, no change to an existing one.

CREATE TABLE dho_chain_applied (
    revision text PRIMARY KEY,
    sha256 text,
    source text NOT NULL,
    applied_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT ck_dho_chain_applied_source CHECK (source IN ('seed', 'apply')),
    CONSTRAINT ck_dho_chain_applied_sha256 CHECK (sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_dho_chain_applied_seed_has_no_sha CHECK ((source = 'seed') = (sha256 IS NULL))
);
