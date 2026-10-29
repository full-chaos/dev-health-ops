-- Revision 0152 (down_revision 0151), written by hand.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE daily_metrics_runs ADD COLUMN touched_checked_at TIMESTAMP WITH TIME ZONE;
