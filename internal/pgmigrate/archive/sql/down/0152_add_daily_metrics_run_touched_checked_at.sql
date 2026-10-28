-- Revision 0152 downgrade (0152 -> 0151), the reverse of ../0152_add_daily_metrics_run_touched_checked_at.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE daily_metrics_runs DROP COLUMN touched_checked_at;
