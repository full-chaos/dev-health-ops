-- Alembic revision 0149 downgrade (0149 -> 0148), the reverse of ../0149_add_daily_metrics_run_touched_take_at.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE daily_metrics_runs DROP COLUMN touched_take_at;
