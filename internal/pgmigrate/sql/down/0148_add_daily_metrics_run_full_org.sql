-- Alembic revision 0148 downgrade (0148 -> 0147), the reverse of ../0148_add_daily_metrics_run_full_org.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE daily_metrics_runs DROP COLUMN full_org;
