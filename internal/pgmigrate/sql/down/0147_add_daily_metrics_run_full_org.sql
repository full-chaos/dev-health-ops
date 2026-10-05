-- Alembic revision 0147 downgrade (0147 -> 0146), the reverse of ../0147_add_daily_metrics_run_full_org.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE daily_metrics_runs DROP COLUMN full_org;
