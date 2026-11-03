-- Alembic revision 0149 (down_revision 0148), rendered with `alembic upgrade 0148:0149 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE daily_metrics_runs ADD COLUMN touched_take_at TIMESTAMP WITH TIME ZONE;
