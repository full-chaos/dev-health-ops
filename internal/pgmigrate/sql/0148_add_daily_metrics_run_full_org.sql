-- Alembic revision 0148 (down_revision 0147), rendered with `alembic upgrade 0147:0148 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE daily_metrics_runs ADD COLUMN full_org BOOLEAN DEFAULT false NOT NULL;

UPDATE daily_metrics_runs SET full_org = true WHERE starts_with(generation, 'fixed-schedule:daily_metrics_fanout:');
