-- Alembic revision 0147 (down_revision 0146), rendered with `alembic upgrade 0146:0147 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE daily_metrics_runs ADD COLUMN full_org BOOLEAN DEFAULT false NOT NULL;

UPDATE daily_metrics_runs SET full_org = true WHERE starts_with(generation, 'fixed-schedule:daily_metrics_fanout:') OR starts_with(generation, 'post-sync:');
