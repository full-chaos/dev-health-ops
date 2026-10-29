-- Alembic revision 0139 (down_revision 0138), rendered with `alembic upgrade 0138:0139 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE refunds ALTER COLUMN stripe_refund_id DROP NOT NULL;

ALTER TABLE refunds ALTER COLUMN stripe_charge_id DROP NOT NULL;

ALTER TABLE refunds ADD COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX uq_refunds_org_idempotency_key ON refunds (org_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
