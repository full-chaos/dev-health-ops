-- Alembic revision 0139 downgrade (0139 -> 0138), the reverse of ../0139_refunds_write_first_idempotency_key.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP INDEX uq_refunds_org_idempotency_key;

ALTER TABLE refunds DROP COLUMN idempotency_key;

ALTER TABLE refunds ALTER COLUMN stripe_charge_id SET NOT NULL;

ALTER TABLE refunds ALTER COLUMN stripe_refund_id SET NOT NULL;
