-- Alembic revision 0143 (down_revision 0142), rendered with `alembic upgrade 0142:0143 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE go_api_proof_run DROP CONSTRAINT ck_go_api_proof_run_stage;

ALTER TABLE go_api_proof_run ADD CONSTRAINT ck_go_api_proof_run_stage CHECK (stage IN ('dual_run', 'deployed_executed', 'shadow', 'canary', 'write_executed'));

ALTER TABLE go_api_proof_run ADD CONSTRAINT ck_go_api_proof_run_write_executed_shape CHECK (stage <> 'write_executed' OR (side_effect_digest IS NOT NULL AND measurement_route IS NOT NULL));
