-- Alembic revision 0143 downgrade (0143 -> 0142), the reverse of ../0143_go_api_proof_run_write_executed.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE go_api_proof_run DROP CONSTRAINT ck_go_api_proof_run_write_executed_shape;

ALTER TABLE go_api_proof_run DROP CONSTRAINT ck_go_api_proof_run_stage;

ALTER TABLE go_api_proof_run ADD CONSTRAINT ck_go_api_proof_run_stage CHECK (stage IN ('dual_run', 'deployed_executed', 'shadow', 'canary'));
