-- Alembic revision 0145 downgrade (0145 -> 0144), the reverse of ../0145_add_go_api_proof_orgs.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP INDEX ix_go_api_proof_org_audits_org_recorded;

DROP TABLE go_api_proof_org_audits;

DROP TABLE go_api_proof_orgs;
