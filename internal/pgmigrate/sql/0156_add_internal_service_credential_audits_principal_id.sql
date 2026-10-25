-- Revision 0156 (down_revision 0155), written by hand.
-- principal_id is the workload token subject (a prn_ id) of an internal acr request whose token verified. It is
-- NULL for every row written before this revision, for every svc_acr_ row, and for every request refused before or
-- at verification. A row names a credential or a principal, never both.

ALTER TABLE internal_service_credential_audits ADD COLUMN principal_id text;

ALTER TABLE internal_service_credential_audits
    ADD CONSTRAINT ck_internal_service_credential_audits_one_actor CHECK (credential_id IS NULL OR principal_id IS NULL);

ALTER TABLE internal_service_credential_audits
    ADD CONSTRAINT ck_internal_service_credential_audits_principal_id_length CHECK (principal_id IS NULL OR char_length(principal_id) <= 255);
