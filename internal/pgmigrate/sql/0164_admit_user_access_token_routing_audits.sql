-- Revision 0164 (down_revision 0163), written by hand.
-- The go-api routing verbs (`dho goapi routing seed` and `enable`) present a user access token to query-api's
-- /buildinfo instead of the effective-principal envelope, and an audit row names the credential actually used. The two
-- CHECKs of go_api_routing_audits admit the user_access_token class: like the envelope, it names its subject.
-- Additive: every row the old CHECKs admit (an envelope row with its subject, an operator_direct row with none) the
-- new ones admit. The table is append-only; no row is changed.

ALTER TABLE go_api_routing_audits
    DROP CONSTRAINT ck_go_api_routing_audits_credential_class,
    ADD CONSTRAINT ck_go_api_routing_audits_credential_class
        CHECK (credential_class IN ('effective_principal_envelope', 'operator_direct', 'user_access_token')),
    DROP CONSTRAINT ck_go_api_routing_audits_principal_pairing,
    ADD CONSTRAINT ck_go_api_routing_audits_principal_pairing
        CHECK ((credential_class IN ('effective_principal_envelope', 'user_access_token')) = (principal_id IS NOT NULL));
