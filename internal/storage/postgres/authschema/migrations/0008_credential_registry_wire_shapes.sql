-- 0008 credential_registry columns carry the wire contract's shapes.
-- ADDITIVE ONLY. See 0001's header.
--
-- The credential wire contract (credential-entry.v1, contracts/auth/v1) is a
-- projection of this table, so a row the table accepts but the contract rejects
-- is a credential nobody can list or revoke through the API. Codex executed it
-- against the wire contract: 0003 accepted an arbitrary credential_class, a
-- display_prefix of any length or alphabet, and free-text revoked_reason prose
-- (which can also carry an address or a secret into list responses and audit
-- trails). The producer gets the wire's rules, so the two cannot disagree.
--
-- Plain CHECKs, not NOT VALID: credential_registry has no non-test writer yet
-- (the auth schema takes no production writes until Wave 2), so there is no row
-- to skip and a validated constraint is the stronger statement. A deployment
-- that somehow holds a nonconforming row fails to apply this loudly, and the fix
-- is a data correction.
--
-- Each rule is the wire schema's, verbatim, and tests keep them equal:
--   credential_class   the closed vocabulary principal.v1 carries (credential.class
--                      enum, the 30 frozen Wave 0 classes plus the platform
--                      credentials TRD section 11 defines). A new class is a
--                      migration: unknown denies.
--   display_prefix     at most 16 characters of [A-Za-z0-9_]: the only fragment of
--                      a secret that may appear outside the one-time receipt.
--   revoked_reason     a reason CODE, never prose.
--   revoked_at and revoked_reason are ALL-OR-NONE: the wire entry requires both
--                      when status is revoked and forbids both otherwise, and
--                      status is derived from revoked_at alone, so a row holding
--                      one without the other has no valid projection. Every
--                      conditional the entry schema states over columns has a
--                      producer constraint (TestEveryEntryConditionalHasAProducerCheck,
--                      in the credential contract's PR, sweeps the schema for them).
ALTER TABLE credential_registry
    ADD CONSTRAINT credential_registry_credential_class_closed
    CHECK (credential_class IN (
        'acr_client_credential',
        'acr_device_flow_code',
        'acr_web_assertion',
        'acr_workload_identity_exchange',
        'auth_js_browser_session',
        'effective_principal_envelope',
        'email_verification_token',
        'external_push_ingest_token',
        'github_app_installation_token',
        'github_webhook_signature',
        'gitlab_webhook_signature',
        'impersonation_session',
        'infrastructure_deployment_credentials',
        'internal_svc_acr_token',
        'internal_svc_worker_token',
        'jira_webhook_signature',
        'legacy_ingest_api_key',
        'legacy_ingest_hmac_signature',
        'license_key',
        'llm_api_keys',
        'ops_access_token_hs256',
        'ops_refresh_token',
        'org_context_header',
        'org_invite_token',
        'pagerduty_oauth_credential',
        'password_reset_token',
        'refresh_credential',
        'social_oauth_exchange',
        'sso_oidc_exchange',
        'sso_saml_exchange',
        'stripe_webhook_signature',
        'user_access_token',
        'worker_operational_bridge_token',
        'workload_token'
    )),
    ADD CONSTRAINT credential_registry_display_prefix_shape
    CHECK (display_prefix ~ '^[A-Za-z0-9_]{0,16}$'),
    ADD CONSTRAINT credential_registry_revoked_reason_code
    CHECK (revoked_reason IS NULL OR revoked_reason ~ '^[a-z][a-z0-9_]{2,63}$'),
    ADD CONSTRAINT credential_registry_revoked_pair
    CHECK ((revoked_at IS NULL) = (revoked_reason IS NULL));
