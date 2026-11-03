-- 0036 credential_registry.credential_class closes the two retired legacy ingest
-- classes (legacy_ingest_api_key, legacy_ingest_hmac_signature).
-- NOT ADDITIVE in one place, on purpose (additiveExemptions): it REDEFINES the
-- class CHECK and only NARROWS it, by exactly those two values; the list is the
-- 0032 list without them.
--
-- The legacy ingest routes are retired (CHAOS-11813): nothing mints, stores or
-- validates either class, and contracts/auth/v1/credential-classes.json and the
-- wire schemas no longer name them (CHAOS-11820). worker_operational_bridge_token
-- stays in the CHECK as a check-only retired class (CHAOS-6761).
--
-- The pre-check counts stored rows of either class and refuses with the count
-- only (never row data), before the CHECK is redefined. The pre-check, the DROP
-- and the ADD run in the migration's one transaction, and the ADD validates every
-- row (it is not NOT VALID), so a row written between the pre-check and the ADD
-- fails the ADD: the narrowing is never partial.
--
-- The ALTER takes ACCESS EXCLUSIVE on credential_registry for as long as the
-- table scan to validate the CHECK takes.
DO $$
DECLARE
    retired bigint;
BEGIN
    SELECT count(*) INTO retired FROM credential_registry
    WHERE credential_class IN ('legacy_ingest_api_key', 'legacy_ingest_hmac_signature');
    IF retired > 0 THEN
        RAISE EXCEPTION 'migration refused: credential_registry holds % row(s) of a retired legacy ingest credential class; remove them, then migrate again', retired
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$$;

ALTER TABLE credential_registry
    DROP CONSTRAINT credential_registry_credential_class_closed,
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
        'internal_svc_authsess_token',
        'internal_svc_worker_token',
        'jira_webhook_signature',
        'kubernetes_sa_token_exchange',
        'license_key',
        'link_credential',
        'llm_api_keys',
        'ops_access_token_hs256',
        'ops_refresh_token',
        'org_context_header',
        'org_invite_token',
        'pagerduty_oauth_credential',
        'pagerduty_webhook_signature',
        'password_reset_token',
        'refresh_credential',
        'social_oauth_exchange',
        'sso_oidc_exchange',
        'sso_saml_exchange',
        'stripe_webhook_signature',
        'user_access_token',
        'worker_operational_bridge_token',
        'workload_bootstrap_credential',
        'workload_token'
    ));
