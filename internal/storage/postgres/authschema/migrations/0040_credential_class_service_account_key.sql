-- 0040 credential_registry.credential_class admits service_account_key.
-- NOT ADDITIVE in one place, on purpose (additiveExemptions): it REDEFINES the
-- class CHECK and only WIDENS it (nothing is removed from the 0037 list).
--
-- service_account_key is the key of an organization's service account: fcsak_
-- and 32 random bytes. The registry keeps the SHA-256 digest of the whole
-- value, never the value. contracts/auth/v1/credential-classes.json and
-- principal.v1 now name it. The new list is the 0037 list plus that one value,
-- so no stored row can violate it and no pre-flight count is needed. A test
-- compares the list with principal.v1's enum in both directions.
--
-- The ALTER takes ACCESS EXCLUSIVE on credential_registry for as long as the
-- table scan to re-validate the CHECK takes.
ALTER TABLE credential_registry
    DROP CONSTRAINT credential_registry_credential_class_closed,
    ADD CONSTRAINT credential_registry_credential_class_closed
    CHECK (credential_class IN (
        'acr_browser_refresh_token',
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
        'service_account_key',
        'social_oauth_exchange',
        'sso_oidc_exchange',
        'sso_saml_exchange',
        'stripe_webhook_signature',
        'user_access_token',
        'worker_operational_bridge_token',
        'workload_bootstrap_credential',
        'workload_token'
    ));
