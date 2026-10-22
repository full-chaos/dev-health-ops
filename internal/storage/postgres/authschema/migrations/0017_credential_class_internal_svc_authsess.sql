-- 0017 credential_registry.credential_class admits internal_svc_authsess_token
-- (CHAOS-10465). NOT ADDITIVE in one place, on purpose: it REDEFINES one CHECK,
-- and only WIDENS it.
--
-- 0008 closed credential_class to the vocabulary principal.v1 carries, and said
-- a new class is a migration: unknown denies. internal_svc_authsess_token is
-- the go-api service credential (svc_authsess_) that POST /internal/v1/sessions
-- accepts; contracts/auth/v1/credential-classes.json and principal.v1 now name
-- it. The new list is the 0008 list plus that one value, so no stored row can
-- violate it and no pre-flight count is needed. A test compares the list with
-- principal.v1's enum in both directions.
--
-- The ALTER takes ACCESS EXCLUSIVE on credential_registry for as long as the
-- table scan to re-validate the CHECK takes.
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
    ));
