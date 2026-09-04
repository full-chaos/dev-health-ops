// Package llmorgsettings is the Go read path for an organization's own
// (BYO -- bring-your-own) LLM provider configuration, deferred out of
// CHAOS-4978's Go categorization port and closed here (CHAOS-4989).
//
// Storage: the Postgres `settings` table (models/settings.py's Setting),
// scoped by (org_id, category='llm'), with keys "provider"/"model"/
// "api_key"/"base_url"/"concurrency" ("concurrency" is not read here --
// no Go caller needs it yet). A value may be Fernet-encrypted
// (is_encrypted=true); decryption reuses providerfoundation.FernetDecryptor,
// already wire-compatible with core/encryption.py, keyed by the same
// SETTINGS_ENCRYPTION_KEY/SETTINGS_ENCRYPTION_SALT env vars (never a
// hardcoded value -- see that package's own doc comment).
//
// Oracle: llm/credentials.py's resolve_usable_org_llm_provider,
// org_byo_provider_matches, resolve_llm_org_settings_credentials,
// resolve_llm_org_settings_model, _resolve_org_byo_credentials, and
// validate_llm_base_url; the byo_llm feature-flag gate mirrors
// api/services/licensing.py's byo_llm_flag_state (a hard TEAM-tier floor
// that a positive per-org override must NOT bypass) over
// licensing/feature_decisions.py's evaluate_org_feature_sync precedence,
// narrowed to the "byo_llm" feature key specifically (it is in neither
// EXPLICIT_PURCHASE_FEATURES nor ORG_OVERRIDE_ONLY_FEATURES --
// licensing/registry.py -- so those two decide_feature branches never
// trigger for it and are not ported).
//
// Deliberately NOT ported (narrower than Python, documented here rather
// than silently dropped -- flag this gap in any PR touching this
// package): the audit-log write + Prometheus alert on an SSRF-rejected
// org base_url falling back to the platform default
// (_audit_org_byo_base_url_fallback) -- this package fails back to the
// platform default the same way Python does, it just does not write the
// audit trail Python writes alongside that fallback. A caller that needs
// that trail must add it itself.
package llmorgsettings
