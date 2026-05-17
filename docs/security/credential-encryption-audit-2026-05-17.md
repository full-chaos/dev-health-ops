# Credential encryption at rest audit — 2026-05-17

Scope: SQLAlchemy models under `src/dev_health_ops/models/`, with focused review of settings, SSO, credential, token, billing, subscription, and identity-related storage. This audit reframes CHAOS-1552 as hardening: encryption already existed in `dev_health_ops.core.encryption` and callers use it for the primary credential paths.

## Summary by table

| Table | Secret-bearing column(s) | Encrypted at rest? | Risk | Action |
| --- | --- | --- | --- | --- |
| `settings` | `value` when `is_encrypted=true` | Yes, through `SettingsService.set(..., encrypt=True)` → `encrypt_value`; read via `SettingsService.get` → `decrypt_value` | Medium | Harden KDF/versioning; operators must set `encrypt=True` for sensitive settings. |
| `integration_credentials` | `credentials_encrypted` | Yes, through `IntegrationCredentialsService.set` → JSON serialize → `encrypt_value`; read via service/worker `decrypt_value` | Low | Harden KDF/versioning; run v0→v1 re-encryption utility. |
| `sso_providers` | `encrypted_secrets` values such as OIDC `client_secret` | Yes for service-managed writes via `SSOService.create_provider/update_provider` → per-value `encrypt_value`; OIDC token exchange decrypts via `decrypt_value` | Medium | Harden KDF/versioning; retain legacy plaintext fallback until separately migrated/removed. |
| `sso_providers` | `config` JSON | No, but reviewed contents are non-secret SAML/OIDC metadata; SAML certificate is public IdP signing material | Low | Keep secrets in `encrypted_secrets`; do not add client secrets to `config`. |
| `refresh_tokens` | `token_hash`, `replaced_by_hash` | Not encrypted; token material is hashed, not stored | Low | No encryption required; continue storing only hashes. |
| `password_reset_tokens` | `token_hash` | Not encrypted; token material is hashed, not stored | Low | No encryption required; continue storing only hashes. |
| `email_verification_tokens` | `token_hash` | Not encrypted; token material is hashed, not stored | Low | No encryption required; continue storing only hashes. |
| `billing_audit_log` | `local_state`, `stripe_state` | No | Low | Reviewed as audit/provider state, not credential storage; avoid logging webhook secrets or payment secrets. |
| `billing_plans`, `billing_prices`, `subscriptions`, `subscription_events` | Stripe IDs, metadata | No | Low | IDs are not API credentials; avoid storing provider secrets in metadata. |

## Detailed findings

### `settings.value`

- Location: `src/dev_health_ops/models/settings.py`, `Setting.value` with `Setting.is_encrypted`.
- Secret types: generic application secrets stored as settings when callers pass `encrypt=True`.
- Encryption path: `src/dev_health_ops/api/services/settings.py` `SettingsService.set` encrypts values with `encrypt_value` when `encrypt=True`; `SettingsService.get` decrypts when `is_encrypted` is true.
- Status: encrypted at rest for encrypted settings.
- Severity: Medium, because correctness depends on callers marking sensitive settings with `encrypt=True`.
- Recommendation: keep sensitive settings on this path, document operator expectations, and use the v0→v1 re-encryption utility after deployment.

### `integration_credentials.credentials_encrypted`

- Location: `src/dev_health_ops/models/settings.py`, `IntegrationCredential.credentials_encrypted`.
- Secret types: provider API tokens/keys for GitHub, GitLab, Jira, Linear, Atlassian, LaunchDarkly, telemetry, and similar integrations.
- Encryption path: `IntegrationCredentialsService.set` normalizes credential keys, serializes the credential JSON, and writes `encrypt_value(json.dumps(credentials))`; `IntegrationCredentialsService.get/get_decrypted` and `workers/task_utils.py` decrypt with `decrypt_value`.
- Status: encrypted at rest.
- Severity: Low after hardening; previously used a raw SHA-256-derived Fernet key.
- Recommendation: run `python scripts/reencrypt_settings_credentials.py --apply` to rewrite legacy unprefixed ciphertexts as `v1:`.

### `sso_providers.encrypted_secrets`

- Location: `src/dev_health_ops/models/sso.py`, `SSOProvider.encrypted_secrets`.
- Secret types: OIDC/OAuth client secrets and transient SSO state/nonce values when supplied as secret fields.
- Encryption path: `SSOService.create_provider` and `SSOService.update_provider` encrypt every string value in `encrypted_secrets`; `_exchange_oidc_code` decrypts `client_secret` before token exchange.
- Status: encrypted at rest for service-managed writes.
- Severity: Medium, because `_exchange_oidc_code` still has a compatibility fallback that treats undecryptable `client_secret` as pre-encryption plaintext.
- Recommendation: use the v0→v1 re-encryption utility for encrypted values and file a follow-up to remove plaintext fallback after confirming no legacy plaintext rows remain.

### `sso_providers.config`

- Location: `src/dev_health_ops/models/sso.py`, `SSOProvider.config`.
- Secret types: none expected. The model comments list SAML IdP entity/URLs, public signing certificate, OIDC issuer/endpoints/JWKS, scopes, and claim mapping.
- Encryption path: none.
- Status: not encrypted; acceptable for non-secret metadata.
- Severity: Low.
- Recommendation: keep OIDC `client_secret` out of `config`; store it only in `encrypted_secrets`.

### Token tables

- Locations:
  - `src/dev_health_ops/models/refresh_token.py`, `RefreshToken.token_hash`, `RefreshToken.replaced_by_hash`
  - `src/dev_health_ops/models/password_reset_token.py`, `PasswordResetToken.token_hash`
  - `src/dev_health_ops/models/email_verification_token.py`, `EmailVerificationToken.token_hash`
- Secret types: bearer token equivalents, but only hashes are persisted.
- Encryption path: none; not required because plaintext token material is not stored.
- Status: hashed at rest.
- Severity: Low.
- Recommendation: continue storing hashes only.

### Billing and subscription models

- Locations:
  - `src/dev_health_ops/models/billing.py`
  - `src/dev_health_ops/models/billing_audit.py`
  - `src/dev_health_ops/models/subscriptions.py`
- Secret types: none identified. Stored values are Stripe product/price/customer/subscription IDs, metadata, events, and audit snapshots.
- Encryption path: none.
- Status: not encrypted; no credential columns identified.
- Severity: Low.
- Recommendation: ensure audit/event state never includes Stripe webhook signing secrets, API keys, or payment secrets.

## Dependency discovery

- `cryptography` is used directly by `dev_health_ops.core.encryption` and is now declared as a direct dependency.
- `PyNaCl` is used by licensing (`src/dev_health_ops/licensing/generator.py`, `src/dev_health_ops/licensing/validator.py`) and is not removed.
