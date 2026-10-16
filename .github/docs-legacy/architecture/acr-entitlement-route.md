# acr entitlement route contract

`GET /api/v1/internal/acr/entitlements/{org_id}` is served on the internal listener (`internal/apiservice/acr`). The acr entitlement client and `acr-projector purge` read it.

| Answer | Meaning |
| --- | --- |
| 200 `acr_entitlement.v1` body | The organization exists. |
| 404 `{"detail":"Not found","code":"acr_organization_unknown"}` | `org_id` is a well-formed UUID and no `organizations` row exists (deleted or never created). |
| 404 `{"detail":"Not found"}` (no `code`) | `org_id` is not a UUID. Says nothing about deletion. |
| 503 `{"detail":"Service unavailable"}` | The store cannot be read (database outage, no store configured). |

Rules for callers:

- Only the 404 with `code == "acr_organization_unknown"` may be read as "organization gone". Every other 404 (malformed id, unrouted path, proxy) and every 5xx or timeout is "unavailable".
- The code is stable. Changing it is a breaking contract change for acr.
- A new answer must never reuse the code for a store failure.

Code: `UnknownOrganizationCode` in `internal/apiservice/acr/handlers.go`; sentinel `ErrOrgUnknown` in `store.go`.

## Org LLM config route

`GET /api/v1/internal/acr/orgs/{org_id}/llm-config` is served on the internal listener, beside the entitlement route. It gives acr an org's bring-your-own (BYO) LLM config. The key stays in the ops settings store, and acr never holds `SETTINGS_ENCRYPTION_KEY`.

Unlike the entitlement route, it checks a bearer token. The token is a `svc_acr_` credential from `dho service-credentials create --service acr --scope llm_config:read`. The check reads `internal_service_credentials` and compares the SHA-256 hash in constant time. A credential made before this scope existed holds only `entitlements:read` and gets 403 until it is rotated with the new scope. The internal-listener boundary still applies.

| Answer | Meaning |
| --- | --- |
| 200 `acr_llm_config.v1` body | `schema_version`, `org_id`, `provider`, `model`, `base_url`, `api_key` (decrypted), `version`. `version` is the newest `updated_at` of the org's llm settings rows, and it changes on every write. `Cache-Control: no-store`. |
| 401 `{"detail":"Unauthorized"}` | Missing, unknown, revoked or expired bearer token. |
| 403 `{"detail":"Forbidden"}` | Valid credential without the `llm_config:read` scope. |
| 404 `{"detail":"Not found"}` | The org id is not a UUID, the org has no BYO settings, or the `byo_llm` gate is closed for it. acr falls back to the platform default, as every other reader of these settings does. |
| 503 `{"detail":"Service unavailable"}` | The credential store, the settings store or the `byo_llm` gate lookup failed, or the route has no database. |

The key is never logged, traced, or put in error text. Each served call writes one log line with the org id, the credential id and the version, and increments `devhealth_acr_llm_config_requests_total{outcome}`.

Code: `internal/apiservice/acr/llmconfig.go`; read and decryption in `llmorgsettings.Store.OrgConfig`. The API database role gains SELECT on `internal_service_credentials` (`apiPosture`).
