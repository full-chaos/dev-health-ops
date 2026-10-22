# acr entitlement route contract

`GET /api/v1/internal/acr/entitlements/{org_id}` is served on the internal listener (`internal/apiservice/acr`). The acr entitlement client and `acr-projector purge` read it.

| Answer | Meaning |
| --- | --- |
| 200 `acr_entitlement.v1` body | The organization exists. Fields: `schema_version`, `org_id`, `agent_context_runtime`, `acr_service_model`, `platform_model_fallback` (booleans). `acr_service_model` is the effective value (`llmorgsettings.EffectiveServiceModel`): a globally disabled or invalid feature is OFF; else the org's active override; else ON for an org with a saved BYO config of any purpose; else the plan or license decision. `platform_model_fallback` opens only on an active org override (the superuser feature-overrides route). |
| 404 `{"detail":"Not found","code":"acr_organization_unknown"}` | `org_id` is a well-formed UUID and no `organizations` row exists (deleted or never created). |
| 404 `{"detail":"Not found"}` (no `code`) | `org_id` is not a UUID. Says nothing about deletion. |
| 503 `{"detail":"Service unavailable"}` | The store cannot be read (database outage, no store configured). |

## Bearer check (CHAOS-9993)

`GET .../entitlements/{org_id}` and `GET .../health` check a `svc_acr_` bearer, as `api/internal/acr.py` did (the Python source is in git history: `git show 64efc37bb^:src/dev_health_ops/api/internal/acr.py`). The credential comes from `dho service-credentials create --service acr --scope entitlements:read`. A token that is not a minted credential is refused.

Order, on both routes:

1. 429 `{"detail":"Too many authentication attempts"}`: more than 100 attempts per minute from one client address (`INTERNAL_ACR_AUTH_ATTEMPT_IP_LIMIT`). One bucket per address, shared by both routes and every org id. A rate-limit store error is 500.
2. 401 `{"detail":"Unauthorized"}`: no `Authorization: Bearer svc_acr_...` header (the scheme is case-sensitive), an unknown token, a revoked or expired token, or a credential of another service. A credential is still valid at its `expires_at` instant.
3. 403 `{"detail":"Forbidden"}`: a valid credential without `entitlements:read`.
4. The route's own answers above.

The entitlement route also writes `last_used_at` (after the validity check, before the scope check) and one `internal_service_credential_audits` row per answer, action `acr_entitlement_lookup`, `requested_org_id` the raw path value, `credential_id` NULL when the token matched no credential. Outcomes: `missing_or_malformed_token`, `unknown_token`, `inactive_token`, `insufficient_scope`, `organization_not_found` (404, both kinds), `entitlement_lookup_failed` (503 or 500), `success`. The health route writes neither. The health route's 503 is a failed credential or store read.

Differences from Python: a credential-store, `last_used_at` or audit write that fails is 503 on the entitlement route (Python raised an unhandled 500), and Python rolled back `last_used_at` with the failed lookup while the Go route keeps it. A route with no credential store answers 503; it never serves unauthenticated. Telemetry: `devhealth_acr_internal_requests_total{route,outcome}`; the token is never logged.

The API database role needs SELECT and UPDATE on `internal_service_credentials` and INSERT on `internal_service_credential_audits` (`apiPosture`); the River GRANT migration must run before the API rollout, or the entitlement route answers 503.

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
| 200 `acr_llm_config.v1` body | `schema_version`, `org_id`, `provider`, `model`, `base_url`, `api_key` (decrypted), `version`, and `prompt_cache` (the boolean `true`, present only for an org that opted in to the Anthropic prompt cache on an `anthropic` provider; absent means off, so acr's strict decoder sees the field only where an org asked for it). `version` is the hex sha256 over the org's BYO rows (`provider`, `model`, `api_key`, `base_url`) and its `prompt_cache` row, sorted by key, each as key, `updated_at` and the sha256 of the stored value; any write, delete or value change moves it, a flip of the opt-in included, and a non-BYO llm row never does. `provider` is served in the writer's canonical form (stripped and lower-cased, `admin/llmsettings.go:393`). `Cache-Control: no-store`. |
| 401 `{"detail":"Unauthorized"}` | Missing, unknown, revoked or expired bearer token. |
| 403 `{"detail":"Forbidden"}` | Valid credential without the `llm_config:read` scope. |
| 404 `{"detail":"Not found"}` | The org id is not a UUID, the org has no BYO settings, its provider is empty, `auto`, `mock` or `none`, or the `byo_llm` gate is closed for it. Every no-config 404 carries `X-Org-Llm-Config: none`. Two of them also carry `X-Org-Llm-Config-Reason`: `byo_llm_gate_closed` when BYO settings are stored but the `byo_llm` gate is closed, and `byo_no_investigate_config` (on the worker route, `byo_no_categorize_config`) when the org has a saved BYO config of another purpose only. acr refuses both with a reason and never uses the platform model in their place; with no reason header the org has no BYO config, and acr uses the platform model only with `acr_service_model` and `platform_model_fallback`. |
| 503 `{"detail":"Service unavailable"}` | The credential store, the settings store or the `byo_llm` gate lookup failed, or the route has no database. |
| 409 `{"detail":"Stored config unreadable"}` | A stored BYO setting could not be decrypted (wrong settings key or a corrupt row) while the gate is open. acr treats it as BYO configured but unusable and fails closed. |

The key is never logged, traced, or put in error text. Each served call writes one log line with the org id, the credential id and the version, and increments `devhealth_acr_llm_config_requests_total{outcome}`.

Code: `internal/apiservice/acr/llmconfig.go`; read and decryption in `llmorgsettings.Store.OrgConfig`. The API database role gains SELECT on `internal_service_credentials` (`apiPosture`).

`base_url`: no provider requires one (`llmorgsettings/resolve.go` `credentialsComplete` checks only the api key), so an empty `base_url` is valid and the consumer uses the provider's public endpoint. The writer validates a non-empty `base_url` with `ValidateBaseURLChecked` (`llmorgsettings/ssrf.go:61`): no whitespace, no userinfo, `https` only, and a literal or resolved host must be public. This route serves what was stored and adds no validation.

Rotation: a `svc_acr_` credential minted before `llm_config:read` existed gets 403 here. Rotate with `dho service-credentials rotate <id> --scope entitlements:read --scope llm_config:read`.
