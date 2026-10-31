# acr entitlement route contract

`GET /api/v1/internal/acr/entitlements/{org_id}` is served on the internal listener (`internal/apiservice/acr`). The acr entitlement client and `acr-projector purge` read it.

| Answer | Meaning |
| --- | --- |
| 200 `acr_entitlement.v1` body | The organization exists. Fields: `schema_version`, `org_id`, `agent_context_runtime`, `acr_service_model`, `platform_model_fallback` (booleans). `agent_context_runtime` is the `ask_dev` decision OR the legacy `agent_context_runtime` grant (CHAOS-3695: "ACR = Ask Dev"; the legacy grant retires in CHAOS-12292, and acr stops reading the field in CHAOS-12291). `acr_service_model` is the effective value (`llmorgsettings.EffectiveServiceModel`): a globally disabled or invalid feature is OFF; else the org's active override; else ON for an org with a saved BYO config of any purpose; else the plan or license decision. `platform_model_fallback` opens only on an active org override (the superuser feature-overrides route). |
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
| 200 `acr_llm_config.v1` body | `schema_version`, `org_id`, `provider`, `model`, `base_url`, `api_key` (decrypted), `version`, and `prompt_cache` (the boolean `true`, present only for an org that opted in to the Anthropic prompt cache on an `anthropic` provider; absent means off, so acr's strict decoder sees the field only where an org asked for it). `version` is the hex sha256 over the org's effective BYO source (the investigate override's rows when one is stored, else `provider`, `model`, `api_key`, `base_url`) and its `prompt_cache` row, sorted by key, each as key, `updated_at` and the sha256 of the stored value; any write, delete or value change in those rows moves it, a flip of the opt-in included, and a row outside them (a non-BYO llm row, or the default row while an override is in use) never does. `provider` is served in the writer's canonical form (stripped and lower-cased, `admin/llmsettings.go:393`). `Cache-Control: no-store`. |
| 401 `{"detail":"Unauthorized"}` | Missing, unknown, revoked or expired bearer token. |
| 403 `{"detail":"Forbidden"}` | Valid credential without the `llm_config:read` scope. |
| 404 `{"detail":"Not found"}` | The org id is not a UUID, the org has no BYO settings, its provider is empty, `auto`, `mock` or `none`, or the `byo_llm` gate is closed for it. Every no-config 404 carries `X-Org-Llm-Config: none`. Three of them also carry `X-Org-Llm-Config-Reason`: `byo_llm_gate_closed` when BYO settings are stored but the `byo_llm` gate is closed, `byo_no_investigate_config` (on the worker route, `byo_no_categorize_config`) when the org has a saved BYO config of another purpose only, and `byo_base_url_carries_key` when the stored `base_url` carries an api key the org stores (the served key, the default row's key or a purpose override's key; raw, escaped, hex or base64 form, up to four layers of percent-escapes), so the route never serves the url. acr refuses all three with a reason and never uses the platform model in their place; with no reason header the org has no BYO config, and acr uses the platform model only with `acr_service_model` and `platform_model_fallback`. |
| 503 `{"detail":"Service unavailable"}` | The credential store, the settings store or the `byo_llm` gate lookup failed, or the route has no database. |
| 409 `{"detail":"Stored config unreadable"}` | A stored BYO setting could not be decrypted (wrong settings key or a corrupt row) while the gate is open. acr treats it as BYO configured but unusable and fails closed. |

The key is never logged, traced, or put in error text. Each served call writes one log line with the org id, the credential id and the version, and increments `devhealth_acr_llm_config_requests_total{outcome}`.

Code: `internal/apiservice/acr/llmconfig.go`; read and decryption in `llmorgsettings.Store.OrgConfig`. The API database role gains SELECT on `internal_service_credentials` (`apiPosture`).

`base_url`: no provider requires one (`llmorgsettings/resolve.go` `credentialsComplete` checks only the api key), so an empty `base_url` is valid and the consumer uses the provider's public endpoint. The writer validates a non-empty `base_url` with `ValidateBaseURLChecked` (`llmorgsettings/ssrf.go:61`): no whitespace, no userinfo, `https` only, and a literal or resolved host must be public. This route serves what was stored and adds no validation, except one refusal: a `base_url` that carries an api key is never served (the marked 404 with `byo_base_url_carries_key`; the log line and the metric outcome carry the reason only, never the url or the key).

Rotation: a `svc_acr_` credential minted before `llm_config:read` existed gets 403 here. Rotate with `dho service-credentials rotate <id> --scope entitlements:read --scope llm_config:read`.

## Principal access read (CHAOS-12084)

**Mounted** (CHAOS-12128): `acr.Routes` serves it from `Deps.Access`, which go-api's `InternalRoutes` builds over its pool, the policy store and its ClickHouse connection. Its endpoint-profile row is `GET /api/v1/internal/acr/orgs/{org_id}/principals/{user_id}/access [dev-health-ops-api @internal]` (`contracts/auth/v1/endpoint-profiles.go-api.json`). The registration line landed first (CHAOS-12127) so that the row's anchor holds at a commit on `main`. Deploy order: prod `dho_api_ch` gains `GRANT SELECT ON default.repos` in the same step as the go-api image that carries the mount, or go-api's readiness check refuses the login (CHAOS-12121).

`GET /api/v1/internal/acr/orgs/{org_id}/principals/{user_id}/access[?sid=<session id>]` is served on the internal listener, beside the entitlement route. acr-api calls it on **every** protected call by a user-class credential (browser, OAuth user and native device credentials: the investigation POST, replay, status and evidence, and each call acr-mcp forwards) to re-check, live, what it may do for that user (North Star check 18: authorization is re-checked every turn, never carried as memory). Design: Linear "Ask Dev: browser calls ACR directly with an ACR OAuth credential" §3.1. The acr consumer is a separate acr item (CHAOS-12092).

Bearer: a `svc_acr_` credential with the scope `principal_access:read` (`dho service-credentials create --service acr --scope entitlements:read --scope llm_config:read --scope principal_access:read`), or the acr-api workload token. A credential minted before this scope existed gets 403 until it is rotated with it. Order: 429 (more than 6000 attempts per minute from one client address, its own bucket `internal_acr_access_attempt`, not the entitlement route's 100/minute one), 401, 403, then the answers below. The route writes no audit row and no `last_used_at` (one per acr call would put a write on the read path); every answer is logged with the org id, user id and credential id, and counted in `devhealth_acr_internal_requests_total{route="principal_access",outcome}`.

`sid` is optional. With it, the answer carries `session`; without it (a row-only credential class), `session` is absent. Any other query parameter, a second `sid`, a query string that does not decode (`sid=%zz`, a `;` separator), or a `sid` that is not a session id in its wire form (`ses_` + unpadded base64url of 16 bytes) is a 400: an undecodable pair is never read as "no sid".

| Answer | Meaning |
| --- | --- |
| 200 `acr_principal_access.v1` body | See below. |
| 400 `{"detail":"Invalid request","code":"acr_access_invalid_sid"}` | `sid` is not a session id in its wire form. |
| 400 `{"detail":"Invalid request","code":"acr_access_invalid_query"}` | A query parameter other than one `sid`, or a query string that does not decode. |
| 404 `{"detail":"Not found","code":"acr_organization_unknown"}` | `org_id` is a well-formed UUID and no `organizations` row exists. |
| 404 `{"detail":"Not found"}` (no `code`) | `org_id` or `user_id` is not a UUID. |
| 503 `{"detail":"Service unavailable","code":"acr_access_repository_scope_over_limit"}` | The repository inventory is above 100 repositories (`repoinventory.Limit`; CHAOS-12267). acr types it as an access limitation, not a retryable outage (CHAOS-12228); an acr that reads no 503 body (acr main 968bdcb0) handles it as before. |
| 503 `{"detail":"Service unavailable","code":"acr_access_project_scope_over_limit"}` | With `ACR_ACCESS_PROJECT_SCOPE=true` only: the project inventory is above 100 projects (`projectinventory.Limit`). acr types it as an access limitation, not a retryable outage (CHAOS-12228). |
| 503 `{"detail":"Service unavailable"}` (no `code`) | Any read failed (Postgres, auth-service's session state read, ClickHouse), or the route has no store (go-api started without `API_DATABASE_URI`; without a ClickHouse connection the inventory read fails). With `sid`, also: go-api has no session issue client (no auth-service URL or session issuer credential), or auth-service's state route answered anything but its state or `404 session_not_found` (a timeout, a redirect, a refusal, an auth-service without the route). Never a partial or stale answer; never 401. |

Body, in this key order:

```json
{
  "schema_version": "acr_principal_access.v1",
  "org_id": "<path value>",
  "user_id": "<path value>",
  "member": true,
  "impersonating": false,
  "session": {"live": true, "impersonating": false},
  "entitlements": {"ask_dev": true, "agent_context_runtime": true},
  "repository_scope": {"kind": "repos", "repos": ["acme/api", "acme/web"]}
}
```

- `member`: the `users` row is active and a `memberships` row joins the user to the org. No superuser bypass. An unknown well-formed `user_id` is `member: false` (200), not 404.
- `session.live`: decided on auth-service's own answer for `sid` (acp ruling D6559 on CHAOS-12122): auth-service's `POST /internal/v1/sessions/state` (`internal/auth/authapi/session_state.go`), read by go-api's session issue client and credential (`internal/api/sessionissue/state.go`). go-api reads no auth table on this route. The session is live when it exists, belongs to `user_id`, is bound to `org_id` (a session bound to no organization is not), is not revoked (auth-service's effective revoke: an explicit revoke, or a password change after the session was created), is not expired (auth-service's effective expiry: the absolute cap or the idle expiry, whichever is first, on auth-service's clock), carries a `token_version` pin, and that pin equals the user's current `users.token_version`. The reasons (logged as `session_reason`, never in the answer), in that order: `session_not_found`, `session_of_another_user`, `session_org_mismatch`, `session_revoked`, `session_expired`, `session_pin_missing`, `user_unknown`, `token_version_stale`. Only auth-service's `404 session_not_found` is "no such session"; every other failure of that read is a 503. The state route reads `Store.Current` alone and writes nothing (no audit row, no `last_seen_at` stamp), so the access read never extends a session's idle life. Roll order: auth-service first, then go-api.
- `impersonating`: the user holds an active `impersonation_sessions` row as the admin. The table is keyed by the user, not by session, so it is read and served on **every** call, with or without `sid`: acr denies every user-class call while it is true (design §3.1 step 3; chris decision 6 denies all Ask Dev calls while impersonating). `session.impersonating` carries the same value inside `session`.
- `entitlements`: the `ask_dev` decision, and `agent_context_runtime` = `ask_dev` OR the legacy `agent_context_runtime` grant (CHAOS-3695), both from `internal/api/licensing`, the same engine as the entitlement route.
- `repository_scope`: the org's repository inventory, `internal/repoinventory` (the canonical `owner/name` slugs of ClickHouse `repos FINAL` for the org), the same list query-api's `catalog(orgId, dimension: REPO)` serves and web reads as the Ask Dev repository scope. Both run one implementation. A non-empty inventory is `{"kind": "repos", "repos": [...]}` (sorted). An empty inventory is `{"kind": "none"}` with **no** `repos` key: an empty array never stands for any scope (acr reads an empty `RepositoryScopes` slice as unrestricted). An inventory above 100 repositories is refused (503), never truncated. go-api's ClickHouse login holds `SELECT ON default.repos` for it (`chclickhouse.APIPosture`; `docker/clickhouse-users.d/dho_api_ch.xml` is generated from it). A login without that grant answers 503, never an empty scope.

- `project_scope` (CHAOS-12224; **sent only with `ACR_ACCESS_PROJECT_SCOPE=true` on go-api, default off**): the org's project inventory, `internal/projectinventory`. It is the `(provider, id)` pair of every ClickHouse `projects FINAL` row of the org with a non-empty provider and id, as stored (no lowercasing, trimming or Ops encoding; acr derives the canonical `project.v2` identity from each pair, design CHAOS-12107 v3 F6).
  - It is present only when the flag is on **and** `repository_scope.kind` is `"none"`. With repositories, or with the flag off, the answer has no `project_scope` key and is byte-identical to the answer before the field existed.
  - A non-empty inventory is `{"kind": "projects", "projects": [{"provider": "jira", "id": "10001"}, ...]}`, sorted by provider then id. An empty inventory is `{"kind": "none"}` with **no** `projects` key.
  - An inventory above 100 projects is refused, never truncated: 503 with `code` `acr_access_project_scope_over_limit` (the counter's outcome `project_scope_over_limit`, one Warn line naming the inventory and the limit). The read binds `LIMIT 101`, so an over-limit org is seen, not cut. A failed read is the code-less 503. The read is live on every call, with no cache.
  - The repository inventory over its limit has its own code too, `acr_access_repository_scope_over_limit` (outcome `repository_scope_over_limit`; CHAOS-12267). It changed a live 503 body, so it landed only after acr was shown, by an executed acr test, to reject a 503 with a `code` exactly as a code-less one.
  - acr turns it into the `project_only` scope only for a `*` consent grant (design D2); an explicit repository grant in a no-repo org stays `no_repository`.
  - **Roll order (strict).** acr's decoder refuses unknown fields (`DisallowUnknownFields`, acr `internal/entitlements/principal_access.go`). So the flag goes on only after an acr that accepts `project_scope` (CHAOS-12107, gates CHAOS-12228) is live everywhere; before that, every user-class call answers 503.
  - **Grant.** With the flag on, go-api's ClickHouse login must also hold `SELECT ON default.projects` (`chclickhouse.ProjectScopeGrant`; the posture is `APIPostureFor(db, true)`). The readiness check is exact both ways: the grant without the flag, or the flag without the grant, makes go-api unready. So the users.d line (and, on ClickHouse Cloud, the GRANT) must move with the flag, and **no canonical path does that yet**: `cigate bigboy render-ch-users` and `grants-compare` parse `APIPosture` only (20 grants, not the flag-on 21), and `ci/bigboy/check-dho-api-ch-user.sh` refuses a users.d file that is not that render. A flag-on go-api therefore fails closed: unready, or the preflight reports `CH_API_USER_FILE_MISMATCH`. **CHAOS-12268** adds the flag-on render, the checks and the prod GRANT step, and it blocks the flip.

There is no cache on either side: every call reads every source. acr must not cache the answer across calls.

The answers are written with the JSONResponse writer (`policy.WriteJSON`): the route is Go-only, with no FastAPI ancestor and so no response-model entry.

Code: `internal/apiservice/acr/access.go` (handler, body, registration), `access_store.go` (reads), `handlers.go` (`Routes` mounts it), `internal/apiservice/service.go` (`InternalRoutes` wiring), `internal/repoinventory`, `internal/projectinventory`, `internal/apiservice/acr_project_scope.go` (the flag). Tests: `access_test.go` (each field, each refusal, each read failure, no cache), `access_integration_test.go` (real Postgres and auth schema; the session read through auth-service's real state route over HTTP, mounted by `authapi.CompatRoutes` from the embedded endpoint-profile row with the real service credential check: organization binding, no organization, password change, absolute and idle expiry, an auth-service without the route; ClickHouse inventory equal to the catalog; p50/p95 receipt), `user_tokens_refused_test.go` (every user-token class 401 before any read), `workloadauth_test.go` (the workload token on every route), `internal/apiservice/internal_listener_test.go` (no writer violation on any answer through the server wrapper), `internal/apiservice/internal_access_integration_test.go` (the mounted read over real Postgres and a ClickHouse login granted exactly `APIPosture`; 503 on a login without the repos grant).
