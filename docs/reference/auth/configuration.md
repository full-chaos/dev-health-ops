---
page_id: ref-auth-configuration
summary: Every setting of auth-service and auth-migrate - flag, environment variable, default and bounds - with the rules for secrets (direct value versus _FILE, mutual exclusion, the rejected direct signing key) and the signing-key file contract.
content_type: reference
owner: engineering
source_of_truth:
  - internal/auth/authconfig/config.go
  - internal/auth/authconfig/options.go
  - internal/platform/secrets/source.go
  - internal/auth/keystore/keystore.go
  - cmd/auth-migrate/main.go
applicability: current
lifecycle: active
---

# Auth Control Plane configuration and secret-file reference

`auth-service --help` prints the same registry. A **flag overrides the
environment variable** for the same setting. Secrets have no flag, because
process arguments are readable by anyone who can run `ps` or `docker top`.
**The environment is readable too** (`docker inspect`, `/proc/<pid>/environ` for the same
user), so a DSN given as an environment variable is exposed to anyone with that
access; the local Compose profile does this for its development defaults. For
anything else use the `_FILE` form with a mounted secret.
Non-secret settings supplied only through the environment make the service log
one startup warning listing their names, so a deployment configured that way is
not invisible.
{: .fc-page-lede }

## auth-service

A **syntactic** fault (both a secret and its `_FILE`, an unparseable duration, a
malformed address, a missing required setting, a value out of bounds) stops the
process before it starts, with exit code 1. A **dependency** fault (an
unreachable database, a missing or unsafe key file) does not: the process starts
and `/readyz` fails closed, naming the check. See the
[readiness reference](readiness-and-slo.md).

| Flag | Environment | Default | Bounds / rule |
| --- | --- | --- | --- |
| `--log-level` | `AUTH_SERVICE_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `--shutdown-timeout` | `AUTH_SERVICE_SHUTDOWN_TIMEOUT` | `30s` | 500ms to 10m. Total budget of the ordered shutdown; set it **below** the pod's `terminationGracePeriodSeconds` (Compose default stop grace is 10 s). |
| `--health-check-timeout` | `AUTH_SERVICE_HEALTH_CHECK_TIMEOUT` | `2s` | 50ms to 30s. Per-check bound for each required readiness check. |
| `--http-addr` | `AUTH_SERVICE_HTTP_ADDR` | `:8095` | `host:port`. The API listener. It mounts the session routes only when both session secrets below are set, and no route otherwise. |
| `--operator-http-addr` | `AUTH_SERVICE_OPERATOR_HTTP_ADDR` | `:8096` | `host:port`. `/healthz`, `/readyz`, `/metrics`. Must differ from the API address. |
| `--request-timeout` | `AUTH_SERVICE_REQUEST_TIMEOUT` | `10s` | 100ms to 5m. Per-request handler deadline. |
| `--max-body-bytes` | `AUTH_SERVICE_MAX_BODY_BYTES` | `1048576` | 1024 to 33554432. |
| `--rate-limit` | `AUTH_SERVICE_RATE_LIMIT_PER_SECOND` | `20` | 0.01 to 100000, per route bucket. |
| `--rate-limit-burst` | `AUTH_SERVICE_RATE_LIMIT_BURST` | `40` | 1 to 100000, per route bucket. |
| `--public-max-in-flight` | `AUTH_SERVICE_PUBLIC_MAX_IN_FLIGHT` | `256` | 1 to 100000. In-flight cap of the key and discovery routes, apart from the shared cap of 64. |
| `--public-rate-limit` | `AUTH_SERVICE_PUBLIC_RATE_LIMIT_PER_SECOND` | `500` | 0.01 to 100000, per key or discovery route bucket. |
| `--public-rate-limit-burst` | `AUTH_SERVICE_PUBLIC_RATE_LIMIT_BURST` | `1000` | 1 to 100000, per key or discovery route bucket. |
| (none, secret) | `AUTH_DATABASE_URI` or `AUTH_DATABASE_URI_FILE` | required | The **runtime** login's DSN. Exactly one of the two. |
| `--database-schema` | `AUTH_DATABASE_SCHEMA` | `auth` | Lowercase unquoted identifier `[a-z][a-z0-9_]*`, at most 63 characters. |
| `--database-max-conns` | `AUTH_DATABASE_MAX_CONNS` | `8` | 1 to 256, per replica. The chart sets it from `authService.database.maxConns` (1 to 32) and can check the total against `authService.database.connectionBudget`. |
| `--database-connect-timeout` | `AUTH_DATABASE_CONNECT_TIMEOUT` | `5s` | 100ms to 1m. |
| `--signing-key-file` | `AUTH_SIGNING_KEY_FILE` | required (or the key set) | Path to the Ed25519 PKCS#8 PEM. |
| `--signing-key-id` | `AUTH_SIGNING_KEY_ID` | required (or the key set) | The JWKS `kid`: letters, digits, `-`, `_`, `.`, at most 64. |
| `--signing-keyset-file` | `AUTH_SIGNING_KEYSET_FILE` | unset | Path to a key-set manifest, in place of the two settings above (setting both forms fails startup). |
| (none, secret) | `AUTH_JWT_SECRET_KEY` or `AUTH_JWT_SECRET_KEY_FILE` | unset | HS256 key of the compatibility access token: the same value as go-api's `JWT_SECRET_KEY`, at least 32 characters. Set together with `AUTH_SESSION_SUCCESSOR_KEY`; with neither, no session route is mounted; with one, startup fails. |
| (none, secret) | `AUTH_SESSION_SUCCESSOR_KEY` or `AUTH_SESSION_SUCCESSOR_KEY_FILE` | unset | 32 bytes, base64url without padding. Derives the rotated refresh credential that a 30 s grace replay returns again. |
| `--jwt-issuer` | `AUTH_JWT_ISSUER` | `dev-health-ops` | `iss` of the access token; must equal go-api's `JWT_ISSUER`. |
| `--jwt-audience` | `AUTH_JWT_AUDIENCE` | `dev-health-api` | `aud` of the access token; must equal go-api's `JWT_AUDIENCE`. |
| (none) | `AUTH_LEGACY_JWT_AUDIENCE` | `dev-health-api` | `aud` that the `eddsa` class checks go-api's HS256 compatibility access token with; must equal go-api's `JWT_AUDIENCE` before the eddsa flip. Set but empty is refused at start. The `hs256_compat` class ignores it and uses `AUTH_JWT_AUDIENCE`. |
| (none, secret) | `AUTH_VALKEY_URI` or `AUTH_VALKEY_URI_FILE` | unset | `redis://` or `rediss://` URI on database 1 (path `/1`) of the shared rate-limit store. Required, and refused at startup when absent, while the session routes are mounted (they carry the compatibility refresh limit, and the login, credential and social link limits); with the session routes off it is checked for shape only and never dialled. Nothing dials at startup: an unreachable store fails the `valkey` readiness check and answers the limited routes with a 500. |
| `--orgs-owned` | `AUTH_ORGS_OWNED` | `false` | `true` makes auth-service the only writer of organizations: the sign-in organization projection then reads no public table and writes nothing, and an organization missing from `auth.organizations` answers not found. Mounts no route. Turn it on only after the organization backfill is complete; once it is `true`, `dho auth orgs-backfill` refuses (exit 3, dry run included) before it connects to any database. Any other value than `true` or `false` fails startup. |
| `--revision-feed` | `AUTH_REVISION_FEED` | `off` | `on` gives the auth outbox publisher a Valkey sink (one Lua call: high-water revision hash plus a publish) and subscribes this replica to the revision feed channel; `off` keeps publishing disabled, exactly as before. Needs `AUTH_VALKEY_URI` (startup is refused without it) and the ACL rule above, which carries the feed's channel and commands. Mounts no route. Only `on` and `off` are values. See `docs/contribute/architecture/auth-control-plane-audit-outbox.md`. |
| `--social-link-routes` | `AUTH_SOCIAL_LINK_ROUTES` | `false` | `true` mounts the bearer-only social link and unlink routes (`POST /v1/identities/social`, `DELETE /v1/identities/social/{id}`; see [identity linking](identity-linking.md)); any other value than `true` or `false` fails startup, and `true` needs the session secrets. |
| `--membership-routes` | `AUTH_MEMBERSHIP_ROUTES` | `false` | `true` mounts the internal membership, invite and organization deprovision routes (see [provisioning routes](#provisioning-routes)); any other value than `true` or `false` fails startup, and `true` needs the session secrets and both invite keys. |
| (none, secret) | `AUTH_INVITE_TOKEN_SECRET` or `AUTH_INVITE_TOKEN_SECRET_FILE` | unset | Signing key of new invite tokens: go-api's link signing key (`LINK_TOKEN_SECRET`, else its `JWT_SECRET_KEY` chain), so a token either side minted is accepted by both. Required while `AUTH_MEMBERSHIP_ROUTES` is `true`; startup names the missing variable only. |
| (none, secret) | `AUTH_INVITE_TOKEN_SECRET_PREVIOUS` or `AUTH_INVITE_TOKEN_SECRET_PREVIOUS_FILE` | unset | Retired invite key: accepted, never used to sign (go-api's `LINK_TOKEN_SECRET_PREVIOUS`). Required while `AUTH_MEMBERSHIP_ROUTES` is `true`; with no retired key, set it to the signing key. A token is checked against the signing key, this key, then `AUTH_JWT_SECRET_KEY`, as go-api checks its links. |
| (none) | `AUTH_ACCESS_TOKEN_CLASS` | `hs256_compat` | The one class of access token this service mints: `hs256_compat` or `eddsa` (exact match; any other value fails startup). `eddsa` needs `AUTH_TOKEN_ISSUER_URL` (the `iss`, byte for byte), a signing key, and `AUTH_JWT_AUDIENCE` of `dev-health-api` or `auth-service`; tokens last 60 minutes and carry the access-token.v1 claims only. The session routes still verify HS256 tokens only. |
| `--version` | | | Prints build metadata as JSON. |
| `--healthcheck` | | | Probes this deployment's own operator `/readyz` over loopback; exit 0 only on HTTP 200. For container healthchecks. |

Durations take Go syntax (`30s`, `5m`).

### Session route bearer

The session, social link and change-password routes read their bearer through the route's endpoint-profile row. With `AUTH_TOKEN_ISSUER_URL` set, they accept the EdDSA user access token of audience `auth-service` (access-token.v1), checked against the keys the JWKS publishes: it needs a valid `sid`, its session must be live, and the session's `token_version` pin must equal the user's current `token_version`. The HS256 compatibility access token stays accepted on the same rows. Without `AUTH_TOKEN_ISSUER_URL` the routes verify the HS256 compatibility access token only.

A password change issues the caller's new session in the organization of the session it replaces only while the caller is still a member of that organization; if the membership is gone, the new session has no organization, as in go-api.

The session routes accept the EdDSA token with audience `auth-service` only. While the session routes are mounted, the service therefore refuses to start with the eddsa access token class unless `AUTH_JWT_AUDIENCE` is `auth-service`; the chart renders `dev-health-api` for that class, so the class stays off until the issuer switch mints the session route audience. Under that class the HS256 compatibility access token is checked with `AUTH_LEGACY_JWT_AUDIENCE` (go-api's `JWT_AUDIENCE`, `dev-health-api` when unset), not with `AUTH_JWT_AUDIENCE`.

### Rate limits

The login, credential and social link routes count in a shared fixed-window store, so a limit holds across replicas. Every counter id starts with `auth_svc_`; go-api's counters are never read or written.

- **Store login.** Give auth-service its own Valkey ACL login, limited to its prefix, and put its URI in the `AUTH_VALKEY_URI` Secret (the chart takes it by `secretKeyRef`, `authService.valkey`): `ACL SETUSER auth-service-ratelimit on >PASSWORD resetkeys ~dho:rl:auth_svc_* resetchannels &auth_svc_* -@all +hello +select +ping +get +eval +incr +pexpire +hget +hset +publish +subscribe +client|tracking (~dho:rl:auth_svc_rev_hwm +hgetall)`.
- **In-chart Valkey.** `ACL SETUSER` on a running Valkey is lost at the next restart. With the chart's own Valkey, set `valkey.acl.enabled=true` with `valkey.acl.secretName` (and `valkey.acl.secretKey`, default `users.acl`): the chart then starts Valkey with `--aclfile` on that Secret key, so the users survive a restart. The render refuses `enabled` with an empty name or key, and nothing refers to the Secret while `enabled` is false. The file defines every user: the `default` user line (`user default on nopass ~* &* +@all`, unchanged from today, which the probes and the other planes use) and `user auth-service-ratelimit on #<sha256 hex of the password> <the rule above>`, with a hashed password, never the plain one. The Secret is created outside the chart first, holds the service's `AUTH_VALKEY_URI` under its own key, and each pod reads only its own key. Limit: the default user keeps every key, so it can still read and write the `auth_svc_` counters; what bounds who can do that is the network policy, not the ACL, so the render refuses `valkey.acl.enabled` unless `networkPolicy.enabled` is true. That policy admits every pod of the release namespace, so it does not isolate other clients in the same namespace. The Secret file is mounted with mode 0444 (it holds hashes only). Enabling restarts the Valkey Deployment once. Rotation: a changed Secret does not roll any Deployment and Valkey keeps its loaded users; after you change the Secret, either restart the Valkey Deployment or run `ACL LOAD` on it once the mounted file shows the new content (the kubelet refreshes it within about a minute). A changed `AUTH_VALKEY_URI` needs a new auth-service pod, because it is an environment variable.
- **Limits.** Login: 20 per 15 minutes per client address, 10 per 15 minutes per client address and account. Compatibility refresh: 10 per 15 minutes per client address (go-api's `auth_refresh` budget; counted after the body is validated, so a refused body costs nothing). Reset password: 20 per hour per client address, 5 per hour per client address and link secret. Verify 10, resend 3, forgot 3, change password 5 per hour (as go-api); resend and forgot also 20 per hour per client address across all e-mails (go-api has none). Social link and unlink: 10 per hour per client address and signed-in principal. A body that fails validation (or an unlink id that does not parse) costs nothing; verify counts per client address only, never by a value in its query.
- **Keys.** The client address is the socket peer, or the one forwarded address a peer in `AUTH_SERVICE_TRUSTED_PROXIES` sent. A list entry is `/28` or longer (IPv6 `/124`) freely; an IPv4 `/24`..`/27`, such as the node pod range, needs `authService.trustedProxiesWideAccepted: true` and its node exclusions; anything wider, and any IPv6 network wider than `/124`, is refused at render. An entry `!<address>` excludes one address inside a listed network: it is never trusted. The node pod range holds the node's own addresses (the network address and the bridge at the next one), from which a host process or a hostNetwork pod reaches the API port past the go-api pod selector, so a wide entry renders only with both: `["10.42.0.0/24", "!10.42.0.0", "!10.42.0.1"]`. The service refuses to start on a `/0` network or any IPv4-mapped entry. go-api reads its own `TRUSTED_PROXIES` with the same parser and refuses to start on a list it cannot read (no entry is dropped); its start line logs `api: trusted proxies entries=N exclusions=M`. A literal `goApi.extraEnv` `TRUSTED_PROXIES` value is checked at render with the same rules, the ack band aside. The rule holds at every `authService.phase`, the proxy off included; the ack changes nothing for a list of narrow entries. The address list trusts every pod in a range as a proxy, so the auth-service NetworkPolicy (go-api pods alone on the API port) is the boundary. An IPv6 client is keyed by its /64. Accounts, link secrets and principals enter a key as an HMAC-SHA256 digest (128 bits), never as written. The HMAC key is derived by HKDF-SHA256 from `AUTH_JWT_SECRET_KEY` (info `dev-health/auth-service/ratelimit-key/v1`), so there is no Secret of its own; it is used for nothing else and is never logged or exported. **Rotating `AUTH_JWT_SECRET_KEY` changes every digest and so resets every bucket** (a rotation window grants each caller a fresh budget).
- **Refusal.** Past a limit the route answers `429` with the `rate_limited` envelope, which names no limit and no key. A store error answers `500` (closed) and counts in `dev_health_api_rate_limit_store_errors_total{limit}`; the series carries the limit id only.

### Session issue route

With the compatibility routes mounted, auth-service also serves `POST /internal/v1/sessions`. go-api calls it, while auth-service owns sessions (`AUTH_PROXY_SESSIONS`), for the sign-in flows go-api still serves (login, switch-org, accept-invite, change-password, social and SSO login).

- The caller presents `Authorization: Bearer svc_authsess_...`, an `auth-session-issuer` row of go-api's `internal_service_credentials` with scope `sessions:issue`. auth-service checks its SHA-256 there (`authschema.PublicReadColumns` grants the read). A missing, revoked, expired, other-service or other-scope credential gets 401.
- With `AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT` set (a workload principal id, `prn_` and 22 base64url characters; it needs `AUTH_TOKEN_ISSUER_URL`; default unset, which refuses every workload token), the route also accepts the go-api workload token: header `typ` `workload+jwt`, audience `auth-service`, `sub` equal to that value, signed by the auth-service key set. No table is read to decide it. Every other credential (a user or edge access token, `fcacr_`, `fcpush_`, `fcwlb_`, `whsec_`, `dhr1_`, another `svc_`) and every wrong audience, subject, signature or time is the same `401` envelope; a key set that cannot be read is `503`. The audit row names the workload principal as actor and `workload_jti`, never a `credential_id`.
- `svc_authsess_` stays accepted until its removal; `auth.session.issue_compat_uses{class="internal_svc_authsess_token"}` counts each accepted use.
- Session issue credential (go-api): with `AUTH_PROXY_SESSIONS=true`, `AUTH_SESSION_ISSUE_CREDENTIAL` selects what go-api's session issue call presents. Unset or `session_compat` (default) presents `AUTH_SESSION_ISSUE_TOKEN` as before, byte for byte. `workload` presents go-api's workload token (audience `auth-service`), asked of the workload client on every call, and reads no `AUTH_SESSION_ISSUE_TOKEN`; every other header of the issue call, the end user's client address included, is unchanged. Any other value is a start-up error naming the variable, a padded or whitespace-only value included (the value is compared as it is). The workload inputs are the ones of the provisioning credential below (`AUTH_WORKLOAD_SUBJECT_FILE` or `AUTH_WORKLOAD_BOOTSTRAP_FILE`, `AUTH_WORKLOAD_ISSUER`), and when both selectors choose `workload` one workload client and one exchange serve both. A missing or invalid input is a start-up error naming the variable; a failed exchange fails the sign-in as 503 with no request to the route and no fallback to `svc_authsess_`.
- The body is `principal_id`, `organization_id` (a UUID or `null`) and `amr`, and optionally `predecessor_session_id` (a lowercase UUID, the caller's own session); any other member, a missing one or a duplicate is `401 malformed`. The user must exist and be active, and, with an organization, be a member of it (403 otherwise). With `predecessor_session_id`, the issue supersedes that session (an organization switch, CHAOS-12085): under the predecessor's row lock auth-service revokes it and every refresh credential of it, creates the new session, and writes the `auth.session.issued` row and one `auth.session.revoked` row (`target_id` = the predecessor) in **one** transaction. A predecessor that is unknown, revoked or expired is `401 session_revoked` and nothing is written, so two concurrent switches from one session leave at most one live successor; a predecessor of another principal is `403 not_entitled`. No other session of the user is touched. go-api's switch-org sends the caller's own session (the `sid` its access token names, checked live) and answers `401` when the predecessor ended first; a token without `sid` sends no predecessor. The other sign-in flows never send one.
- The session is pinned to the user's `token_version` and created with one `auth.session.issued` audit row (`source` `go-api-delegated`, the `amr`, the service credential's id, the end user's client address and user agent) in one transaction. The answer is `{"access_token", "refresh_token", "token_type": "bearer", "expires_in"}`.
- Credential steps, at roll go:
  - mint: `dho service-credentials create --service auth-session-issuer --scope sessions:issue`; the token goes into go-api's `AUTH_SESSION_ISSUE_TOKEN` Secret key;
  - rotate: `dho service-credentials rotate <id> --overlap-seconds 3600`, update the Secret, and roll go-api inside the overlap;
  - revoke: `dho service-credentials revoke <id>`.

### Provisioning routes

With `AUTH_MEMBERSHIP_ROUTES=true`, auth-service serves the membership, invite and organization writes go-api makes while auth-service owns users: `POST /internal/v1/memberships/{add,role,remove,transfer}`, `POST /internal/v1/invites/{create,accept,revoke}` and `POST /internal/v1/organizations/{deprovision,reactivate,suspend}`. go-api is the policy enforcement point and calls them; the go-api proxy never forwards them.

- Each route has a row in `contracts/auth/v1/endpoint-profiles.auth-service.json` that lists every credential class the route admits, and the binary embeds that document. Every route admits the `svc_authsess_` credential. When the go-api workload binding is configured (`AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT`), the routes that take the go-api workload token (all but invite revoke and organization suspend) admit `workload_token` too, so their rows must list it. With `AUTH_MEMBERSHIP_ROUTES=true`, auth-service refuses to start when any of the ten routes has no such row. It exits 1, and the startup log line (`configure runtime dependencies`) gives `reason` `auth_provisioning_profile_row_missing`, the `route` (for example `POST /internal/v1/memberships/add`) and the `required_classes` its row must list (class names only). That start failure line is an Error line, so every log level shows it. `organizations/state` follows the same rule, but a missing or short row leaves only that route unmounted (it answers 404). That case is logged at Info only (`auth provisioning route not mounted`), so at `AUTH_SERVICE_LOG_LEVEL=warn` or `error` no line names it.
- The caller presents the go-api workload token on the same terms as the session issue route: header `typ` `workload+jwt`, audience `auth-service` and `sub` equal to `AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT` (unset refuses every workload token). The token has no action claim, so one go-api token covers both the session issue and these routes. Until it is retired, the `svc_authsess_` credential with scope `memberships:write` (no other service may hold it) is also accepted, and every such request counts in `auth.provisioning.compat_credential_uses{operation}`; a workload call is not counted. The class is read from the bearer text before any store or key-set read. `invites/revoke` and `organizations/suspend` take only the `svc_authsess_` credential: a workload token on them gets the same 401 before any read, and a route takes the workload token only once it is on the route list of that path. Any other bearer, a wrong audience or subject and a token the verifier refuses all get the same 401, and a key set that cannot be read is a 503.
- Every audit row a route writes names the human actor (`actor_principal_id`) and the calling credential: `caller_principal_id` and `workload_jti` for the workload token, `credential_id` for the `svc_authsess_` credential.
- The body is one JSON object of the route's members (at most 4 KiB): a duplicate, unknown or mistyped member, or a non-canonical id, is a 400 `malformed`. Each body names the acting account (`actor_principal_id`, or the joining account's `principal_id` for an accept). An invite's `description` and `request_metadata` go to go-api's `audit_logs` row and are never logged; neither is an invite token or an address.
- auth-service checks go-api's rule again, before anything is read for the request and again inside the write's transaction after its locks: the actor is an active account and a superuser, or an owner or admin member of the organization; a non-superuser may not change a superuser member; a deprovision needs a superuser. Refusals are 403 `principal_deactivated`, `admin_required` or `superuser_required`. An invite create or revoke locks the actor's `auth.principals` and `public.users` rows, and those of the invite's inviter, before that check (the membership writes' lock order), so a demotion or disable of the actor either commits first, and the write is refused, or waits for the write.
- An accept of an invite whose organization the auth schema does not hold yet (an invite go-api made) projects that organization from go-api's row first, as the other writes do before they start. It does this only for a token that names a pending, unexpired invite: an unusable token projects nothing. The token is checked at the first read and the projection commits on its own, so a later refusal (an expiry at the invite lock included) keeps the projected organization, which mirrors go-api's row. An organization go-api does not have either is 400 `invite_invalid`; a slug that another auth organization holds is 409 `organization_conflict`.
- A write's refusals are named `error.v1` reason codes (for example 409 `last_owner`, `organization_suspended`, `already_member`, 404 `membership_not_found`, 400 `invite_invalid` for every unusable token); a store failure is a 503.
- `organizations/deprovision` revokes every pending invite, removes every live membership (both tables each) and suspends the organization in one commit with one `auth.organization.deprovisioned` audit row; a repeat is a no-op. Afterwards the membership and invite writes of that organization refuse.
- `invites/revoke` ends one pending invite: the body is `organization_id`, `invite_id` and `actor_principal_id`. The actor rule (an active superuser, or an owner or admin member) runs before the invite is read, so a refused actor gets the same 403 for an existing and an unknown invite. The rule runs again under the revoke's locks (the actor's and the inviter's rows, then the organization row). The answers: 200 `{invite_id, status: "revoked"}`, also for a repeat, which writes no second audit row; 404 `invite_not_found`; 409 `invite_not_pending` for an accepted or expired invite (the expiry is checked against the clock read after the invite row lock). A revoke writes no go-api `audit_logs` row.
- `organizations/reactivate` has its row like the other routes. It ends an organization's suspension (a deprovision's included): the body is `{"organization_id", "actor_principal_id"}`, the actor must be an active superuser (read through the users seam, before anything else and again under the organization row lock), and the answer is `{"organization_id", "status": "active", "reactivated"}`. An organization that is not suspended answers 200 with `reactivated` false and writes nothing. A reactivation writes one `auth.organization.reactivated` audit row naming the actor, the `organization.reactivated` event and the revision, in one commit. It does not restore the memberships and invites a deprovision ended. A suspension in the auth schema ends only by this route: go-api's own `is_active` flows into the schema as a suspension, never as a reactivation.
- `organizations/state` has its row. It answers `{"organization_id", "status", "revision"}` for the organization named by the body `{"organization_id"}`, the status and the revision read in one statement; go-api's `is_active` PATCH sends that revision as `if_revision` to suspend and reactivate, and a stale one answers 409 `state_changed` and writes nothing. It takes the session issuer service credential or the go-api workload token (admitted as the other provisioning routes are), reads no actor and writes no audit row; an unknown organization is 404 `organization_not_found` and a store fault 503 `store_unavailable`.
- `organizations/suspend` mirrors reactivate: the body is `{"organization_id", "actor_principal_id"}`, the actor must be an active superuser (read through the users seam before anything else and again under the organization row lock), and the answer is `{"organization_id", "status": "suspended", "suspended"}`; an organization that is already suspended answers 200 with `suspended` false and writes nothing. It writes one `auth.organization.suspended` audit row naming the actor, the `organization.suspended` event and the revision in one commit. An actor with no `auth.principals` row, or any fault of the store, answers the fail-closed 503 `store_unavailable`.
- go-api calls the reactivate route from `PATCH /api/v1/admin/orgs/{org_id}` with `is_active=true`, and the suspend route with `is_active=false`, only when `AUTH_PROXY_ORG_PROVISIONING=true` (chart value `goApi.authProxy.orgProvisioning`, default off, refused unless `authService.extraEnv` sets `AUTH_MEMBERSHIP_ROUTES=true`; needs `AUTH_PROXY_URL` and the `svc_authsess_` credential, and go-api refuses to start with the switch on and no credential). With it on, go-api asks auth-service first, for every found organization (an organization already in that state answers a no-op), and writes `public.organizations` only on a 200; a refusal or an outage answers 503 and writes nothing. With it off the PATCH is unchanged, and auth-service takes a go-api suspension at the next refresh of the organization, so `orgs-reconcile` can list `org_active_diff` until then. With the switch on and the suspend route not mounted (no profile row) every `is_active=false` answers 503.
- With `AUTH_PROXY_ORG_PROVISIONING=true` go-api also compares every organization with `organizations/state` every 5 minutes, one read per organization per replica, and stores auth's state in `organizations.is_active` when the two differ, with one Info line per row (`org_id`, `from`, `to`). A difference is read again with the row locked; a row another transaction holds waits for the next pass. A row auth refuses, a 404 included, is left as it is and counted in one WARN per pass; a read failure ends the pass. With the switch off nothing runs. Before the switch is turned on, and again before it is turned back on after a rollback, `dho auth orgs-reconcile` must show `org_active_diff=0`: with the switch off a PATCH suspends in go-api's row only, and the first pass after the flip would clear that suspension to match auth.
- The same switch sends go-api's member, invite and delete writes to these routes (CHAOS-10988): `POST/PATCH/DELETE /api/v1/admin/orgs/{org_id}/members[/{user_id}]` and `.../transfer-ownership` to `memberships/{add,role,remove,transfer}`, `POST /api/v1/admin/orgs/{org_id}/invites` to `invites/create`, `POST /api/v1/auth/accept-invite` and `POST /api/v1/auth/onboard` with `join_org` to `invites/accept`, and `DELETE /api/v1/admin/orgs/{org_id}` (not a dry run) to `organizations/deprovision` after the ClickHouse preflight and before any PagerDuty revoke or purge. `POST /api/v1/admin/orgs` with `owner_user_id` writes the organization row alone, then sends the first owner to `memberships/add` with the calling superuser as the actor (CHAOS-12029); when that add does not land, go-api answers from the owner row if auth-service wrote it, else it deprovisions the organization and deletes its row with one statement that keeps it while any membership exists (an owner go-api does not hold is today's 500 before any write; after the cleanup an actor refusal is its 403 and any other refusal or an outage is 503, where the switch off would add the owner; a failed cleanup is 500 (no retry onto the half-written state) and the ERROR `admin: organization created without its owner`, see the flip runbook). go-api runs its own checks first as before, holds no transaction or row lock during the call and writes none of those rows (nor the `member_invited` or `member_joined` audit rows) itself; it answers from the row auth-service wrote. Each refusal go-api has today's answer for is that answer (for example 409 `last_owner` on a removal is today's 400 "Cannot remove the last owner of an organization"); any other refusal, or an outage, is 503 and nothing is written. A deprovision 404 (auth-service holds no such organization) lets the delete go on. The chart refuses the switch below `authService.phase` 4: the routes refuse until auth-service owns users. With the switch on, two rules are stricter than go-api alone (CHAOS-11800): an add or invite with a role outside `owner`, `admin`, `member`, `viewer` is 400 "Invalid role: <role>", and a role change away from the last owner is 400 "Cannot remove the last owner of an organization". `AUTH_USERS_OWNED` with the stage `owned` and the switch off leaves go-api's member writes in the public tables only; set the switch with the flip (runbook). With the switch on, the operator CLI (`dho admin`, which reads the same variable) refuses `users update` with `--org` or `--remove-from-org`, `orgs create` with `--owner-email` and a real `orgs delete` (a dry run still runs) with a message naming the switch, before any database write: done there, they would change go-api's rows only.
- Credential steps: production uses the workload token and never mints `memberships:write`; set `AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT` to go-api's workload principal. The `svc_authsess_` scope is a compatibility path for development stacks (`dho service-credentials create --service auth-session-issuer --scope sessions:issue --scope memberships:write`) and is retired once `auth.provisioning.compat_credential_uses` stays at 0.
- Provisioning credential (go-api): with `AUTH_PROXY_ORG_PROVISIONING=true`, `AUTH_PROVISIONING_CREDENTIAL` selects what go-api's member, invite, delete, reactivate and suspend calls present to auth-service. Unset or `session_compat` (default) presents `AUTH_SESSION_ISSUE_TOKEN` as before, byte for byte. `workload` presents go-api's workload token (audience `auth-service`) and reads no `AUTH_SESSION_ISSUE_TOKEN` for these calls. Any other value is a start-up error.

  | Environment | Rule |
  | --- | --- |
  | `AUTH_PROVISIONING_CREDENTIAL` | `workload` or `session_compat`; unset is `session_compat`. |
  | `AUTH_WORKLOAD_SUBJECT_FILE` | `workload` only: path of the projected ServiceAccount token file (Kubernetes). Set exactly one of this and `AUTH_WORKLOAD_BOOTSTRAP_FILE`. |
  | `AUTH_WORKLOAD_BOOTSTRAP_FILE` | `workload` only: path of the workload bootstrap credential file (Compose). |
  | `AUTH_WORKLOAD_ISSUER` | `workload` only, required: auth-service's token issuer URL (`AUTH_TOKEN_ISSUER_URL`, https); every workload token must carry it. |

  The exchange goes to the origin of `AUTH_PROXY_URL` (`POST /internal/v1/token/exchange`, which auth-service mounts only with `AUTH_WORKLOAD_EXCHANGE=true`); the subject file is read again for every exchange, and one exchange serves all the clients that selected `workload` (the provisioning clients and the session issue client). `workload` with a missing or invalid input is a start-up error naming it. A failed exchange fails that call (503) with no request to the provisioning routes; there is no fallback to `svc_authsess_`. The suspend route accepts the workload token only once its endpoint-profile row lists it.

## auth-migrate

| Flag | Environment | Rule |
| --- | --- | --- |
| | `AUTH_MIGRATION_DATABASE_URI` or `AUTH_MIGRATION_DATABASE_URI_FILE` | Required. The **migration** login's DSN. Must not authenticate as the runtime role. |
| | `AUTH_RUNTIME_DATABASE_ROLE` | Required to apply (not for `--check`). The login to grant the runtime privileges to. |
| | `AUTH_DATABASE_SCHEMA` | Default `auth`. |
| | `AUTH_SESSION_READER_ROLES` | Optional, comma-separated. The logins of go-api and query-api: each is granted exactly `SELECT (id, principal_id, revoked_at, legacy_token_version)` on the auth schema's `sessions` and `USAGE` on the schema, nothing else. With none, go-api and query-api cannot read the session, so every request with an access token auth-service issued fails (an error naming the missing grant, never a pass). Each role must exist. |
| `--check` | | Report the lineage position without issuing DDL. |
| `--version` | | Print build metadata. |

The run is bounded to five minutes. Each migration's lock wait is bounded to 5 s
(`lock_timeout`, set on that migration's transaction only), with 3 attempts and
a 5 s pause between them; when every attempt times out, the run fails and logs a
warning per attempt. See
[database, roles and migrations](../../contribute/architecture/auth-control-plane-database.md).

## Secret handling

- **Direct value versus `_FILE`.** For a DSN, set `KEY` **or** `KEY_FILE`; both
  is a configuration error, never a silent choice. A `_FILE` value must name a
  readable file with a non-empty value; one trailing newline is removed and no
  other whitespace is touched. A read failure names the key and a fixed error
  class, never the path or the content.
- **The signing key is a file only.** `AUTH_SIGNING_KEY_FILE` is the path itself
  (there is no `AUTH_SIGNING_KEY_FILE_FILE`). A direct-value **`AUTH_SIGNING_KEY`
  is rejected at load** (ACP-ADR-02 section 4), not ignored.
- **The key set.** `AUTH_SIGNING_KEYSET_FILE` names a JSON manifest,
  `{"keys": [{"kid": "auth-2026-10", "state": "active", "file": "auth-2026-10.pem"}]}`.
  `state` is one of `pending`, `active`, `retiring`, `retired`, `compromised`;
  exactly one key is `active` and signs; `pending`, `active` and `retiring` keys
  are published. `file` is a bare name in the manifest's own directory. The
  manifest is closed (an unknown member is refused) and follows the key file
  contract below (at most 16 KiB). It is re-read on every use, so replacing it
  by an atomic rename rotates the set with no restart; give a new key a new
  file. A kid the process has seen `compromised` never signs again in that
  process. The single key form is a one-key set whose key is `active`.
  The Helm chart renders the single key form by default. With
  `authService.keySet.enabled` it renders this manifest (kid, state and file
  only) into a ConfigMap, stages each named key from its own Secret at mode 0400,
  and passes `--signing-keyset-file`; `authService.keySet` replaces
  `authService.signingKey`, and `AUTH_SIGNING_KEYSET_FILE` stays refused in
  `authService.extraEnv`. The pod holds a static copy, so a change to the set is
  a new rollout, not a live rename.
- **The key file contract**, checked live on every readiness request: a regular
  file opened with `O_NOFOLLOW` (a symlink is refused), at most 8 KiB, mode with
  **no group or world bit** (`0600` or tighter), exactly one `PRIVATE KEY` PEM
  block that parses as an Ed25519 PKCS#8 key. Readiness also signs a probe with
  the active key and verifies it under the published public key. On Kubernetes this needs the key
  staging described in [deployment](../../contribute/architecture/auth-control-plane-deployment.md).
- **Redaction.** DSNs and their passwords are registered with the redactor;
  configuration errors that could quote a value pass through it before they are
  written. The startup log carries `database_configured` and
  `signing_key_configured` as booleans, never a path or a value.
- **DSN form.** Either a URI (`postgres://user:pass@host/db`, with reserved
  characters in the password percent-encoded) or the libpq keyword form
  (`host=... user=... password='...' dbname=...`), which the Compose profile uses.

## Verified against

- `internal/auth/authconfig/config.go`: constants (defaults and bounds), `Load`, `signingKey`, `validateKeyID`, `validateIdentifier`.
- `internal/auth/authconfig/options.go`: the option registry (flags, environment names, secret marker).
- `internal/platform/secrets/source.go`: `Resolve` (mutual exclusion, newline trim, error wording).
- `internal/auth/authruntime/runtime.go`: `--version`, `--healthcheck`.
- `cmd/auth-migrate/main.go`: environment names, `--check`, `runBudget`.
