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
| (none, secret) | `AUTH_DATABASE_URI` or `AUTH_DATABASE_URI_FILE` | required | The **runtime** login's DSN. Exactly one of the two. |
| `--database-schema` | `AUTH_DATABASE_SCHEMA` | `auth` | Lowercase unquoted identifier `[a-z][a-z0-9_]*`, at most 63 characters. |
| `--database-max-conns` | `AUTH_DATABASE_MAX_CONNS` | `8` | 1 to 256, per replica. |
| `--database-connect-timeout` | `AUTH_DATABASE_CONNECT_TIMEOUT` | `5s` | 100ms to 1m. |
| `--signing-key-file` | `AUTH_SIGNING_KEY_FILE` | required (or the key set) | Path to the Ed25519 PKCS#8 PEM. |
| `--signing-key-id` | `AUTH_SIGNING_KEY_ID` | required (or the key set) | The JWKS `kid`: letters, digits, `-`, `_`, `.`, at most 64. |
| `--signing-keyset-file` | `AUTH_SIGNING_KEYSET_FILE` | unset | Path to a key-set manifest, in place of the two settings above (setting both forms fails startup). |
| (none, secret) | `AUTH_JWT_SECRET_KEY` or `AUTH_JWT_SECRET_KEY_FILE` | unset | HS256 key of the compatibility access token: the same value as go-api's `JWT_SECRET_KEY`, at least 32 characters. Set together with `AUTH_SESSION_SUCCESSOR_KEY`; with neither, no session route is mounted; with one, startup fails. |
| (none, secret) | `AUTH_SESSION_SUCCESSOR_KEY` or `AUTH_SESSION_SUCCESSOR_KEY_FILE` | unset | 32 bytes, base64url without padding. Derives the rotated refresh credential that a 30 s grace replay returns again. |
| `--jwt-issuer` | `AUTH_JWT_ISSUER` | `dev-health-ops` | `iss` of the access token; must equal go-api's `JWT_ISSUER`. |
| `--jwt-audience` | `AUTH_JWT_AUDIENCE` | `dev-health-api` | `aud` of the access token; must equal go-api's `JWT_AUDIENCE`. |
| `--social-link-routes` | `AUTH_SOCIAL_LINK_ROUTES` | `false` | `true` mounts the bearer-only social link and unlink routes (`POST /v1/identities/social`, `DELETE /v1/identities/social/{id}`); any other value than `true` or `false` fails startup, and `true` needs the session secrets. |
| `--sso-jit` | `AUTH_SSO_JIT` | `false` | `true` lets a federated sign-in join a known account to the provider's organization and create a new account, in one audit commit with the session; a provider row must allow it too (`auto_provision_users`), and it needs the users cutover. Any other value than `true` or `false` fails startup. |
| (none, secret) | `AUTH_SSO_STATE_KEY` or `AUTH_SSO_STATE_KEY_FILE` | unset | Key the OIDC and OAuth state is derived from. With `APP_BASE_URL` and `SETTINGS_ENCRYPTION_KEY` it mounts the OIDC and OAuth sign-in routes; SAML needs the last two only. |
| (none) | `APP_BASE_URL` | unset | go-api's public base URL; the callback and ACS URLs the identity providers hold are built from it. Absolute `http(s)` only. |
| (none, secret) | `SETTINGS_ENCRYPTION_KEY` or `SETTINGS_ENCRYPTION_KEY_FILE` | unset | go-api's provider-secret key (`SETTINGS_ENCRYPTION_SALT` sets its salt). |
| (none) | `AUTH_ACCESS_TOKEN_CLASS` | `hs256_compat` | The one class of access token this service mints: `hs256_compat` or `eddsa` (exact match; any other value fails startup). `eddsa` needs `AUTH_TOKEN_ISSUER_URL` (the `iss`, byte for byte), a signing key, and `AUTH_JWT_AUDIENCE` of `dev-health-api` or `auth-service`; tokens last 60 minutes and carry the access-token.v1 claims only. The session routes still verify HS256 tokens only. |
| `--version` | | | Prints build metadata as JSON. |
| `--healthcheck` | | | Probes this deployment's own operator `/readyz` over loopback; exit 0 only on HTTP 200. For container healthchecks. |

Durations take Go syntax (`30s`, `5m`).

### Session issue route

With the compatibility routes mounted, auth-service also serves `POST /internal/v1/sessions`. go-api calls it, while auth-service owns sessions (`AUTH_PROXY_SESSIONS`), for the sign-in flows go-api still serves (login, switch-org, accept-invite, change-password, social and SSO login).

- The caller presents `Authorization: Bearer svc_authsess_...`, an `auth-session-issuer` row of go-api's `internal_service_credentials` with scope `sessions:issue`. auth-service checks its SHA-256 there (`authschema.PublicReadColumns` grants the read). A missing, revoked, expired, other-service or other-scope credential gets 401.
- The body is exactly `{"principal_id", "organization_id" (a UUID or null), "amr"}`. The user must exist and be active, and, with an organization, be a member of it (403 otherwise).
- The session is pinned to the user's `token_version` and created with one `auth.session.issued` audit row (`source` `go-api-delegated`, the `amr`, the service credential's id, the end user's client address and user agent) in one transaction. The answer is `{"access_token", "refresh_token", "token_type": "bearer", "expires_in"}`.
- Credential steps, at roll go:
  - mint: `dho service-credentials create --service auth-session-issuer --scope sessions:issue`; the token goes into go-api's `AUTH_SESSION_ISSUE_TOKEN` Secret key;
  - rotate: `dho service-credentials rotate <id> --overlap-seconds 3600`, update the Secret, and roll go-api inside the overlap;
  - revoke: `dho service-credentials revoke <id>`.

## auth-migrate

| Flag | Environment | Rule |
| --- | --- | --- |
| | `AUTH_MIGRATION_DATABASE_URI` or `AUTH_MIGRATION_DATABASE_URI_FILE` | Required. The **migration** login's DSN. Must not authenticate as the runtime role. |
| | `AUTH_RUNTIME_DATABASE_ROLE` | Required to apply (not for `--check`). The login to grant the runtime privileges to. |
| | `AUTH_DATABASE_SCHEMA` | Default `auth`. |
| | `AUTH_SESSION_READER_ROLES` | Optional, comma-separated. The logins of go-api and query-api: each is granted exactly `SELECT (id, principal_id, revoked_at)` on the auth schema's `sessions` and `USAGE` on the schema, nothing else. With none, go-api and query-api cannot read the session, so every request with an access token auth-service issued fails (an error naming the missing grant, never a pass). Each role must exist. |
| `--check` | | Report the lineage position without issuing DDL. |
| `--version` | | Print build metadata. |

The run is bounded to five minutes. See
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
  The Helm chart stages one key and renders the single key form; it refuses
  `AUTH_SIGNING_KEYSET_FILE` in `authService.extraEnv`. Key-set staging in the
  chart comes with the rotation state machine (CHAOS-10449).
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
