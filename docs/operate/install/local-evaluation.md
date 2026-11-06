---
page_id: op-local
summary: Evaluate the platform locally with repository Compose artifacts and non-production data.
content_type: tutorial
owner: platform-operations
source_of_truth:
  - compose.yml
applicability: current
lifecycle: active
---

# Evaluate locally

1. Check out a reviewed repository revision.
2. Use the repository Compose artifact and example configuration as a starting point.
3. Supply non-production credentials and isolated data stores.
4. Start the API, workers, queue, and required stores.
5. Verify `/health`, logs, worker readiness, and one bounded source synchronization.
6. Remove local credentials and data when evaluation ends.

A local evaluation is not a production hardening, backup, availability, or scale test.

## Sessions and the local token issuer

The local stack signs users in as production does (CHAOS-13107). A plain `docker compose up` starts
auth-service and its one-shot jobs, because go-api waits for them:

- go-api forwards the auth aggregates production forwards to auth-service: sessions, local login,
  credentials (sign-up, e-mail verification, password reset) and social sign-in
  (`AUTH_PROXY_SESSIONS`, `AUTH_PROXY_LOCAL_LOGIN`, `AUTH_PROXY_CREDENTIALS` and `AUTH_PROXY_SOCIAL`
  are `true`). OIDC and SAML sign-in stay on go-api. Sessions without local login is refused: it picks
  the earliest-join organization at login and records the gateway IP.
- auth-service owns users (`AUTH_USERS_OWNED=true`, production's phase 4) and mints the EdDSA access
  token (`AUTH_ACCESS_TOKEN_CLASS=eddsa`, audience `dev-health-api`). go-api does not mint an HS256
  session itself.
- auth-service sends the verify and reset mail. The local default is the console provider
  (`AUTH_MAIL_ALLOW_CONSOLE=true`), which delivers no mail. Set `EMAIL_PROVIDER` and its keys in
  `.env` for real mail.
- go-api asks auth-service for each session with the `session_compat` service credential
  `AUTH_SESSION_ISSUE_TOKEN`. compose.yml has a local default for it, and the
  `auth-session-issue-seed` job writes the credential's row.
- The token issuer is `https://auth.dev-health.internal:8443`. The `auth-issuer` service serves it,
  and it forwards only the discovery document and the JWKS of auth-service. go-api and query-api
  verify the token against this issuer. They trust its certificate through `SSL_CERT_DIR`.
- The `auth-issuer-tls-init` job makes the certificate (ECDSA P-256, self-signed, valid 365 days)
  into the volume `auth_issuer_tls`. It makes the pair one time. To get a new pair, remove that
  volume.

A host tool that verifies the token, such as the web dev server, must reach the issuer and trust the
local CA:

1. Add `127.0.0.1 auth.dev-health.internal` to `/etc/hosts`. `auth-issuer` publishes port 8443 on
   loopback only.
2. Copy the CA certificate out of the volume:
   `docker compose cp auth-issuer:/run/tls/ca/issuer.pem ./auth-issuer-ca.pem`.
3. Start the web dev server with `NODE_EXTRA_CA_CERTS=$PWD/auth-issuer-ca.pem`. Set
   `AUTH_ACCESS_TOKEN_CLASS=eddsa` and
   `AUTH_ACCESS_TOKEN_ISSUER=https://auth.dev-health.internal:8443` in its environment.

A browser does not call the issuer. To open the issuer URL in a browser, import
`auth-issuer-ca.pem` into the browser's trust store.

Before auth-service serves, the `auth-users-backfill` job copies `public.users` into the auth schema
(`dho auth users-backfill`, with `AUTH_USERS_OWNED=false` for that run only). Without the copy,
auth-service does not become ready (`users_owned`): a fresh database already holds one service user,
and a database from before this change can hold more. The job runs on every `docker compose up`.

Known limit: the local stack uses the `session_compat` credential (`svc_authsess_`), not the
workload credential of production. It stops working when `svc_authsess_` acceptance is removed
(W11-19). Before that removal, `bootstrap-create -if-absent` and a `_FILE` form of the session
issue workload subject are necessary.
