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

The local stack signs users in as production does. A plain `docker compose up` starts
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
- go-api asks auth-service for each session with its workload token, as in production
  (`AUTH_SESSION_ISSUE_CREDENTIAL=workload`). The `auth-workload-bootstrap` job writes go-api's
  credential file once (0600, a named volume go-api mounts read-only), and the
  `auth-workload-subject` job writes the principal id auth-service accepts to a second volume it
  mounts read-only (`AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT_FILE`). Both jobs run on every
  `docker compose up` and a second run changes nothing. No credential is in the environment, in
  `.env` or in a log.
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
(`dho auth users-backfill -insert-only`, with `AUTH_USERS_OWNED=false` for that run only). It creates
the auth users that are missing and never changes an existing one. Without the copy,
auth-service does not become ready (`users_owned`): a fresh database already holds one service user,
and an older database can hold more. The job runs on every `docker compose up`.

`docker compose down -v` removes both volumes and the registry with the database, so the next `up`
starts clean. If only the credential volume is lost, the registry still holds the live credential and
the job skips it: run `auth-migrate workloads bootstrap-disable -workload api -apply`, then `up` again.
