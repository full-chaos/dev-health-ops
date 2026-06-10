# GitHub App Marketplace + Frictionless Install

Status: **Planning**
Scope: `dev-health-ops` (backend) + `dev-health-web` (frontend)
Linear: CHAOS (parent + per-phase sub-issues)

## Goal

Let users connect GitHub by **installing the Dev Health GitHub App in one click**
(and, eventually, installing it from the GitHub Marketplace) instead of manually
wiring `GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY_PATH`, and
`GITHUB_APP_INSTALLATION_ID` (see [GitHub App auth](../user-guide/github-app-auth.md)).

Two distinct outcomes:

1. **Frictionless install** — the actual "easier setup": user clicks **Install**,
   picks repos on GitHub, lands back connected. No env vars, no PEM juggling.
2. **Marketplace listing** — makes the App discoverable and installable from the
   GitHub Marketplace, with plan/entitlement webhooks. Depends on (1).

## Current state (verified by code audit)

The hard capability already exists. The missing pieces are glue + one gap fix.

| Capability | Status | Location |
| --- | --- | --- |
| Mint installation tokens (RS256 JWT -> install token) | Exists | `connectors/utils/github_app.py` (`create_github_app_jwt`, `GitHubAppTokenProvider`) |
| `GitHubCredentials` supports App auth (`app_id`/`private_key`/`installation_id`) | Exists | `credentials/types.py` (`is_app_auth`) |
| Encrypted per-org credential storage | Exists | `integration_credentials` table, `models/settings.py` (`IntegrationCredential`) |
| Credential resolution (CLI > env > DB) | Exists | `processors/sync.py` (`_resolve_github_sync_credentials`), `credentials/resolver.py` |
| Inbound webhook router w/ `X-Hub-Signature-256` verify | Exists | `api/webhooks/router.py`, `api/webhooks/auth.py` |
| Stripe webhook (entitlement pattern to mirror) | Exists | `api/billing/router.py` |
| Admin REST: credentials CRUD + test | Exists | `api/admin/routers/credentials.py` |
| Admin REST: sync-configs CRUD + trigger/backfill | Exists | `api/admin/routers/sync.py` |
| Sync config model (parent/child, targets, options) | Exists | `models/settings.py` (`SyncConfiguration`) |
| Tenancy: Organization / User / Membership | Exists | `models/users.py` |
| Web integrations UI (PAT form today) | Exists | `web/src/app/(app)/admin/integrations/[provider]/` |
| Web -> backend proxy (injects `Authorization` + `X-Org-Id`) | Exists | `web/src/proxy.ts` |
| Web GitHub *social login* (OAuth, NextAuth v5) | Exists (different concern) | `web/src/lib/auth.ts` |

## Gaps

1. **Background sync ignores App auth (bug, prerequisite).**
   `workers/sync_runtime.py` (~L508) and `discovery/repos.py` extract only the
   `"token"` key and raise `ValueError("Missing GitHub token ...")` when absent.
   App-auth sync configs therefore **fail in the Celery worker today**.
2. **No installation persistence.** No `installation_id -> org` mapping table and
   no install callback. App auth today requires an admin to manually paste
   `app_id` + PEM + `installation_id` into an `IntegrationCredential`.
3. **No install / Marketplace webhook events.** The webhook router handles
   push-style provider events, not `installation` or `marketplace_purchase`.
4. **Web has no "Connect GitHub App" path** — only the PAT form and OAuth social
   login (note: `web/docs/github-app-auth.md` documents social login, which is a
   different mechanism from the ops installation-token sync auth).

## Plan

### Phase 0 — Fix worker App-auth gap (prerequisite, ops only)

Without this, none of the later phases produce working syncs.

- Make `workers/sync_runtime.py` and `discovery/repos.py` resolve a full
  `GitHubCredentials` (PAT **or** App) instead of grabbing the `"token"` key.
  Reuse `GitHubCredentials.is_app_auth` and the existing resolver.
- Add a worker test that runs a sync with an App-auth sync config.

Touches: `src/dev_health_ops/workers/sync_runtime.py`,
`src/dev_health_ops/discovery/repos.py`, `tests/`.

### Phase 1 — Frictionless install capture (ops + web)

The actual "easier setup". Depends on Phase 0.

**ops**

- New table `github_app_installations`: `installation_id`, `account_login`,
  `account_type`, `org_id` (FK), `suspended_at`, timestamps. New Alembic
  migration under `src/dev_health_ops/alembic/versions/`.
- Extend `api/webhooks/router.py` to handle the `installation` event
  (`created` / `deleted` / `suspend` / `unsuspend`), verified with the existing
  `X-Hub-Signature-256` dependency.
- New endpoint `POST /api/v1/admin/integrations/github/install-callback` that
  maps `installation_id` -> org (via signed `state`) and writes an
  `IntegrationCredential` in App mode. The app-level private key (PEM) comes from
  a single server secret/env, **not** from the end user.

**web**

- "Connect GitHub" button -> `https://github.com/apps/<app-slug>/installations/new?state=<signed-org-token>`.
- New route handler `web/src/app/(app)/.../github-app/callback/route.ts` that
  receives `installation_id` + `setup_action` and forwards to the backend through
  the existing `src/proxy.ts` passthrough.
- Surface App-install as an option in `admin/integrations/[provider]` alongside
  the existing PAT form.

Result: user clicks Install -> selects repos on GitHub -> returns connected.

### Phase 2 — Marketplace listing (ops + GitHub.com config) — VERIFY FIRST

> The reference pass on current Marketplace-listing prerequisites was incomplete.
> Run a focused GitHub-docs pass and update this section before committing work.

- **ops:** extend the webhook router for `marketplace_purchase`
  (`purchased` / `changed` / `cancelled` / `pending_change`) and map plan ->
  org entitlement, reusing the existing Stripe/billing entitlement patterns.
- **GitHub.com (non-code):** make the App public, complete publisher
  verification, write listing copy, define pricing plans, add screenshots.

Open items to verify: free vs paid plan implications, publisher verification
requirements, whether the listing requires the App to be public, and the exact
current `marketplace_purchase` payload fields.

### Phase 3 (optional) — Self-hosted App Manifest helper

For OSS / self-hosted operators: use the GitHub App Manifest flow
(`POST /app-manifests/{code}/conversions`, which returns `id`, `pem`,
`webhook_secret`, `client_id`/`client_secret`) so they one-click **create** their
own App rather than following the manual setup doc. Independent of Phases 1-2.

## Dependencies

```
Phase 0 (worker fix) --+--> Phase 1 (install capture) --> Phase 2 (marketplace)
                       |
                       +--> (also unblocks any App-auth sync)

Phase 3 (manifest) -- independent
```

## References

- Manual setup (today): [`user-guide/github-app-auth.md`](../user-guide/github-app-auth.md)
- Token minting: `src/dev_health_ops/connectors/utils/github_app.py`
- Credential model: `src/dev_health_ops/credentials/types.py`
- Webhook router: `src/dev_health_ops/api/webhooks/router.py`
- Stripe entitlement pattern: `src/dev_health_ops/api/billing/router.py`
