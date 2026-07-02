# Customer-Push Source Registration & Ingest Token Authz (CHAOS-2696/2712)

This document covers the authn/authz/credential-lifecycle boundary for the
external customer-push ingestion API (epic CHAOS-2690): source registration,
ingest tokens, and how ownership conflicts with FullChaos-managed sync are
resolved. It does not cover the `POST /batches` business logic, the
stream/worker, or normalization -- see
`docs/superpowers/plans/2026-06-26-external-customer-push-ingestion-api.md`
and the epic's master implementation spec for those.

## Tables

Two new Postgres tables, both org-scoped, not extensions of `integrations`/
`integration_credentials`:

- `external_ingest_sources` (model `IngestSource`, `models/ingest_auth.py`) --
  one row per `(org_id, system, instance)`. `instance` is repo/project grain:
  GitHub/GitLab repo full name (`acme/api`), Jira project key, Linear team
  key, or a stable slug for `custom`.
- `external_ingest_tokens` (model `IngestToken`) -- hashed bearer credentials
  scoped to an org and optionally a single source.

The `external_ingest_*` naming matches the sibling `external_ingest_batches`/
`external_ingest_rejections`/`external_ingest_payloads` tables (CHAOS-2694)
and deliberately avoids the legacy `/api/v1/ingest` router's vocabulary --
the two systems are unrelated and must never be confused in `\dt` output or
test-file listings.

`IntegrationCredential` is reversibly Fernet-encrypted for *outbound*
provider credentials FullChaos must decrypt to call GitHub/GitLab/Jira APIs.
An ingest token is an *inbound* bearer secret FullChaos only ever compares,
never recovers -- the wrong shape for that table, hence a parallel model.

## Single registry for all three ownership modes

`IngestSource.mode` is one of `fullchaos_sync | customer_push | disabled` --
`IngestSource` is the single registry for all three states, not just
customer-push rows, despite this issue's title. This is what makes the
one-active-owner XOR check enforceable: two enabled rows can never exist for
the same `(org_id, system, instance)` (enforced by a `UniqueConstraint`);
`mode` is *mutated* via `PATCH`, never a second insert.

## One-active-owner: per-provider ownership matching (CC5)

Registration-time conflict detection runs when `mode=customer_push`. It
checks the *instance-grain* managed catalog (`integration_sources`), **not**
the *provider-grain* `Integration` table (which has no `instance` column --
one row per `org_id + provider`).

Per-provider matching, org- and provider-scoped. The provider comparison
itself is case-insensitive (`lower(IntegrationSource.provider) == system` /
`lower(Integration.provider) == system`): neither `IntegrationCreate` nor the
sync-config creation paths enforce lowercase provider values, so a
mixed-case managed row (`"GitHub"`) must still be found by a lowercase
`system="github"` customer-push registration -- a bare `==` would silently
let it through:

| system | match condition |
|---|---|
| github | `external_id == instance` OR `full_name == instance` |
| gitlab | `full_name == instance` OR `metadata_->>'path_with_namespace' == instance` OR `external_id == instance` (external_id is GitLab's *numeric* project_id, not a slug) |
| jira | `external_id == instance` OR `full_name == instance` |
| linear | `external_id == instance` OR `full_name == instance` OR `name == instance`; **plus** any enabled Linear `IntegrationSource` with `metadata_.org_wide_placeholder == true` (or `external_id == 'linear'`) owns **all** Linear instances for the org |
| custom | never conflicts -- no managed equivalent |

Matching is case- and whitespace-sensitive against the stored `instance`, so
the admin API trims the submitted `instance` before both matching and
persistence (and 422s on a blank/whitespace-only value) -- an un-trimmed
`"acme/api "` must not create a distinct `(org_id, system, instance)` row
that silently bypasses the 409 against the trimmed managed-source value.

Outcome:

- Matched row is **actively owned** -- i.e. `IntegrationSource.is_enabled`
  AND its parent `Integration.is_active` are both true -- ->
  `409 source_owned_by_fullchaos_sync`. Registration is rejected outright.
  Both flags are required: a source row an operator never explicitly
  disabled but whose parent `Integration` has since been deactivated no
  longer counts as active ownership (nothing in this codebase cascades
  `Integration.is_active=false` down to its `IntegrationSource.is_enabled`
  rows, so the two must be checked independently, not just the source flag).
- Matched row exists but is **not actively owned** (source disabled, or its
  integration inactive) -> registration succeeds; the match's id is
  persisted in `external_ingest_sources.matched_integration_source_id` so a
  later accept-time check (owned by CHAOS-2695's `ownership.py`) can detect
  if that managed source becomes actively owned again without a second
  registration lookup.
- No match -> registration succeeds, `matched_integration_source_id` is `NULL`.

This check re-runs on `PATCH .../sources/{id}` whenever the patch results in
the source becoming write-eligible (`mode=customer_push AND enabled=true`) --
"creating/enabling" a customer-push row is the trigger, not just the initial
`POST`. The admin-time check is best-effort UX; the **authoritative** guard is
the accept-time re-check on every `POST /batches` (CHAOS-2695), which also
catches a managed source created or re-enabled *after* registration.

### Residual narrowing this overrules

An earlier draft of this feature (Design Decision 8 in the original brief)
proposed a **warn-only** stance for all managed-sync conflicts -- reasoning
that `Integration` has no `instance` grain, so a hard block against "any
active `Integration` row for this provider" would wrongly prevent mixed
fleets (e.g. `github.com/acme/repo-a` on managed sync, `repo-b` on
customer-push). That reasoning is still correct for the **provider-level**
check, which stays a **non-blocking warning** (see below) -- but a
post-critique review found the *instance-level* check itself was
under-verified against three of four providers' actual `external_id`
semantics (GitLab's is a numeric project_id, not a full-name slug; Jira and
Linear fall back through `sync_options`/team-UUID paths that a bare
`external_id ==` comparison misses entirely). With the corrected per-provider
matching above, instance-level ownership can be resolved precisely, so the
hard 409 is restored for that case. **This document's stated behavior (hard
409 on an enabled instance-grain match) is authoritative; do not revert to
warn-only.**

## Provider-level non-blocking warning (surviving half of Decision 8)

Independent of the instance-level check, registering/enabling a
customer-push source also checks whether *any* enabled `Integration` row
exists for the same `(org_id, provider)` -- regardless of instance. If one
does, the response includes a `warnings: [...]` entry (not a 409):

> "Managed sync is also configured for provider '{system}' in this
> organization -- verify this is a different repository/workspace."

This stays non-blocking because `Integration` has no `instance` grain --
treating "managed sync exists anywhere for this provider" as a hard block
would forbid the legitimate mixed-fleet case above.

## Auth dependency does not reuse `OrgIdMiddleware`/`get_current_user`

`OrgIdMiddleware` only understands user JWTs; for an `fcpush_...` bearer
token, `AuthService.authenticate_access_token` safely returns `None` and the
middleware takes its anonymous pass-through branch -- it does not set the
`org_id` contextvar. The ingest-token auth dependency (`resolve_ingest_token`,
owned by CHAOS-2712, wave 2) must independently call
`set_current_org_id(token.org_id)` and reset it in a `finally`, mirroring the
middleware's own token/reset pattern, so downstream org-scoped code sees the
right org. This module (`models/ingest_auth.py`) only defines the
token-hashing primitives (`generate_ingest_token`/`hash_ingest_token`); the
dependency itself lands in `api/external_ingest/auth.py` in a later wave.

## Audit scope: lifecycle always, ingest-auth outcomes only on failure

Token/source lifecycle events (create/rotate/revoke/register/enable/disable)
are audited unconditionally via `emit_audit_log` + the `AuditAction`/
`AuditResourceType` enums (`INGEST_TOKEN_CREATED`/`_ROTATED`/`_REVOKED`,
`INGEST_SOURCE_REGISTERED`/`_MODE_CHANGED`, resource types `INGEST_TOKEN`/
`INGEST_SOURCE`). Ingest-auth *outcomes* (the data-plane dependency, wave 2+)
are audited only on failure (401/403) plus the initial `POST /batches`
accept -- not on every `GET /batches/{id}` poll, since CI/CD producers poll
frequently and the ingestion status table is already the durable per-request
record for successful reads. `AuditAction.INGEST_AUTH_FAILED` is reserved
here for that later wave.

**Commit-before-raise.** Every `emit_audit_log(...)` call in the admin CRUD
router is followed by an explicit `await session.commit()` on its own line.
`get_postgres_session`'s ambient commit only fires on a clean return --  it
rolls back on *any* exception, including a deliberately-raised
`HTTPException`, which would silently discard the just-added `AuditLog` row.
This mirrors the confirmed pattern in `api/auth/routers/login.py`.

## Token format, hashing, and lifecycle

- Format: `fcpush_<43-char urlsafe-base64 secret>` (`secrets.token_urlsafe(32)`,
  256 bits of entropy). The `fcpush_` prefix lets support/log/regex scanners
  identify a leaked token (same reasoning as GitHub's `ghp_`/`github_pat_`)
  and disambiguates from JWTs in the `Authorization: Bearer` header at a
  glance.
- Hash: `sha256(token).hexdigest()`, no per-token salt/pepper -- matching the
  house convention (`RefreshToken`, `PasswordResetToken`, `OrgInvite`,
  `EmailVerificationToken`). A high-entropy random secret makes a fast hash
  safe here; this is not a password.
- Display: the first 12 characters of the full token (including the
  `fcpush_` marker) are stored in a plaintext `token_prefix` column for
  UI/audit display. The full plaintext is returned exactly once, in the
  create/rotate response body, and never stored or logged again.
- Scopes: `schema:read`, `ingest:write`, `ingest:status`. `source_id` is
  nullable on `IngestToken` -- `NULL` means "all sources in this org" and is
  only legal when scopes are a subset of `{schema:read, ingest:status}`
  (never `ingest:write`); enforced as a `400` at creation time in the admin
  endpoint (`POST /customer-push/tokens`, the org-wide/unbound path) rather
  than a many-to-many join table.
- Rotation is a **hard, immediate cutover**, not a grace-window/
  successor-token scheme (`RefreshToken`'s `successor_jti` exists to smooth
  browser session races, which don't apply to an operator-triggered,
  infrequent CI-secret rotation). `rotate` = one transaction: set
  `revoked_at=now()` on the old row, insert a new row with the same
  `org_id`/`source_id`/`scopes`, recompute `expires_at` from *now* + the
  original TTL if the original had one (else `NULL`), return the new
  plaintext token once. The old token is invalid immediately after commit.
  Rotate (and revoke) fetch the target row with `SELECT ... FOR UPDATE`
  (matching `RefreshToken`'s own rotation locking), so two concurrent rotate
  requests for the same token can't both observe `revoked_at IS NULL` and
  each mint a live successor -- the second waits for the first's commit and
  then correctly 400s on "already revoked".

## Reserved columns (must-not-foreclose CHAOS-2715)

`external_ingest_sources.webhook_mode` (`disabled | customer_relay |
fullchaos_hosted`, default `disabled`) and `webhook_secret_id` (nullable
UUID) are added in migration `0032` now, ahead of CHAOS-2715's
webhook-assisted ingestion work, so that feature doesn't need a follow-up
migration to add columns to a table this feature already owns. v1 only
accepts `disabled`/`customer_relay` at the admin API layer -- `fullchaos_hosted`
400s (`webhook_secret_id` is unused until Option B webhook support lands).

## Admin REST surface

`/api/v1/admin/customer-push/*` (`api/admin/routers/customer_push.py`),
included in the existing parent admin router
(`APIRouter(prefix="/api/v1/admin", dependencies=[Depends(require_admin)])`)
-- gated by `require_admin` (owner/admin/superuser) like every other admin
router, no extra per-route dependency needed. snake_case field naming,
matching every other admin Pydantic schema module -- a deliberate
inconsistency versus the data-plane `external-ingest` batch envelope (which
is camelCase); the two are different namespaces built by different
sub-issues and are not reconciled.

```
POST   /api/v1/admin/customer-push/sources
GET    /api/v1/admin/customer-push/sources
GET    /api/v1/admin/customer-push/sources/{source_id}
PATCH  /api/v1/admin/customer-push/sources/{source_id}
GET    /api/v1/admin/customer-push/sources/{source_id}/tokens
POST   /api/v1/admin/customer-push/sources/{source_id}/tokens
GET    /api/v1/admin/customer-push/tokens
POST   /api/v1/admin/customer-push/tokens                    # org-wide, unbound (no ingest:write)
POST   /api/v1/admin/customer-push/tokens/{token_id}/rotate
POST   /api/v1/admin/customer-push/tokens/{token_id}/revoke
```
