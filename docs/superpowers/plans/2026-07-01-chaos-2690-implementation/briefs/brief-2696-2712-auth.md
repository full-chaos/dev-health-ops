# Implementation Brief: CHAOS-2696 + CHAOS-2712 — Source Registration, Ingest Tokens, Authz/Credential Lifecycle

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. **Table renames**: `ingest_sources` → **`external_ingest_sources`**, `ingest_tokens` →
>    **`external_ingest_tokens`** (feature-family consistency with external_ingest_batches/
>    rejections/payloads; avoids confusion with the legacy `/api/v1/ingest` router).
>    Constraint/index names follow (`uq_external_ingest_sources_org_system_instance`, etc.).
>    Model classes/file (`IngestSource`/`IngestToken` in `models/ingest_auth.py`) unchanged.
> 2. **Wave split** (avoids file collision with 2691): CHAOS-2696 = wave 1 — models,
>    migration **0032**, admin CRUD router/schemas, audit enum additions, authz doc. Token
>    helpers `generate_ingest_token`/`hash_ingest_token` move to `models/ingest_auth.py`
>    (NOT api/external_ingest/auth.py). CHAOS-2712 = wave 2 — replaces the body of 2691's
>    interim `api/external_ingest/auth.py` (resolve_ingest_token + require_ingest_scope,
>    last-used tracking, failure audit); signatures/call sites unchanged.
> 3. **Rate limiting moves to CHAOS-2691 (wave 1)**: `get_ingest_token_key` +
>    `INGEST_BATCH_LIMIT`/`INGEST_VALIDATE_LIMIT` land in `api/middleware/rate_limit.py`
>    with 2691's router (shared limiter singleton, per Decision 10's design). 2712 verifies
>    and owns the 401/403 audit path only.
> 4. **0032 gains three extra columns** on external_ingest_sources: reserved (CHAOS-2715
>    must-not-foreclose contract) `webhook_mode TEXT NOT NULL DEFAULT 'disabled'`
>    (`disabled|customer_relay|fullchaos_hosted`; API 400s on `fullchaos_hosted` in v1) and
>    `webhook_secret_id UUID NULL` (unused in v1); PLUS post-critique (CC5)
>    `matched_integration_source_id UUID NULL` — the managed `integration_sources` row id
>    resolved by per-provider matching at registration time (see header item 8-bis).
>    Admin schemas expose `webhook_mode`.
> 5. Auth context pinned epic-wide: `IngestAuthContext(org_id: str, scopes: frozenset[str],
>    token_id: str | None, source: IngestSource | None)`; the single dependency factory the
>    data-plane uses is `require_ingest_scope(scope)`.
> 6. `instance` grain = repo/project level (CC5): github/gitlab repo full name (`acme/api`),
>    jira project key, linear team key — UI copy + validation accordingly (plan-doc
>    `github.com/acme` examples are corrected in docs).
> 7. Data-plane auth errors use the `ExternalIngestError` envelope (CC16): 401
>    `invalid_token`, 403 `insufficient_scope`/`source_mismatch`/`source_disabled`/
>    `source_not_registered`/`source_owned_by_fullchaos_sync`.
> 8. decisionsNeeded resolved: legacy `/api/v1/ingest` = new follow-up issue (CC28,
>    pinned non-GA-blocking); snake_case admin vs camelCase data-plane RATIFIED (two
>    surfaces, two conventions).
> 8-bis. **POST-CRITIQUE (CC5/CC14): body Decision 8's warn-only stance is OVERRULED.**
>    Registration runs per-provider matching against `integration_sources` (instance
>    grain — NOT the provider-grain `integrations` table the body reasoned from):
>    github `external_id|full_name == instance`; gitlab `full_name|metadata_->>
>    'path_with_namespace'|external_id == instance` (external_id is the NUMERIC
>    project_id, sync/discovery.py:123-133); jira `external_id|full_name == instance`
>    (stores project_id|project_key|config-name, api/admin/routers/sync.py:775-820);
>    linear `external_id|full_name|name == instance` PLUS any enabled org-wide
>    placeholder row (`external_id == 'linear'` / `metadata_.org_wide_placeholder`)
>    owns ALL linear instances; custom never conflicts. If the matched managed source is
>    ENABLED → **409 `source_owned_by_fullchaos_sync`**; if matched-but-disabled →
>    register and persist the id in `matched_integration_source_id`. The provider-level
>    `Integration.is_active` check REMAINS a non-blocking warning (mixed fleets across
>    DIFFERENT instances stay allowed). Accept-time re-check (2695's ownership.py,
>    wave 4) is authoritative; 2696's wave-1 registration check implements the same CC5
>    matching inline and 2695 absorbs it without behavior change.
> 9. **POST-CRITIQUE (CC14): 2691's interim auth is mechanically gated** — it HARD-FAILS
>    `503 auth_not_configured` unless `EXTERNAL_INGEST_INSECURE_AUTH=1` (local
>    compose/test only). CHAOS-2712 deletes the flag and interim body entirely.

Epic: CHAOS-2690 External customer-push ingestion API
Owns: CHAOS-2696 (Customer source registration and ingest token scopes), CHAOS-2712 (Customer-push authorization
model and credential lifecycle)
Repo: `dev-health-ops` (`/Users/chris/projects/full-chaos/dev-health/ops`, worktree
`.claude/worktrees/chaos-2690-integration`, branch `chaos-2690-external-ingest`)
Plan docs (authoritative):
- `docs/superpowers/plans/2026-06-26-external-customer-push-ingestion-api.md`
- `docs/superpowers/plans/2026-06-28-customer-push-webhooks-and-setup-design.md`
- web: `docs/customer-push-ingestion-setup-design.md` (Screens 2/3/7 are the UI contract this brief's REST API must
  satisfy)

This brief covers **only** the authn/authz/credential-lifecycle boundary: source registration, ingest tokens,
the FastAPI auth dependency, rate limiting, admin CRUD surface, and audit logging. It does NOT cover
`POST /batches` business logic, the stream/worker, normalization, or the `dev-hops push` CLI (other sub-issues
of CHAOS-2690) — but it defines the exact interface (`IngestAuthContext`) those consumers depend on.

## Scope

1. **`IngestSource` model + registration** (Postgres, SQLAlchemy) — one row per `(org_id, system, instance)`
   tracking which of `fullchaos_sync | customer_push | disabled` currently owns that source instance, plus an
   `enabled` toggle. Satisfies CHAOS-2696's "reject unknown/disabled/mismatched sources".
2. **`IngestToken` model + lifecycle** — hashed bearer credential, scoped to org + (optionally) a single source,
   scopes `schema:read` / `ingest:write` / `ingest:status`, optional expiration, one-time plaintext display on
   creation, rotate, revoke, `last_used_at`/`last_used_ip` tracking.
3. **FastAPI auth dependency** (`resolve_ingest_token`) that turns `Authorization: Bearer <token>` into an
   `IngestAuthContext(org_id, source, scopes, token_id)`, independently sets the `org_id` contextvar (does NOT
   rely on `OrgIdMiddleware`, which only understands JWTs), and is the single place `401`/`403` semantics for
   CHAOS-2690's data-plane endpoints are decided.
4. **Per-token rate limiting**, reusing the existing app-wide `slowapi` `Limiter` singleton with a new
   token-derived key function.
5. **Admin REST CRUD** for sources + tokens under `/api/v1/admin/customer-push/*`, following the existing
   `api/admin/routers/*` convention exactly (this is the contract CHAOS-2714's web screens 2/3/7 consume).
6. **Audit logging** for token/source lifecycle events and ingest auth failures, using the existing
   `emit_audit_log` + `AuditAction`/`AuditResourceType` enums, respecting the commit-before-raise discipline.
7. **One-active-owner conflict semantics** for `fullchaos_sync` vs `customer_push`, including how this new
   registry interacts with the pre-existing `Integration` (managed-sync) Postgres table, which has no
   `instance`-level granularity today.

## Out of scope (explicitly deferred to sibling sub-issues)

- `POST /api/v1/external-ingest/batches`, `/validate`, `GET /batches/{id}`, `GET /schemas*` route *bodies* and
  the Redis/Valkey stream + worker + normalization pipeline (separate sub-issue; this brief only defines the
  `Depends(resolve_ingest_token)` / `Depends(require_scope(...))` contract those routes call).
- `dev-hops push` CLI.
- Web UI implementation (`/org/admin/integrations/[provider]/customer-push/...` screens) — this brief specifies
  the REST contract those screens call, not the React code.
- FullChaos-hosted webhook endpoint (Option B in the addendum doc) and source-scoped webhook secrets — v1 ships
  customer-owned relays only, which reuse this same ingest-token auth path with no new backend work.
- Provider-specific scopes (`ingest:github`, `ingest:gitlab`, `ingest:jira`, `ingest:linear`) — schema/model
  supports storing arbitrary scope strings (see Design Decision 14), but no endpoint enforces them in v1; treat
  as reserved/no-op.
- Reconciling/deprecating the legacy `/api/v1/ingest/*` router (global `INGEST_API_KEYS` env-var auth) — flagged
  as a cross-cutting decision the epic owner must make; this brief does not touch that router. Do not let the new
  `ingest_tokens` table or auth dependency be confused with it.
- GraphQL: confirmed by direct code search (`grep -rn "class Mutation"` returns nothing in
  `src/dev_health_ops/api/graphql/`) that this codebase's GraphQL schema is **query-only** — there is no
  precedent for GraphQL admin mutations anywhere, for any domain. Do not add GraphQL mutations for source/token
  management. All admin config surfaces (credentials, sync, settings, teams, users) are REST under
  `/api/v1/admin/*`; customer-push admin CRUD follows the same convention. This resolves the epic's "REST vs
  GraphQL for admin config" ambiguity definitively — no decision needed at the epic level.

## Design decisions (each with rationale)

1. **Two new Postgres tables, not extensions of `integrations`/`integration_credentials`.**
   `IntegrationCredential` (`models/settings.py`) is reversibly Fernet-encrypted for *outbound* provider
   credentials FullChaos must decrypt to call GitHub/GitLab/Jira APIs. An ingest token is an *inbound* bearer
   secret FullChaos only ever needs to compare, never recover — wrong shape for that table. `Integration`/
   `IntegrationSource` represent FullChaos-managed sync configuration at `(org_id, provider)` /
   `(org_id, provider, external_id)` granularity with no concept of a customer-controlled bearer credential.
   Building parallel `IngestSource`/`IngestToken` tables is cleaner than overloading either.

2. **Token hashing: `sha256(token).hexdigest()`, no per-token salt/pepper**, matching the exact convention already
   used four times in this codebase (`RefreshToken` via `api/services/refresh_tokens.py:_hash_token`,
   `PasswordResetToken`, `OrgInvite`, `EmailVerificationToken`). Do not invent a new scheme (e.g. bcrypt/argon2)
   — a high-entropy random secret (`secrets.token_urlsafe(32)` ⇒ 256 bits) makes a fast hash function safe for
   this purpose (it's the established house pattern, not a password).

3. **Token format: `fcpush_<43-char urlsafe-base64 secret>`.** The `fcpush_` prefix (a) lets support/logs/regex
   scanners identify a leaked ingest token by pattern (same reasoning as GitHub's `ghp_`/`github_pat_`
   prefixes), and (b) disambiguates from JWTs in the `Authorization: Bearer` header at a glance during
   debugging. Store the **first 12 characters** of the full token (including the `fcpush_` marker) in a
   plaintext `token_prefix` column for UI/audit display (Screen 7 "credential name / scopes / ... " table needs
   *some* human-recognizable identifier without ever re-displaying the secret) — never store or log the full
   plaintext token anywhere after the creation response.

4. **`IngestSource` is the single registry for all three ownership modes** (`fullchaos_sync`, `customer_push`,
   `disabled`), not just customer-push rows, despite CHAOS-2696's title. The plan's JSON example
   (`docs/superpowers/plans/2026-06-26-...md:278-287`) shows a `mode` field that must represent all three states
   for the one-active-owner XOR check to be enforceable against a single table. Table name: `ingest_sources`,
   model class `IngestSource` (not `CustomerPushSource`).

5. **`mode` and `system` are Python `str, Enum` types stored as `Text` columns, not native Postgres `ENUM`.**
   Every comparable model in this codebase (`Setting.category`, `Integration.provider`, `IntegrationProvider`)
   stores enum-like fields as `Text` validated at the Python/API layer, never a DB-level `ENUM` — this avoids
   `ALTER TYPE ... ADD VALUE` migrations when `ingest:github`-style provider scopes or new systems are added
   later, and is required for portability: `tests/conftest.py` runs the full unit suite against
   `sqlite:///:memory:` (`DATABASE_URI` override), and SQLite has no native enum/array support. `scopes` on
   `IngestToken` is `JSON` (`list[str]`), same reasoning — mirrors `Integration.config: Mapped[dict] = JSON`.

6. **`org_id` is `Text`, not `GUID`, on both new tables, with no FK to `organizations`.** This matches the
   dominant convention among the "ops config" model family this feature belongs to — `Integration`,
   `IntegrationCredential`, `Setting`, `ProviderRateLimitObservation` all use bare `Text` `org_id` with no FK
   (`recon-persistence-migrations.md` flags this as an existing repo-wide inconsistency vs. the "identity layer"
   models — `RefreshToken`, `AuditLog` — which use `GUID` FK to `organizations.id`). Pick the config-layer
   convention deliberately since `IngestSource`/`IngestToken` are configuration, not session/identity state.

7. **`IngestToken.source_id` is nullable; NULL means "all sources in this org" and is only legal when the
   token's scopes are a subset of `{schema:read, ingest:status}` (never `ingest:write`).** The web design doc's
   Screen 3 shows a single-select "source binding" field per token (implying 1 token → 1 source is the common
   case), but CHAOS-2712's acceptance criteria says "Token for one source cannot write to another source
   *unless explicitly granted*" and the setup doc's Screen 7 lists an org-wide "source binding" state alongside
   per-source ones — so a pure 1:1 model is too restrictive for e.g. an org-wide dashboard-status token. Enforce
   the write-requires-binding rule at **creation time** in the admin endpoint (400 if `ingest:write` in scopes
   and `source_id` is null) rather than modeling a many-to-many join table — keeps the schema simple and matches
   the UI's single-select field for the common (write) case.

8. **[OVERRULED post-critique — see header item 8-bis; registration DOES hard-409 against an ENABLED
   instance-grain `integration_sources` match. The warn-only design below survives ONLY for the
   provider-grain `Integration` check.]** ~~One-active-owner enforcement is scoped to
   `(org_id, system, instance)` inside `ingest_sources` only — it does NOT hard-block against the legacy
   `Integration` table.~~ `Integration` has no `instance` field (it's one
   row per `org_id + provider`, not per repo/workspace instance), so a strict block ("no active `Integration` row
   for this provider anywhere in the org") would prevent an org that already syncs `github.com/acme/repo-a` via
   managed sync from ever customer-pushing `github.com/acme/repo-b`, which is a real, expected use case (mixed
   fleets are explicitly allowed — only *the same instance* under both modes is banned). Decision: creating/
   enabling an `ingest_sources` row with `mode=customer_push` triggers a **non-blocking warning** in the API
   response (`warnings: ["managed sync is also configured for provider 'github' in this org — verify this is a
   different repository/workspace"]`) by checking `Integration.org_id==X AND Integration.provider==system AND
   Integration.is_active==True`, but does not 409. The hard XOR (409) applies only within `ingest_sources` itself
   — you cannot have two enabled rows for the same `(org_id, system, instance)` with different modes (enforced
   by the `UniqueConstraint` below: one row per identity, `mode` is mutated via `PATCH`, never a second insert).
   Document this explicitly in `docs/architecture/customer-push-authz.md` (see Files section) since it's a
   deliberate narrowing of the plan's stricter-sounding wording.

9. **Auth dependency does NOT reuse `OrgIdMiddleware`/`get_current_user`.** Verified directly:
   `OrgIdMiddleware.__call__` (`api/middleware/__init__.py`) calls `get_authenticated_user_from_headers`, which
   tries to JWT-decode the `Authorization` header; for a `fcpush_...` bearer token this safely returns `None`
   (confirmed `AuthService.authenticate_access_token` returns `None`, does not raise, on any invalid/undecodable
   token) and the middleware takes its "anonymous request, pass through" branch — it does **not** set the
   `org_id` contextvar and does **not** reject the request. So `resolve_ingest_token` must independently call
   `set_current_org_id(token.org_id)` itself (and reset it in a `finally`, mirroring the middleware's own
   token/reset pattern) so downstream ClickHouse-scoped code (used later in the pipeline) sees the right org.

10. **Rate-limit key function hashes the raw token directly from the header — it does not depend on
    `request.state` set by another dependency.** This exactly mirrors the existing precedent
    `get_validate_key` in `api/middleware/rate_limit.py` (used for `POST /auth/validate`, which also
    rate-limits by a digest of the submitted token instead of IP, with the same comment: "never the raw token,
    which must not appear in limiter storage or error messages"). New function `get_ingest_token_key(request)`:
    extract the `Authorization: Bearer` value (no DB call — the limiter must run cheaply even for a request
    that will ultimately fail auth), `sha256(token)[:16]` prefixed `ingest-token:`, else fall back to
    `get_forwarded_ip(request)` (so unauthenticated flooding is still IP-limited, matching `get_admin_user_key`'s
    fallback shape). Register on the same shared `limiter` singleton already installed as `app.state.limiter`
    with `SlowAPIMiddleware` (no new Limiter instance, no new middleware registration) — apply via
    `@limiter.limit(INGEST_BATCH_LIMIT, key_func=get_ingest_token_key)` on `POST /batches`/`POST /validate` (the
    data-plane routes owned by the sibling sub-issue; this brief only needs to land the key function + rate
    constant so that work can consume it without re-deriving the pattern). Suggested limit:
    `INGEST_BATCH_LIMIT = "60/minute"` (generous for CI/cron producers, low enough to bound abuse of a leaked
    token) — treat as a starting value, not a hard requirement.

11. **`last_used_at`/`last_used_ip` updates happen in their own short-lived Postgres session, decoupled from the
    request's main session.** If it shared the request's `Depends(get_postgres_session_dep)` session, a later
    `403`/`409` raised by the route handler would roll back the `last_used_at` bump too (the exact
    `get_postgres_session` commit/rollback-on-any-exception trap documented for CHAOS-2498) — but "was this token
    used, even for a rejected request" is exactly the audit signal CHAOS-2712 asks for ("Audit last-used
    timestamp and source usage"), so it must survive regardless of what the route handler does afterward. Fire
    an isolated `async with get_postgres_session() as s: ...; await s.commit()` immediately inside
    `resolve_ingest_token`, wrapped in `try/except Exception: logger.exception(...)` so a transient DB blip on
    this best-effort bookkeeping write never turns into a 500 for the actual ingest request.

12. **Commit-before-raise applies to every audit-log write followed by an HTTP error in this feature.**
    Concretely: the admin CRUD routes' `emit_audit_log(...)` calls, and (see Decision 13) the ingest auth
    dependency's failure-path audit log, must each be followed by an explicit `await db.commit()` on its own
    line before `raise HTTPException(...)`, exactly matching the confirmed live fix pattern in
    `api/auth/routers/login.py` (4+ call sites). Do not rely on `get_postgres_session`'s ambient
    commit-on-success path if the code raises afterward — it rolls back on *any* exception, including
    `HTTPException`.

13. **Audit scope: log token/source lifecycle events (create/rotate/revoke/register/enable/disable) always;
    log ingest-auth outcomes only on failure (401/403) and on the initial `POST /batches` accept — not on every
    `GET /batches/{id}` poll.** CHAOS-2712's acceptance criteria ("Audit logging captures credential identity,
    org, source, endpoint, and result") read literally would audit every request, but CI/CD producers poll
    `GET /batches/{id}` frequently (`--poll` in the CLI examples) and that would flood the audit log with
    low-value rows; the ingestion status itself (owned by the sibling sub-issue's `ingestion_batches` table) is
    the durable per-request record for successful reads. Security-relevant events (auth failures, credential
    lifecycle) go through `AuditLog`; operational status goes through the status store. Document this split
    explicitly — it's a real, defensible narrowing of a literal reading of the acceptance criteria, not an
    oversight.

14. **Extend `AuditAction`/`AuditResourceType` enums, don't invent a parallel audit mechanism.** Add to
    `models/audit.py`: `AuditAction.INGEST_TOKEN_CREATED`, `INGEST_TOKEN_ROTATED`, `INGEST_TOKEN_REVOKED`,
    `INGEST_SOURCE_REGISTERED`, `INGEST_SOURCE_MODE_CHANGED`, `INGEST_AUTH_FAILED`; add
    `AuditResourceType.INGEST_TOKEN`, `INGEST_SOURCE` (check exact existing `AuditResourceType` member names/casing
    before adding — read the full enum, not just the `AuditAction` excerpt inspected during recon, before writing
    code).

15. **Admin REST endpoints live under `/api/v1/admin/customer-push/*`**, a new router module
    `api/admin/routers/customer_push.py`, included in `api/admin/router.py`'s existing `router.include_router(...)`
    list. The parent `router = APIRouter(prefix="/api/v1/admin", ..., dependencies=[Depends(require_admin)])`
    already gates every admin route with `require_admin` (owner/admin/superuser) — no extra per-route admin
    dependency needed, exactly like `credentials_router`/`sync_router`/etc. Field naming: **snake_case**, matching
    every existing admin Pydantic schema (`api/admin/schemas/integrations.py` — no `alias_generator`/camelCase
    anywhere in the admin schema layer). This is a deliberate, real inconsistency vs. the *data-plane*
    `external-ingest` batch envelope (which the core plan writes in camelCase: `schemaVersion`, `idempotencyKey`)
    — the two surfaces are different namespaces built by different sub-issues; do not "fix" one to match the
    other without an explicit cross-team decision (flagged in `decisionsNeeded`).

16. **Rotation is a hard, immediate cutover — no grace-window/successor-token complexity.** `RefreshToken` has a
    `successor_jti` grace-window mechanism because session refresh races are a real concurrency problem for
    browser sessions. Ingest token rotation is an operator-triggered, infrequent action (rotate a CI secret) —
    CHAOS-2712's acceptance criteria explicitly permits "supported or explicitly tracked as follow-up" simplicity.
    `rotate` = single transaction: set `revoked_at=now()` on the old row, insert a new row with the same
    `org_id`/`source_id`/`scopes`/`expires_at`-*policy* (recompute `expires_at` from *now* + original TTL if the
    original had one, else null), return the new plaintext token once. Old token is invalid immediately after
    commit.

## API / DDL / schema sketches

### Alembic migration `0032_add_customer_push_ingest_auth.py` (`down_revision = "0031"`)

```python
"""Add ingest_sources and ingest_tokens for customer-push ingestion (CHAOS-2696/2712).

Why Postgres, not ClickHouse: this is transactional config/authz state (source
ownership + bearer-credential validity) consulted synchronously on every
external-ingest request, mirroring ProviderRateLimitObservation's justification
(migration 0031) — ClickHouse is a separate analytics cluster with no
transactional read path suitable for per-request auth checks.

Guarded individually (create-if-missing / add-column-if-missing) per the
0025/0031 convention so a partially-applied run can be re-run safely.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "0032"
down_revision = "0031"
branch_labels = None
depends_on = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on", "upgrade", "downgrade"]

_SOURCES_TABLE = "ingest_sources"
_TOKENS_TABLE = "ingest_tokens"


def upgrade() -> None:
    if not _table_exists(_SOURCES_TABLE):
        op.create_table(
            _SOURCES_TABLE,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("system", sa.Text(), nullable=False),
            sa.Column("instance", sa.Text(), nullable=False),
            sa.Column("display_name", sa.Text(), nullable=True),
            sa.Column("mode", sa.Text(), nullable=False, server_default="disabled"),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
            # post-critique CC5: managed-source match resolved at registration time
            sa.Column("matched_integration_source_id", UUID(as_uuid=True), nullable=True),
            sa.Column("created_by_user_id", UUID(as_uuid=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "org_id", "system", "instance", name="uq_ingest_sources_org_system_instance"
            ),
        )
    _create_index_if_missing("ix_ingest_sources_org_id", _SOURCES_TABLE, ["org_id"])

    if not _table_exists(_TOKENS_TABLE):
        op.create_table(
            _TOKENS_TABLE,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column(
                "source_id", UUID(as_uuid=True),
                sa.ForeignKey(f"{_SOURCES_TABLE}.id"), nullable=True,
            ),
            sa.Column("name", sa.Text(), nullable=False),
            sa.Column("token_hash", sa.Text(), nullable=False),
            sa.Column("token_prefix", sa.Text(), nullable=False),
            sa.Column("scopes", sa.JSON(), nullable=False),
            sa.Column("created_by_user_id", UUID(as_uuid=True), nullable=True),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_used_ip", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("token_hash", name="uq_ingest_tokens_token_hash"),
        )
    _create_index_if_missing("ix_ingest_tokens_org_id", _TOKENS_TABLE, ["org_id"])
    _create_index_if_missing("ix_ingest_tokens_source_id", _TOKENS_TABLE, ["source_id"])
    _create_index_if_missing(
        "ix_ingest_tokens_org_active", _TOKENS_TABLE, ["org_id", "revoked_at"]
    )


def downgrade() -> None:
    if _table_exists(_TOKENS_TABLE):
        op.drop_table(_TOKENS_TABLE)
    if _table_exists(_SOURCES_TABLE):
        op.drop_table(_SOURCES_TABLE)


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    return table_name in sa.inspect(bind).get_table_names()


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    bind = op.get_bind()
    existing = {ix["name"] for ix in sa.inspect(bind).get_indexes(table_name)}
    if index_name not in existing:
        op.create_index(index_name, table_name, columns)
```

### SQLAlchemy models

`src/dev_health_ops/models/ingest_auth.py` (new file):

```python
"""Customer-push source registration and ingest-token models (CHAOS-2696/2712).

See docs/architecture/customer-push-authz.md for the one-active-owner and
token-scoping design rationale.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class IngestSourceMode(str, Enum):
    FULLCHAOS_SYNC = "fullchaos_sync"
    CUSTOMER_PUSH = "customer_push"
    DISABLED = "disabled"


class IngestTokenScope(str, Enum):
    SCHEMA_READ = "schema:read"
    INGEST_WRITE = "ingest:write"
    INGEST_STATUS = "ingest:status"


class IngestSource(Base):
    __tablename__ = "ingest_sources"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    system: Mapped[str] = mapped_column(Text, nullable=False)
    instance: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    mode: Mapped[str] = mapped_column(Text, nullable=False, default=IngestSourceMode.DISABLED.value)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("org_id", "system", "instance", name="uq_ingest_sources_org_system_instance"),
    )

    def is_write_eligible(self) -> bool:
        return self.enabled and self.mode == IngestSourceMode.CUSTOMER_PUSH.value


class IngestToken(Base):
    __tablename__ = "ingest_tokens"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    source_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID, ForeignKey("ingest_sources.id"), nullable=True, index=True
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    token_hash: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    token_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    scopes: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_used_ip: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        Index("ix_ingest_tokens_org_active", "org_id", "revoked_at"),
    )

    def is_valid(self, now: datetime) -> bool:
        if self.revoked_at is not None:
            return False
        if self.expires_at is not None and now > self.expires_at:
            return False
        return True
```

### Auth dependency (`src/dev_health_ops/api/external_ingest/auth.py` — module path per the core plan's file list)

```python
"""Ingest-token authentication for /api/v1/external-ingest/*.

Independent of get_current_user / OrgIdMiddleware: those only understand
user JWTs. See docs/architecture/customer-push-authz.md.
"""
from __future__ import annotations

import hashlib
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.services.auth import set_current_org_id, _current_org_id
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.ingest_auth import IngestSource, IngestSourceMode, IngestToken

logger = logging.getLogger(__name__)

TOKEN_PREFIX = "fcpush_"


def generate_ingest_token() -> str:
    return f"{TOKEN_PREFIX}{secrets.token_urlsafe(32)}"


def hash_ingest_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class IngestAuthContext:
    token_id: str
    org_id: str
    scopes: frozenset[str]
    source: IngestSource | None  # None only for org-wide read-scoped tokens


def _extract_bearer(request: Request) -> str | None:
    header = request.headers.get("authorization")
    if not header or not header.lower().startswith("bearer "):
        return None
    return header[7:].strip() or None


async def _bump_last_used(token_id, ip: str | None) -> None:
    """Best-effort, isolated from the request's main session (Decision 11)."""
    try:
        async with get_postgres_session() as s:
            token = await s.get(IngestToken, token_id)
            if token is not None:
                token.last_used_at = datetime.now(timezone.utc)
                token.last_used_ip = ip
            await s.commit()
    except Exception:
        logger.exception("Failed to record ingest token last_used_at (non-fatal)")


async def resolve_ingest_token(
    request: Request,
    db: AsyncSession = Depends(get_postgres_session_dep),
) -> IngestAuthContext:
    raw = _extract_bearer(request)
    if raw is None or not raw.startswith(TOKEN_PREFIX):
        raise HTTPException(status_code=401, detail="Missing or invalid ingest token")

    token_hash = hash_ingest_token(raw)
    result = await db.execute(select(IngestToken).where(IngestToken.token_hash == token_hash))
    token = result.scalar_one_or_none()
    now = datetime.now(timezone.utc)
    if token is None or not token.is_valid(now):
        raise HTTPException(status_code=401, detail="Missing or invalid ingest token")

    source: IngestSource | None = None
    if token.source_id is not None:
        source = await db.get(IngestSource, token.source_id)

    ctx = IngestAuthContext(
        token_id=str(token.id),
        org_id=token.org_id,
        scopes=frozenset(token.scopes),
        source=source,
    )
    set_current_org_id(ctx.org_id)  # see Decision 9 — OrgIdMiddleware doesn't do this for us

    client_ip = request.client.host if request.client else None
    request.state.ingest_token_id = ctx.token_id  # for diagnostics only, NOT the rate-limit key (Decision 10)
    import asyncio

    asyncio.create_task(_bump_last_used(token.id, client_ip))

    return ctx


def require_scope(scope: str):
    async def _dep(ctx: IngestAuthContext = Depends(resolve_ingest_token)) -> IngestAuthContext:
        if scope not in ctx.scopes:
            raise HTTPException(status_code=403, detail=f"Missing required scope: {scope}")
        return ctx

    return _dep


async def require_matching_source(
    system: str,
    instance: str,
    ctx: IngestAuthContext,
) -> IngestSource:
    """Call from POST /batches and /validate after parsing the envelope body.

    Enforces: payload source must match the token's bound source (CHAOS-2696),
    disabled/wrong-mode source -> 403, cross-org access impossible (org_id is
    baked into ctx from the token, never taken from the request body).
    """
    source = ctx.source
    if source is None or source.system != system or source.instance != instance:
        raise HTTPException(status_code=403, detail="Payload source does not match registered source for this token")
    if not source.is_write_eligible():
        raise HTTPException(status_code=403, detail="Source is disabled or not in customer_push mode")
    return source
```

Note: `_current_org_id` reset (mirroring `OrgIdMiddleware`'s `finally: _current_org_id.reset(token)`) should be
handled by whichever request-scoped middleware/dependency wraps the whole external-ingest router — recommend a
small ASGI-level reset in the router's own dependency chain or a `finally` in the route handler; do not leave the
contextvar set across requests in a shared worker process. Get this reviewed against how `OrgIdMiddleware` itself
resets, since two different code paths (JWT via middleware, ingest-token via this dependency) both write the same
contextvar and must not stomp on the reset semantics.

### Rate limiting (`src/dev_health_ops/api/middleware/rate_limit.py` additions)

```python
INGEST_BATCH_LIMIT = "60/minute"


def get_ingest_token_key(request: Request) -> str:
    """Per-token rate-limit key for /api/v1/external-ingest/*, mirroring get_validate_key."""
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
        if token:
            digest = hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
            return f"ingest-token:{digest}"
    return f"ingest-ip:{get_forwarded_ip(request)}"
```

### Admin REST endpoints (`src/dev_health_ops/api/admin/routers/customer_push.py`, new)

All under `/api/v1/admin/customer-push` (parent admin router already applies `Depends(require_admin)` + is
already org-scoped via `get_admin_org_id`). snake_case JSON (Decision 15).

```
POST   /api/v1/admin/customer-push/sources
GET    /api/v1/admin/customer-push/sources
GET    /api/v1/admin/customer-push/sources/{source_id}
PATCH  /api/v1/admin/customer-push/sources/{source_id}      # mode, enabled, display_name
GET    /api/v1/admin/customer-push/sources/{source_id}/tokens
POST   /api/v1/admin/customer-push/sources/{source_id}/tokens
GET    /api/v1/admin/customer-push/tokens                    # org-wide list, Screen 7
POST   /api/v1/admin/customer-push/tokens/{token_id}/rotate
POST   /api/v1/admin/customer-push/tokens/{token_id}/revoke
```

Pydantic sketches (`src/dev_health_ops/api/admin/schemas/customer_push.py`, new, re-exported from
`schemas/__init__.py` matching the `integrations.py` pattern):

```python
class IngestSourceCreate(BaseModel):
    system: str = Field(..., min_length=1)            # github|gitlab|jira|linear|custom (validated, not DB enum)
    instance: str = Field(..., min_length=1)
    display_name: str | None = None
    mode: str = "customer_push"

class IngestSourceResponse(BaseModel):
    id: str
    org_id: str
    system: str
    instance: str
    display_name: str | None
    mode: str
    enabled: bool
    created_at: datetime
    updated_at: datetime
    warnings: list[str] = []   # Decision 8 non-blocking managed-sync-conflict warning
    model_config = ConfigDict(from_attributes=True)

class IngestSourcePatch(BaseModel):
    display_name: str | None = None
    mode: str | None = None
    enabled: bool | None = None

class IngestTokenCreate(BaseModel):
    name: str = Field(..., min_length=1)
    scopes: list[str]                        # subset of {schema:read, ingest:write, ingest:status}
    expires_at: datetime | None = None
    # source_id taken from the path (.../sources/{source_id}/tokens) for the bound case;
    # POST /api/v1/admin/customer-push/tokens (org-wide) omits it -> null, only if
    # scopes has no ingest:write (Decision 7, enforced 400 otherwise)

class IngestTokenCreateResponse(BaseModel):
    id: str
    name: str
    token: str          # PLAINTEXT — present only in this one response, never again
    token_prefix: str
    scopes: list[str]
    source_id: str | None
    expires_at: datetime | None
    created_at: datetime

class IngestTokenResponse(BaseModel):   # list/detail views — no `token` field
    id: str
    org_id: str
    source_id: str | None
    name: str
    token_prefix: str
    scopes: list[str]
    expires_at: datetime | None
    revoked_at: datetime | None
    last_used_at: datetime | None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)
```

401/403 contract for the data-plane routes (owned by the sibling sub-issue, but must match this brief exactly):

| Condition | Status |
|---|---|
| Missing/malformed `Authorization` header | 401 |
| Token hash not found, revoked, or expired | 401 |
| Token valid, missing required scope | 403 |
| Token valid, `source_id` set, payload `source.system`/`source.instance` mismatch | 403 |
| Source resolved but `enabled=false` or `mode != customer_push` | 403 |
| Token org_id ≠ resolved source org_id (should be structurally impossible via FK, defense-in-depth only) | 403 |

## Files to create/modify

- `src/dev_health_ops/models/ingest_auth.py` — new, `IngestSource`/`IngestToken`/enums (sketch above).
- `src/dev_health_ops/alembic/versions/0032_add_customer_push_ingest_auth.py` — new (sketch above).
- `src/dev_health_ops/api/external_ingest/__init__.py`, `auth.py` — new (per plan's module list); `auth.py`
  sketch above is the `resolve_ingest_token`/`require_scope`/`require_matching_source` contract.
- `src/dev_health_ops/api/middleware/rate_limit.py` — add `INGEST_BATCH_LIMIT`, `get_ingest_token_key` (do not
  create a second `Limiter` instance).
- `src/dev_health_ops/api/admin/routers/customer_push.py` — new, admin CRUD router (sketch above).
- `src/dev_health_ops/api/admin/schemas/customer_push.py` — new, Pydantic schemas (sketch above).
- `src/dev_health_ops/api/admin/schemas/__init__.py` — re-export new schema module (mirror how
  `schemas/integrations.py` is currently NOT flat-re-exported — check whether it needs its own explicit
  `from .customer_push import *` line or is picked up via the flat-module `*` re-export; verify at
  implementation time, don't assume).
- `src/dev_health_ops/api/admin/router.py` — add `customer_push_router` to the `router.include_router(...)`
  list and `.routers` import in `.routers/__init__.py`.
- `src/dev_health_ops/api/admin/routers/__init__.py` — export `customer_push_router` alongside the existing
  routers (mirror `credentials_router` etc.).
- `src/dev_health_ops/models/audit.py` — add new `AuditAction`/`AuditResourceType` members (Decision 14). Read
  the full enum bodies first (only a partial excerpt was reviewed during recon) to avoid name collisions/casing
  drift.
- `docs/architecture/customer-push-authz.md` — new, documenting Decisions 4, 8, 9, 13, 16 in the same changeset
  (house rule: "document decisions in ops/docs/architecture ... same changeset").
- `tests/api/external_ingest/__init__.py`, `test_auth.py`, `test_admin_customer_push.py` — new (see Test plan).
- `tests/test_ingest_auth_admin.py` or similar — decide a single home; follow existing `tests/api/webhooks/`
  subpackage layout (`tests/api/external_ingest/` mirroring `tests/api/webhooks/`) rather than the flatter
  `tests/test_ingest_*.py` naming used by the *legacy* `/api/v1/ingest` module, to avoid the two systems'
  test files being visually confused in `tests/` listings.

Not modified (verify unaffected, do not touch): `src/dev_health_ops/api/ingest/*` (legacy router — explicitly
out of scope), `src/dev_health_ops/api/webhooks/*`, `src/dev_health_ops/api/middleware/__init__.py`
(`OrgIdMiddleware` itself — read-only dependency for this work, not modified).

## Test plan

Unit (no live DB required beyond the sqlite-backed `tests/conftest.py` default, `POSTGRES_URI` fixture DB, or
mocked sessions — follow `tests/api/webhooks/test_auth.py`'s direct-dependency-function-call style):

- Token hashing round-trip: `generate_ingest_token()` → `hash_ingest_token()` → DB lookup match.
- `resolve_ingest_token`: missing header → 401; malformed prefix → 401; unknown hash → 401; revoked → 401;
  expired → 401; valid+unscoped source → context returned with `source=None`.
- `require_scope`: present scope passes; missing scope → 403.
- `require_matching_source`: system/instance match + `enabled+customer_push` → passes; system/instance mismatch
  → 403; `mode=disabled` → 403; `mode=fullchaos_sync` (source exists but never released to customer_push) → 403.
- `is_write_eligible()` / `is_valid()` model methods: table-driven boolean matrix (revoked×expired×mode×enabled).
- `get_ingest_token_key`: returns `ingest-token:<digest>` for a valid bearer header, falls back to
  `ingest-ip:<ip>` when missing, and never leaks the raw token in the returned string (assert digest length /
  no substring match against the input token).
- Admin router: `IngestSourceCreate` duplicate `(org_id, system, instance)` → 409 (unique constraint violation
  surfaced as a clean 409, not a raw IntegrityError 500 — add explicit try/except around the insert).
  `IngestTokenCreate` with `ingest:write` scope and no bound `source_id` (org-wide POST) → 400. Token creation
  response includes plaintext `token` exactly once; the subsequent `GET` list/detail never includes it (assert
  field absence, not just a placeholder value). Rotate: old token hash invalid immediately after, new token
  returned once. Revoke: `revoked_at` set, subsequent `resolve_ingest_token` with the old raw token → 401.
- One-active-owner warning (Decision 8): create `Integration(provider="github", is_active=True)` fixture row,
  then `POST .../sources` with `system="github", mode="customer_push"` for a *different* instance → 201 with
  non-empty `warnings`, not a 409.
- Audit: creating/rotating/revoking a token or registering/mode-changing a source produces exactly one
  `AuditLog` row with the correct `AuditAction`/`AuditResourceType`/`org_id`; a rejected ingest auth attempt
  (401/403) that emits an audit log survives even though the request itself raises (regression test for the
  commit-before-raise trap — assert the `AuditLog` row exists in a *fresh* session/query after the request,
  proving it wasn't rolled back with the rest of the failed request's session).
- Rate limiting: with a real `Limiter` (not the `_NoOpLimiter`, set `ENVIRONMENT=production`/unset in the test to
  force `slowapi` path — check how existing rate-limit tests force the real limiter, likely
  `tests/api/*rate_limit*` or `tests/test_admin_password_reset_rate_limit.py`-style, follow that precedent) two
  different tokens hitting the same route do not share a bucket; the same token across two different source
  paths does share a bucket.

Live-DB (`@pytest.mark.clickhouse` is NOT needed here — this feature is pure Postgres; if any test needs a real
Postgres beyond sqlite's coverage — e.g. asserting the actual `UniqueConstraint`/FK DDL fires correctly, which
SQLite enforces more loosely — mark it `@pytest.mark.postgres` if that marker exists in this repo, else run it
under the standard Postgres-backed integration test fixture used elsewhere for alembic-migration-dependent
tests; verify the exact marker/fixture convention in `tests/conftest.py`/`pytest.ini` before writing, don't
assume `clickhouse` is the only opt-in marker).

## Gate commands

ops (from `/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration`):

```bash
# Full local gate (ruff format/check, mypy, full unit tier, isolated live-CH argMax proof).
# This feature adds no ClickHouse tables, so SKIP_CLICKHOUSE=1 is safe and faster while iterating;
# run the full (non-skip) version at least once before declaring done, since it's the literal CI parity gate.
bash ci/local_validate.sh
SKIP_CLICKHOUSE=1 bash ci/local_validate.sh   # faster inner loop

# mypy standalone (same invocation as typecheck.yml / local_validate.sh step 3)
mypy --install-types --non-interactive .

# Targeted test run while iterating
pytest tests/api/external_ingest/ tests/api/admin/ -x -q
```

Per house rule "run the LITERAL CI commands locally pre-push" — do not substitute a partial `pytest -k
customer_push` run for the final gate; `ci/local_validate.sh` runs the byte-for-byte CI unit tier.

No `web` gates apply to this brief's scope (no web files touched) — CHAOS-2714's owner runs `ci/run_tests.sh
format/quality/unit` + targeted Playwright e2e against the REST contract defined here once it's merged.

## Live verification procedure (against the dev compose stack)

Postgres/API only — no ClickHouse, no worker, no stream involved in this sub-issue's scope.

```bash
# 1. Apply the new migration against the real dev Postgres (compose stack already running).
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration
.venv/bin/dev-hops migrate postgres upgrade   # or: POSTGRES_URI=... .venv/bin/dev-hops migrate postgres upgrade
.venv/bin/dev-hops migrate postgres current   # confirm head == 0032

# 2. Register a source (as an org admin JWT — reuse an existing dev login flow / admin token)
curl -sS -X POST "$API_URL/api/v1/admin/customer-push/sources" \
  -H "Authorization: Bearer $ADMIN_JWT" -H "X-Org-Id: $ORG_ID" -H "Content-Type: application/json" \
  -d '{"system":"github","instance":"github.com/acme","mode":"customer_push"}'
# expect 201, mode=customer_push, enabled=true

# 3. Create a token bound to that source
curl -sS -X POST "$API_URL/api/v1/admin/customer-push/sources/$SOURCE_ID/tokens" \
  -H "Authorization: Bearer $ADMIN_JWT" -H "X-Org-Id: $ORG_ID" -H "Content-Type: application/json" \
  -d '{"name":"ci-runner","scopes":["ingest:write","ingest:status"]}'
# expect 201, response includes plaintext "token": "fcpush_..." — capture it, this is the ONLY time it's shown

# 4. Confirm the plaintext is never re-displayed
curl -sS "$API_URL/api/v1/admin/customer-push/tokens" -H "Authorization: Bearer $ADMIN_JWT" -H "X-Org-Id: $ORG_ID" | jq
# expect token_prefix only, no "token" field anywhere

# 5. Exercise the auth dependency directly against whatever stub/health route currently depends on it
#    (once the sibling sub-issue's /api/v1/external-ingest router exists — until then, a throwaway
#    Depends(resolve_ingest_token) route in a scratch script is the fastest live-verify path)
curl -sS -o /dev/null -w '%{http_code}\n' "$API_URL/api/v1/external-ingest/schemas" \
  -H "Authorization: Bearer $CAPTURED_TOKEN"          # expect 200 (schema:read not granted above -> expect 403 instead, verify against actual granted scopes)
curl -sS -o /dev/null -w '%{http_code}\n' "$API_URL/api/v1/external-ingest/schemas"
# no Authorization header -> expect 401

# 6. Revoke and confirm 401 afterward
curl -sS -X POST "$API_URL/api/v1/admin/customer-push/tokens/$TOKEN_ID/revoke" \
  -H "Authorization: Bearer $ADMIN_JWT" -H "X-Org-Id: $ORG_ID"
curl -sS -o /dev/null -w '%{http_code}\n' "$API_URL/api/v1/external-ingest/schemas" \
  -H "Authorization: Bearer $CAPTURED_TOKEN"          # expect 401

# 7. Inspect the audit trail directly
docker exec dev-health-postgres-1 psql -U devhealth -d devhealth -c \
  "select action, resource_type, resource_id, status, created_at from audit_logs where org_id = '$ORG_ID' order by created_at desc limit 10;"
```

Never run this against the shared dev `default` Postgres database's production data path outside a
disposable/dev org — this feature writes real rows (sources, tokens, audit logs) with no soft-delete-only
guarantees beyond `revoked_at`/`enabled` flags.

## Dependencies on other sub-issues

- **Depends on nothing upstream within CHAOS-2690** — source registration + tokens are foundational; every other
  sub-issue (`POST /batches` route, worker, `dev-hops push` CLI, web screens) depends on the
  `IngestAuthContext`/`require_scope`/`require_matching_source` contract this brief defines.
- **CHAOS-2696's sibling data-plane sub-issue** (the one implementing `POST /batches`/`/validate`/`GET
  /batches/{id}`/`GET /schemas*`) must import `resolve_ingest_token`/`require_scope`/`require_matching_source`
  from `api/external_ingest/auth.py` rather than re-deriving auth — flag this brief's file as the canonical
  source the moment it lands.
- **CHAOS-2714** (web setup screens) consumes the `/api/v1/admin/customer-push/*` REST contract defined here
  verbatim for Screens 2/3/7; hand this brief's "Admin REST endpoints" section to that implementer directly to
  avoid them re-deriving field names/casing.
- **CHAOS-2715** (webhook-assisted ingestion) reuses this same ingest-token auth path unchanged for the
  customer-owned-relay v1 mode (Option A) — no new auth work for that sub-issue; only Option B
  (FullChaos-hosted webhook) would need source-scoped *webhook* secrets, explicitly deferred (see addendum
  doc's "Required backend design details" section), out of this brief's scope.
- **Legacy `/api/v1/ingest/*` reconciliation decision** (flagged by 3 of the 8 recon briefs as unaddressed by
  either plan doc) is a prerequisite the epic owner should resolve before or in parallel with this work, but
  does not block it — the new `ingest_tokens`/`ingest_sources` tables and `/api/v1/external-ingest` prefix are
  namespaced distinctly enough to coexist regardless of that decision's outcome.

## Risks

- **Decision 8 (non-blocking managed-sync-conflict warning instead of a hard block)** is a deliberate narrowing
  of the plan's stricter-sounding prose ("Do not allow FullChaos sync and customer push to both own the same
  source_system + source_instance"). If the epic owner intended provider-level (not instance-level) exclusivity,
  this is wrong and should be caught in review — flagged explicitly rather than silently assumed.
- **`asyncio.create_task` for the best-effort `last_used_at` bump** (Decision 11) creates an unawaited background
  task per ingest request; in a test/short-lived-event-loop context (e.g., `httpx.ASGITransport` in tests) this
  can produce "Task was destroyed but it is pending" warnings or flaky assertions if a test doesn't wait for it.
  Mitigate by exposing a way to await it in tests (e.g., return the task or make the bump synchronous behind an
  `AWAIT_LAST_USED_UPDATE` test-only flag) — call this out to the implementer, don't let it become an
  intermittent CI flake once written.
- **snake_case admin API vs camelCase data-plane API (Decision 15)** is a real inconsistency a reviewer may want
  "fixed" to match; changing either breaks an established convention on one side or the other. Escalated to
  `decisionsNeeded` below rather than silently picked, even though this brief's opinionated default is to leave
  both as-is.
- **`OrgIdMiddleware` still runs on every `/api/v1/external-ingest/*` request** even though it can't authenticate
  ingest tokens; it does one wasted `authenticate_access_token` DB round-trip (JWT decode failure) per ingest
  request before falling through to "anonymous, pass through." Low cost, but worth noting as a minor
  perf/architecture wart rather than something silently invisible — not worth fixing in this sub-issue (would
  require middleware-order changes flagged as fragile in `api/_middleware.py`'s own comments).
- **The `IngestSource`/`IngestToken` schema was designed without a concrete sighting of the sibling `POST
  /batches` sub-issue's actual `IngestionBatch`/status-store schema** (neither exists yet on this branch per
  `recon-persistence-migrations.md`). If that sub-issue's status table also wants a `source_id` FK, confirm it
  points at `ingest_sources.id` (this brief's table), not a re-derived duplicate concept.

## decisionsNeeded (escalate to epic owner / synthesizer, not resolved unilaterally here)

1. Fate of the legacy `/api/v1/ingest/*` router (global `INGEST_API_KEYS` auth) — deprecate, coexist
   indefinitely, or migrate its callers onto the new `ingest_tokens` model? Affects whether this brief's auth
   model should also become that router's auth model eventually.
2. Confirm the one-active-owner enforcement granularity (Decision 8: instance-level hard block + provider-level
   soft warning) matches product intent, vs. a stricter provider-level hard block the plan's prose could also
   support.
3. Confirm snake_case admin API vs camelCase data-plane API (Decision 15) is acceptable, or whether the
   sibling data-plane sub-issue should be asked to switch its envelope to snake_case for consistency (higher
   blast radius change, affects the plan doc's own examples and the `dev-hops push` CLI's JSON parsing).
