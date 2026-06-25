# CHAOS-2622 — Rebuild Team (and Identity) Drift-Review on ClickHouse

> **Status:** Proposed (Oracle-reviewed). Not yet scheduled.
> **Repos:** `dev-health-ops` (primary), `dev-health-web` (wire change).
> **Related:** [`team-attribution.md`](../architecture/team-attribution.md) §0,
> [`chaos-2600-implementation-plan.md`](../architecture/chaos-2600-implementation-plan.md),
> [`database-architecture.md`](../architecture/database-architecture.md).

## 0. Framing (read this first)

CHAOS-2600 (CS5/CS6, CHAOS-2607) moved the **system of record for the team catalog and
identity→team membership** from Postgres to ClickHouse. That migration was meant to **move
management to ClickHouse**, **not** to remove the ability to maintain **manual mappings** —
manually curated teams/identities and manual attribution fallbacks for entities that providers
do not surface. Manual mapping remains a **first-class, preserved capability**.

What the migration *did* drop, as collateral, was the **drift-review reconciliation surface**: the
admin workflow that detects when provider discovery disagrees with the curated/manual config and
lets an admin approve or dismiss each change. It was built entirely on Postgres `TeamMapping`
columns (`flagged_changes` / `sync_policy` / `managed_fields` / `last_drift_sync_at`) +
`TeamDriftSyncService`, all deleted in CS6. The four admin endpoints that backed it are now HTTP
501 stubs.

**This plan rebuilds drift-review natively on ClickHouse**, covering both the **team** dimension
and the **identity/member manual-mapping** dimension, so manual config and provider discovery can
be reconciled instead of one silently clobbering the other.

## 1. Current broken surface

The web admin **Pending Changes** panel (`dev-health-web`
`src/components/admin/teams/PendingChangesPanel.tsx`) calls four ops endpoints on mount; all four
are HTTP 501 stubs in `src/dev_health_ops/api/admin/routers/teams.py` (the drift-review section,
~L332–L385). The panel therefore always toasts *"Failed to load pending changes: …"*.

| Endpoint | Purpose | Current |
| --- | --- | --- |
| `GET /admin/teams/pending-changes` | list flagged drift across teams | 501 |
| `POST /admin/teams/{team_id}/approve-changes` | apply flagged changes | 501 |
| `POST /admin/teams/{team_id}/dismiss-changes` | drop flagged changes | 501 |
| `POST /admin/teams/trigger-drift-sync` | re-discover providers + recompute drift | 501 |

## 2. What was deleted (and why it cannot be ported as-is)

The old engine (`TeamDriftSyncService`, recoverable from git `9365ed802~1`) ran on Postgres
`TeamMapping` columns with **no ClickHouse counterpart**:

- `flagged_changes` JSONB = `{"pending": [change, …]}` — the review lane.
- `sync_policy` Int: `0` auto-apply, `1` flag-for-review, `2` manual/none.
- `managed_fields` list: which fields are drift-tracked (`name`, `description`, `repo_patterns`,
  `project_keys`).
- `last_drift_sync_at`.

`run_drift_sync(provider, discovered_teams)` looked up each discovered team by
`extra_data.provider_team_id`, diffed managed fields, then either auto-applied (`policy 0`) or
appended `{change_type, field, old_value, new_value, discovered_at}` to `flagged_changes.pending`
(`policy 1`). Catalog teams missing from discovery → `provider_removed`; discovered teams with no
mapping → counted `new_available` (not auto-imported). `approve_changes` applied pending changes by
index and removed them; `dismiss_changes` removed them without applying.

The CH `teams` table (`002`/`025`/`051` migrations) is `ReplacingMergeTree(updated_at)` ORDER BY
`(id)` holding **one** set of curated values — there is no provider-observed-vs-curated split, no
review lane, and no per-team policy.

## 3. Core problem (Oracle-confirmed)

**Drift-review is incoherent unless the sync write-path stops clobbering admin-managed fields.**
Today the auto-importers (`workers/team_autoimport_{github,gitlab,jira,linear}.py`) write provider
values straight into `teams`. By the time anyone opens the panel, the diff is already empty because
sync overwrote it. A faithful rebuild **must** re-separate provider-observed state from curated
state in ClickHouse.

The CH admin write path makes "policy columns on `teams`" unsafe:
`ClickHouseTeamAdminService.create_or_update` rewrites the **whole** team row with `provider=""`
and `native_team_key=None` on every update — any policy stored on `teams` would be clobbered unless
every writer is audited. Policy therefore lives in a **sidecar** table.

## 4. Recommended architecture — Option C-lite

Re-separate the two layers and centralize the write through **one drift-aware projector** instead
of scattering policy logic across four importers. Existing readers keep using `teams FINAL` as the
resolved catalog.

### 4.1 New ClickHouse tables

All `ReplacingMergeTree`. Use **low-cardinality strings, not `Enum8`**, for `status` / `change_type`
to avoid enum-widening migration ordering before new values can be emitted.

```sql
-- Per-team drift policy (sidecar; never stored on `teams`).
CREATE TABLE team_sync_policies (
  org_id String,
  team_id String,
  sync_policy UInt8 DEFAULT 0,          -- 0 auto-apply, 1 flag-for-review, 2 manual/none
  managed_fields Array(String) DEFAULT [],
  updated_by Nullable(String),
  updated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, team_id);

-- Provider-observed truth layer (what discovery last saw).
CREATE TABLE team_provider_observations (
  org_id String,
  provider LowCardinality(String),
  native_team_key String,
  team_id String,
  name Nullable(String),
  description Nullable(String),
  members_json String,
  project_keys_json String,
  repo_patterns_json String,
  is_active UInt8,
  parent_team_id Nullable(String),
  discovered_at DateTime64(6, 'UTC'),
  updated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, provider, native_team_key);

-- Pending-review read model (decision table, NOT a source of provider truth).
CREATE TABLE team_drift_changes (
  org_id String,
  change_id String,                     -- value-fingerprint hash (see 4.2)
  entity_type LowCardinality(String),   -- 'team' now; 'identity' in the identities slice
  entity_id String,
  provider LowCardinality(String),
  native_team_key Nullable(String),
  change_type LowCardinality(String),   -- field_changed / provider_removed / new_team_available
  field Nullable(String),
  old_value_json String,
  new_value_json String,
  status LowCardinality(String),        -- pending / approved / dismissed / resolved / superseded
  first_seen_at DateTime64(6, 'UTC'),
  last_seen_at DateTime64(6, 'UTC'),
  decided_at Nullable(DateTime64(6, 'UTC')),
  decided_by Nullable(String),
  updated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, change_id);
```

### 4.2 Correctness rules (do not skip)

- **`change_id` must fingerprint the values:**
  `hash(org_id, entity_type, entity_id, change_type, field, old_value_json, new_value_json)`.
  `hash(team_id, change_type, field)` alone makes a dismissed `A→B` wrongly suppress a later
  `A→C`.
- **No resurrection.** On each projector run: if a `change_id` already exists as `dismissed` or
  `approved`, do **not** re-insert a `pending` row for the same value pair. Only a *different*
  value fingerprint creates new pending drift.
- **Lifecycle.** Drift that disappears from discovery → mark stale `pending` rows `resolved`.
  Provider value changes for the same `(team, field)` → mark the prior `pending` row `superseded`
  and insert a new `pending` row.
- **Backwards-compatible default.** `sync_policy = 0` (auto-apply) is the default, so existing orgs
  see **no behavior change**. Only `policy 1` teams route managed-field changes into the pending
  lane.

### 4.3 Drift-aware projector

Replace the final `insert_teams` write in the four auto-importers **and** in
`ClickHouseTeamAdminService.import_teams` with one projector that:

1. Always writes the latest `team_provider_observations` row.
2. Reads `team_sync_policies` for the team.
3. `policy 0` → apply observed values to `teams` (current behavior).
4. `policy 1` → for each changed managed field, emit/refresh a `team_drift_changes` `pending` row
   instead of overwriting `teams`.

`approve` applies the observed value into `teams` via `create_or_update` and marks the change
`approved`; `dismiss` marks it `dismissed` (catalog unchanged).

### 4.4 Identities / members manual-mapping dimension (in scope — sequenced after teams)

Per the framing in §0, manual member mappings are a preserved capability, so member drift is **in
scope**, not deferred to product demand. It is, however, a **different shape** from team-field
drift and ships as its own slice:

- Auto-import writes both `teams.members` (identity facets) **and** the `team_memberships`
  attribution dimension (read at metrics time, `metrics/loaders/clickhouse.py`). Reviewing only
  `teams.members` would show a misleading UI while attribution changes underneath — the member
  slice must gate the attribution-impacting dimension too.
- Surface conflicts where auto-import would change a manually-curated membership or where
  `manual_attribution_fallbacks(scope_type='member')` disagrees with discovered membership; flag
  instead of clobber; approve applies the membership change surgically (add/remove by facet, never
  full recompute — preserving the existing surgical-by-facet invariant).
- Reuse `team_drift_changes` via `entity_type='identity'`, or a parallel `identity_drift_changes`
  table if the membership change shape diverges enough in review.

### 4.5 Worker

New `sync_team_drift` Celery task on the `sync` queue (worker-supplied provider credentials; the
CLI no longer supplies them). It runs discovery across configured providers, calls the drift-aware
projector, and writes pending rows. `POST /admin/teams/trigger-drift-sync` dispatches it.

## 5. Wire contract (resolved)

There is a three-way mismatch today:

- **Live web caller** (`dev-health-web` `src/lib/admin/api/teams.ts`) is **index-based**:
  `{change_indices: number[], approve_all|dismiss_all: bool}`; the `FlaggedChange` type carries
  `change_index`, and the panel approves/dismisses by `(team_id, change_index)`.
- **The 501 stub signatures** are also index-based.
- **Ops `schemas_flat.py`** already has an **orphaned, unused** `ApproveChangesRequest.change_ids:
  list[str]` and a `FlaggedChange` without `change_index` — a half-finished migration toward stable
  IDs.

Index-based approve/dismiss is racy against a shifting pending list. **Decision:** make `change_id`
canonical (matching the orphaned schema's intent) and do a small mechanical web change — add
`change_id` to `FlaggedChange` and send `change_ids` instead of `change_indices`. We own both
repos and this is a from-scratch rebuild, so there is no reason to preserve the fragile index path.

## 6. Scope

**In scope, teams slice (first):** provider-owned team field drift over managed fields `name`,
`description`, `project_keys`, `repo_patterns` (+ `members` as a display-only catalog field whose
review does **not** yet gate `team_memberships`). Approve writes observed value into `teams`;
dismiss acknowledges that exact old/new pair; disappearance → `resolved`; value change →
`superseded` + new `pending`.

**In scope, identities slice (second):** member/identity drift + `manual_attribution_fallbacks`
(`scope_type='member'`) reconciliation, gating the `team_memberships` attribution dimension (§4.4).

**Out of scope (separate follow-ups):** `new_team_available` auto-import gating; any overlay rewrite
of all `teams FINAL` readers.

## 7. Deletion intent is superseded (no live conflict)

**Decision (owner): CHAOS-2622 supersedes the full deletion scope — the four endpoints and the
web `PendingChangesPanel` are NOT deleted; they are rebuilt on ClickHouse.**

There is no live deletion ticket. CHAOS-2608 — originally cited as the deletion ticket — is in fact
the **Done** CS7 *web provenance / boundary* ticket and never touched the drift-review endpoints.
The "removed in CS7" intent only survives as two stale references that must be rewritten (not
executed) when the rebuild lands, tracked by **CHAOS-2649**:

1. `ops/src/dev_health_ops/api/admin/routers/teams.py:340` — "the endpoints + their web caller are
   removed together in CS7."
2. `ops/AGENTS.md:38` — "…remain as HTTP 501 stubs until CS7 removes them with the web caller —
   CHAOS-2608…".

Both are replaced with the CHAOS-2622 rebuild framing. No Linear ticket cancellation is required.

## 8. Ordered implementation checklist

1. **Scrub the two stale deletion references** (`teams.py:340` + `ops/AGENTS.md:38`) — CHAOS-2649.
2. CH migrations: `team_sync_policies`, `team_provider_observations`, `team_drift_changes`.
3. Add all three tables to the **org-deletion purge** path.
4. **Drift-aware projector** centralizing the four auto-importer writes + `import_teams`.
5. `ClickHouseTeamDriftService`: `get_pending_changes` / `approve` / `dismiss` by `change_id` over
   `team_drift_changes FINAL`, with the §4.2 lifecycle rules.
6. New `sync_team_drift` Celery task + `trigger-drift-sync` dispatch.
7. Repoint the four stub endpoints to the service; switch the wire to `change_id`.
8. Web: add `change_id` to `FlaggedChange`, send `change_ids` (`dev-health-web`).
9. Docs (`team-attribution.md` §0) + tests: provider×entity matrix green, plus
   resurrection/supersede/resolve lifecycle assertions, in the same PR.
10. **Identities/members slice** (§4.4): member + `manual_attribution_fallbacks(member)` drift,
    gating `team_memberships`.

## 9. Effort & risk

- **Teams slice:** Medium. **Identities slice:** Large (membership + attribution-impacting writes).
- **Top risks:** (a) `change_id` not value-fingerprinted → wrong suppression; (b) re-discovery
  resurrecting dismissed changes; (c) reviewing `teams.members` without gating `team_memberships`
  giving a misleading UI; (d) a future dev acting on the stale "removed in CS7" references and
  deleting the rebuilt surface (mitigated by CHAOS-2649 scrubbing them first).

## 10. Decisions (owner — resolved)

1. **Sequencing:** **teams slice first**, identities/members slice second (CHAOS-2656 blocked-by the
   teams projector + service).
2. **Deletion scope:** **CHAOS-2622 supersedes the full deletion scope.** CHAOS-2608 is an unrelated
   Done web ticket; the stale references are scrubbed via CHAOS-2649. No endpoints/panel deleted.
