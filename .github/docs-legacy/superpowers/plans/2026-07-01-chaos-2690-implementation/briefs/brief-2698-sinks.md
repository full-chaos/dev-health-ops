# CHAOS-2698 — External ingest sink writes — Implementation Brief

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. D1 RATIFIED epic-wide (CC8): `source_id Nullable(UUID)` on the 9 tables, migration
>    `065_external_ingest_source_id.sql`; source-registration table's canonical name is
>    **`external_ingest_sources`** (CHAOS-2696, Postgres migration 0032).
> 2. D2/D3 RATIFIED and now binding on 2691's wire schema: `repository.v1.externalId` IS
>    the provider full name (`owner/repo`); `work_item.v1` carries provider-native
>    `externalKey`; this issue owns **`external_ingest/ids.py`** with
>    `derive_repo_uuid(system, instance, external_id)` and
>    `derive_work_item_id(system, instance, external_key, work_item_type)` implementing the
>    verified formats (`jira:{key}`, `linear:{id}`, `gh:{repo}#{n}`, `ghpr:{repo}#{n}`,
>    `gitlab:{repo}#{iid}`, `gitlab:{repo}!{iid}`, `custom:{instance}:{key}`).
>    `work_item_transition/dependency.v1` use optional `workItemType` for namespace
>    disambiguation (default issue).
> 3. This issue also owns **`external_ingest/types.py`** (NormalizedBatch, SinkWriteResult,
>    SinkWriteError, AffectedScope as sketched below). CHAOS-2699 does NOT import these —
>    its planner takes primitives; CHAOS-2697 maps AffectedScope → kwargs (CC21).
> 4. This issue OWNS the live-ClickHouse per-kind round-trip tests (settles the 2697/2698
>    overlap), including an identity-continuity test: repo UUID from a pushed
>    `repository.v1` equals the UUID from a native-sync-style `Repo(repo=full_name)` write.
> 5. Landing wave: 3 (parallel with 2693/2699/2700/2714; consumed by 2697 in wave 4).
>    `write_batch(batch, *, clickhouse_dsn)` signature pinned.
> 6. Kind×system matrix (CC6) is enforced upstream in 2697's validate step; sinks may
>    assert-but-not-reject. `Repo.provider` gains legal value `"custom"` — add a reader-
>    tolerance note/test.
> 8. **POST-CRITIQUE (CC24): clamp customer `updatedAt`** — when used as the RMT
>    version column (identities/teams pass-through), values more than 5 minutes in the
>    future are replaced with server now() and recorded as a per-record WARNING
>    diagnostic (not a rejection). Prevents `updatedAt=2100-01-01` from permanently
>    pinning an RMT row against all future pushes/native-sync corrections. Add a unit
>    test + a live-CH test asserting the clamped row loses to a later legitimate write.

Parent epic: CHAOS-2690 (External customer-push ingestion API)
Sibling sub-issues referenced: CHAOS-2696 (source registration + tokens),
CHAOS-2695 (idempotency + ownership policy), CHAOS-2697 (worker
normalization), CHAOS-2699 (bounded recomputation planner), CHAOS-2694
(ingest status + rejected-record diagnostics).

Repo: `dev-health-ops`, worktree
`/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration`.
All paths below are relative to that root unless stated otherwise.

---

## Scope

Implement the **sink-write layer** of the external-ingest worker: given a
batch of already-schema-validated, kind-normalized records (the output of
CHAOS-2697's normalizer — see "Interface with CHAOS-2697" below), stamp
`org_id` + provenance, and write each of the 9 v1 record kinds through the
**existing** ClickHouse sink methods, preserving current
append/current-state/dedup semantics. Produce a machine-readable summary
(per-kind counts written, write-time errors, affected scope) that CHAOS-2694
(status/error persistence) and CHAOS-2699 (bounded recompute) consume.

In scope:

- A new module `src/dev_health_ops/external_ingest/sinks.py` that owns all
  sink-write logic for the 9 record kinds.
- A new provenance/attribution mechanism (`source_id` column, see Design
  Decision D1) added via ClickHouse migration, threaded through every sink
  call for the 9 kinds.
- `repository.v1` repo-UUID derivation matching native sync exactly (D2).
- `work_item.v1` / family `work_item_id` derivation matching native sync ID
  conventions where the declared `system` is a real provider, with a
  `custom:` fallback scheme (D3).
- Identity-resolution interplay: reuse `resolve_identity()` for
  `work_item.v1` assignee/reporter canonicalization; do **not** resolve
  identities for git-family author/reviewer fields (matches native sync) (D4).
- `identity.v1` / `team.v1` write path via `insert_identities` /
  `insert_teams` (dict-shaped rows, no new dataclass — see D5).
- Batching (chunked inserts, not per-record) for every sink call.
- Two client lifecycles: one `ClickHouseStore` (async) instance and one
  `ClickHouseMetricsSink` (sync) instance per worker invocation, both scoped
  to the batch's `org_id` (D6).
- Unit tests (mocked sinks) + live `@pytest.mark.clickhouse` tests proving
  RMT dedup / re-push / FINAL-read semantics for all 9 kinds.
- `ops/docs/architecture/external-ingest-sink-writes.md` documenting D1–D8
  in the same changeset (house rule: document decisions in repo docs).

Out of scope (see "Out of scope" section):

- Schema validation, JSON-Schema generation, `/validate` and `/batches`
  endpoints (CHAOS-2697 / router work, not this issue).
- Idempotency-key / payload-hash comparison and the batch-level 409 logic
  (CHAOS-2695).
- Source registration, one-active-owner enforcement, and ingest-token auth
  (CHAOS-2696) — this issue **consumes** a resolved `(org_id, source_id,
  system, instance)` tuple, it does not create or validate it.
- Actually enqueuing bounded recompute Celery tasks (CHAOS-2699) — this
  issue only emits the affected-scope struct that CHAOS-2699 will consume.
- Ingest status/rejected-record Postgres persistence (CHAOS-2694) — this
  issue returns write results in-process; persisting them to the status/error
  store is CHAOS-2694's job.
- Webhooks, dev-hops CLI, web UI (other sub-issues).

---

## Out of scope (explicit exclusions worth calling out)

- **Do not** add a Postgres write path for any of the 9 record kinds.
  ClickHouse remains sole source of truth (confirmed convention, CHAOS-2600).
- **Do not** touch `metrics/job_daily.py`, `metrics/job_work_items.py`, or
  any native-sync processor — this issue only adds a new caller of the
  existing sink methods, it does not modify sink method signatures except
  for the additive `source_id` field (D1), which must default to `NULL` /
  absent for every existing native-sync call site (backward compatible).
- **Do not** write to `git_commit_stats` (per-file stats), `git_files`,
  blame, CI, deployment, incident, or security-alert tables — those are not
  v1 record kinds.
- **Do not** implement the legacy-`/api/v1/ingest` reconciliation decision —
  that is a cross-cutting epic-owner decision (flagged in `decisionsNeeded`),
  this issue's sink-write code has zero dependency on the legacy ingest
  router/consumer and must not import from `api/ingest/*`.

---

## Design decisions

**D1 — Provenance column, not a `provider` value.**
Add a new nullable `source_id UUID` column (via `ALTER TABLE ... ADD COLUMN
IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL`) to the 9 tables this
issue writes to: `repos`, `git_commits`, `git_pull_requests`,
`git_pull_request_reviews`, `teams`, `identities`, `work_items`,
`work_item_transitions`, `work_item_dependencies`. `source_id` is the FK to
whatever CHAOS-2696 names its source-registration Postgres table's PK
(`source_registrations.id` — confirm exact name with CHAOS-2696 owner, see
`decisionsNeeded`). Every row written by this sink-write layer sets
`source_id` to the resolved source's UUID; every row written by native sync
processors leaves it `NULL` (no changes needed at existing call sites — the
column is additive and every sink method's row-builder already tolerates
extra optional dict keys / dataclass fields defaulting to `None`).
Rationale: `repos.provider` already means "which upstream system" (github,
gitlab, jira, linear) and is read by existing queries/processors as such;
overloading it with `customer_push` would break those call sites and
conflate "what system" with "who ingested it." A dedicated provenance column
is additive, queryable (`WHERE source_id IS NOT NULL` = "any customer-pushed
row"), and gives CHAOS-2696's one-active-owner policy and CHAOS-2694's
support tooling a join key without needing per-row `provider` string parsing.
`ORDER BY` keys are unchanged (source_id is not part of any sort key), so
this migration is a cheap, safe additive `ALTER TABLE`.

**D2 — `repository.v1` repo UUID must be derived identically to native sync.**
The normalizer (CHAOS-2697) will hand this layer a `Repo`-shaped record with
some `repo` identifier string. This layer's `write_repository()` MUST call
`get_repo_uuid_from_repo(repo_full_name)` (`models/git.py:72`) using the
**exact same string shape** native sync uses: `"{owner}/{name}"` /
`"{group}/{project}"`, lower-cased (verified: `processors/github.py:1574`
passes `repo=repo_info.full_name`; `processors/gitlab.py:1815/2096` passes
`repo=full_name` / `project_info.full_name` — both are `owner/repo` style,
not full URLs). Rationale: this is required for the one-active-owner
handoff to actually dedupe — if an org switches a source instance from
`fullchaos_sync` to `customer_push` (or vice versa) for the "same" repo, the
repo UUID must match or `repo_id` FKs silently fork into two rows for the
same logical repo. The external-ingest `repository.v1` schema (CHAOS-2697
schema definition) MUST require a `fullName` (or equivalently-named)
field in `owner/repo` shape for `system in {github, gitlab}`, not just a
free-text display name — flag this schema requirement in
`decisionsNeeded`/CHAOS-2697 handoff if the schema doc doesn't already
capture it. For `system == "custom"` (no matching real provider), derive the
UUID from `f"custom:{source_instance}:{external_id}"` instead (own
namespace, no collision risk with real-provider UUIDs since the hash input
differs).

**D3 — `work_item_id` / dependency IDs must match native sync's ID-space
when the declared system is a real provider.**
`WorkItem.work_item_id` follows a provider-prefixed convention already
baked into every downstream query (`jira:ABC-123`, `gh:owner/repo#123`,
`gitlab:group/project#456`, `linear:CHAOS-123`). This layer's
`write_work_item()` MUST construct `work_item_id` using that exact format
when `source.system` is `jira|github|gitlab|linear`, from the customer's
`externalId` field (e.g. `system=github, externalId=42, repoFullName=acme/api`
→ `work_item_id = "gh:acme/api#42"`). For `system == "custom"`, use
`f"custom:{source_instance}:{external_id}"`. `work_item_dependency.v1`
records reference `source_work_item_id` / `target_work_item_id` which must
be derived the same way — the normalizer (CHAOS-2697) or this layer (pick
one, see Files below) must apply this derivation consistently to both the
`work_item.v1` and `work_item_dependency.v1`/`work_item_transition.v1`
records in a batch, since dependency edges must resolve against work items
written in the same or an earlier batch. Rationale: same handoff-consistency
argument as D2 — this is the single biggest correctness risk in this issue
(silently forking `work_items` rows for what should be the same Jira/GitHub
issue depending on ingestion path).

**D4 — Identity resolution interplay: reuse `resolve_identity()` for
work-item fields, do nothing for git-family fields.**
`dev_health_ops.metrics.identity.resolve_identity(provider, raw_fields)`
(`metrics/identity.py:27`) is a config-driven (YAML alias map) canonicalizer
used by every native work-item connector to populate `WorkItem.assignees` /
`.reporter`. It is **independent** of the ClickHouse `identities` table
populated by `identity.v1` records (`insert_identities` — CHAOS-2600 CS5,
an admin-facing canonical-identity directory, not a resolution source).
Decision: `write_work_item()` in this layer calls `resolve_identity(system,
{"email": ..., "username": ..., "account_id": ..., "display_name": ...})`
on each raw assignee/reporter the normalizer hands it, exactly mirroring
native connector behavior, so cross-provider identity rollups (e.g. "same
person" across a customer-pushed Jira feed and a native GitHub sync) work
consistently regardless of ingestion path. For `commit.v1` / `pull_request.v1`
/ `review.v1`, native sync stores **raw** `author_name`/`author_email`/
`reviewer` strings with zero resolution at write time (verified:
`GitCommit`/`GitPullRequest`/`GitPullRequestReview` have no identity-resolved
field) — this layer must do the same: pass through whatever raw
name/email/login string the customer payload supplies, unresolved.
`identity.v1` records populate `insert_identities` purely as a directory
side-effect; they do NOT feed `resolve_identity`'s alias map (that map is a
static YAML config file, out of scope to make dynamic in this issue — flag
as a known limitation, not a bug to fix here).

**D5 — `identity.v1` / `team.v1` stay dict-shaped, no new dataclasses.**
`insert_identities` / `insert_teams` already duck-type `dict | Any` rows
(`clickhouse.py:1598`, `:1526`). Introducing new frozen dataclasses for
`CanonicalIdentity` / `CustomerTeam` would be pure ceremony for this issue
(CHAOS-2697 already needs to emit *some* normalized shape from JSON; a plain
`dict` with the exact keys `insert_identities`/`insert_teams` expect is the
lowest-friction contract and matches the existing convention used
everywhere else in the codebase for these two kinds). If CHAOS-2697 prefers
typed dataclasses for validation ergonomics, this layer accepts either —
`_item_getter`-style duck typing costs nothing extra here — but the
row-shape contract (exact field names) below is authoritative regardless of
which CHAOS-2697 chooses.

**D6 — Two client lifecycles per worker invocation, not per-record.**
Confirmed (`recon-models-sinks.md` §0): `ClickHouseStore` (async,
`repository`/`pull_request`/`review`/`commit`/`team`/`identity`) and
`ClickHouseMetricsSink` (sync, `work_item*`) are different classes with
different construction. This layer's public entrypoint
(`write_batch(records: NormalizedBatch, *, org_id: str, source_id: uuid.UUID)
-> SinkWriteResult`) is an `async def` that:
1. Constructs one `ClickHouseStore` via `create_store(dsn, "clickhouse")`,
   sets `store.org_id = org_id`, opens it as an async context manager, and
   batches all `repository`/`pull_request`/`review`/`commit`/`team`/
   `identity` records from the batch into single calls per sink method
   (`insert_repo` is one-at-a-time per the existing signature — call it in a
   loop but still inside the one `async with store` block, not one store per
   repo).
2. Constructs one `ClickHouseMetricsSink` via `create_sink(dsn)` (sync;
   run inside `asyncio.to_thread` from the async caller so it does not block
   the event loop — mirrors how `ClickHouseStore._insert_rows` already
   wraps its own sync client calls in `asyncio.to_thread`), and calls
   `write_work_items` / `write_work_item_transitions` /
   `write_work_item_dependencies` once each with the full batch's rows for
   that kind.
Rationale: matches the recon's explicit call-out that this is a genuine
two-client-class problem the plan doc glossed over; one construction per
worker invocation (not per record, not per store-per-call) keeps connection
overhead bounded and batching correct.

**D7 — Re-push of updated records is "just write it again."**
Every one of the 9 tables is `ReplacingMergeTree` keyed so that a later
`last_synced`/`updated_at` on the same natural key wins on merge (see table
in "Sink methods & dedup semantics" below). This layer does **not** need to
implement upsert/read-before-write logic: on re-push, normalize + derive the
same deterministic IDs (D2/D3) as the first push, set a fresh
`last_synced`/`updated_at` (handled automatically — every sink method stamps
`datetime.now(timezone.utc)` for `last_synced` itself; `identities`/`teams`
use `updated_at` sourced from the row, defaulting to "now" if absent — see
D8 below for the one exception), and call the same sink method again. The
only kind requiring special handling is `work_item_transitions`: because its
`ORDER BY` includes `occurred_at` (not just the work item), re-pushing the
*same* transition (same `occurred_at`) with a corrected `actor`/status
already replaces correctly via RMT; but two *different* transition rows for
the same status change (e.g. a customer retries a batch and generates a new
`occurred_at` by mistake) will NOT be deduped by `ORDER BY` and require the
`semantic_deduped_subquery` treatment **at read time** (already true for
native sync — no new work here, just don't assume writes alone guarantee
uniqueness for this one table. Document this explicitly for CHAOS-2694/2699
readers).

**D8 — `identities`/`teams` `updated_at` must come from the customer
payload's timestamp, not "now," when the customer supplies one.**
`identities`/`teams` are `ReplacingMergeTree(updated_at)` with **full
replacement** semantics (`054_identities.sql` comment: "the caller supplies
a fresh `updated_at` so the latest write wins"). If this layer always
stamps `updated_at = now()` regardless of what the customer's payload says,
an out-of-order batch replay (e.g. customer replays an older batch after a
newer one already landed) would incorrectly become "latest" and clobber
newer data. Decision: `write_identity()` / `write_team()` pass through
`record.updatedAt` from the payload when present (parsed to UTC
`datetime`), falling back to "now" only if the payload omits it. This is the
one place in this layer where "now" is *not* always correct — call this out
loudly in the module docstring since every other sink call in this layer
uses `last_synced = now()` (the correct behavior for `last_synced`-keyed
tables, which are receive-time markers, not payload-content timestamps).

---

## Interface with CHAOS-2697 (worker normalization) — contract this issue depends on

CHAOS-2697 hands this layer one `NormalizedBatch` per accepted stream entry
(post schema-validation, post per-record `kind` dispatch). Until CHAOS-2697
lands its own shape, this issue assumes and should implement against:

```python
# src/dev_health_ops/external_ingest/sinks.py

from dataclasses import dataclass, field
from typing import Any
import uuid

@dataclass
class NormalizedBatch:
    org_id: str
    source_id: uuid.UUID
    source_system: str        # "github" | "gitlab" | "jira" | "linear" | "custom"
    source_instance: str      # e.g. "github.com/acme"
    ingestion_id: uuid.UUID
    repositories: list[dict[str, Any]] = field(default_factory=list)       # -> Repo-shaped
    identities: list[dict[str, Any]] = field(default_factory=list)         # -> insert_identities row shape
    teams: list[dict[str, Any]] = field(default_factory=list)              # -> insert_teams row shape
    work_items: list[Any] = field(default_factory=list)                    # WorkItem | dict
    work_item_transitions: list[Any] = field(default_factory=list)         # WorkItemStatusTransition | dict
    work_item_dependencies: list[Any] = field(default_factory=list)        # WorkItemDependency | dict
    pull_requests: list[dict[str, Any]] = field(default_factory=list)      # -> GitPullRequest-shaped
    reviews: list[dict[str, Any]] = field(default_factory=list)            # -> GitPullRequestReview-shaped
    commits: list[dict[str, Any]] = field(default_factory=list)            # -> GitCommit-shaped
    # per-record provenance for error reporting back to CHAOS-2694
    record_index_by_kind: dict[str, list[int]] = field(default_factory=dict)
```

If CHAOS-2697's actual shape differs, this layer's `write_batch()` should be
the single translation point — do not let sink-shape assumptions leak
upstream into CHAOS-2697's normalizer beyond "produce dict/dataclass rows
whose keys match the tables below." **This is a cross-cutting interface —
flag any mismatch to the CHAOS-2697 implementer before merge**, not after
(see `decisionsNeeded`).

## Interface with CHAOS-2699 (bounded recomputation planner) — contract this issue produces

```python
@dataclass
class SinkWriteResult:
    ingestion_id: uuid.UUID
    org_id: str
    counts_written: dict[str, int]           # {"repository": 1, "commit": 40, ...}
    errors: list["SinkWriteError"]           # write-time failures only (not validation)
    affected_scope: "AffectedScope"

@dataclass
class SinkWriteError:
    record_index: int
    kind: str
    external_id: str | None
    code: str            # e.g. "clickhouse_insert_failed", "missing_repo_full_name"
    message: str

@dataclass
class AffectedScope:
    org_id: str
    source_systems: set[str]
    source_instances: set[str]
    repo_ids: set[uuid.UUID]
    team_ids: set[str]
    work_item_ids: set[str]
    min_timestamp: "datetime | None"
    max_timestamp: "datetime | None"
    record_kinds: set[str]
```

`write_batch()` returns `SinkWriteResult`. CHAOS-2699 consumes
`affected_scope` to build its `dispatch_investment_materialize_partitioned`
-style scoped recompute call (reuse that existing kwargs pattern per the
celery-metrics recon — do not design a new bounded-recompute abstraction in
this issue). CHAOS-2694 consumes `counts_written` + `errors` to update the
status/error store; this issue does **not** write to Postgres itself (no
`get_postgres_session` calls in this module).

---

## API/DDL/schema sketches

### ClickHouse migration (new file, next number after 064)

`src/dev_health_ops/migrations/clickhouse/065_external_ingest_source_id.sql`
(confirm actual next number at implementation time — 064 was
`work_unit_repo_effort.sql` per persistence-migrations recon; re-check
`ls src/dev_health_ops/migrations/clickhouse/ | sort | tail -5` before
naming the file, numbers may have advanced since this brief was written):

```sql
-- Migration 065: add source_id provenance column to the 9 external-ingest
-- record-kind tables (CHAOS-2698). NULL for every existing native-sync row;
-- set to the resolved customer-push source's UUID (CHAOS-2696's
-- source_registrations.id) by the external-ingest sink-write layer.
-- Not part of any ORDER BY key -- purely a queryable attribution column.
ALTER TABLE repos                    ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE git_commits               ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE git_pull_requests         ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE git_pull_request_reviews  ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE teams                     ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE identities                ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE work_items                ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE work_item_transitions     ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
ALTER TABLE work_item_dependencies    ADD COLUMN IF NOT EXISTS source_id Nullable(UUID) DEFAULT NULL;
```

Apply/verify locally with `dev-hops migrate clickhouse upgrade` /
`dev-hops migrate clickhouse status --check` against the scratch DB only
(never `default` — house rule).

### Sink-write module skeleton

`src/dev_health_ops/external_ingest/sinks.py`:

```python
async def write_batch(
    batch: NormalizedBatch,
    *,
    clickhouse_dsn: str,
) -> SinkWriteResult:
    store = create_store(clickhouse_dsn, "clickhouse")
    store.org_id = batch.org_id
    errors: list[SinkWriteError] = []
    counts: dict[str, int] = {}

    async with store:
        if batch.repositories:
            for row in batch.repositories:
                repo = _build_repo(row, source_id=batch.source_id)  # D2 UUID derivation here
                await store.insert_repo(repo)
            counts["repository"] = len(batch.repositories)
        if batch.commits:
            await store.insert_git_commit_data(
                [_stamp_source_id(_build_commit(r), batch.source_id) for r in batch.commits]
            )
            counts["commit"] = len(batch.commits)
        if batch.pull_requests:
            await store.insert_git_pull_requests(
                [_stamp_source_id(_build_pr(r), batch.source_id) for r in batch.pull_requests]
            )
            counts["pull_request"] = len(batch.pull_requests)
        if batch.reviews:
            await store.insert_git_pull_request_reviews(
                [_stamp_source_id(_build_review(r), batch.source_id) for r in batch.reviews]
            )
            counts["review"] = len(batch.reviews)
        if batch.teams:
            await store.insert_teams(
                [{**t, "source_id": str(batch.source_id)} for t in batch.teams]
            )
            counts["team"] = len(batch.teams)
        if batch.identities:
            await store.insert_identities(
                [_apply_d8_updated_at({**i, "source_id": str(batch.source_id)}) for i in batch.identities]
            )
            counts["identity"] = len(batch.identities)

    if batch.work_items or batch.work_item_transitions or batch.work_item_dependencies:
        sink = create_sink(clickhouse_dsn)
        try:
            def _write_work_graph() -> None:
                if batch.work_items:
                    sink.write_work_items(_stamp_work_items(batch.work_items, batch))
                if batch.work_item_transitions:
                    sink.write_work_item_transitions(_stamp_work_items(batch.work_item_transitions, batch))
                if batch.work_item_dependencies:
                    sink.write_work_item_dependencies(_stamp_work_items(batch.work_item_dependencies, batch))
            await asyncio.to_thread(_write_work_graph)
            counts["work_item"] = len(batch.work_items)
            counts["work_item_transition"] = len(batch.work_item_transitions)
            counts["work_item_dependency"] = len(batch.work_item_dependencies)
        finally:
            sink.close()

    return SinkWriteResult(
        ingestion_id=batch.ingestion_id,
        org_id=batch.org_id,
        counts_written=counts,
        errors=errors,
        affected_scope=_build_affected_scope(batch),
    )
```

(`_stamp_source_id`/`_stamp_work_items`/`_build_*`/`_apply_d8_updated_at`
are private helpers in the same module; `errors` accumulation around each
`await store.insert_*`/`asyncio.to_thread` call should catch narrow
ClickHouse client exceptions per-call — not swallow everything with a bare
`except Exception`, since a write failure here is exactly the kind of thing
CHAOS-2694's rejected-record diagnostics need to surface. Use
`try/except Exception as exc: errors.append(SinkWriteError(...)); logger.exception(...)`
per sink-method call, not per-record — sink methods are batch calls, a
failure fails the whole batch's rows for that kind; that's an acceptable v1
granularity given ClickHouse insert() is all-or-nothing per call anyway.)

### `insert_identities` row shape (exact keys the normalizer must produce)

```python
{
    "canonical_id": str,             # required
    "org_id": str,                   # this layer's write_batch sets/overrides with batch.org_id
    "identity_uuid": str | None,     # optional, sink derives if absent
    "display_name": str | None,
    "email": str | None,
    "provider_identities": str,      # JSON-encoded dict[str, list[str]], default "{}"
    "team_ids": list[str],
    "is_active": int,                # 0/1
    "updated_at": datetime | str,    # D8: from payload when present
    "source_id": str,                # this layer stamps
}
```

### `insert_teams` row shape

```python
{
    "id": str,                       # slug PK
    "team_uuid": str | None,
    "name": str,
    "description": str | None,
    "members": list[str],
    "project_keys": list[str],
    "repo_patterns": list[str],
    "is_active": int,
    "updated_at": datetime | str,    # D8
    "org_id": str,
    "provider": str,                 # source.system value
    "native_team_key": str | None,
    "parent_team_id": str | None,
    "source_id": str,
}
```

---

## Sink methods & dedup semantics (authoritative table for this issue)

| Record kind | Sink method | Client | ORDER BY (dedup key) | Re-push behavior |
|---|---|---|---|---|
| `repository.v1` | `ClickHouseStore.insert_repo(repo: Repo)` | async | `(org_id, id)`, id = `get_repo_uuid_from_repo(full_name)` (D2) | full replace on newer `last_synced` |
| `commit.v1` | `ClickHouseStore.insert_git_commit_data(rows)` | async | `(org_id, repo_id, hash)` | full replace on newer `last_synced`; hash is intrinsic, no ID-mapping needed |
| `pull_request.v1` | `ClickHouseStore.insert_git_pull_requests(rows)` | async | `(org_id, repo_id, number)` | full replace; `number` must be the provider's native PR/MR number (int), not a synthetic ID |
| `review.v1` | `ClickHouseStore.insert_git_pull_request_reviews(rows)` | async | `(org_id, repo_id, number, review_id)` | full replace; `review_id` and parent `number` both required from payload |
| `team.v1` | `ClickHouseStore.insert_teams(rows)` | async | `(org_id, id)` | full replace (D8: use payload `updated_at`) |
| `identity.v1` | `ClickHouseStore.insert_identities(rows)` | async | `(org_id, canonical_id)` | full replace (D8: use payload `updated_at`) |
| `work_item.v1` | `ClickHouseMetricsSink.write_work_items(rows)` | sync (thread) | `(org_id, repo_id, work_item_id)` | full replace on newer `last_synced`; `work_item_id` per D3 |
| `work_item_transition.v1` | `ClickHouseMetricsSink.write_work_item_transitions(rows)` | sync (thread) | `(org_id, repo_id, work_item_id, occurred_at)` | append-like; semantic dedup needed at READ time only (D7), not this layer's problem to solve, but must not be assumed solved by ORDER BY alone |
| `work_item_dependency.v1` | `ClickHouseMetricsSink.write_work_item_dependencies(rows)` | sync (thread) | `(org_id, source_work_item_id, target_work_item_id, relationship_type)` | full replace |

`work_items.repo_id` is non-nullable in the sink's row builder (defaults to
`uuid.UUID(int=0)` when absent — `work_graph.py:649`). For Jira/Linear work
items with no repo association, this layer must pass `repo_id=None` (not
omit the key) so the sink's own `uuid.UUID(int=0)` fallback fires — do not
invent a different sentinel.

`org_id` stamping mechanism differs by client (recon §4, verified):
`ClickHouseStore._insert_rows` auto-injects `self.org_id` into any row
missing it; `ClickHouseMetricsSink.write_work_items`/`_transitions` read
`org_id` **directly off each record** with no fallback — this layer must
explicitly set `.org_id` (or `["org_id"]` for dict rows) on every
work-item-family record before calling those three methods, never rely on
sink-side injection for that family.

`project_key` / `project_id` / `native_team_key` semantics for
`work_item.v1` — reuse the exact convention from `WorkItem.work_scope_id`
(`models/work_items.py:87-105`) so downstream reporting that calls this
property continues to work uniformly for pushed and native data:
- `system == "jira"` → populate `project_key` (Jira project key).
- `system in {"github", "gitlab"}` → populate `project_id` as
  `"{owner}/{repo}"` / `"{group}/{project}"`.
- `system == "linear"` → populate `project_id` (Linear project UUID) when
  the issue belongs to a project, else `project_name`, else
  `native_team_key` (Linear team key) as last-resort scope, matching the
  in-code docstring's explicit precedence.
- `system == "custom"` → populate `project_key` with whatever scope string
  the customer payload supplies (documented as best-effort, no cross-org
  normalization attempted for a fully custom system).

---

## Files to create/modify

Create:
- `src/dev_health_ops/external_ingest/__init__.py`
- `src/dev_health_ops/external_ingest/sinks.py` — module described above
  (`write_batch`, `NormalizedBatch`, `SinkWriteResult`, `SinkWriteError`,
  `AffectedScope`, private `_build_*`/`_stamp_*` helpers).
- `src/dev_health_ops/external_ingest/repo_identity.py` — small shared
  helper module: `derive_repo_id(system, full_name) -> uuid.UUID` (D2) and
  `derive_work_item_id(system, external_id, repo_full_name=None) -> str`
  (D3), so CHAOS-2697's normalizer and this issue's sink layer both import
  the *same* derivation functions instead of duplicating the string-format
  logic (prevents drift between "what CHAOS-2697 validates as a valid
  external ID shape" and "what this layer hashes/formats"). **Coordinate
  file ownership with CHAOS-2697 implementer** — whichever issue lands
  first creates this file, the other imports it.
- `src/dev_health_ops/migrations/clickhouse/0NN_external_ingest_source_id.sql`
  (D1 — confirm next number, see DDL section).
- `tests/external_ingest/test_sinks_unit.py` — mocked-client unit tests.
- `tests/external_ingest/test_sinks_clickhouse.py` — live
  `@pytest.mark.clickhouse` round-trip tests.
- `ops/docs/architecture/external-ingest-sink-writes.md` — D1–D8 write-up
  (house rule: document decisions in the same changeset).

Modify:
- None of the existing sink files (`storage/clickhouse.py`,
  `metrics/sinks/clickhouse/work_graph.py`) need functional changes — the
  new `source_id` column is additive and every existing row-builder already
  passes through unknown-key-tolerant dict construction or explicit field
  lists; confirm at implementation time that `_insert_rows`'s explicit
  `columns` list additions include `source_id` when present (i.e. the
  `insert_repo`/`insert_git_commit_data`/etc. column lists **do** need a
  one-line addition of `"source_id"` to their column-name lists — this is
  the one small, additive, backward-compatible touch to the existing sink
  files; it is not optional, the new column is invisible to `_insert_rows`
  unless it's an explicit column name since `Repo`/`GitCommit`/etc.
  dataclasses don't have a `source_id` attribute for `_insert_rows`'s
  attribute-based row extraction to find). Verify exact insertion points
  with `grep -n "columns = \[" src/dev_health_ops/storage/clickhouse.py`
  and the equivalent `column_names = [` blocks in `work_graph.py` before
  writing this change — do not guess line numbers, they will have drifted.
- `docs/superpowers/plans/2026-06-26-external-customer-push-ingestion-api.md`
  — NOT modified by this issue (plan docs are the spec, not living docs);
  any correction belongs in the architecture doc created above, cross-linked.

---

## Test plan

### Unit tests (`tests/external_ingest/test_sinks_unit.py`, no live DB)

- Mock `create_store`/`create_sink` (or inject fakes implementing the same
  method surface) and assert:
  - `write_batch` calls `insert_repo` once per repository record with a
    `Repo.id` equal to `get_repo_uuid_from_repo("owner/repo".lower())` for a
    `system="github"` input with `fullName="Owner/Repo"` (case-insensitivity
    proof for D2).
  - `write_batch` calls `write_work_items` with every record's `.org_id`
    (or `["org_id"]`) set to `batch.org_id`, proving D6's "no reliance on
    sink-side injection for work-item family" claim.
  - `work_item_id` derivation matches `"gh:owner/repo#42"` /
    `"jira:ABC-123"` / `"gitlab:group/project#7"` / `"linear:CHAOS-1"` /
    `"custom:my-instance:ext-1"` for each system (D3 table-driven test).
  - `identity.v1`/`team.v1` rows preserve a customer-supplied `updatedAt`
    verbatim (D8) rather than being overwritten with "now" — assert the
    row passed to `insert_identities`/`insert_teams` has the exact input
    timestamp.
  - A raised exception from one sink-method call (e.g. mock
    `insert_git_pull_requests` raises) produces a `SinkWriteError` in
    `SinkWriteResult.errors` and does NOT prevent other kinds in the same
    batch from being written (partial-batch resilience).
  - `AffectedScope` aggregates `repo_ids`/`work_item_ids`/`min/max_timestamp`
    correctly across a mixed batch (repo + PR + work item in one batch).
- Mark none of these `@pytest.mark.clickhouse` — pure Python + mocks, must
  run in the default `unit_tests()` CI tier.

### Live ClickHouse tests (`tests/external_ingest/test_sinks_clickhouse.py`, `@pytest.mark.clickhouse`)

For each of the 9 kinds, against a real scratch ClickHouse:
1. Write a batch with one record of that kind, `source_id=<uuid1>`.
2. Read back with `FINAL` (or `argMax`) filtered by `org_id` — assert exactly
   one row, `source_id` matches, and `provider` (for git-family/team) or
   `system`-derived fields are correct.
3. Re-push the same logical record with one changed field and a fresh
   timestamp (`last_synced`/`updated_at` per D7/D8) — read back with `FINAL`
   again, assert exactly one row (no duplicate) and the changed field wins.
4. For `work_item_transition.v1` specifically: push two transitions with the
   **same** semantic columns (D7's `WORK_ITEM_TRANSITION_SEMANTIC_COLUMNS`)
   but different `last_synced` — assert a raw `FINAL` read returns 2 rows
   (RMT ORDER BY doesn't cover `occurred_at`-identical-but-distinct-insert
   cases the same way) but
   `semantic_deduped_subquery("work_item_transitions", WORK_ITEM_TRANSITION_SEMANTIC_COLUMNS)`
   returns 1 — this is the proof for D7's read-time-dedup callout.
5. Repository handoff test: write a `repository.v1` batch via this layer
   for `system=github, fullName=acme/api`, then independently call
   `get_repo_uuid_from_repo("acme/api")` (simulating what a native
   `fullchaos_sync` GitHub processor would derive) — assert the two UUIDs
   are identical (D2's core correctness property, tested end-to-end not
   just via mocks).
- Use `SCRATCH_DB=ci_local_validate_2698` (per-issue override, house rule)
  when running these locally so parallel agent runs on other sub-issues
  don't collide on the shared `ci_local_validate` scratch DB name.

---

## Gate commands

From the worktree root
(`/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration`),
after `uv sync --all-extras --dev` (fresh — house rule, avoids
pytest-asyncio false-fails in worktrees):

```bash
# Format/lint/typecheck/full-unit + isolated live-CH proof, all in one gate:
SCRATCH_DB=ci_local_validate_2698 bash ci/local_validate.sh

# mypy explicitly (also covered by local_validate.sh, run standalone if iterating):
.venv/bin/mypy --install-types --non-interactive .

# Targeted live-DB tests only, once local_validate.sh has proven the scratch
# DB is migrated (or run against dev-health-clickhouse-1's own migrated
# schema in a dedicated non-default database — never `default`):
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/ci_local_validate_2698" \
  .venv/bin/pytest -m clickhouse tests/external_ingest/ -v

# Unit-only fast loop while iterating:
.venv/bin/pytest tests/external_ingest/test_sinks_unit.py -v
```

No `dev-health-web` changes in this issue — no web gate commands required.

---

## Live verification procedure (against the running dev compose stack)

This issue has no HTTP endpoint of its own (that's CHAOS-2697/router work),
so live verification is a Python-level smoke test invoking `write_batch()`
directly against the dev ClickHouse, then confirming rows via
`clickhouse-client` (house rule: verify with real data before iterating).

```bash
# 1. Confirm dev ClickHouse is up and has the new migration applied to a
#    scratch DB (NEVER apply directly to `default`).
docker exec dev-health-clickhouse-1 clickhouse-client --user ch --password ch \
  --query "CREATE DATABASE IF NOT EXISTS ci_local_smoke_2698"
CLICKHOUSE_URI="clickhouse://ch:ch@localhost:8123/ci_local_smoke_2698" \
  .venv/bin/dev-hops migrate clickhouse upgrade

# 2. Run a small Python smoke script (write via .venv/bin/python -c or a
#    scratch script in $TMPDIR) that constructs a NormalizedBatch with one
#    repository + one PR + one work_item, calls
#    `await write_batch(batch, clickhouse_dsn=CLICKHOUSE_URI)`, and prints
#    SinkWriteResult.

# 3. Verify rows landed with correct org_id/source_id and FINAL dedup:
docker exec dev-health-clickhouse-1 clickhouse-client --user ch --password ch \
  --database ci_local_smoke_2698 \
  --query "SELECT id, repo, provider, source_id FROM repos FINAL WHERE org_id = 'smoke-org'"

docker exec dev-health-clickhouse-1 clickhouse-client --user ch --password ch \
  --database ci_local_smoke_2698 \
  --query "SELECT work_item_id, project_key, project_id, source_id FROM work_items FINAL WHERE org_id = 'smoke-org'"

# 4. Drop the scratch smoke DB when done.
docker exec dev-health-clickhouse-1 clickhouse-client --user ch --password ch \
  --query "DROP DATABASE IF EXISTS ci_local_smoke_2698"
```

Never point any of the above at `CLICKHOUSE_URI=.../default` — that is the
real dev DB (house rule, repeated deliberately given this issue creates a
new migration).

---

## Dependencies on other sub-issues

- **CHAOS-2696** (source registration + ingest token scopes): this issue
  needs the `source_registrations` table's PK name/type finalized before
  D1's `source_id` FK type can be locked in (assumed `UUID` in this brief —
  confirm). Also needs CHAOS-2696's one-active-owner check to exist
  *upstream* of this layer (this layer trusts that `write_batch()` is only
  ever invoked for a source already resolved+authorized as `customer_push`
  mode; it does not re-check ownership itself).
- **CHAOS-2697** (worker normalization): this issue's `NormalizedBatch`
  input contract and the shared `repo_identity.py` derivation helpers (D2,
  D3) must be agreed with whoever implements CHAOS-2697 — ID-derivation
  logic should live in exactly one place and be imported by both. Treat
  this as a blocking interface dependency, not just an FYI.
- **CHAOS-2695** (idempotency + ownership policy): batch-level idempotency
  (same idempotency key + payload hash) is resolved before this layer is
  invoked; this layer only needs to be safe under at-least-once *sink*
  invocation (i.e., a batch's `write_batch()` might be called twice for the
  same accepted batch after a worker crash/retry) — RMT semantics already
  make that safe (D7), so no additional idempotency work is needed here,
  but confirm CHAOS-2695's retry contract doesn't assume this layer does
  its own row-level idempotency checks (it doesn't, and shouldn't).
- **CHAOS-2699** (bounded recomputation planner): consumes
  `SinkWriteResult.affected_scope` — confirm the exact field names/types in
  `AffectedScope` above match what CHAOS-2699's planner expects before
  either side is merged.
- **CHAOS-2694** (ingest status + rejected-record diagnostics): consumes
  `SinkWriteResult.counts_written`/`.errors` to persist to the status/error
  Postgres tables — confirm CHAOS-2694's error-row schema (`record_index`,
  `record_kind`, `external_id`, `code`, `message`, `path` per the plan doc)
  matches `SinkWriteError`'s fields exactly (this brief's `SinkWriteError`
  is missing `path` — CHAOS-2694 may need this layer to add a `path` field
  for JSON-pointer-style error locations; flag if CHAOS-2697's validation
  errors and this layer's write errors need a unified error shape).

---

## Risks

1. **`work_item_id`/repo-UUID derivation drift (D2/D3) is the single
   highest-impact bug surface.** If CHAOS-2697's normalizer and this
   layer's sink-write derive IDs differently (or if either drifts from
   native-sync's derivation over time), a customer switching between
   `fullchaos_sync` and `customer_push` for the same source instance will
   silently fork rows instead of cleanly handing off — this breaks the
   epic's one-active-owner guarantee at the data layer even though the
   auth-layer policy (CHAOS-2696) is correctly enforced. Mitigate by
   sharing one derivation module (see Files) and adding the D2 handoff
   live test.
2. **`source_id` column backward-compatibility.** Every native-sync sink
   call site (processors/github.py, processors/gitlab.py,
   metrics/job_work_items.py, etc.) must continue to work unmodified after
   this migration — the new column must be nullable with no default-value
   surprises. Verify with a live-CH test that a native-sync-style call
   (`insert_repo` with no `source_id` in the row) still succeeds and yields
   `source_id IS NULL`.
3. **`ClickHouseMetricsSink` is a blocking/sync client wrapped in
   `asyncio.to_thread`** — if CHAOS-2697's worker later wants to write many
   batches concurrently, thread-pool exhaustion is a real risk; not an
   issue at expected v1 customer-push volumes but worth a comment in the
   module so it's not silently forgotten (do NOT over-engineer a fix in
   this issue — flag it, defer it).
4. **`work_item_transitions` semantic-dedup is a read-time contract, easy
   to violate accidentally.** Any downstream consumer (CHAOS-2699's bounded
   recompute jobs, or a future dashboard) that queries this table with raw
   `FINAL` instead of `semantic_deduped_subquery` will double-count
   transitions ingested via customer push exactly as it would for native
   sync — this is a pre-existing landmine this issue does not introduce but
   must not paper over; document loudly (done, see D7 + architecture doc).
5. **Legacy `/api/v1/ingest` module ambiguity is a cross-cutting unresolved
   question** (flagged across nearly every recon brief) — this issue's
   sink-write code has no direct dependency on it, but if the epic owner
   ultimately decides to deprecate/merge the legacy path, some of its
   consumer code (`api/ingest/consumer.py`) may turn out to duplicate parts
   of this issue's sink-write responsibility. Out of scope to resolve here;
   flagged in `decisionsNeeded`.
6. **`project_key`/`project_id`/`native_team_key` mapping is
   customer-payload-quality-dependent.** Nothing in this layer validates
   that a customer's `system=jira` payload actually supplies a real Jira
   project key vs. a made-up string — `work_scope_id` groupings for pushed
   data are only as good as customer data hygiene. Not fixable at the sink
   layer; note as a known v1 limitation, not a defect.
