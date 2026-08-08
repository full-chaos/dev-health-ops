# CHAOS-3499 baseline inventory appendix

**Required ADR artifact** (amended PRD/TRD §15.2, amended CHAOS-3499).
Repos at the commits inventoried: `dev-health-ops` @ `chaos-3499-discovery`
(off `feature/chaos-3498-context-fabric`), `dev-health-acr` @ working tree,
2026-08-07.

This appendix answers one question: **what temporal capability already ships,
before the trial scores anything?** Everything asserted below carries
`file:line`. Everything not verified is marked UNVERIFIED and says what would
verify it. Nothing here is a measurement of an arm — no arm has run.

---

## 0. Headline results

1. **`as_of` is real and complete on the ACR read path.** All **17** catalog
   sources apply an as-of bound. Verified count matches the PRD's claim.
2. **The axis is not what the PRD assumes.** Sixteen of seventeen sources bound
   on an *observed/ingestion* timestamp. Exactly one (`incidents.v1`) applies a
   genuine valid-time interval. The `axis` field the amendment adds is
   therefore not a formality: today's `as_of` silently means observed-time
   nearly everywhere.
3. **There are TWO native history gaps, not one.** The PRD documents declared
   state (`projects`). Blocker/relationship history is a *second, undocumented*
   gap: `work_graph_edges` has `discovered_at` but **no `valid_to`**, and its
   ReplacingMergeTree key omits `discovered_at`, so a re-discovered edge
   overwrites its predecessor. CHAOS-3563 as scoped does not close this.
4. **The squash-merge premise is wrong.** `work_graph_pr_commit` is *not*
   near-empty for squash orgs — a documented heuristic tier (CHAOS-2435) was
   built for exactly that case. The real, narrower gap is that residual squash
   formats are dropped **with no coverage signal anywhere**.
5. **A production-grade transactional outbox already exists** (`worker_job_outbox`,
   Python→River). The projector does not need a new event mechanism.
6. **The `/metrics` prerequisite is half-satisfied.** Python/Celery workers
   expose none. **Go workers already expose `/metrics` on :8080.** If the
   projector is a Go worker, §18's blocking prerequisite is already met.
7. **The CHAOS-3068/3069 tripwire is CLEAN.** No fixture still lists
   `incidents.v1` in `expected_unavailable_sources`.
8. **A latent as-of defect found:** `operational_service_repository_mappings.valid_from`
   is `Nullable`, and `NULL <= as_of` is false in ClickHouse, so a null-start
   mapping is silently dropped from **every** as-of filter found.

---

## 1. Evaluation questions by class (§15.2)

Assignments and evidence live in code at
[`corpus/questions.py`](../corpus/questions.py); the table restates them.

| Q | Question | Class | Deciding evidence |
|---|---|---|---|
| Q1 | What did we try last time this CI failure occurred? | **(c)** | `acr/internal/storage/interfaces.go:85-91` — no episode list method; `acr/internal/api/app.go:77` — POST-only route |
| Q2 | What was blocking Project X on July 15, and what changed since? | **(b)** — result must cite CHAOS-3563 state | `ops/.../native_status_change.py:369-379`; `ops/.../014_work_graph.sql:5-15` (projects RMT, `updated_at` not in ORDER BY); `014_work_graph.sql:6-22` (edges have no `valid_to`) |
| Q3 | Which decision superseded the original deployment design? | **(c)** | `ops/.../work_graph/models.py:37-84` — closed 30-member `EdgeType` enum, no `supersedes` |
| Q4 | Show prior agent attempts touching this subsystem and outcomes. | **(c)** | `acr/internal/storage/interfaces.go:85-91`; `acr/internal/sidecar/config.go:194` (`ACR_ENABLE_WRITEBACK` default `false`) |
| Q5 | Which current project facts conflict with earlier evidence? | **(c)** | `ops/.../014_work_graph.sql:6-22` — RMT key omits `discovered_at`, so the prior version is gone |
| Q6 | What recurring failure pattern is supported across these incidents? | **(c)** | `ops/.../066_operational_canonical.sql:69-79` (timelines native); `models.py:37-84` (no shared-cause edge) |
| Q7 | What was true about this dependency as of date Y? | **(a)** | `066_operational_canonical.sql:243-264`; `ops/.../operational_edges.py:44-48`; `acr/.../source_queries.go:62` |

**Distribution: (a) × 1, (b) × 1, (c) × 5.**

This distribution is itself a finding the ADR must carry. The question set is
heavily weighted toward the capability under evaluation, so **any aggregate
"temporal answerability" score over these seven questions flatters an
extraction-capable arm regardless of merit**. That is not an argument to change
the questions — they are the questions the product wants answered — it is the
concrete reason §15.2's per-class reporting requirement is load-bearing rather
than procedural.

**Class (b) carries a dependency the ADR must record.** Any class-(b) result
is uninterpretable without stating which state of **CHAOS-3563**
(declared-state retention, in flight in lane-ops-pretrial) it was measured
against: "the baseline scored 0 on class (b)" means one thing before that
lands and something else after. The harness enforces this — an unrecorded
dependency renders class (b) **NOT COMPARABLE** rather than emitting a number
(`harness/runner.py`, `DependencyState`). The branch state is obtained through
the orchestrator, not by reading that lane's worktree.

Two questions straddle classes and the ADR must not hide the split:

- **Q2** — the "what changed since" half is class (a): `work_item_transitions`
  keeps `occurred_at` **in its sorting key**
  (`009_raw_work_items.sql:30-42`, `027_add_org_id_to_sorting_keys.py:70`), so
  status history is genuinely append-only. Only the as-of half is gated.
- **Q6** — retrieving the incidents is (a); asserting a shared pattern is (c).
- **Q4** — becomes (a) the moment CHAOS-3564 lands the read path. The ADR must
  state which side of that landing the trial measured, because the same
  question changes class.

---

## 2. `as_of` coverage — all 17 ACR catalog sources

Registry: `acr/internal/contextpacket/source_queries.go:48-68`
(`SourceQueryCatalogV1`), executed by `source_catalog.go:55`.

| # | Source | Line | as-of bound column | Axis |
|---|---|---|---|---|
| 1 | `repository_freshness.v1` | `source_queries.go:49` | `last_synced` | observed |
| 2 | `work_items.v1` | `:50` | `updated_at` | observed |
| 3 | `work_item_dependencies.v1` | `:51` | `d.last_synced` | observed |
| 4 | `git_commits.v1` | `:52` | `c.committer_when` | observed |
| 5 | `git_commit_files.v1` | `:53` | `c.last_synced` | observed |
| 6 | `pull_requests.v1` | `:54` | `p.last_synced` | observed |
| 7 | `pull_request_reviews.v1` | `:55` | `r.submitted_at` | observed |
| 8 | `ci_pipeline_runs.v1` | `:56` | `c.started_at` | observed |
| 9 | `work_graph.v1` | `:57` | `discovered_at` | observed |
| 10 | `ai_workflow_runs.v1` | `:58` | `observed_at` | observed |
| 11 | `ai_workflow_artifacts.v1` | `:59` | `observed_at` | observed |
| 12 | `ai_review_outcomes.v1` | `:60` | `observed_at` | observed |
| 13 | `deployments.v1` | `:61` | `coalesce(deployed_at, started_at, last_synced)` | observed |
| 14 | `incidents.v1` | `:62` | `coalesce(started_at, source_event_at, observed_at)` **plus** `m.valid_from <= as_of AND (m.valid_to IS NULL OR m.valid_to > as_of)` | observed **+ valid** |
| 15 | `deployment_incident_provenance.v1` | `:63` | `observed_at` | observed |
| 16 | `file_hotspots.v1` | `:66` | `toDateTime(day)` | observed |
| 17 | `file_complexity.v1` | `:67` | `toDateTime(as_of_day)` | observed |

**Wire definitions.** `scope.as_of`: `acr/internal/contracts/v1/types.go:94`
(`RequestedScope.AsOf`), `mcp_types.go:59`,
`contracts/jsonschema/v1/context_packet_request.v1.schema.json:78`,
`mcp_context_for_task_request.v1.schema.json:57`. Threaded via
`internal/mcp/context_scope.go:66` → `internal/contextpacket/read_adapter.go:64`.

**No server default** for `as_of` or `time_window_days`:
`read_adapter.go:64-65` and `context_scope.go:66-67` pass both through
verbatim; a nil/zero value is interpreted as *unbounded* by the SQL predicates
(`{as_of:...} IS NULL OR ...`). This is corpus case C20, and the trial must
*define* a bounded default rather than inherit unboundedness by accident.

---

## 3. Native event history

| Table | Migration | Engine / ORDER BY | Genuinely append-only? |
|---|---|---|---|
| `work_item_transitions` | `009_raw_work_items.sql:30-42`; org_id key `027_add_org_id_to_sorting_keys.py:70` | `ReplacingMergeTree(last_synced)`, `(org_id, repo_id, work_item_id, occurred_at)` | **YES** — `occurred_at` is in the key, so only identical re-syncs collapse |
| `work_item_reopen_events` | `011_work_item_extras.sql:21-31`; `027_...py:72` | `ReplacingMergeTree(last_synced)`, `(org_id, work_item_id, occurred_at)` | YES structurally — but **write-only in practice**: `WORK_ITEM_REOPEN_EVENTS_DEDUPED` (`idempotency.py:65-67`) has no call site found |
| `operational_incident_timeline_events` | `066_operational_canonical.sql:69-79` | `ReplacingMergeTree(source_version_at)`, `(org_id, id)` | Per distinct event id, yes. All reads go through `current_operational_rows_sql` (`operational_current.py:25-66`), which collapses to one current row per `(org_id, id)` — no full-history read path exists |
| `team_drift_changes` | `058_team_drift_changes.sql:1-18` | `ReplacingMergeTree(updated_at)`, `(org_id, change_id)` | **PARTIAL** — `change_id` is a content hash of the value pair (`clickhouse_team_drift_projector.py:73-92`), so re-observed identical transitions collapse; all readers use `FINAL` (`clickhouse_team_drift.py:151`, `..._projector.py:399,412`), so decision history is not queryable |

The distinction that matters for classification: **`occurred_at` in the sorting
key** is what separates a real event log from a current-state table wearing an
event-log name. Only `work_item_transitions` (and structurally
`work_item_reopen_events`) clears that bar.

---

## 4. Native valid-time intervals

| Table | Migration | `valid_from` | `valid_to` |
|---|---|---|---|
| `team_memberships` | `014_work_graph.sql:28-43` | `:39` (NOT NULL) | `:40` Nullable |
| `team_project_ownership` | `014_work_graph.sql:45-59` | `:55` (NOT NULL) | `:56` Nullable |
| `team_repo_ownership` | `014_work_graph.sql:61-76` | `:72` (NOT NULL) | `:73` Nullable |
| `operational_service_repository_mappings` | `066_operational_canonical.sql:243-264` | `:261` **Nullable** | `:262` Nullable |

Real interval joins: `ops/.../metrics/loaders/clickhouse.py:417-418, 451-452,
491-492` (three as-of interval filters inside `load_team_attribution_context`),
`ops/.../api/dev/native_status_change.py:786-787`,
`ops/.../work_graph/operational_edges.py:44-48`,
`acr/.../source_queries.go:62`.

For the three `team_*` tables, `valid_from` is part of the sorting key, so
successive intervals are distinct rows — these are genuine valid-time history.

> **Defect found during inventory.** `operational_service_repository_mappings.valid_from`
> is `Nullable` (`066_operational_canonical.sql:261`) while every as-of filter
> found applies `valid_from <= {as_of}`. A `NULL` comparison is false in
> ClickHouse, so a mapping row with a null interval start is **silently dropped
> from every as-of answer**, on both axes. No comment or code explains why this
> column is nullable when its three siblings are not. The trial plants such a
> row (corpus C01, oracle `O7_null_valid_from`) so the behaviour is measured
> rather than assumed. **This is an ops finding, not a graph finding** — the
> ADR must not let a graph arm take credit for surfacing it.

---

## 5. `work_graph_edges` — provenance is present, history is not

Migration `014_work_graph.sql:6-22`. Confirmed columns: `provenance String`
(`:15`), `confidence Float32` (`:16`), `evidence String` (`:17`),
`discovered_at DateTime64(3,'UTC')` (`:18`). Later additions: `event_ts`, `day`
(`016_work_graph_event_ts.sql:2-3`). Closed relation vocabulary:
`EdgeType` — 30 members, `ops/.../work_graph/models.py:37-84`; `Provenance`
(`native|explicit_text|heuristic`) at `models.py:87-92`.

**The gap.** The ReplacingMergeTree key is
`(org_id, source_type, source_id, edge_type, target_type, target_id)`
(`027_add_org_id_to_sorting_keys.py:75`) — `discovered_at` is **not** in it, and
there is **no `valid_to` column at all**. Consequences:

- re-discovering an edge with different `confidence`/`provenance` overwrites
  the prior values with no retained history (this is why Q5 is class (c));
- the interval during which a blocker *held* is not representable, so
  `_BLOCKERS_SQL` (`native_status_change.py:456-499`) cannot answer "what was
  blocking on July 15" even in principle (this is why Q2 is class (b)).

**This second gap is not the one the PRD documents.** §14.b and CHAOS-3563
address declared state on `projects`. Relationship valid-time on
`work_graph_edges` is a distinct, undocumented gap and needs its own
disposition in the ADR — closing only the first still leaves Q2 unanswerable.

---

## 6. Declared-state history — the documented gap, mechanism confirmed

`ops/src/dev_health_ops/api/dev/native_status_change.py:369-379`:

> `FINAL` collapses the RMT to its single CURRENT row per key — there is no
> history left to read here … an as-of snapshot of a since-changed declared
> state is simply unavailable.

Mechanism: `_PROJECT_DECLARED_FACTS_SQL` (`native_status_change.py:400-409`)
reads `projects FINAL`. `projects` is
`ReplacingMergeTree(updated_at) ORDER BY (org_id, provider, id)`
(`014_work_graph.sql:5-15`) — **`updated_at` is the version column but is not in
the sorting key**, so `FINAL` keeps only the highest-`updated_at` row and every
prior declared state is discarded. The query's own `updated_at <= {as_of}`
filter therefore cannot look backwards; it can only return the current row or
nothing (`HAVING count() = 1` → fail-closed).

Owned by **CHAOS-3563** (lane-ops-pretrial). Not built here.

---

## 7. Episodes — write-only, off by default, empty

| Aspect | Evidence |
|---|---|
| Contract | `acr/internal/contracts/v1/types.go:304-321` (`AgentEpisodeCreate`), `:348-354` (`AgentEpisode`) |
| Structured subsystem signal | `types.go:336-340` — `EpisodeArtifacts{FilesTouched, ArtifactURIs, TestsRun}`. **This needs no extraction to use.** |
| `outcome` enum | `validate.go:117-124` — includes **`superseded`**, confirming the PRD §6.3 naming collision with the `supersedes` predicate is real |
| `retention_class` | `validate.go:126-133` — `default_90d, short_30d, legal_hold, no_persist` |
| Storage | `acr/migrations/postgres/0001_acr_core.sql:55-88` |
| Write path | `acr/internal/api/app.go:77` (POST), `internal/mcp/server.go:88-95` (conditional MCP tool) |
| Gate | `internal/mcp/record_episode.go:12-14` (`recordEpisodeEnabled`) |
| Off-by-default | `internal/sidecar/config.go:194` — `ACR_ENABLE_WRITEBACK` defaults `false`. Note the manifest's `"disabled_by_default": true` (`contracts/mcp/tools.v1.json:22`) is **descriptive**; no runtime code reads it. Two mechanisms, same effect |
| **Read path** | **NONE.** `internal/storage/interfaces.go:85-91` — `EpisodeStore` has `PreflightIdempotency`, `CreateIdempotent`, `GetByClientEpisodeID` (idempotency-conflict only, `postgres/episodes.go:186`), `Redact`, `PurgeExpired`. No list/query. `contracts/openapi/acr-v1.yaml:132-170` defines no GET |
| In packets | NOT FOUND — no episode reference anywhere in `internal/contextpacket/` |

Owned by **CHAOS-3564** (read path) and **CHAOS-3565** (cohort writeback).

---

## 8. Authorization, revocation, and the integration seam

- **The seam is already named for this work.**
  `acr/internal/storage/interfaces.go:69-70`: *"EvidenceStore is read-only.
  Implementations may use ClickHouse now and a temporal graph later without
  changing the public v1 contracts."* Interface at `:69-77`; sole consumer is
  `contextpacket/assembler.go:34,38`.
- **Scope derivation** — `contextpacket/read_adapter.go:42-69`
  (`BuildReadPlanV1`): rejects an empty `principal.OrgID` (`:44`), calls
  `auth.AuthorizeRepository` (`:53`), sets `OrgID` from the principal (`:58`).
  Invoked on every `ResolveScope` and `ContextForTask`
  (`clickhouse.go:157,180`).
- **Revocation is live, not cached** — `postgres/credentials.go:96-102` filters
  `revoked_at IS NULL AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)`
  in the per-request `FindByTokenHash`; `auth/middleware.go:121-125` rejects
  revoked credentials. No credential cache exists.
  **Caveat:** `internal/entitlements/cache.go` *does* cache the
  `agent_context_runtime` boolean — an entitlement change has cache-TTL lag
  even though credential revocation does not. Worth an explicit measurement in
  the deletion/revocation propagation numbers.
- **MCP tools** — `contracts/mcp/tools.v1.json`: exactly
  `context_for_task`, `source_evidence`, `record_episode`. Acceptance oracle
  `mcp_tools_exact` at `tests/fullstack/assertrun/layers.go:331-349`.

---

## 9. `work_graph_neighbors` — the PRD's own precedent, corrected

§10 bounds `related_changes` "the same way `work_graph_neighbors` already is:
depth-one, `max_results` capped at 100."

The service is **not in acr** (exhaustively searched: no such tool, type, or
field). It is in **ops**:
`ops/src/dev_health_ops/api/dev/work_graph_neighbors_service.py`,
`SCHEMA_VERSION = "work_graph_neighbors.v1"` (`:19`).

- Depth: **fixed to one** — `:106-107`, `raise ValueError("Ask Dev V1
  work-graph depth is fixed to one")`. ✅ matches the PRD.
- Cap: **`MAX_NEIGHBORS = 25`** (`:22`), enforced at `:108-109`. ❌ **the PRD
  says 100.**
- Relation allowlist: `ALLOWED_RELATIONSHIP_TYPES` (`:27`), validated at
  `:102` — the closed-allowlist discipline §10 requires already exists.

**Correction required:** either the PRD's `related_changes` bound becomes 25 to
match its stated precedent, or it stops citing `work_graph_neighbors` as the
precedent for 100.

---

## 10. Squash-merge — the PRD's premise is wrong, the real gap is narrower

The PRD (case 16) assumes `work_graph_pr_commit` is "near-empty for squash
orgs". It is not. `WorkGraphBuilder._derive_pr_commit_links`
(`ops/.../work_graph/builder.py:1895-2055`, called unconditionally at `:489`)
was built for exactly this case under CHAOS-2435, with two tiers
(`:2002-2012`):

1. explicit merge keywords → `provenance=explicit_text`, `confidence=0.9`;
2. GitHub's squash subject suffix `<subject> (#N)` → `provenance=heuristic`,
   `confidence=0.6`, admitted only when `N` matches a known PR in the same
   `(org_id, repo_id)` (`:2029-2036`).

The docstring records the motivating measurement (`:1915-1917`): one live org
had 22 explicit-merge edges against ~3218 discarded squash commits.

**The residual gap.** Bare `#N` is deliberately never accepted (`:1919`,
indistinguishable from an issue mention), so a squash commit without GitHub's
default `(#N)` suffix — a custom squash template, or a non-GitHub provider —
is still dropped. And **no coverage signal exists**: `audit/coverage.py` checks
static provider-implementation coverage (`:121-218`), not runtime data
completeness; `work_graph_projection_runs` is scoped solely to
`projection_name = 'issue_blockers'` (`builder.py:1032`); `native_evidence.py`
has no `pr_commit` check.

Corpus C16 and oracle `O1_ci_prior_attempts_squash` are written against the
**corrected** premise: the failure under test is *silent* emptiness, not
emptiness.

---

## 11. Governance the projector must inherit

**Provider policy.** `resolve_agent_provider_selection`
(`ops/.../llm/agent/policy.py:120-159`); `AgentProviderPolicy` at `:111-117`;
hard stop at `:127-128` when `ask_dev_enabled` is false or `llm_globally_disabled`.
Org kill switch `emergency_disabled` enforced earlier at the router
(`api/dev/router.py:391,964,971`).

**Budget.** `ASK_DEV_RUN_COST_HARD_MAX_MICROUSD = 5_000_000`
(`api/dev/org_policy.py:25`); monthly caps at `:20-24`.
Ordering: monthly allowance is enforced **before run creation**
(`persistence/service.py:1873-1876` → `:3961-3986`, Valkey counters); per-call
BYO cost is guarded **around** the provider call
(`llm/budget.py:684-745` → `guard_byo_call` at `:329`).

**Reusability — partially, not wholly.** `guard_byo_call` is provider-neutral
by signature (`budget.py:329-340`, no Ask Dev object). But
`attach_agent_budget_guard`, `AgentProviderPolicy`, and
`resolve_agent_provider_selection` each have exactly **one** caller today
(`api/dev/production_runtime.py:522,738`), and the monthly-allowance tier is
Ask-Dev-coupled by construction — Valkey keys are literally
`askdev:allowance:{org_id}:{YYYY-MM}` (`askdev_allowance_counters.py:9-11`) and
cold-start recompute reads `dev_runs`.

> **Correction to PRD §2/§7.2.** "Governed by the same machinery already
> applied to Ask Dev narrative generation" is true of the *per-call* guard and
> the *policy decision shape*, but the **monthly platform cap tier needs new
> wiring for a non-Ask-Dev caller** — it is not a drop-in reuse. The ADR should
> record this as integration cost rather than assume it away.

**Retention.** `ask_dev_retention_days` (`org_policy.py:13`), allowed values
`{0, 30}` (`models/dev_persistence.py:35`), default `30` (`org_policy.py:97`,
fail-closed at `:135`). `= 0` means no readable transcript ever
(`persistence/service.py:1547-1549`) with a 1-hour abandoned grace
(`service.py:162,1331`). §9's rule that such an org does not receive temporal
projection without a separate compatible opt-in is well-founded.

**Entitlement.** `EXPLICIT_PURCHASE_FEATURES`
(`ops/.../licensing/registry.py:23-30`) includes `agent_context_runtime`;
resolved per org at `licensing/gating.py:386-393`; seeded by
`alembic/versions/0037_seed_agent_context_runtime_feature_flag.py:24-27`.
ACR consumes it at `internal/entitlements/client.go:66-85` — which is
**hardcoded to that single entitlement string** (`:66-69`), so ACR cannot check
any other ops feature flag through this seam today.

---

## 12. Org deletion — the gap is wider than the PRD states

`ops/src/dev_health_ops/api/services/org_deletion.py:133-154`
(`_clickhouse_tables_from_migrations`) discovers tables by regex-scanning one
directory, `_CLICKHOUSE_MIGRATIONS_DIR` (`:75-77`):

- `_CREATE_TABLE_RE` `:78-81`, `_ALTER_ORG_ID_RE` `:82-85`, `_PY_TABLE_RE` `:86`;
- `@lru_cache(maxsize=1)`, consumed by `_purge_clickhouse` `:543-573`.

No derived-store registry exists (searched `derived_store`, `DerivedStore`,
`store_registry`, `StoreRegistry` — no hits). `_postgres_targets()` `:164-416`
is a hand-maintained ORM list.

**Two additional org-scoped stores already outside its reach** — beyond the
future graph store the PRD anticipates:

1. **Valkey allowance counters**, keyed `askdev:allowance:{org_id}:{YYYY-MM}`
   (`askdev_allowance_counters.py:9-11`). Never referenced by `org_deletion.py`.
2. **`worker_job_outbox`** (`models/worker_job_outbox.py:35`) — has **no
   `org_id` column at all**; the org id lives inside an opaque `args` JSON
   blob (`:41`). Not in `_postgres_targets()` and structurally unscannable.

CHAOS-3566 owns the registry. These two should be in its scope, and they are
present-day deletion-completeness gaps independent of this epic.

---

## 13. Infrastructure the shadow harness can build on

**A real outbox already exists.** `internal/joboutbox` —
*"the generic Python-to-River transactional bridge"* (`types.go:1`).
Producer `producer.go:22-51` (caller supplies the transaction, so a domain
transition and its handoff cannot commit independently, `:19-21`); states
`types.go:25-28`; relay `relay.go:1-40`, `loop.go`; Postgres model
`models/worker_job_outbox.py:37`. Documented flow:
`docs/contribute/architecture/data-and-storage.md:47-60`, including
*"unknown or invalid kinds terminalize with bounded evidence rather than
disappearing"* — precisely the property a shadow projector needs.

A second, domain-specific `sync_dispatch_outbox` also exists
(`models/integrations.py:414-454`) and *is* covered by org deletion
(`org_deletion.py:282-286`).

No CDC mechanism exists (searched `cdc`, `debezium`, `logical replication`,
`wal2json` — NOT FOUND in either repo).

**Worker `/metrics` — the §18 prerequisite is half-met.**

- Python/Celery workers: **no `/metrics`**, confirmed by the code's own
  docstring (`workers/ask_dev_retention.py:36-45`) and by
  `compose.yml:335-396` (no `ports:` on any worker/beat service);
  `/metrics` is mounted only on the FastAPI app
  (`api/_observability.py:34-49`).
- **Go workers: `/metrics` already exists** —
  `internal/platform/health/server.go:114-118` registers it beside
  `/healthz`/`/readyz`, serving real Prometheus text (`:160-220`);
  `docker/go-worker.Dockerfile:102` exposes 8080;
  `deploy/docker-compose/compose.go-workers.yml:115`.

> **Correction to PRD §18.** "No worker container currently exposes a scraped
> Prometheus endpoint" is accurate for Celery only. If the temporal projector
> is implemented as a Go worker, the §18 blocking prerequisite is **already
> satisfied**, which materially changes the CHAOS-3500/3503 dependency and the
> ownership question in §5.

**Compose / kind.** Root `compose.yml` declares 13 always-on services with
**no `profiles:` anywhere**; the opt-in profile precedent lives in
`deploy/docker-compose/compose.go-workers.yml:16,58,94,108` (`go-workers`) and
`compose.production.yml:400` (`pooler`). Ports taken: 5555, 6432, 8123, 9000,
6379, 8000, 8010, 8800. Project name pinned `name: dev-health-ops`
(`compose.yml:1-13`) after CHAOS-3142. Hard invariant: `worker-external-ingest`
is exactly one replica at concurrency 1 (`compose.yml:376-380`).
The kind stack is **not in ops** — it is `acr/scripts/e2e/kind-fixture.sh`
(config at `:637`, cluster create at `:657`), which stands up its own Postgres
and ClickHouse pods (`:803-959`) and does **not** reference either repo's
`compose.yml`. There is no compose↔kind mapping; they are parallel stacks.

---

## 14. Housekeeping items from the amendment

| Item | Result |
|---|---|
| CHAOS-3068/3069 tripwire: is `incidents.v1` still in `expected_unavailable_sources`? | **NO — clean.** `testdata/fullstack/v1/expected/task-001.oracle.json:13-15` and `task-002` are `[]`; `task-003:13-19` lists five sources, not `incidents.v1`; task-004/005 have no such field. The only occurrences are synthetic inline data in `assertrun_test.go:944,983-987`, which exist to prove the harness fails loudly on this exact class of drift (`layers.go:266`). |
| `deployments.v1` stale for the dev org since 2025-12-25 | **UNVERIFIED — requires a live environment.** No committed artifact in either repo contains the string `2025-12-25`. The mechanisms that would report it: ops `classify_staleness` (`api/services/sync_coverage.py:442-465`, grace `STALE_MINIMUM_GRACE=6h` / `STALE_FALLBACK_GRACE=48h` at `:39-40`), the Ask Dev freshness policy applying the 48h fallback to `deployments` (`api/dev/native_evidence.py:32-82`, `:71`), and ACR's per-request 24h rule (`source_catalog.go:164-168`, `assembler.go:17`). Verifying requires querying live Postgres `sync_runs` or ClickHouse `deployments`, which needs an authorized environment slot. **Reported as unverified rather than asserted.** |

---

## 15. Corrections this inventory requires to the PRD/TRD

Listed so the ADR does not silently inherit an inaccurate baseline. Each is
evidenced above.

| # | PRD claim | Finding | Section |
|---|---|---|---|
| 1 | `related_changes` bounded like `work_graph_neighbors`, "`max_results` capped at 100" | The precedent caps at **25** (`MAX_NEIGHBORS`), and lives in ops, not acr | §9 |
| 2 | Squash orgs have a near-empty `work_graph_pr_commit` | A deliberate two-tier squash handler exists (CHAOS-2435); the real gap is residual formats **with no coverage signal** | §10 |
| 3 | Declared-state history is "the one documented native gap" | There are **two**: declared state *and* relationship valid-time on `work_graph_edges` (no `valid_to`, RMT key omits `discovered_at`) | §5 |
| 4 | "No worker container currently exposes a scraped Prometheus endpoint" | True for Celery; **false for Go workers**, which already serve `/metrics` on :8080 | §13 |
| 5 | Extraction governed by "the same machinery" as Ask Dev narrative | True for the per-call guard; the **monthly cap tier needs new wiring** (Ask-Dev-namespaced Valkey keys, `dev_runs` recompute) | §11 |
| 6 | Org deletion is blind to stores outside the migration directory | Correct, and **wider**: Valkey allowance counters and `worker_job_outbox` (no `org_id` column) are already unreachable today | §12 |
| 7 | — (not stated) | `operational_service_repository_mappings.valid_from` is Nullable; `NULL <= as_of` is false, so null-start rows are dropped from every as-of answer | §4 |
| 8 | — (not stated) | A production transactional outbox (`worker_job_outbox`) already exists; the projector needs no new event mechanism | §13 |

---

## 16. What this inventory does not establish

- **No arm has been measured.** Nothing here says whether a temporal graph
  helps. That is the corpus's job.
- **Per-org ingestion coverage is unmeasured.** §14 records the mechanisms;
  the values need an authorized environment.
- **The `entitlements` cache TTL** (`acr/internal/entitlements/cache.go`) is a
  known revocation-lag path that has not been timed.
- **CHAOS-3500's "semantically equivalent" definition has not landed**, so the
  §16 rebuild gate has no operational definition yet. Per the amendment, the
  ADR must name which definition the trial ran against or flag the gap.
