# Architecture: Work-Item Team Attribution & Linked-Issue Inheritance

**Status:** Authoritative
**Scope:** dev-health-ops (metrics/compute, sync, loaders, providers)
**Related:** [data-pipeline.md](data-pipeline.md) (§4 Metrics → Work-item team attribution),
[investment-data-model.md](investment-data-model.md),
[team-catalog-source-of-truth.md](../api/team-catalog-source-of-truth.md)

> First slice of the system-wide architecture-documentation epic. Documents how
> every work item (issue, PR, MR) is stamped with a `team_id`, why PRs used to
> land as `unassigned`, and how cross-provider linked-issue inheritance recovers
> team attribution for the investment **allocation-coverage** and
> **team-exchange chord** views.

## Why this exists

Team resolution historically used three signals — the provider work scope
(repo / project key), the Linear/Jira project key, and assignee membership.
**A GitHub/GitLab PR matches none of them**: its repo rarely maps 1:1 to a
team, it has no project key, and its author often isn't a team member. So PRs
were stamped `team_id = 'unassigned'` and never shared a team dimension with
the issue trackers — leaving TEAM COVERAGE at 0% and the team-exchange chord
empty (no two teams ever co-occur on a work scope).

The fix adds a fourth, **provider-agnostic** tier: a work item with no team of
its own inherits the team of an issue it links to via `work_item_dependencies`.
A GitHub PR closing Linear `CHAOS-2400` borrows that issue's `CHAOS` team.

---

## 0. Target state (CHAOS-2600) — ClickHouse-only team attribution

> **Governing target contract.** This §0 is the source of truth for the intended model and the
> debugging navigation aid; **new code must follow it.** It is implemented across CHAOS-2600
> CS1–CS7 — the ClickHouse enum widening lands in **CS1** (see *Schema prerequisite* below), the
> precedence tests are inverted in **CS2**, and the legacy Postgres bridge path is removed in
> **CS5/CS6**. Until then, §1 below still describes the live (pre-CHAOS-2600) cascade and the
> existing tests still encode the old precedence.

> **CS6 reality (CHAOS-2607).** ClickHouse is the system of record for **both** the team
> catalog **and** identity→team membership. As of CS6 the Postgres `team_mappings` / `identity_mappings`
> tables and models are **deleted** (Alembic `0020`), along with the `TeamMappingService` /
> `IdentityMappingService` / `TeamDriftSyncService` classes, the `sync-team-drift` /
> `reconcile-team-members` tasks, and the Postgres-backed drift engine. (The four admin drift-review
> endpoints currently stand as HTTP 501 stubs and are being **rebuilt natively on ClickHouse — not
> deleted — under CHAOS-2622**; see §0.5. The earlier "removed in CS7 with the web caller —
> CHAOS-2608" intent is superseded — CHAOS-2608 is an unrelated Done web ticket.) (CS5 had already deleted the
> Postgres→ClickHouse team bridge `providers/team_bridge.py` and `providers/team_reconcile.py`.) Admin
> team/identity CRUD goes through `ClickHouseTeamAdminService` + `ClickHouseIdentityStore`, writing the
> ClickHouse `teams` and `identities` tables directly. Identity membership uses **surgical replacement**
> semantics: updating an identity removes its facets from teams it left and replaces changed facets in
> teams it stayed in, editing `teams.members` add/remove-by-facet (never a full recompute) so Auto
> Import / catalog members are preserved. See *CS6 status (CHAOS-2607)* at the end of §4.

**ClickHouse is the only source used for analytics attribution. Postgres does not store or resolve
team attribution mappings.** Manual mappings are ClickHouse fallback records only — never overrides,
never outranking WTI-native facts. PR/MR attribution comes from an **actual linked issue donor**; an
external issue-key *prefix* alone is not linked-issue inheritance.

Every final attribution carries provenance: `org_id, work_item_id, provider, team_id, team_name,
source, confidence, evidence, is_primary, computed_at`.
`source ∈ {native_team, issue_project, project_ownership, repo_ownership, assignee_membership,
linked_issue, manual_fallback, unassigned}`; `confidence ∈ {high, medium, low, manual, none}`.

> **Schema prerequisite (CS1).** The `issue_project` / `manual_fallback` sources and the `manual` /
> `none` confidence values require the ClickHouse `Enum8` widening on `work_item_team_attributions`
> (migration 053) to land **before** any resolver emits them — emitting an unknown enum value fails
> the insert. This is CHAOS-2600 ordering rule §4.1: migrate enums (CS1) → then emit (CS2/CS3).

### 0.1 Resolution decision tree

Resolution is **staged by precedence**. The resolver evaluates the applicable sources and persists
**all** matching ones as candidates; the *winner* (`is_primary`) is the highest-precedence source
present. "Wins" means *primary selection* — it does not mean lower-precedence sources go
unevaluated or unrecorded. **To debug:** read `team_attribution_source` (the winner) from
provenance, jump to that node, and verify no higher-precedence stage matched.

```mermaid
flowchart TD
    Start(["Work item"]) --> COLLECT["Evaluate EVERY applicable source → persist a candidate row per match (provenance).<br/>The linked_issue candidate requires a real work_item_dependencies donor row resolving to a team;<br/>a bare issue-key prefix produces NO linked_issue candidate (it may match a manual_fallback instead)."]
    COLLECT --> SEL{{"Select winner: is_primary = the highest-precedence candidate present"}}
    SEL --> NT{"0 · native_team candidate?"}
    NT -->|"yes"| Win["is_primary = matched source"]
    NT -->|"no"| IP{"1 · issue_project candidate?"}
    IP -->|"yes"| Win
    IP -->|"no"| PO{"2 · project_ownership candidate?"}
    PO -->|"yes"| Win
    PO -->|"no"| RO{"3 · repo_ownership candidate?"}
    RO -->|"yes"| Win
    RO -->|"no"| AM{"4 · assignee_membership candidate?"}
    AM -->|"yes"| Win
    AM -->|"no"| LK{"5 · linked_issue candidate?<br/>(real donor row resolving to a team)"}
    LK -->|"yes"| Win
    LK -->|"no"| MF{"6 · manual_fallback candidate?<br/>repo / project / member / issue_key_prefix"}
    MF -->|"yes"| Win
    MF -->|"no"| UN["is_primary = unassigned (7)"]
    Win --> P["Persist work_item_team_attributions:<br/>ALL candidate rows; is_primary on the winner"]
    UN --> P
    P --> API["Expose source / confidence / evidence via GraphQL"]
    API --> UI["Frontend renders only — no recompute"]
```

**Invariants:** the **winner is the highest-precedence matching source** (all matching sources are
still persisted as candidates — precedence decides `is_primary`, not which sources are computed);
`linked_issue` (5) requires a real `work_item_dependencies` donor row resolving to a `work_items`
row whose **own team came from a first-class fact (sources 0–4)** — a donor resolved only by
`manual_fallback` is NOT a valid donor, so a bare prefix can never be laundered into rank-5
inheritance (it falls through to 6); `manual_fallback` (6) can only beat `unassigned`; a whole org
at `unassigned` usually means the ClickHouse `teams` dimension is empty.

### 0.2 Source reference matrix

| # | `source` | Resolves from (ClickHouse) | Confidence | Beats | Never overrides | Evidence keys |
|--:|---|---|---|---|---|---|
| 0 | `native_team` | `WorkItem.native_team_key` → `teams` | high | all below | — (top) | `native_team_key` |
| 1 | `issue_project` | native issue project → owning team | high | 2–7 | 0 | `project_id, owner_team` |
| 2 | `project_ownership` | `team_project_ownership` | high | 3–7 | 0–1 | `project_id, provider` |
| 3 | `repo_ownership` | `team_repo_ownership` | medium | 4–7 | 0–2 | `repo_full_name` |
| 4 | `assignee_membership` | `team_memberships` (assignee identity) | medium | 5–7 | 0–3 | `member_id, identity` |
| 5 | `linked_issue` | `work_item_dependencies` donor → donor's team | medium | 6–7 | 0–4 | `dependency_type, donor_work_item_id, donor_provider` |
| 6 | `manual_fallback` | `manual_attribution_fallbacks` (repo/project/member/issue_key_prefix) | manual\|low | 7 only | 0–5 | `scope_type, scope_id, reason` |
| 7 | `unassigned` | — (nothing matched) | none | — (floor) | — | `reason` |

### 0.3 Off-the-rails matrix (symptom → diagnosis → fix)

| Symptom | Likely stage | Diagnose | Fix |
|---|---|---|---|
| A whole org is `unassigned` | 7 (floor) | `get_all_teams()` empty? CH `teams` populated for `org_id`? | re-home teams population; verify daily-chain order |
| PR attributed to a surprising team via `linked_issue` | 5 | which `work_item_dependencies` edge? donor's own team? extkey ambiguous? | confirm donor row + `_canonical_target`; check `_INHERITABLE_RELATIONSHIP_TYPES` |
| `manual_fallback` beats a real team | precedence | `_SOURCE_ORDER` has `manual_fallback=6`? loader merging manual at the wrong rank? | restore rank — manual is the lowest non-unassigned tier |
| A bare prefix (e.g. `CHAOS`) attributes as `linked_issue` | 5 vs 6 | did a full key resolve to a real `work_items` row, or did a prefix shortcut leak in? | no prefix→team in `linked_issue`; route to manual `issue_key_prefix` |
| A PR inherits via `linked_issue` from a donor that only has a `manual_fallback` (e.g. `issue_key_prefix`) rule | 5 (donor) | is the donor's *primary* source in 0–4? a rank-6 fallback must never be relabeled rank-5 | donors gated to `_DONOR_SOURCES` (0–4) in `build_linked_issue_team_resolver`; a manual-only donor is never a linked_issue donor (done CS3) |
| Same scope shows duplicate ownership candidates / bloats over time | RMT read | `valid_from` is in the ownership tables' `ORDER BY`, so `FINAL` cannot collapse re-imports (each daily run is a new sort key) | reads dedup per *logical* scope via `argMax((updated_at, valid_from))`, NOT `FINAL` (done CS3, `load_team_attribution_context`); manual-fallback read keeps `FINAL` (its sort key has no `valid_from`) |
| Team flips / stale team lingers after a re-org | write side | ownership writers set `valid_from=now` but never `valid_to`, so a reassigned scope keeps the old-team row active; readers can't tell stale from co-ownership | needs writer-side `valid_to` expiry on re-derivation — tracked **CHAOS-2610** (read-side `argMax` already makes the newest the primary by recency tiebreak) |
| `manual_fallback` resolves the wrong team | scope match | which `manual_attribution_fallbacks` row matched (repo/project/member/issue_key_prefix)? | check `_manual_fallback_candidates` scope match + rule `priority`; manual is rank 6 (done CS3) |
| Provenance absent in the API | GraphQL | resolver SELECTs the provenance columns? SDL has the fields? | expose `source/confidence/evidence` |
| Web shows a different team than the backend | client recompute | any client-side mapping derived from `evidence`? | render-only; delete client derivation |

> Full data-flow and data-object-hierarchy diagrams: see the CHAOS-2600 plan §1.6–1.7 / `team-flow.md`.

### 0.4 Provider coverage contract (attribution is provider-agnostic)

Attribution is **provider-agnostic** — the resolver and precedence (§0.1) never branch on provider.
That is a **testable contract**: every WTI provider × every normalized entity must be covered, not
just Linear. **Attribution changes MUST keep this matrix green; never add Linear-only coverage.**

| provider \ entity | teams | projects | members | issues |
|---|---|---|---|---|
| jira   | yes | yes | yes | yes |
| gitlab | yes | yes     | yes  | yes |
| github | yes     | n/a¹    | yes     | yes |
| linear | yes     | yes | yes     | yes |

`yes` = normalized in src AND asserted in tests · `partial` = only sink/integration assertion (no
unit test of the normalizer) · `no` = normalized but output never asserted · `n/a` = provider does
not natively produce this entity. ¹ GitHub has no native Project entity (the repo is the scope).

> **The matrix above tracks TEST coverage, not whether the data is pulled.** Functionally we ingest teams, projects, and members for *every* provider that supports them (auto-import, when the option is selected). Don't read a `partial`/`no` cell as "not consumed" — it means "not yet asserted."

#### 0.4a Provider × entity **consumption** (functional — what `run_team_autoimport` actually pulls)

| provider | teams | projects | members | repo ownership | member store written |
|---|---|---|---|---|---|
| linear | ✓ `discover_linear` | ✓ `associations.project_keys` | ✓ `discover_members_linear` | — | edges **+ roster** |
| jira   | ✓ `discover_jira` | ✓ `associations.project_keys` | ✓ `discover_members_jira_bulk` | — | edges **+ roster** |
| github | ✓ `discover_github` | n/a (repo = scope) | ✓ `discover_members_github` | ✓ `team_repo_ownership` | edges **+ roster** (this CS) |
| gitlab | ✓ `discover_gitlab` | ✓ (GitLab project paths) | ✓ `discover_members_gitlab` | — | edges **+ roster** (this CS) |

One path: `run_team_autoimport` → `team_autoimport_<provider>.populate()` → `discover_*` → ClickHouse. (`LinearClient.iter_projects` is vestigial dead code, never a path.) **Two member representations — do not conflate:** `team_memberships` (edges) = canonical attribution source, read by the ladder, all 4 providers; `teams.members` (roster) = secondary resolver + admin/display, this CS populates it for github/gitlab too. **Chain:** members → assignee identity → issues → PRs/MRs → (maybe) commits; commit authors are a separate git-side source, member↔author reconciliation deferred (not CHAOS-2600).

> **Identity must match what the assignee path produces — UNDER THE ORG ALIAS MAP (CHAOS-2609).**
> Both consumers key on the *resolver-consumed* identity. Auto-import resolves each member through the
> **same** `IdentityResolver` the assignee path uses — `load_identity_resolver()` (the global
> `identity_mapping.yaml` / `IDENTITY_MAPPING_PATH`) — via `IdentityResolver.membership_facets`, so an
> **aliased** member resolves to the **same canonical identity** an aliased assignee does (e.g.
> `github:lead` → `lead@example.com`), and a non-aliased member stays `github:<login>` /
> `gitlab:<username>` / `jira:accountid:<account_id>`. Deriving the identity directly (bypassing the
> alias map) is the bug that broke aliased orgs. `membership_facets` returns *every* identity an
> assignee for this member could resolve to — the no-email identity, the provider-qualified id, AND
> (when the member has an email) the resolver-mapped canonical + normalized **email**. ALL of them are
> persisted to the `team_memberships.identity_facets` `Array(String)` column (migration **060**); the
> loader `argMax`-reads it and fans **every** facet into the ladder's `member_by_identity` (alongside
> the legacy `raw_provider_user_id` = `facets[0]` + `raw_email` slots), **and** writes them to the
> `teams.members` roster (read by `TeamResolver`). This closes the deferred
> **email-alias-distinct-canonical** edge (**CHAOS-2625**): when an org maps a member's provider id and
> email to *different* canonicals (`github:lead` → canonicalA, `personal@…` → canonicalB), an assignee
> resolving to canonicalB now hits the canonical ladder directly with `assignee_membership` provenance
> instead of the weaker roster fallback. The `member_id` **primary** keeps its `gh:`/`gl:`/`jira:<id>`
> form (untouched — it is the ReplacingMergeTree dedup key). A `members` cell is `yes` only when this
> end-to-end resolution is **proven** (a no-email assignee — aliased AND non-aliased — resolves to the
> auto-imported team via *both* paths —
> `tests/workers/test_team_autoimport_e2e_sync_surface.py`), not when a row is merely written.

- **Resolver row (CS2):** the precedence resolver (`resolve_team_attribution`) is exercised for all
  four providers — Linear (`test_issue_project_wins_over_linked_issue`,
  `test_assignee_membership_wins_over_linked_issue`), GitHub (`gh:` items in
  `test_project_ownership_wins_over_linked_issue` / `test_repo_ownership_wins_over_linked_issue`),
  GitLab (`test_gitlab_mr_resolver_precedence_with_gitlab_donor` — MR as item + GitLab issue as
  same-provider donor), Jira (`test_jira_issue_project_wins_over_linked_issue`,
  `test_assignee_membership_wins_over_jira_linked_donor`). (Provider *link-capture* — distinct from
  the resolver — is also tested per provider, e.g. `test_gitlab_captures_external_key_*`.)
- **Chart and drilldown team attribution:** Investment Sankey, GraphQL TEAM
  flow-matrix/chord, GraphQL REPO flow-matrix's cross-repo team bridge, team
  Cycle Time × Throughput quadrant axes, work-unit investment team evidence,
  issue drilldowns, and flame issue details read the primary
  `work_item_team_attributions` snapshot before rolling up or displaying team
  identity. Cycle-time rows can still provide activity windows, durations,
  work-scope/repo/type bridges, and unassigned/no-WITA detail rows, but not the
  owning team identity. Person cohort selection reads ClickHouse `identities`
  membership (`team_ids`) instead of metric rollup team snapshots so a person's
  current team comparison does not lag behind admin/team-autoimport membership.
- **Why it matters:** the team/project/member **dimension** is populated by the per-provider
  team/project/member sync. **"Auto Import" is a UX option** (checkboxes to import teams, projects,
  and members from an integration → `run_team_autoimport`, writing ClickHouse directly); manual
  fallback is the separate explicit-override option. Because jira/github/gitlab work items carry
  `native_team_key = None` (only Linear sets it real), non-Linear attribution depends *entirely* on
  this dimension — so its coverage cells are the highest-risk. (CHAOS-2600 does not change these
  sync ops; CS5 removes only the Postgres bridge.)
- **Open gaps → CLOSED by CHAOS-2609 (CS-COV):** the dimension's test holes are now asserted —
  gitlab/members (was normalized but never asserted), gitlab epics (`gitlab_epic_to_work_item`), jira
  team/member coverage (403-skip + member de-dupe), linear **and** jira native `ProjectRecord` fields
  (linear native projects ARE ingested via `team.associations.project_keys` — the prior "not ingested"
  note was wrong; it was only a *test* gap, now closed), and gitlab nested-subgroup specificity.
  **Plus an attribution-correctness fix:** github/gitlab/jira auto-import now write the
  *resolver-consumed* member identity (see the §0.4a identity callout), so a no-email assignee actually
  resolves to its team via both the canonical ladder and the roster — previously the roster stored a
  bare login the resolver never matched, so member attribution silently missed for no-email
  github/gitlab/jira assignees. The matrix above is the source of truth for what is/ isn't proven.
- **Email-alias-distinct-canonical edge → CLOSED by CHAOS-2625:** the canonical ladder now indexes
  *every* facet a member resolves to via the `team_memberships.identity_facets` `Array(String)` column
  (migration 060) + loader fan-out, so a member mapped to *two different* canonicals (provider id →
  canonicalA, email → canonicalB) attributes via the ladder on **either** canonical — previously only
  `facets[0]` + `raw_email` were indexed, so an assignee resolving to canonicalB missed the ladder and
  fell back to the weaker roster path. Proven in `tests/test_team_autoimport_executor.py` (canonicalB
  ladder hit) + the provider×entity writer assertions in
  `tests/workers/test_team_autoimport_{github_gitlab,jira,linear}.py`.

### 0.5 Drift-review reconciliation (CHAOS-2622) — rebuilt on ClickHouse

The CHAOS-2600 migration dropped the Postgres-backed **drift-review** surface as collateral: the
admin workflow that detects when provider discovery disagrees with the curated/manual config and
lets an admin approve or dismiss each change. It was built on Postgres `TeamMapping` columns
(`flagged_changes` / `sync_policy` / `managed_fields`) + `TeamDriftSyncService`, all deleted in CS6,
leaving the four admin endpoints as HTTP 501 stubs. **CHAOS-2622 rebuilds it natively on ClickHouse
— the four endpoints and the web `PendingChangesPanel` are NOT deleted.** (The earlier "removed in
CS7 with the web caller — CHAOS-2608" intent is superseded; CHAOS-2608 is an unrelated Done web
ticket that never touched these endpoints.)

**Provider-observed vs curated split.** A faithful rebuild re-separates the two layers that Postgres
held in `TeamMapping` and that the CH `teams` `ReplacingMergeTree` collapsed into a single curated
row. Three sidecar `ReplacingMergeTree` tables hold the review state, while `teams FINAL` stays the
resolved catalog every reader (§0.1–0.2) keeps using:

| Table | Role |
|---|---|
| `team_sync_policies` | per-team drift policy sidecar (`sync_policy`, `managed_fields`); kept off `teams` because `ClickHouseTeamAdminService.create_or_update` rewrites the whole team row (`provider=""`, `native_team_key=None`) on every update and would clobber any policy stored there |
| `team_provider_observations` | provider-observed truth layer — what discovery last saw, keyed `(org_id, provider, native_team_key)` |
| `team_drift_changes` | pending-review read model (decision table) keyed `(org_id, change_id)`; `status ∈ pending / approved / dismissed / resolved / superseded` |

**Policy (low-cardinality, default-safe).** `sync_policy = 0` (auto-apply) is the default, so
existing orgs see **no behavior change** — discovery writes straight to `teams`. Only `policy 1`
(flag-for-review) routes managed-field changes (`name`, `description`, `project_keys`,
`repo_patterns`; `members` is a display-only catalog field this slice and does **not** yet gate
`team_memberships`) into the pending lane instead of clobbering the catalog. `policy 2` is
manual/none. `status` / `change_type` are low-cardinality strings, not `Enum8`, to avoid
enum-widening migration ordering before new values can be emitted.

**Drift-aware projector.** The final team write in the four auto-importers
(`workers/team_autoimport_{github,gitlab,jira,linear}.py`) **and**
`ClickHouseTeamAdminService.import_teams` route through one projector instead of scattering policy
logic: it always records the latest `team_provider_observations` row, reads the team's policy, then
either applies observed values into `teams` (policy 0, current behavior) or emits/refreshes a
`team_drift_changes` pending row per changed managed field (policy 1).

**`change_id` value-fingerprint + lifecycle (correctness-critical).** `change_id =
hash(org_id, entity_type, entity_id, change_type, field, old_value_json, new_value_json)` —
it fingerprints the *values*, not just `(team, field)`, so a dismissed `A→B` does not wrongly
suppress a later `A→C`. The projector enforces:

- **No resurrection** — if a `change_id` already exists as `dismissed` or `approved`, do NOT
  re-insert a `pending` row for the same value pair; only a *different* fingerprint creates new
  pending drift.
- **Supersede** — a provider value change for the same `(team, field)` marks the prior `pending`
  row `superseded` and inserts a new `pending` row.
- **Resolve** — drift that disappears from discovery marks stale `pending` rows `resolved`.

**Endpoints repointed by `change_id`.** `ClickHouseTeamDriftService` backs the four endpoints over
`team_drift_changes FINAL`: `GET /admin/teams/pending-changes` lists flagged drift; approve/dismiss
act **by `change_id`** (`{change_ids: [...], approve_all|dismiss_all}`, replacing the old racy
index-based wire) — approve applies the observed value into `teams` via `create_or_update` and marks
the change `approved`, dismiss marks it `dismissed` (catalog unchanged); `POST
/admin/teams/trigger-drift-sync` dispatches the `sync_team_drift` Celery task on the `sync` queue
(worker-supplied provider credentials). The web side adds `FlaggedChange.change_id` and sends
`change_ids`. All three tables join the org-deletion purge path.

> **Identities/members slice (sequenced second).** Member/identity drift +
> `manual_attribution_fallbacks(scope_type='member')` reconciliation reuses `team_drift_changes` via
> `entity_type='identity'` (or a parallel `identity_drift_changes` table if the shape diverges) and
> must gate the `team_memberships` attribution dimension — not just the `teams.members` roster —
> applying membership changes surgically by facet (never a full recompute). See the CHAOS-2622 plan §4.4.

---

## 1. Attribution cascade (decision flow)

> **Implemented model: see §0 (CHAOS-2600).** As of CS2 the resolver applies the 8-source staged
> precedence in §0 (`native_team > issue_project > project_ownership > repo_ownership >
> assignee_membership > linked_issue > manual_fallback > unassigned`) — `linked_issue` is now a true
> fallback below ownership/assignee, and the issue's own project key resolves as `issue_project`.
> The 4-tier cascade below predates that change and is kept for historical context; where they
> differ, **§0 governs**.

`resolve_base_team()` runs tiers 1–3; the linked-issue resolver is tier 4. The
first match wins and nothing ever overrides a real team.

```mermaid
flowchart TD
    A["Work item"] --> B{"Tier 1: ProjectKeyTeamResolver<br/>resolve(work_scope_id)"}
    B -- match --> T["team_id"]
    B -- miss --> C{"Tier 2: retry with project_key<br/>(Linear TEAM key)"}
    C -- match --> T
    C -- miss --> D{"Tier 3: assignee membership<br/>assignee in ClickHouse teams.members?"}
    D -- match --> T
    D -- miss --> E{"Tier 4: LinkedIssueTeamResolver<br/>linked donor issue has a team?"}
    E -- match --> T
    E -- miss --> U["normalize to 'unassigned'"]

    T --> N["normalize_team_id / normalize_team_name"]
    U --> N
    N --> R[("stamp team_id on the row")]
```

**Inheritance is gated**, so it never imports a wrong team:
- only **inheritance-safe** relationship types transfer a team
  (`relates_to`, `relates`, `duplicates`, `external_issue_key`); blocking links
  (`blocks` / `blocked_by`) routinely span teams and are ignored;
- a cross-provider `extkey:KEY` that exists in **both** Linear and Jira is
  ambiguous and dropped;
- multiple donors → the lexicographically smallest canonical target wins
  (stable, since ClickHouse rows are unordered);
- per `(source,target)` the **latest** edge by `last_synced` wins, so a flip
  from `relates_to` to `blocked_by` stops inheriting.

---

## 2. Cross-provider link capture & inheritance (sequence)

Edges are captured during sync; the resolver is built once per run and applied
to every work-item metric family.

```mermaid
sequenceDiagram
    autonumber
    participant Prov as Provider API (GitHub/GitLab/Jira)
    participant Norm as Normalizer (providers normalize)
    participant Job as job_work_items (sync)
    participant CH as ClickHouse
    participant Build as build_linked_issue_team_resolver
    participant Comp as compute_work_item_metrics_daily

    Prov->>Norm: issues / PRs / MRs
    Norm->>Norm: extract WorkItems + WorkItemDependency edges
    Note over Norm: PR body magic-words + head branch to extkey:KEY;<br/>keyword sets relationship_type (blocking stays non-inheritable)
    Norm-->>Job: work_items, dependencies
    Job->>Job: stamp org_id on items, transitions AND dependencies
    Job->>CH: write_work_items / write_work_item_dependencies
    Job->>CH: load donor items for fresh-edge targets (bounded, FINAL, org-scoped)
    Job->>Build: work_items (synced plus donors), fresh edges
    Build->>Build: resolve_base_team per item to donor_team map + key_index
    Build->>Build: collapse edges by source,target latest; apply relationship allowlist
    Build-->>Job: LinkedIssueTeamResolver
    loop each day in window
        Job->>Comp: work_items, transitions, linked_issue_resolver
        Comp->>CH: write work_item_cycle_times (team_id stamped)
    end
```

`job_daily` (the scheduled recompute) follows the same build → compute path but
**reads** persisted edges instead of extracting them — see §4.

### Link capture sources & precedence

A PR/MR only inherits a team if an edge to its issue exists. The link is
captured from where it actually lives, in descending order of authority (PR
#924 — the primary/secondary sources; #921 added the tertiary):

| Tier | Source | Trust gate | Edge |
|---|---|---|---|
| Primary | **Linear issue attachment** (the integration's PR/MR link) | integration `sourceType` **AND** allowlisted host (public SaaS + `LINEAR_TRUSTED_SCM_HOSTS`) | `ghpr:…`/`gitlab:… → linear:KEY` (direct id) |
| Secondary | **GitHub PR comment** (the Linear bot's linkback) | exact `linear[bot]` actor (`GITHUB_LINEAR_LINKBACK_BOTS`) + `linear.app` URL | `ghpr:… → extkey:KEY` |
| Tertiary | **PR body / head branch** (the author's own ref) | magic-word / Linear branch convention | `ghpr:… → extkey:KEY` |

The authoritative link runs **Linear → source control** (the issue's attachment
points at the PR/MR), so the edge is emitted with the PR/MR as the *source* and
the team-bearing issue as the *target* — fitting the source-inherits-from-target
resolver unchanged. **Accepted residual:** a trusted org member linking a real
PR to their own issue drives that PR's attribution — the feature working as
intended on collaborative data, not a forgery (same-org analytics, not an authz
boundary).

---

## 3. Data flow & relationships (ER)

```mermaid
erDiagram
    work_items ||--o{ work_item_dependencies : "source of edges"
    work_items ||--o{ work_item_cycle_times : "completed to cycle row"
    work_items ||--o{ work_item_team_attributions : "primary attribution candidates"
    work_item_dependencies }o--|| work_items : "target or extkey to donor issue"
    teams ||--o{ work_item_team_attributions : "team_id"
    work_item_team_attributions ||--o{ investment_coverage : "team/repo coverage %"
    work_item_team_attributions ||--o{ team_exchange_chord : "team identity"
    work_item_cycle_times ||--o{ team_exchange_chord : "activity/day/scope bridge"

    work_items {
        string work_item_id PK
        string provider
        string project_key
        string project_id
        uuid   repo_id
        string org_id
    }
    work_item_dependencies {
        string source_work_item_id
        string target_work_item_id "id or extkey:KEY"
        string relationship_type
        datetime last_synced
        string org_id
    }
    work_item_cycle_times {
        string work_item_id
        string work_scope_id
        date   day
        string org_id
    }
    work_item_team_attributions {
        string work_item_id
        string team_id "latest primary owner"
        string source
        uint8  is_primary
        datetime computed_at
        string org_id
    }
    teams {
        string id PK
        string org_id
        string project_keys
    }
```

Coverage and team-identity hydration read latest primary rows from
`work_item_team_attributions`. Cycle-time rows can still provide activity dates,
durations, and co-occurrence bridges, but they are not the owning team source.

---

## 4. Component & job map (who reads/writes what)

Two jobs build the resolver. Both are **tenant-scoped** (org-wide reads only
under an explicit `org_id`) and **bounded** (never a full-history scan).

```mermaid
flowchart LR
    subgraph providers ["Providers"]
        GH["github/normalize"]
        GL["gitlab/normalize"]
        JI["jira normalize"]
    end

    subgraph sync ["job_work_items — sync"]
        S1["extract items + extkey edges"]
        S2["stamp org_id + write"]
        S3["load bounded donors<br/>(fresh edges authoritative)"]
        S4["build resolver"]
        S5["compute cycle_times + state_durations<br/>+ issue-type/investment via _get_team"]
    end

    subgraph daily ["job_daily — scheduled recompute"]
        D1["load run-window work items"]
        D2["load_work_item_dependencies(source_ids)<br/>bounded + FINAL"]
        D3["load_work_item_dependencies_donors<br/>by referenced id/key"]
        D4["build resolver"]
        D5["compute cycle_times + state_durations"]
    end

    GH --> S1
    GL --> S1
    JI --> S1
    S1 --> S2 --> S3 --> S4 --> S5

    D1 --> D2 --> D3 --> D4 --> D5

    CH[("ClickHouse:<br/>work_items, work_item_dependencies,<br/>work_item_cycle_times,<br/>teams, identities")]

    S2 -->|write| CH
    S3 -->|read| CH
    S5 -->|write| CH
    D1 -->|read| CH
    D2 -->|read| CH
    D3 -->|read| CH
    D5 -->|write| CH
    CH -. team resolvers .-> S4
    CH -. team resolvers .-> D4
```

> **No Postgres in the team/identity path (CHAOS-2600).** The team resolvers read ClickHouse
> `teams` / `identities` (and the ownership dimensions). The Postgres `team_mappings` /
> `identity_mappings` tables and their models/services were dropped in CS6 (CHAOS-2607); the
> Postgres→ClickHouse bridge (`team_bridge.py`), `team_reconcile.py`, the `sync-team-drift` /
> `reconcile-team-members` tasks are all deleted; the four admin drift-review endpoints remain as HTTP
> 501 stubs until CS7 (CHAOS-2608). Admin
> team/identity CRUD writes ClickHouse via `ClickHouseTeamAdminService` / `ClickHouseIdentityStore`;
> identity membership is edited surgically (add/remove-by-facet) so Auto Import members are preserved.

**Key boundary differences**

### Manual QA: auto-imported ownership coverage

Use this check when validating CHAOS-2401/2547 against a real tenant. It proves
the sync surface fills the ClickHouse ownership dimensions that the attribution
resolver reads, then verifies the user-visible Investment → Allocation coverage
does not collapse to `unassigned`.

1. In Admin → Sync, create or edit a real Linear work-items sync and enable
   **Auto-import teams, projects & members** (`sync_options.auto_import_teams=true`).
2. Trigger the sync through the sync-config UI or worker-backed trigger endpoint
   so the configured worker credentials are used.
3. After the sync succeeds, run daily metrics with the same analytics database:

   ```bash
   CLICKHOUSE_URI=clickhouse://... dev-hops metrics daily
   ```

4. Open `dev-health-web` in a real browser (Playwright is preferred for evidence)
   and navigate to **Investment → Allocation**.
5. Verify team coverage is greater than 0% and the allocation view includes named
   teams from the Linear import, not only `unassigned`.
6. Optional SQL spot-checks against ClickHouse before opening the browser
   (replace `<org_id>` with the tenant being verified):

   ```sql
   SELECT count() FROM projects WHERE org_id = '<org_id>' AND provider = 'linear';
   SELECT count() FROM members WHERE org_id = '<org_id>';
   SELECT count() FROM team_memberships WHERE org_id = '<org_id>' AND provider = 'linear';
   SELECT count() FROM team_project_ownership WHERE org_id = '<org_id>' AND provider = 'linear';
   SELECT team_id, count() FROM work_item_team_attributions FINAL
   WHERE org_id = '<org_id>'
     AND is_primary = 1
     AND (work_item_id, computed_at) IN (
       SELECT work_item_id, max(computed_at)
       FROM work_item_team_attributions
       WHERE org_id = '<org_id>'
       GROUP BY work_item_id
     )
   GROUP BY team_id;
   ```

| Aspect | `job_work_items` (sync) | `job_daily` (recompute) |
|---|---|---|
| Edge source | freshly extracted (authoritative) | persisted, `FINAL`, bounded by run-window source ids |
| Removed link | absent on re-extract → stops inheriting | persists until next sync re-stamps (see limitation) |
| Donor items | bounded to fresh-edge targets | bounded to referenced targets |
| Tenant scope | reads only when `org_id` set | reads only when `org_id` set |

> **Known limitation.** `work_item_dependencies` is an append-only
> `ReplacingMergeTree` with no tombstone, so a *removed* link is not deleted. A
> standalone `job_daily` recompute between syncs can keep honoring it until the
> next sync re-extracts the source. A link-lifecycle/tombstone (which also
> affects the work-graph) is a tracked follow-up.

### CS6 status (CHAOS-2607)

- **Drift-review implementation is removed; endpoints kept as 501 stubs.** The Postgres-backed drift
  engine (`TeamDriftSyncService` + the `TeamMapping` flagged-changes substrate) is **deleted** in CS6.
  The four admin drift-review endpoints (`GET /teams/pending-changes`,
  `POST /teams/{id}/approve-changes`, `/dismiss-changes`, `POST /teams/trigger-drift-sync`) **remain as
  HTTP 501 compatibility stubs** so the web admin keeps getting a clean 501; they are removed together
  with the web caller (`PendingChangesPanel`) in CS7 — see **CHAOS-2608**. A ClickHouse-backed
  drift-review rebuild is tracked separately by **CHAOS-2622**.
- **Postgres mapping deletion is done.** The `TeamMappingService` / `IdentityMappingService` /
  `TeamDriftSyncService` classes, the dead `JiraActivityInferenceService.match_and_confirm` /
  `TeamMembershipService.confirm_links` paths, the `sync-team-drift` / `reconcile-team-members` tasks,
  and the Postgres `TeamMapping` / `IdentityMapping` models + tables are all **deleted in CS6** (Alembic
  `0020` drops the tables).
- **Known limitations.** (1) `ClickHouseTeamAdminService.add_members` has a read-modify-write
  lost-update window under concurrent admin edits (deferred — admin surface is low-concurrency).
  (2) The surgical facet remove can rarely over-remove a **shared facet** when two distinct
  identities share a facet value and one is updated — for a shared **`email`** (the common case,
  e.g. two records carrying the same address) or, for email-less identities, a shared
  **`display_name`**; provider-ids (which are unique per identity, enforced by the confirm-path
  409 check) are unaffected. Deferred — same low-concurrency bucket as the lost-update.
  (3) Confirm-path membership writes are **non-transactional across teams**: ClickHouse has no
  multi-statement transactions, so the two-pass design makes only the **validation** all-or-nothing
  (a 409/404 leaves zero mutations). A ClickHouse error *mid-apply* (PASS 2) returns 500 with a
  possible partial `team.members` / identity-record update; re-running the confirm is idempotent.

---

## 5. Recovery / backfill runbook

After deploying the inheritance + capture changes, existing orgs need a
**recompute** to populate `team_id` on historical rows — there is **no schema
migration**, only a data replay.

### Why a plain backfill is not enough

The investment **allocation** views derive team at *query time*: the coverage %, 
team-exchange chord, team Cycle Time × Throughput quadrant, and work-unit
investment evidence read `work_unit_investments` / cycle-time activity and join
latest primary `work_item_team_attributions` rows for team identity. So three
things must be true, and the backfill **runner only re-runs
`run_work_items_sync_job` — it does NOT fan out** to the work-graph or investment
jobs (only the live sync path chains those). They must be triggered explicitly.

```mermaid
flowchart TD
    DEP["1. Merge + deploy (#921, #923, #924)"] --> SYNC
    subgraph SYNC ["2. Work-items sync/backfill — ALL providers"]
        L["Linear (issues + attachment edges)"]
        G["GitHub / GitLab (PRs/MRs + comment/body edges)"]
    end
    SYNC --> CT["work_item_dependencies (extkey/attachment edges)<br/>+ work_item_team_attributions (latest primary owner)<br/>+ work_item_cycle_times (activity bridge)"]
    CT --> WG["3. work-graph build"]
    WG --> IM["4. investment materialize (--force)"]
    IM --> Q["5. allocation coverage % + chord<br/>recover via query-time join to primary attribution"]
```

### Ordered steps (per affected org)

1. **Merge + deploy** #921 (mechanism), #923 (backfill CLI), #924 (capture).
2. **Backfill all providers** — Linear **and** GitHub/GitLab. Linear-only does
   nothing: the PR/MR rows and their edges come from the git providers, and the
   donor issues come from Linear. A single `--provider all` run (or per-provider
   with Linear synced so its issues are present) writes the edges and recomputes
   `work_item_team_attributions`. The org is derived from the sync config
   (#923), so `--org` is optional.
3. **Work-graph build**, then
4. **Investment materialize (`--force`)** — these rebuild `work_unit_investments`
   + its `structural_evidence_json.issues` (the coverage join keys); the backfill
   does not trigger them.
5. **Verify & recover** — the coverage %, chord, team Cycle Time × Throughput
   quadrant, and work-unit investment evidence recover automatically via the
   query-time join to primary attribution. Confirm the links were captured:

   ```sql
   SELECT relationship_type_raw, count()
   FROM work_item_dependencies FINAL
   WHERE org_id = {org}
     AND relationship_type_raw IN
         ('linear_attachment', 'github_comment_linear_url', 'external_issue_key')
   GROUP BY relationship_type_raw
   ```

   Zero `linear_attachment` rows after a Linear backfill means the org's issues
   carry no integration PR/MR attachments — there is then no link to inherit
   from, and an empty chord is **correct** (data-driven), not a bug.

> Exact CLI flags vary per command — confirm with `<cmd> --help`. The relevant
> entry points: `sync work-items` / `backfill run` → `run_work_items_sync_job`;
> `work-graph build` → `run_work_graph_build`; `investment materialize` →
> `run_investment_materialize`; `metrics daily` → `run_daily_metrics`.

---

## Source map

| Concern | Location |
|---|---|
| Attribution cascade + resolver builder | `metrics/compute_work_items.py` (`resolve_base_team`, `build_linked_issue_team_resolver`) |
| Resolver type | `providers/teams.py` (`LinkedIssueTeamResolver`, `ProjectKeyTeamResolver`, `TeamResolver`) |
| State-duration parity | `metrics/compute_work_item_state_durations.py` |
| Sync wiring | `metrics/job_work_items.py` |
| Scheduled recompute wiring | `metrics/job_daily.py` |
| Bounded donor/edge loads | `metrics/loaders/clickhouse.py` (`load_work_item_dependencies`, `load_work_item_dependencies_donors`) |
| Linear attachment capture (primary) | `providers/linear/normalize.py` (`extract_linear_dependencies`, `_is_scm_attachment`), `providers/linear/client.py` (`get_issue_attachments`) |
| GitHub comment / body capture | `providers/github/normalize.py` (`extract_github_comment_dependencies`, `extract_github_dependencies`) |
| GitLab capture | `providers/gitlab/normalize.py` |
| Recovery runbook | §5 above; backfill `backfill/runner.py`, investment `workers/work_graph_tasks.py` |
| Tests | `tests/test_linked_issue_team_inheritance.py`, `tests/test_pr_issue_link_capture.py` |
