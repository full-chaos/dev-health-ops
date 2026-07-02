# External-ingest sink writes (CHAOS-2698)

Part of the [customer-push ingestion epic](adr-003-external-ingest-rest-boundary.md)
(CHAOS-2690). This doc records the sink-write-layer decisions (D1-D8): given a
schema-validated, kind-normalized batch (CHAOS-2697's worker output), how
`external_ingest/sinks.py::write_batch()` stamps provenance and writes each of
the 9 v1 record kinds through the **existing** ClickHouse sink methods
(`storage/clickhouse.py::ClickHouseStore`,
`metrics/sinks/clickhouse::ClickHouseMetricsSink`), preserving current
append/ReplacingMergeTree-dedup semantics.

Module map: `external_ingest/ids.py` (server-side ID derivation),
`external_ingest/types.py` (`NormalizedBatch` / `SinkWriteResult` /
`SinkWriteError` / `AffectedScope`), `external_ingest/sinks.py`
(`write_batch()` and the per-kind row builders).

## D1 — Provenance column, not a `provider` value

A new nullable `source_id UUID` column (migration
`065_external_ingest_source_id.sql`) was added to the 9 tables this layer
writes: `repos`, `git_commits`, `git_pull_requests`,
`git_pull_request_reviews`, `teams`, `identities`, `work_items`,
`work_item_transitions`, `work_item_dependencies`. Every row this layer
writes stamps `source_id` to the resolved `external_ingest_sources.id`
(CHAOS-2696, Postgres migration 0032); every native-sync row leaves it
`NULL`. `repos.provider` / `teams.provider` / `work_items.provider` keep
meaning "which upstream system" (github/gitlab/jira/linear/custom) —
overloading them with `"customer_push"` would flip provider on an RMT
replace and break every provider-branching reader (master-spec CC8; this
was an explicitly refuted design from an earlier draft). `Repo.provider`
gains `"custom"` as a legal value for custom-system repos; readers must
tolerate it (covered by `test_repository_round_trip_and_repo_identity_handoff`
—no crash on a `provider="custom"` row, though no reader-side assertion was
added since no existing reader branches on `provider` in a way that would
reject an unrecognized value).

Two of the 9 tables required more than an additive `ALTER TABLE` on the
Python side, because their sink methods extract rows by an **explicit
column-name list**, not by forwarding arbitrary dict/dataclass keys:
`storage/clickhouse.py`'s `insert_repo` / `insert_git_commit_data` /
`insert_git_pull_requests` / `insert_git_pull_request_reviews` /
`insert_teams` / `insert_identities`, and
`metrics/sinks/clickhouse/work_graph.py`'s `write_work_items` /
`write_work_item_transitions` / `write_work_item_dependencies`, all needed a
one-line `"source_id"` addition to their column list (plus the
dict/attribute extraction line). `write_work_item_dependencies` also needed
a new `source_id: uuid.UUID | None = None` field on the
`WorkItemDependency` frozen dataclass (`models/work_items.py`), because that
one method routes through `_ClickHouseSinkBase._insert_rows`, which builds
rows via `dataclasses.asdict()` — dict rows are not accepted there, unlike
the other two work-item-family methods. All of these are additive,
backward-compatible one-liners; no existing call site changed behavior
(proven live: `test_native_sync_repo_write_leaves_source_id_null`).

## D2 — `repository.v1` repo UUID must match native sync exactly

`external_ingest/ids.py::derive_repo_uuid(system, instance, external_id)`
calls `get_repo_uuid_from_repo()` (`models/git.py:72`) with the **same
string shape** native sync uses for github/gitlab: the provider full name
(`owner/repo` / `group/subgroup/project`), unchanged — verified against
`processors/github.py:1574` (`repo=repo_info.full_name`) and
`processors/gitlab.py:1815/2096` (`repo=full_name` /
`project_info.full_name`). `get_repo_uuid_from_repo` lower-cases its input,
so `Owner/Repo` and `owner/repo` derive the identical UUID — proven live by
`test_repository_round_trip_and_repo_identity_handoff`, which also proves
the actual identity-continuity property this exists for: a repo UUID
derived from a pushed `repository.v1` record equals
`get_repo_uuid_from_repo()` called directly, i.e. what a native
`fullchaos_sync` GitHub processor would derive for "the same" repo. For
`system == "custom"` (no matching real provider), the seed is
`f"custom:{instance}:{external_id}"` instead — a distinct hash-input
namespace with no collision risk against real-provider UUIDs
(`test_derive_repo_uuid_custom_system_uses_distinct_namespace`).

`derive_repo_uuid` inherits `get_repo_uuid_from_repo`'s `REPO_UUID`
environment-variable override (returns that UUID for *every* call,
regardless of input, when set) — this is existing native-sync behavior,
not something this layer introduces or should special-case; a
worker-external-ingest deployment must simply never set `REPO_UUID` (it is
a single-repo CLI/local-dev override, not meant for a long-running
process).

## D3 — `work_item_id` / dependency IDs must match native sync's ID-space

`external_ingest/ids.py::derive_work_item_id(system, instance, external_key,
work_item_type)` reproduces the verified formats: `jira:{key}`,
`linear:{id}`, `gh:{repo}#{n}` / `ghpr:{repo}#{n}` (`work_item_type ==
"pr"`), `gitlab:{repo}#{iid}` / `gitlab:{repo}!{iid}`
(`work_item_type == "merge_request"`), `custom:{instance}:{key}` — see
`providers/github/normalize.py:108`, `providers/gitlab/normalize.py:57,216`,
`providers/jira/normalize.py:271`, `providers/linear/normalize.py:213`.
Table-driven proof: `test_derive_work_item_id_matches_native_sync_formats`.
Neither `instance` (the repo full name) nor `external_key` is lower-cased —
matching native sync exactly, which means (as with D2's env-var caveat) two
pushes disagreeing only in `repositoryExternalId` casing derive the *same*
repo UUID but *different* `work_item_id` strings; this is inherited
native-sync behavior, not a new defect.

`instance` is the repo full name for `system in {"github", "gitlab"}`
(taken from the per-record `repositoryExternalId` when present, else the
batch's `source_instance`) and unused for `jira`/`linear`.
`work_item_dependency.v1` has no `provider`/`system` field of its own (only
`sourceExternalKey`/`targetExternalKey` + optional per-side
`workItemType`), so `_build_dependency()` derives both ends from the
**batch's** `source_system`/`source_instance` — sound because a batch is
scoped to exactly one source instance (master-spec CC5).

Server-side derivation is explicitly bounded to the primary work-item
identifier and the dependency source/target keys (the frozen contract's
`externalKey`/`sourceExternalKey`/`targetExternalKey` fields). `parentId` /
`epicId` / `sprintId` on `WorkItemV1` are passed through **verbatim** as the
customer supplied them, with no namespaced-ID derivation attempted — native
sync does derive these (e.g. `jira:{parent_key}`), but the wire schema
gives no signal about which system a `parentId`/`epicId` string belongs to
beyond "presumably the same system as the work item", and getting this
wrong silently would be worse than a known, documented pass-through gap.
Flagged as a v1 limitation, consistent with the brief's risk #6
(`project_key`/`project_id` mapping quality is customer-payload-dependent,
"not fixable at the sink layer").

## D4 — Identity resolution: `resolve_identity()` for work items, raw for git

`write_batch()` calls `metrics.identity.resolve_identity(system, {...})` on
every work-item-family assignee/reporter/actor string, mirroring native
connector behavior so cross-provider identity rollups stay consistent
regardless of ingestion path. Customer payloads supply one opaque string
per person (`WorkItemV1.assignees: list[str]`,
`WorkItemV1.reporter: str | None`) rather than native connectors'
structured `{email, username, account_id, display_name}` — `sinks.py`'s
`_resolve_customer_identity()` is a documented heuristic: the raw string is
always passed as both `username` and `display_name`, and additionally as
`email` when it contains `"@"` (since `resolve_identity` prioritizes email
when present). Git-family (`commit.v1`/`pull_request.v1`/`review.v1`)
`author_name`/`author_email`/`reviewer` fields are passed through **raw**,
unresolved — matching native sync exactly (`GitCommit` /
`GitPullRequest` / `GitPullRequestReview` have no identity-resolved field).
`identity.v1` records populate `insert_identities` purely as a directory
side-effect; they do not feed `resolve_identity`'s static YAML alias map.

## D5 — `identity.v1` / `team.v1` stay dict-shaped

`NormalizedBatch.identities` / `.teams` are plain `dict`s. `_build_identity_row`
/ `_build_team_row` accept CHAOS-2697 handing either the wire-schema's
snake_case field names (`IdentityV1`/`TeamV1`) or fields already shaped to
`insert_identities`/`insert_teams`'s row contract — `provider_identities` in
particular is accepted as either a `dict[str, list[str]]` (json-encoded
here) or an already-JSON-encoded string, so this layer is tolerant of
either upstream choice per the brief's "this layer accepts either" clause.

## D6 — Two client lifecycles per invocation, only when needed

One async `ClickHouseStore` (via `create_store(dsn, "clickhouse")`) handles
`repository`/`commit`/`pull_request`/`review`/`team`/`identity`; one sync
`ClickHouseMetricsSink` (via `create_sink(dsn)`, run off the event loop with
`asyncio.to_thread`) handles the work-item family. Both are constructed at
most once per kind-family per `write_batch()` call — **not** once per
record. One deliberate deviation from the brief's skeleton: the async store
is only opened when at least one of `repositories`/`commits`/
`pull_requests`/`reviews`/`teams`/`identities` is non-empty
(`_maybe_store()`), so a pure work-item batch (a jira/linear-only push) does
not pay for an unused ClickHouse connection. The three work-item-family
writes (`write_work_items`/`write_work_item_transitions`/
`write_work_item_dependencies`) each get their **own** `create_sink()` /
`asyncio.to_thread` call rather than being bundled into one thread hop —
this trades a little connection overhead for the partial-batch-resilience
property below (each kind's failure is independent).

`org_id` stamping differs by client, per the recon: `ClickHouseStore.
_insert_rows` auto-injects `self.org_id` into any row missing it, so it is
still explicitly set on every row this layer builds (never relying on that
fallback); `ClickHouseMetricsSink.write_work_items`/
`write_work_item_transitions` read `org_id` **directly off each record**
with **no fallback** (`item["org_id"] if is_dict else item.org_id` — a
`KeyError`/`AttributeError` otherwise), so `sinks.py` stamps `org_id`
explicitly on every work-item-family row it builds
(`test_write_batch_stamps_org_id_on_work_item_family_rows`).
`write_work_item_dependencies` (the one work-item method that goes through
`_insert_rows`/`asdict`) *does* have a `self.org_id` fallback, but this
layer sets `org_id` on the `WorkItemDependency` instance directly anyway,
for consistency.

`work_items.repo_id` is non-nullable in the sink's row builder (defaults to
`uuid.UUID(int=0)` when the row's `repo_id` key/attribute is falsy —
`work_graph.py:652`). For jira/linear work items (no repo association) this
layer passes `repo_id=None` explicitly (not omitting the key) so that
fallback fires, per the brief's explicit instruction not to invent a
different sentinel.

## D7 — Re-push is "just write it again" (with one live-verified nuance)

All 9 tables are `ReplacingMergeTree`, so `write_batch()` does not
implement upsert/read-before-write logic: re-normalize, re-derive the same
deterministic IDs (D2/D3), stamp a fresh `last_synced` (server `now()` for
every kind except identities/teams — see D8), and call the same sink method
again. Verified live for every kind
(`test_*_round_trip` re-push assertions) — the newer write always wins
after a `FINAL` read.

`work_item_transitions` needed a closer look because its `ORDER BY`
includes `occurred_at` — verified live (`SHOW CREATE TABLE
work_item_transitions`): `ReplacingMergeTree(last_synced) ORDER BY (org_id,
repo_id, work_item_id, occurred_at)`. Re-pushing the *same* transition
(identical `occurred_at`, only `last_synced` differs — e.g. a corrected
`actor`/status for an already-recorded event) is a genuine `ORDER BY`-key
collision: a plain `SELECT ... FINAL` already collapses it to 1 row,
without needing `idempotency.py`'s `semantic_deduped_subquery()` — verified
live in `test_work_item_transition_semantic_dedup`, which also verifies
the sharper hazard the brief calls out: a **non-`FINAL`** `SELECT *`
genuinely returns 2 physical rows until a merge (or `FINAL`) runs, so any
downstream reader of this table that skips `FINAL` (or the semantic
subquery) can double-count. `WORK_ITEM_TRANSITION_SEMANTIC_COLUMNS`
(`idempotency.py`) is a strict superset of the table's physical `ORDER BY`
tuple (it additionally includes `provider`/`from_status`/`to_status`/
`from_status_raw`/`to_status_raw`/`actor`), which is why it agrees with
`FINAL` for this same-`occurred_at` case; it does **not** independently
solve the "customer retry regenerates a slightly different `occurred_at`
for what's semantically the same event" case that motivated the module
(since `occurred_at` is itself part of the semantic tuple, two rows
differing only in `occurred_at` remain two distinct semantic groups too).
This is pre-existing `idempotency.py` design, out of this issue's scope to
change — noted here for whoever next touches read-time transition
de-duplication.

## D8 — `identities`/`teams` `updated_at`: customer timestamp, clamped

`identities`/`teams` are `ReplacingMergeTree(updated_at)` — the caller
supplies the version column, so `write_identity()`/`write_team()` pass
`record.updated_at` through **verbatim**
(`test_identity_and_team_updated_at_pass_through_verbatim`), never
overwriting it with `now()` (unlike every other timestamp in this module,
which is always server-`now()` — a receive-time marker, not payload
content). The one exception (CC24, post-critique): a customer `updatedAt`
more than `UPDATED_AT_CLAMP_SKEW` (5 minutes) in the future is replaced
with server `now()` and recorded as a `SinkWriteResult.warnings` entry
(`code="updated_at_clamped"`), not a rejection — this prevents a
buggy/malicious `updatedAt=2100-01-01` from permanently pinning an RMT row
against every future correction, since no legitimate future write could
ever out-version it otherwise. Verified live
(`test_identity_updated_at_future_clamp_loses_to_later_legitimate_write`):
the clamped row loses to a subsequent, normal-timestamped push. `warnings`
is an additive field on `SinkWriteResult` beyond the interface brief's
original sketch (`counts_written`/`errors`/`affected_scope` only) —
CHAOS-2694 may ignore it; it exists because a clamp is neither a
write-time failure (`errors`) nor silent.

## Two non-nullable-column fixes found by the live tests

Two `CommitV1`/`WorkItemV1` fields are optional on the wire but map to
**non-nullable** `DateTime64` columns (verified via `DESCRIBE TABLE`):
`git_commits.committer_when` and `work_items.updated_at`. Writing `NULL`
there is not a ClickHouse constraint violation — clickhouse-connect's
driver raises an `AttributeError` client-side while serializing the
column (`'NoneType' object has no attribute 'timestamp'`), which
`write_batch()` correctly catches and reports as a `clickhouse_insert_
failed` `SinkWriteError`... but silently failing the whole `commit`/
`work_item` kind for every payload that omits an optional field is not
acceptable. `sinks.py` defaults `committer_when` to `author_when` (git's
own convention: an unamended commit's committer date equals its author
date) and `updated_at` to `created_at` (mirrors `WorkItem`'s dataclass
default of never being `None`) when the customer payload omits them.

## Adversarial-review findings (Codex, applied to trust boundary)

The wire schema (`api/external_ingest/schemas.py`) is deliberately
permissive about what a customer can send — this layer's job is to trust
that input for shape, but not for the invariants native sync gets for free
by construction. Two findings from the pre-merge adversarial review target
exactly that gap:

- **`native_team_key` forgery (fixed).** `_build_work_item_row` originally
  passed a customer's `nativeTeamKey` through unconditionally. Team
  attribution's precedence chain
  (`docs/architecture/team-attribution.md` §0, AGENTS.md) treats any
  non-empty `work_items.native_team_key` as a top-precedence NATIVE fact —
  native sync only ever populates it for Linear
  (`WorkItem.native_team_key` docstring: "None for GitHub/GitLab ... and
  Jira"). A schema-valid github/gitlab/jira `work_item.v1` payload setting
  `nativeTeamKey` could therefore forge rank-0 team ownership that bypasses
  project/member/repo attribution entirely — a real corruption vector
  since customer payloads are untrusted in a way native provider API
  responses are not. Fixed: `native_team_key` is now dropped (forced
  `None`) for every `system` except `"linear"`, regardless of what the
  payload contains. Covered by
  `test_native_team_key_dropped_for_non_linear_work_items` (parametrized
  over github/gitlab/jira) and
  `test_native_team_key_preserved_for_linear_work_items`.

  A second review pass caught that the first fix's `system` gate fell back
  to the **untrusted per-record** `provider` field
  (`get("provider") or batch.source_system`) — exactly as spoofable as
  `nativeTeamKey` itself. A github-scoped batch could smuggle a row
  claiming `"provider": "linear"` and still preserve a forged key.

  A third pass went further: nulling `native_team_key` alone was treating
  the symptom, not the cause — `_build_work_item_row`/`_build_transition_row`
  still derived `system` (and therefore `work_item_id`, `repo_id`, the
  stored `provider` column, project scope, and identity resolution) from
  that same untrusted per-record `provider`. A github-scoped batch with a
  spoofed `provider="linear"` row would still persist as a genuine Linear
  work item — `provider="linear"`, `work_item_id="linear:<key>"` — landing
  in and potentially colliding with real Linear data, even with
  `native_team_key` correctly dropped. Fixed at the root: `system` is now
  **always** `batch.source_system` for `_build_work_item_row` and
  `_build_transition_row` (`_build_dependency` already worked this way) —
  never the per-record `provider` — since a batch is authenticated/
  registered for exactly one system (CHAOS-2696) and there is no legitimate
  case for a batch to contain another system's work items. The record's own
  `provider`, when present and disagreeing with `batch.source_system`, only
  produces a `record_provider_mismatch` warning (`_check_provider_scope()`)
  — the same assert-but-not-reject posture as the git-family instance-scope
  check above, never used for derivation. Covered by
  `test_work_item_spoofed_provider_cannot_escape_batch_namespace`, which
  asserts the row lands under the batch's own namespace
  (`provider="github"`, `work_item_id="gh:...#42"`) with the mismatch
  surfaced as a warning, not silently accepted as `provider="linear"`.

- **Git repo-identity handoff trusts an unenforced customer string
  (mitigated, not rejected).** D2's identity-continuity guarantee depends
  on a git-family record's repo identifier equaling `source.instance`
  (master-spec CC6). This layer derives IDs from the record's own string
  without checking that invariant — by explicit epic-wide design (the
  reconciliation header on brief-2698-sinks.md, item 6: "Kind×system
  matrix (CC6) is enforced upstream in 2697's validate step; sinks may
  assert-but-not-reject"), full rejection is CHAOS-2697's job, not this
  layer's. A record that ships with a URL, a renamed/differently-cased
  string, or any other value that doesn't match `source.instance` would
  previously derive a repo UUID / `work_item_id` silently disjoint from
  what native sync (or a correctly-scoped push) would derive for the same
  logical repo — forking `customer_push` and `fullchaos_sync` history for
  what should be one repo, with no visible signal. Mitigated (not
  rejected, honoring the "assert-but-not-reject" contract):
  `_check_instance_scope()` now flags every repository/commit/
  pull_request/review/work_item record whose repo identifier disagrees
  with `batch.source_instance` as a `record_outside_source_instance`
  `SinkWriteResult.warnings` entry — the record is still written (this
  layer doesn't invent an authority to reject that CC6 explicitly assigns
  elsewhere), but the drift is now surfaced for CHAOS-2694's diagnostics
  instead of silently forking identity. Covered by
  `test_repository_external_id_mismatch_flagged_as_warning_not_rejected`
  and `test_pull_request_matching_source_instance_has_no_warning`.

- **Future-`updatedAt` clamp can still let a bad row outrank a truthful
  older retry (evaluated, not changed — refuted as a fix target).** The
  review correctly observes that a clamped row's `updated_at` (server
  `now()` at the moment of the bad ingest) can still beat a subsequent
  *legitimate* correction whose real source `updatedAt` predates that
  ingest moment (e.g. a backfill/retry carrying an older, truthful
  timestamp). That is a genuine residual risk — but master-spec CC24 is an
  explicitly RATIFIED, epic-wide decision that this exact tradeoff is
  intentional: "customer `updatedAt` used as the RMT version column
  (identities/teams) is clamped ... values more than 5 minutes in the
  future are replaced with server now() and recorded as a per-record
  WARNING diagnostic (**not a rejection**)" (master-spec CC24, also stated
  in the brief-2698-sinks.md reconciliation header). Rejecting a
  future-dated write, as the review recommends, is the alternative CC24's
  synthesizer explicitly considered and declined in favor of the clamp —
  changing that here would silently re-litigate a cross-cutting epic
  decision from a single sub-issue. Not implemented; the residual risk is
  documented above (D8) rather than silently accepted.

## Test plan

- `tests/external_ingest/test_sinks_unit.py` — mocked `create_store`/
  `create_sink`, no live DB: D2 case-insensitivity, D6 org_id stamping,
  D3 table-driven ID-format proof, D8 pass-through + clamp, partial-batch
  resilience on a mid-batch exception, `AffectedScope` aggregation, and the
  two adversarial-review regressions above (`native_team_key` scoping,
  `record_outside_source_instance` warnings).
- `tests/external_ingest/test_sinks_clickhouse.py` (`@pytest.mark.
  clickhouse`) — one round-trip test per record kind against an isolated
  scratch database, plus the D2 repo-identity handoff, the D7
  `work_item_transitions` FINAL-vs-semantic-dedup proof, the D8 clamp
  live proof, and a backward-compatibility proof that a native-sync-style
  `insert_repo()` call with no `source_id` still succeeds with
  `source_id IS NULL`.
