# Provider/dataset capability matrix — v1

`matrix.json` is the frozen provider × dataset contract required by CUT-08 of
the Go worker cutover (`docs/plans/go-worker-cutover-implementation-plan.md`
§9, `docs/architecture/go-worker-cutover-trd.md` §10).

It exists because the matrix previously lived only as hand-maintained code in
two languages, with no test asserting the sets were identical.

## What it freezes

All **59** configured pairs across the six providers in TRD §10.1:

| Provider | Pairs |
|---|---|
| github | 17 |
| gitlab | 19 |
| jira | 6 |
| launchdarkly | 1 |
| linear | 5 |
| pagerduty | 11 |

Per pair: the Python source anchor, cost class, watermark behavior, legacy
targets, processor flags, the fixed Go executor kind, shadow eligibility, the
collapsed route dataset, the route destination manifest, route readiness, and
the provider's credential modes.

## What it deliberately does not freeze

`RouteEnabled` — the per-deployment environment switch. The contract records
*readiness*, which no environment may widen. A pair with `route_ready: false`
cannot be routed by any configuration.

## Who verifies it

Both languages regenerate their own side and fail on divergence:

- Go — `internal/providersync/capability_matrix_test.go`
  (`TestProviderMatrixMatchesCheckedInContract`), which rebuilds the artifact
  from the dataset capability registry and the canonical
  `CompleteRouteSwitches.Descriptor`.
- Python — `tests/workers/test_provider_matrix_contract.py`, which rebuilds the
  pair set and per-pair metadata from `src/dev_health_ops/sync/datasets.py` and
  additionally binds producer route eligibility
  (`ProviderUnitRouteSwitches.is_canary_scope`) to `route_ready`.

Regenerate after an intentional registry change:

```
PROVIDER_MATRIX_UPDATE=1 go test ./internal/providersync \
  -run TestProviderMatrixMatchesCheckedInContract -count=1
```

## Field semantics that are easy to get wrong

- `go_executor` is a statement about **compiled code**, not routing. A pair may
  be `native_go` while `route_ready` is `false` because live non-empty parity
  evidence has not been captured. Capability metadata is not execution
  evidence (TRD §10.1).
- `python_compatibility` is reserved for the bounded API-side compatibility
  endpoint described in TRD §10.2 (bounded identifiers and Go's claim context
  only; no credentials, URLs, module names, or provider payloads; Go retains
  lease, status, completion, and watermark ownership). **No pair uses it
  today** — that endpoint does not exist yet, and a Go test fails if any pair
  claims it.
- `native_shadow` marks pairs whose native fetch output may be compared against
  the Python-owned sink for parity evidence. It never implies routing.
- `route_dataset` differs from `dataset` for the linear/jira work-item aliases,
  which collapse onto one complete `work-items` unit while keeping alias
  identity for audit and watermark compatibility.
- `route_destinations` is empty for pairs with no known sink manifest yet.
  Recording a guessed manifest would be worse than recording none.

## Activation status for `(github, repo-metadata)`

CHAOS-3123 flipped the pair to `native_go` / `route_ready: true` on
fixture-level field parity evidence against the production Python collector.
Routing still requires the separate `GithubRepoMetadata` switch
(`WORKER_GITHUB_REPO_METADATA_ENABLED`), which every existing deployment
leaves off by default — `route_ready: true` alone moves no live traffic; see
`CompleteRouteSwitches.Descriptor`'s `github`/`repo-metadata` case.

What this activation waived and satisfied:

1. `TestGitHubRepositoryLiveParityHarness` remains an unimplemented stub and
   is *not* a precondition: canary staging and live-traffic parity are waived
   for this program (no production users yet). Fixture-level field parity is
   the accepted bar. Live parity against a real credentialed repository is
   still valuable operational evidence to capture before the switch is ever
   turned on in an environment with real traffic.
2. The binary that constructs `GitHubRepositoryRouteHandler` **must** also set
   `EffectCommitter.Readback` to `GitHubRepositoryClickHouseEffects`. The
   `repos` effect is `EffectReadbackRequired`, so a committer without a
   readback fails closed with `ErrEffectRecoveryAmbiguous` rather than
   reinserting. `cmd/dev-health-worker/provider_sync.go`'s `BuildExecutor`
   now selects `Handler`/`Sink`/`Readback` by `session.Claim.Provider` and
   `session.Claim.Dataset` (CHAOS-3123) — a single hardcoded
   `LaunchDarklyRouteHandler`/`LaunchDarklyClickHouseEffects` pair would fail
   closed on every claimed github unit rather than serve it.

## Activation status for `(gitlab, repo-metadata)`

CHAOS-3342 makes the pair `native_go` / `route_ready: true` with the same
fail-closed boundary as the GitHub repository route. The production binary
constructs a GitLab-specific handler and the tenant-keyed `repos` sink plus
readback. Fixture parity constructs the production Python `Repo` model and
executes the same persisted-row encoder used by `ClickHouseStore.insert_repo`,
then compares that row with the production Go row type through the generic
oracle. This boundary pins the model's `repo_tags` field rather than a
hand-assembled `tags` surrogate. GitLab credential URL aliases follow the
Python precedence `gitlab_url > url > base_url`, evaluating decrypted storage
before config only within the same alias; self-managed URL path roots and
non-default ports are retained. The fetched project id must equal the claimed
source id.

Routing remains off unless `WORKER_GITLAB_REPO_METADATA_ENABLED=true` is set
for both the Python producer gate and Go worker. Every checked-in deployment
keeps it false. This route-readiness change therefore transfers no live unit,
does not activate a River scheduler path, and does not change migration 0066.

## Activation status for `(gitlab, commits)`

CHAOS-3346 makes this pair `native_go` / `route_ready: true` without changing
the shared `git_commits` schema or claiming a provider-instance dimension.
The handler resolves the canonical GitLab project, retains self-managed URL
roots, paginates the repository commits endpoint, and fails the whole unit on
an HTTP error or safety-cap hit; no partial batch or watermark is committed.
Nullable commit messages remain nullable.

Row parity is checked against the live production `_map_commit` function and
the exact Python row builder used by `_fetch_gitlab_commits_sync`, with the
complete `GitCommit` field set reflected from the production model. The
ClickHouse effect requires recovery readback and compares the tenant-scoped
winning row through `SELECT ... FINAL`; the integration fixture uses the same
natural key under two organizations and verifies both directions.

Routing remains off unless `WORKER_GITLAB_COMMITS_ENABLED=true` is set for
both the Python producer gate and Go worker. Every checked-in deployment keeps
it false. This route-readiness change therefore transfers no live unit, does
not activate a River scheduler path, and does not change migration 0066.

## Activation status for `(gitlab, commit-stats)`

CHAOS-3349 makes this pair `native_go` / `route_ready: true` without changing
the shared `git_commit_stats` schema or claiming a provider-instance
dimension. It keeps Python's existing unit boundary: the commits window is
fetched first, then at most 50 aggregate detail rows on full sync or
`COMMIT_STATS_MAX_COMMITS` (default 300) rows on incremental sync. List errors
and safety-cap hits fail the unit before effects or watermark advancement;
accepted hashes are deduplicated in first-seen order before that selection, so
one hash cannot emit duplicate natural-key/version rows. Python's soft detail
behavior is retained only for ordinary non-auth failures: authentication, rate
limits, lease or budget loss, and context cancellation/deadlines fail the unit
without effects or watermark advancement. Unknown future provider error
classes also fail closed; only the explicit non-auth detail classes are soft.

Row parity executes the live GitLab `_map_commit_stats` normalizer and the
production row builder, with the full `GitCommitStat` field set reflected from
the production model. The shared ClickHouse commit-stat effect requires
recovery readback through a tenant-scoped `SELECT ... FINAL`; its integration
fixture collides the same natural key across two organizations and proves
retry convergence.

Routing remains off unless `WORKER_GITLAB_COMMIT_STATS_ENABLED=true` is set
for both the Python producer gate and Go worker. Every checked-in deployment
keeps it false. This route-readiness change therefore transfers no live unit,
does not activate a River scheduler path, and does not change migration 0066.

## Effect timestamp stabilization (applies to every complete route)

`BuildEffectBatch` digests the serialized rows, so any wall-clock value inside
a row is part of the effect identity. `CompleteRouteExecutor` therefore loads
the persisted effect ledger on **every** attempt for a unit occurrence — not
only on expired-lease recovery — and reuses its `created_at` as the
normalization instant.

This matters because `ReleaseForRetry` returns a unit to `dispatching`, so the
next claim is *not* `Recovered`. Stabilizing only on recovery left ordinary
River retries regenerating timestamps, changing the digest, and being rejected
by `PrepareEffects` with `ErrEffectLedgerConflict` before any readback could
run — wedging the unit until it exhausted. Any new complete-route handler that
stamps time into a row inherits this guarantee; handlers must take the
`normalizedAt` passed to `Collect` rather than reading the clock themselves.

Readbacks over a `ReplacingMergeTree` destination must compare against the
**winning** version (`argMax` over the version column), never every physical
version: unmerged history from earlier occurrences is normal and would
otherwise read as a conflict.
3. Raw `repos` readers must be audited. `repos` is
   `ReplacingMergeTree(last_synced)` ordered by `(org_id, id)`; readers that
   join it without `FINAL` or `argMax` can double-count between an insert and
   the next merge. `src/dev_health_ops/api/queries/heatmap.py` is a known
   pre-existing example and is not a regression introduced by this contract.

## Activation status for `(github, prs)`

CHAOS-3122 built a real `CompleteRouteHandler`
(`GitHubPullRequestRouteHandler`) and `EffectSink`
(`GitHubPullRequestClickHouseEffects`) — `go_executor: native_go` — but the
pair is deliberately **`route_ready: false`**, not `true`. This is not the
same shape as `(github, repo-metadata)`'s waiver.

An adversarial (codex) review of the first draft returned BLOCK with four
HIGH-severity findings, all fixed before this pair's code landed:

1. **Silent watermark loss on a capped fetch.** A paginated fetch that hit
   its page cap was reported as a successful, complete unit, so the claimed
   watermark advanced past PRs the cap never fetched — permanently, since no
   later incremental run revisits a window the watermark already passed.
   Fixed: `Collect` now fails the whole unit (`ErrPaginationCapExceeded`)
   whenever `CapReached` is true. Never both capped and successful.
2. **Silently dropped PRs with an unparseable `updated_at`.** Python's
   window comparison only applies when `updated_at` type-checks as a
   `datetime`; a missing/null/unparseable value is unconditionally included.
   An earlier version of the Go filter excluded such items instead — the
   empty-success trap: a window where every PR has that shape reported zero
   records instead of including them. Fixed: `pullOutsideKnownWindow` now
   only compares the window when the timestamp is known, mirroring Python's
   guard exactly, as a named, independently mutation-tested clause.
3. **Fail-open repository identity.** A blank/missing `full_name` from the
   repo GET fell back to `claim.SourceExternalID`, writing PRs under a
   *guessed* `repo_id`. Python's `get_repo_uuid_from_repo` raises on a falsy
   input and `Repo.__init__` never even attempts the call, so nothing
   downstream in Python would ever derive that repo_id. Fixed: the fallback
   was deleted; `repositoryIdentity` (already shared with repo-metadata)
   rejects the blank input on its own, and the test that previously
   asserted the fallback was inverted to require the failure.
4. **`route_ready: true` while three columns were always fabricated zeros.**
   `first_review_at`, `reviews_count`, and `changes_requested_count` are
   columns on `git_pull_requests`, but the data that fills them is Python's
   review-enrichment phase (`_enrich_prs_with_reviews_batch`) — a different
   dataset pair's (`github/pr-reviews`) job in this port's per-unit model.
   Writing them as zero while claiming route readiness would let
   review-latency/rework/AI-impact tiles read "never fetched" as "doesn't
   exist". **Resolution: `route_ready` stays `false` until `github/pr-reviews`
   lands and both pairs flip together** — see `CompleteRouteSwitches.Descriptor`'s
   `github`/`prs` case and
   `deploy/go-workers/provider-sync-porting-recipe.md`'s defect class 9 for
   the full column-versus-unit-ownership analysis and the three resolutions
   it lays out for future pairs in the same situation.

Five MEDIUM findings, also fixed, on the ClickHouse effect sink specifically
(`GitHubPullRequestClickHouseEffects`):

5. Provider timestamps were not truncated to millisecond precision at
   construction, so a value with finer-than-`DateTime64(3)` precision could
   compare unequal to what a later readback SELECT scans back. Fixed:
   truncated once, in `parseGitHubPullTime`.
6. The readback "exact" comparison omitted `first_review_at`,
   `first_comment_at`, `changes_requested_count`, and `reviews_count`
   entirely, and collapsed nullable strings through `ifNull(col, '')`,
   losing the NULL/empty-string distinction. Fixed: every column is now
   scanned into a Go pointer type and compared as its own named clause, not
   folded into one boolean expression.
7. State normalization stripped whitespace throughout the string instead of
   only leading/trailing (diverging on internal whitespace) and did not
   strip `\r` (diverging on a trailing carriage return) — not equivalent to
   Python's `raw_state.strip().lower()`. Fixed: `strings.ToLower(strings.TrimSpace(...))`.
8. A non-string `user.login` (e.g. a JSON number) silently became the
   `"Unknown"` fallback instead of being stringified the way Python's
   `str(user["login"])` would. Fixed: decode into `any` and reuse the
   package's existing `stringValue` helper.

What this activation actually proves, and how:

- Fixture-level field parity against the real Python collector
  (`TestGitHubPullRequestRouteEmitsOneBoundedEffect`).
- **Live parity, not a hand-authored comment:** state normalization and the
  `created_at` fallback chain are checked against the REAL, live
  `normalize_pr_state` / `build_git_pull_request` functions, shelled out to
  at test time (`TestGitHubPRSNormalizationMatchesLivePythonFunctions` →
  `testdata/python_github_prs_normalization_oracle.py`) — not a fixture that
  was verified once and pasted into a comment. This closes the codex H9
  finding: an earlier hand-authored fixture omitted the review-enrichment
  phase entirely and then asserted its own zero-valued output as "verified"
  parity, which cannot fail when the omitted phase is wrong.
- A full mutation-testing pass via the shared harness
  (`internal/providersync/testdata/mutation-plans/github_prs.json`, run with
  `scripts/mutation_harness.py`), 12/12 mutations `KILLED` (one additional
  mutation `SURVIVED` on a first pass and identified genuinely dead/redundant
  code — see the plan's `$limitation` field).
- `repo_id` is derived the same way `(github, repo-metadata)` derives its
  repository identity: `repositoryIdentity(fullName)` where `fullName` comes
  from a `GET /repos/{owner}/{repo}` call's `full_name` field, matching
  `get_repo_uuid_from_repo(repo_info.full_name)` in
  `models/git.py::Repo.__init__` — and now fails closed rather than falling
  back, per finding 3 above.
- `comments_count` and `first_comment_at` are NOT part of the review-data
  gap (finding 4): the former comes from the REST pull-detail payload
  directly, and the latter is `None` unconditionally in Python's own
  collector, so leaving it nil in Go is exact parity, not an omission.

A SECOND adversarial pass on the fix commit itself (`140b64eed`) returned
BLOCK again with two more HIGH findings — both in the fixes above, not new
surface:

- The H2 (pagination cap) fix made a deep-history repository with only one
  page inside the incremental window cap, and fail, on every attempt
  forever, because Go fetched every page up to `MaxPages` regardless of
  content while Python's `iter_pulls` stops the moment a listed item's
  `updated_at` is known and older than `since`. Fixed by adding the same
  early stop (`providerfoundation.GitHubPageOptions.StopAt`) rather than
  only filtering the fetched set post-hoc.
- Separately, if an earlier attempt had written to ClickHouse and died
  before `CommitEffect`, and a LATER attempt then failed for any reason
  (the capped fetch above being what made this newly, deterministically
  reachable), the job runtime's retry-release path
  (`PostgresRepository.ReleaseForRetry`) overwrote the unit's whole result
  document instead of merging into it, deleting the effect ledger the next
  attempt needed to classify that in-flight write as exact, absent, or
  conflicting. Fixed by merging (`COALESCE(result, '{}') ||
  jsonb_build_object(...)`) instead of replacing.
- The readback "exact" comparison itself had a deeper defect than the first
  round's field-omission fix addressed: assembling the "winning row" from
  independent per-column `argMax(column, last_synced)` calls is not the
  same as reading the row with the maximum `last_synced` — ClickHouse's
  `argMax` skips a row whose argument is NULL, so a winning row with a NULL
  column could be silently backfilled from an older row's non-NULL value in
  that column. Fixed by switching to `FROM git_pull_requests FINAL WHERE
  org_id = ? AND repo_id = ? AND number = ?` (a bounded point lookup on the
  full `ORDER BY` prefix), which reads one consistent physical row instead.
- Three MEDIUM findings also landed: the live oracle test decoded but never
  asserted the `build_git_pull_request` result's own
  created_at/merged_at/closed_at fields; `stringValue` (M8's fix) handled
  numeric but not boolean JSON scalars, diverging from Python's
  `str(True) == "True"`; and the `normalizedAt`→`last_synced` millisecond
  truncation had no sub-millisecond fixture of its own (a separate
  truncation call site from the one M5's first fix covered).

All five (two HIGH, three MEDIUM) are proven via the shared mutation harness
plan (`testdata/mutation-plans/github_prs.json`) alongside the first round's
findings — 17/17 mutations `KILLED`.

Full recipe, the (now fifteen) defect classes above generalized into a
checklist for the remaining 16 GitHub pairs, and difficulty tiers, are in
`deploy/go-workers/provider-sync-porting-recipe.md`.

## Implementation status for `(github, blame)`

CHAOS-3343 moves this pair to `go_executor: native_go` and
`route_ready: true`. The independent `WORKER_GITHUB_BLAME_ENABLED` switch is
false by default in both Python and Go, so landing the capability does not move
traffic or change deployment ownership.

The checked-in Go foundation can resolve a commit at the claim bound, walk the
recursive tree, fetch GraphQL blame ranges, expand them to the production
`git_blame` row shape, and exercise the sink/readback contract. Its 500-file
resource cap remains unchanged. `GitHubBlameClickHouseCoverage` reads both the
persisted `(org_id, repo_id, path)` coverage set from `git_blame FINAL` and the
separate `github_blame_path_progress` journal. The progress effect is committed
before `git_blame`, so a retry reconstructs the exact in-flight path set by the
PostgreSQL effect generation before ordinary coverage selection. Empty files
advance durably without fabricated line rows. Non-rate-limit per-file failures
stay retryable and rotate behind never-attempted paths; provider rate limits
still abort the whole unit. Successive syncs therefore advance without either
manifest conflicts or permanently unblameable-path starvation.

The one manifest that may be replanned is a GitHub-blame manifest that crashed
before its first progress effect became durable. Replanning requires the first
effect to be `pending` or `writing`, every later effect to remain `pending`, an
exact-generation ClickHouse probe to find zero progress rows, and a
transactional PostgreSQL comparison to prove the ledger has not changed since
that probe. Any accepted progress row or concurrent ledger transition keeps
the original manifest and uses normal exact readback recovery.

Missing, failed, or over-bound coverage reads fail with
`ErrGitHubBlameProgressUnavailable` before any blame GraphQL request, effect, or
watermark. A live Python oracle executes `select_unblamed_paths` for empty,
partial, complete, and bounded inventories.

Readback is a full-order-key point lookup over `git_blame FINAL`, including
`org_id`; integration tests cover both cross-tenant natural-key collisions and
same-organization cross-repository progress isolation. A separate PostgreSQL +
ClickHouse production-executor crash test proves that ordered progress and
blame writes accepted before process death are reconciled as exact without
inserting a duplicate physical version. Two additional executor tests cover
crashes after manifest preparation and after beginning the progress effect but
before its write; both safely replan when a transient per-file error clears.

Foundation normalization parity is executed against the active Python
`_backfill_github_missing_data` producer rather than a hand-authored expected
row. The generic oracle derives the compared fields from the live `GitBlame`
model and executes the real range-expansion/constructor path while replacing
only provider and sink seams. Production construction pins the coverage reader,
handler, effect sink, and readback together under the live lease. Deployment,
scheduler activation, and migration 0066 remain unchanged.

## Implementation status for `(github, tests)`

CHAOS-3336 remains `go_executor: none` / `route_ready: false`. The first
reviewable foundation ports the untrusted report boundary: bounded in-memory
ZIP traversal, a 200-report cap, DTD/entity rejection, JUnit suite/case
normalization, LCOV aggregate normalization, stable IDs, fallback run
timestamps, stack truncation, and incoherent-coverage rejection. A generic
oracle executes the active Python `ingest_report_members` chain and compares
every field in the production TestOps TypedDict contracts. Its LCOV cases
include summary-bearing input, `DA`-only LF/LH fallback with duplicate line
records, and multi-service input whose service is attributed by the majority
of report files (with Python's first-seen tie behavior).

This is intentionally not represented as a complete route. The active Python
`sync_tests` contract also owns Actions run pagination, per-run job pagination,
branch-protection acceptance checks, the authenticated-to-unauthenticated
two-hop artifact download, Cobertura parsing, and six independently persisted
destinations: `ci_pipeline_runs`, `ci_job_runs`, `ci_acceptance_checks`,
`test_suite_results`, `test_case_results`, and `coverage_snapshots`. Native
effect sinks/readbacks, cross-tenant collision proofs, and crash-window recovery
must cover all six before the executor or readiness fields can change. Until
then Python remains the only owner and no native switch exists.

## Known Go/Python divergences (fail-closed by design)

`repositoryIdentity` mirrors Python's `get_repo_uuid_from_repo` for the ASCII
names GitHub issues, and refuses rather than guesses in two cases:

- Python honours a process-global `REPO_UUID` override
  (`models/git.py::get_repo_uuid_from_repo`). Go never sources identity from
  process-global state, so it returns `ErrRepositoryIdentityAmbiguous` when
  `REPO_UUID` is set.
- Python's `str.lower()` applies full Unicode case mapping (`U+0130` lowers to
  `i` plus a combining dot); Go's `strings.ToLower` applies simple per-rune
  mapping. Non-ASCII repository identifiers are therefore rejected. GitHub
  restricts owner and repository names to `[A-Za-z0-9._-]`.

`normalizedProviderInstance` likewise rejects Unicode host labels that Python's
`str.isalnum()` would accept. In every case Go fails the unit instead of
persisting a value it cannot prove matches Python.

## Known contract-versus-TRD gaps

TRD §10.1 states the PagerDuty contract preserves a `region` parameter. No
region field exists in any PagerDuty auth or config class in this repository,
so `credential_modes` does not record one. This is a documented TRD/code gap,
not an omission.
