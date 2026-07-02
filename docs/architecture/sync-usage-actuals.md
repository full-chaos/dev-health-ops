# Sync usage actuals capture (CHAOS-2754)

Status: implemented for CHAOS-2754 (wave 1 of CHAOS-2742). Provider work-item
sync now records **actual** request/page counts per
`(transport, route_family, dimension)` and drains them into the `SyncRunUnit`
result `observations`, so calibration can join actuals against the budget
*estimate* keyed by the same `(route_family, dimension)` vocabulary.

## Shared recorder

`providers/usage.py` holds the single in-memory recorder
(`UsageRecorder`) plus the declarative `UsageRouteFamily` / `OperationResolver`
types. It replaces the three near-identical recorders that previously lived in
`providers/{github,gitlab,jira}/client.py`. The recorder:

- Re-keys observations by `(transport, route_family, dimension)` instead of the
  raw interpolated operation string. Per-issue-number labels
  (`"GET issue events for #123"`) collapse onto one route-family key, which also
  fixes the 50-key cardinality-cap overflow that previously dropped most
  actuals as `summary/overflow`.
- Retains a sampled `example_operation` (the most recent interpolated label) for
  debugging.
- Resolves operations with a per-provider `OperationResolver` built from a
  registry exported by each `providers/<provider>/budget.py`
  (`<PROVIDER>_USAGE_ROUTE_FAMILIES` / `<PROVIDER>_USAGE_RESOLVER`). Those
  registries mirror the shape of `LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES` and
  enumerate the full budget vocabulary each estimator emits — an
  estimator-coverage contract test asserts every emitted `route_family` has a
  registry entry, preventing silently-empty calibration joins.

## Observation key

Drains emit two keys on `ProviderBatch.observations` / the unit-result
`observations`:

- `github_usage` — **legacy**, GitHub-only, unchanged (pinned by
  `test_dataset_adapters.py`, `test_sync_units.py`, and the admin schema
  `linear_page_count` / `linear_batch_count` promotion contract). Kept intact.
- `provider_usage` — **provider-neutral**, emitted alongside `github_usage` and
  by the GitLab / Jira / Linear drains. Consumers should migrate to this key;
  `github_usage` is slated for a later cleanup once web/admin consumers are
  audited (per the CHAOS-2742 adopted-plan decision).

## Instrumented paths

| Provider | Transport | Drain site |
| --- | --- | --- |
| GitHub | REST + GraphQL | `GitHubProvider.ingest` (both keys) |
| GitLab | REST | `GitLabProvider.ingest` **and** `metrics.work_items.fetch_gitlab_work_items` (the live worker path) |
| Jira | REST | `JiraProvider.ingest` (legacy + atlassian paths) and the `job_work_items` client drain |
| Linear | GraphQL | `LinearClient` per-POST counting (`X-RateLimit-Requests-*` capture) drained via `job_work_items` |

Failure preservation: work-item clients are built per sync unit (unit-scoped,
safe to instrument). `run_work_items_sync_job` accumulates observations
incrementally and, on a mid-sync raise, attaches the partial observations to the
exception. The worker merges them into the rate-limit **deferral** stamp
(`workers/sync_units.py`) and `_stamp_sync_unit_failed` **additively** — nesting
actuals under `observations` and promoting `linear_page_count` /
`linear_batch_count` while leaving `error_category` / `next_retry_at` and the
other admin-API-read fields untouched.

### GitLab label coarseness (known limitation)

The GitLab work client labels most paginated reads identically
(`"GET iterator page"`), so per-entity distinctions (issues vs merge_requests vs
notes vs milestones vs epics) cannot be recovered from the operation label. The
resolver maps project-metadata reads to `project` and every other read to the
dominant work-item entity `issues`; the remaining GitLab families are declared
for budget-vocabulary coverage but carry no operation markers. Finer GitLab
attribution would require richer operation labels at the client call sites.

## Out-of-scope actuals gaps (documented, not fixed here)

1. **LaunchDarkly actuals.** LaunchDarkly flag/audit-log fetches still live in
   the frozen `connectors/` path (see
   [`launchdarkly-sync-budgeting.md`](launchdarkly-sync-budgeting.md)), which
   forbids new code. No recorder is wired there. Actuals capture must wait for
   the canonical-provider migration that moves LaunchDarkly raw fetch under
   `providers/launchdarkly/`.
2. **Code-dataset actuals.** The GitHub code datasets (`commits`, `files`,
   `blame`, `cicd`, `tests`, `deployments`, `security`) run through
   `processors/dataset_adapters._run_github_dataset`, which uses a shared/reused
   store rather than an instrumented per-unit work client. Their budget families
   (`git`, `commit_stats`, `files`, `blame`, `cicd`, `tests`, `deployments`,
   `security`) are declared in the GitHub usage registry for coverage but carry
   no operation markers. Instrumenting them needs a recorder threaded through
   the dataset store and is deferred to a follow-up.
