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
| GitHub code datasets | REST | `process_github_repo` / `_backfill_github_missing_data` / `_sync_github_commits` / `_sync_github_commit_stats` (`git` + `commit_stats`, CHAOS-2807) |
| GitLab | REST | `GitLabProvider.ingest` **and** `metrics.work_items.fetch_gitlab_work_items` (the live worker path) |
| GitLab feature-flags | REST | `_sync_gitlab_feature_flags` (CHAOS-2785): `GitLabFeatureFlagsClient`, drained into `provider_usage` |
| Jira | REST | `JiraProvider.ingest` (legacy + atlassian paths) and the `job_work_items` client drain |
| Linear | GraphQL | `LinearClient` per-POST counting (`X-RateLimit-Requests-*` capture) drained via `job_work_items` |
| LaunchDarkly | REST | `_sync_launchdarkly_feature_flags` (CHAOS-2761): `LaunchDarklyClient` (flags, audit_log) + `LaunchDarklyCodeReferencesClient` (code_refs), both drained into `provider_usage` |

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

1. **Remaining code-dataset actuals.** The GitHub/GitLab code datasets (`files`,
   `blame`, plus any family not listed as instrumented above) run through
   `processors/dataset_adapters._run_github_dataset` / `_run_gitlab_dataset`,
   which use a shared/reused store rather than an instrumented per-unit work
   client. GitHub `git` / `commit_stats` are instrumented through
   `providers/github/code_client.py::GitHubCodeClient` as of CHAOS-2807; GitHub
   `files` / `blame` and the remaining GitLab connector-backed code-dataset
   methods stay on frozen PyGithub/python-gitlab connector paths until their own
   canonical-provider changesets.

> **Resolved (CHAOS-2761):** LaunchDarkly flag/audit-log actuals — previously
> gap #1 here, blocked on the frozen `connectors/launchdarkly.py` path — are
> now instrumented. `flags`/`audit_log` moved to the canonical
> `providers/launchdarkly/client.py::LaunchDarklyClient`, and the pre-existing
> canonical `code_refs.py` client was wired to the same shared recorder. See
> [LaunchDarkly sync budgeting](launchdarkly-sync-budgeting.md).

> **Resolved (CHAOS-2785):** GitLab feature-flag actuals — `get_feature_flags`
> / `get_project_name`, previously on the frozen `connectors/gitlab.py`
> `GitLabConnector` riding the un-instrumented `connectors/utils/rest.py`, are
> now instrumented via the canonical `providers/gitlab/feature_flags.py::
> GitLabFeatureFlagsClient`, wired to the shared CHAOS-2754 recorder
> (resolving under the existing `project` route family). The remaining GitLab
> code-dataset methods on the connector (`commits`, `files`, `blame`, `cicd`,
> `tests`, `deployments`, `security`) are unaffected and remain frozen
> pending CHAOS-2773 CS17.
