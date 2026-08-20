# LaunchDarkly sync budgeting

> See also: [Provider Rate-Limit Policy](../providers/rate-limit-policy.md) for
> LaunchDarkly's quota dimensions, headers (`X-RateLimit-Route-Remaining`,
> `Retry-After`), retry/deferral semantics, the canonical-provider-migration
> and actuals-instrumentation history, and how LD route families fit the
> cross-provider rate-limit and credentials-are-not-capacity model.

Status: implemented for CHAOS-2687 feature-flag sync budgeting. The
`feature-flags` dataset now emits LaunchDarkly budget estimates through the
existing `SyncRunUnit` pipeline, and `estimate_provider_budget()` dispatches to
`LaunchDarklyBudgetEstimator` for `provider="launchdarkly"`.

## Current provider status

The canonical `src/dev_health_ops/providers/launchdarkly/` package is now complete for the `feature-flags` dataset (CHAOS-2761). It contains code-reference helpers, a code-reference client (`code_refs.py`), a flags/audit-log client (`client.py`, migrated off the frozen legacy connector), and the LaunchDarkly budget estimator (`budget.py`). All three currently-emitted route families (`flags`, `audit_log`, `code_refs`) record real per-request actuals through the shared CHAOS-2754 recorder, so LaunchDarkly units now produce a `budget_comparison`. The legacy connector (`connectors/launchdarkly.py`) is left in place, unused by the sync path, but still backs the admin credentials "test connection" endpoint (`api/admin/routers/credentials.py::_test_launchdarkly_connection`) — that path never flows through a `SyncRunUnit` or budget estimation, mirroring the same raw/legacy-client pattern already used there for Jira/Linear connectivity checks, so it carries no actuals-instrumentation gap. As with every canonical provider, do not add new code under `connectors/`.

## Estimator contract

The LaunchDarkly estimator implements `dev_health_ops.sync.budget_types.BudgetEstimator`:

```python
class LaunchDarklyBudgetEstimator:
    def estimate(self, context: SyncTaskContext) -> tuple[BudgetEstimate, ...]: ...
```

Implemented behavior:

- Return an empty tuple for non-`launchdarkly` contexts.
- Return `BudgetEstimate` values for the supported LaunchDarkly `SyncRunUnit`: `DatasetKey.FEATURE_FLAGS`.
- Build every bucket with `BudgetBucketKey(provider="launchdarkly", org_id=context.org_id, host=<api-host>, credential_fingerprint=<safe fingerprint>, dimension=<BudgetDimension>)`.
- Use only the shared `BudgetDimension` vocabulary from `budget_types.py`; CHAOS-2687 introduces no new enum values.
- Use route families from `dev_health_ops.providers.launchdarkly.budget.LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES` so operators can override limits by keys such as `launchdarkly:rest_core:flags` or `launchdarkly:secondary_abuse_risk:code_refs`.
- Include no API secrets in bucket fingerprints, observations, logs, exceptions, or test snapshots.
- Treat estimate output as a hard provider-acceptance gate: any new LaunchDarkly sync unit is incomplete until the budget estimator covers its route families and tests assert that `estimate_provider_budget(context)` returns non-empty estimates.

The dispatch budget guard consumes LaunchDarkly estimates through `estimate_provider_budget()`, then defers units when `SYNC_BUDGET_BUCKET_LIMITS` would be exceeded. Successful units persist the estimate automatically in `SyncRunUnit.result.observations.budget_estimate`.

The current `feature-flags` estimate reserves:

- `flags`: `rest_core`, 2 units, medium confidence.
- `audit_log`: `rest_core`, 52 units, low confidence.
- `code_refs`: `rest_core`, 1 unit, medium confidence.
- `code_refs`: `secondary_abuse_risk`, 1 unit, low confidence.

## Planned route families and dimensions

LaunchDarkly REST APIs are request-count and route-limit driven, so the first estimator should reserve abstract request units rather than raw vendor counters. Official API docs describe global limits, route-level limits, `Retry-After`, and paginated list responses; most list endpoints use `limit`/`offset`, while the audit log has a max page size of 20.

| Route family | Dimension | Endpoint patterns | Budget drivers |
| --- | --- | --- | --- |
| `projects` | `rest_core` | `GET /api/v2/projects`, `GET /api/v2/projects/{projectKey}/environments`, `GET /api/v2/projects/{projectKey}/environments/{environmentKey}` | project count, environments per project, `expand=environments` nested pagination |
| `flags` | `rest_core` | `GET /api/v2/flags/{projectKey}` | project count, flag count, environment filtering, `summary=0`, `expand=evaluation,codeReferences,migrationSettings` |
| `segments` | `rest_core` | `GET /api/v2/segments/{projectKey}/{environmentKey}` | project × environment fanout, segment count, big/synced segment expansion |
| `audit_log` | `rest_core` | `GET /api/v2/auditlog`, `POST /api/v2/auditlog` | incremental window size, backfill span, resource-scoped searches, 20-entry page cap |
| `members` | `rest_core` | `GET /api/v2/members` | member count, custom role expansion, role attribute expansion |
| `code_refs` | `rest_core`, `secondary_abuse_risk` | `GET /api/v2/code-refs/repositories` | repository count, branch count, references per flag, default-branch expansion |

Flags embed variations and environment-specific configuration in the flag payload, so `variations` is a cost driver inside `flags`, not a standalone route family.

## Acceptance gate for future LaunchDarkly sync work

CHAOS-2687 satisfies the budgeting gate for the existing `feature-flags` unit. Before additional raw LaunchDarkly fetch/auth/pagination code ships from the canonical provider package, the PR must include all of the following:

1. A `LaunchDarklyBudgetEstimator` under `src/dev_health_ops/providers/launchdarkly/` that implements `BudgetEstimator` and returns non-empty estimates for `DatasetKey.FEATURE_FLAGS`.
2. `estimate_provider_budget()` delegation for `context.provider.lower() == "launchdarkly"`.
3. Unit tests proving LaunchDarkly sync contexts emit `BudgetEstimate` values with provider `launchdarkly`, safe host/credential scoping, and route families for flags plus any route families that the sync unit can call.
4. Budget-guard coverage proving LaunchDarkly estimates participate in reservation and deferral through `SYNC_BUDGET_BUCKET_LIMITS`.
5. Documentation updates when a new endpoint family, route family, or default budget limit is added.
6. No new code under `connectors/`; canonical provider code remains under `providers/launchdarkly/`.

## Follow-ups

- Replace fixed request estimates with dynamic estimates driven by project, environment, flag, audit-log, and code-reference fanout.
- ~~Add GitLab feature-flag budgeting when GitLab feature-flag sync units are enabled.~~ Done: `GitLabBudgetEstimator` already reserves budget for `DatasetKey.FEATURE_FLAGS` (route family `project`, `providers/gitlab/budget.py`), and as of CHAOS-2785 the fetch itself is instrumented too — see [Provider Rate-Limit Policy — GitLab](../providers/rate-limit-policy.md#gitlab) and [Sync usage actuals capture](sync-usage-actuals.md). Dedicated (non-shared) GitLab feature-flag budget families remain a follow-up.
- Reserve budget for the `projects`, `segments`, and `members` route families once a client fetches them (currently modeled but not emitted or instrumented).
- Feed the `X-RateLimit-Route-Remaining` low-budget warning into the deferral/cooldown machinery instead of only logging it (see [Provider Rate-Limit Policy — Known gaps](../providers/rate-limit-policy.md#known-gaps)).

## Initial operator defaults

No new environment variables are required. Extend the JSON value of `SYNC_BUDGET_BUCKET_LIMITS` only when LaunchDarkly provider units are enabled. Suggested starting values for observation or enforced rollout:

```json
{
  "launchdarkly:rest_core": 200,
  "launchdarkly:rest_core:flags": 120,
  "launchdarkly:rest_core:projects": 40,
  "launchdarkly:rest_core:audit_log": 25,
  "launchdarkly:rest_core:code_refs": 20,
  "launchdarkly:secondary_abuse_risk:code_refs": 20
}
```

Start with `SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS` for at least one scheduled sync window, then promote to `SYNC_BUDGET_BUCKET_LIMITS` only after observations match expected project, environment, flag, audit-log, and code-reference fanout.
