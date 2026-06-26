# LaunchDarkly sync budgeting plan

Status: planning contract for CHAOS-2687. This is not a raw sync implementation.

## Current provider status

The canonical `src/dev_health_ops/providers/launchdarkly/` package is partial. It currently contains code-reference helpers and a code-reference client, but it does not contain a full LaunchDarkly sync provider, a feature-flag/audit planner, or a budget estimator. Existing flag and audit-log fetches still live in the frozen legacy connector path and are called by the feature-flag worker; future provider work must move canonical raw fetch, pagination, retry, rate-limit handling, and normalization under `providers/launchdarkly/` before new sync behavior ships.

## Estimator contract

The future LaunchDarkly estimator must implement `dev_health_ops.sync.budget_types.BudgetEstimator`:

```python
class LaunchDarklyBudgetEstimator:
    def estimate(self, context: SyncTaskContext) -> tuple[BudgetEstimate, ...]: ...
```

Required behavior:

- Return an empty tuple for non-`launchdarkly` contexts.
- Return one or more `BudgetEstimate` values for every supported LaunchDarkly `SyncRunUnit`, starting with `DatasetKey.FEATURE_FLAGS`.
- Build every bucket with `BudgetBucketKey(provider="launchdarkly", org_id=context.org_id, host=<api-host>, credential_fingerprint=<safe fingerprint>, dimension=<BudgetDimension>)`.
- Use only the shared `BudgetDimension` vocabulary from `budget_types.py`; CHAOS-2687 introduces no new enum values.
- Use route families from `dev_health_ops.providers.launchdarkly.budget.LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES` so operators can override limits by keys such as `launchdarkly:rest_core:flags` or `launchdarkly:secondary_abuse_risk:audit_log`.
- Include no API secrets in bucket fingerprints, observations, logs, exceptions, or test snapshots.
- Treat estimate output as a hard provider-acceptance gate: a new LaunchDarkly sync unit is incomplete until the budget estimator covers its route families and tests assert that `estimate_provider_budget(context)` returns non-empty estimates.

The dispatch budget guard already consumes provider estimates through `estimate_provider_budget()`, then defers units when `SYNC_BUDGET_BUCKET_LIMITS` would be exceeded. Future LaunchDarkly provider wiring must add `launchdarkly` delegation there before enabling any raw fetch/sync path.

## Planned route families and dimensions

LaunchDarkly REST APIs are request-count and route-limit driven, so the first estimator should reserve abstract request units rather than raw vendor counters. Official API docs describe global limits, route-level limits, `Retry-After`, and paginated list responses; most list endpoints use `limit`/`offset`, while the audit log has a max page size of 20.

| Route family | Dimension | Endpoint patterns | Budget drivers |
| --- | --- | --- | --- |
| `projects` | `rest_core` | `GET /api/v2/projects`, `GET /api/v2/projects/{projectKey}/environments`, `GET /api/v2/projects/{projectKey}/environments/{environmentKey}` | project count, environments per project, `expand=environments` nested pagination |
| `flags` | `rest_core` | `GET /api/v2/flags/{projectKey}` | project count, flag count, environment filtering, `summary=0`, `expand=evaluation,codeReferences,migrationSettings` |
| `segments` | `rest_core` | `GET /api/v2/segments/{projectKey}/{environmentKey}` | project × environment fanout, segment count, big/synced segment expansion |
| `audit_log` | `secondary_abuse_risk` | `GET /api/v2/auditlog`, `POST /api/v2/auditlog` | incremental window size, backfill span, resource-scoped searches, 20-entry page cap |
| `members` | `rest_core` | `GET /api/v2/members` | member count, custom role expansion, role attribute expansion |
| `code_refs` | `secondary_abuse_risk` | `GET /api/v2/code-refs/repositories` | repository count, branch count, references per flag, default-branch expansion |

Flags embed variations and environment-specific configuration in the flag payload, so `variations` is a cost driver inside `flags`, not a standalone route family.

## Acceptance gate for future LaunchDarkly sync work

Before raw LaunchDarkly fetch/auth/pagination code ships from the canonical provider package, the PR must include all of the following:

1. A `LaunchDarklyBudgetEstimator` under `src/dev_health_ops/providers/launchdarkly/` that implements `BudgetEstimator` and returns non-empty estimates for `DatasetKey.FEATURE_FLAGS`.
2. `estimate_provider_budget()` delegation for `context.provider.lower() == "launchdarkly"`.
3. Unit tests proving LaunchDarkly sync contexts emit `BudgetEstimate` values with provider `launchdarkly`, safe host/credential scoping, and route families for flags plus any route families that the sync unit can call.
4. Budget-guard coverage proving LaunchDarkly estimates participate in reservation and deferral through `SYNC_BUDGET_BUCKET_LIMITS`.
5. Documentation updates when a new endpoint family, route family, or default budget limit is added.
6. No new code under `connectors/`; canonical provider code remains under `providers/launchdarkly/`.

## Initial operator defaults

No new environment variables are required. Extend the JSON value of `SYNC_BUDGET_BUCKET_LIMITS` only when LaunchDarkly provider units are enabled. Suggested starting values for observation or enforced rollout:

```json
{
  "launchdarkly:rest_core": 200,
  "launchdarkly:rest_core:flags": 120,
  "launchdarkly:rest_core:projects": 40,
  "launchdarkly:secondary_abuse_risk:audit_log": 25,
  "launchdarkly:secondary_abuse_risk:code_refs": 20
}
```

Start with `SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS` for at least one scheduled sync window, then promote to `SYNC_BUDGET_BUCKET_LIMITS` only after observations match expected project, environment, flag, audit-log, and code-reference fanout.
