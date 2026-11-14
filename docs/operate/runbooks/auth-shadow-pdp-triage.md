---
page_id: op-rb-auth-shadow-triage
summary: Read the shadow policy decision against the legacy go-api guard, triage each mismatch class, add a reviewed class row, and switch the shadow off.
content_type: runbook
owner: platform-operations
source_of_truth:
  - internal/api/policy/shadow.go
  - internal/api/policy/shadow_env.go
  - internal/api/policy/shadow_classes.go
  - internal/api/policy/shadowcounters.go
  - internal/api/policy/shadow_guard.go
  - contracts/auth/v1/vocabulary/reason-codes.json
  - alerts/rules.yml
  - deploy/signoz/dashboards/auth-shadow-mismatch.json
applicability: current
lifecycle: active
---

# Auth shadow PDP triage

The shadow asks the policy decision point (PDP) the same question the legacy go-api guard has already answered, and counts every difference. It never changes a response.
{: .fc-page-lede }

!!! warning "Status: go-api builds the shadow; it is off unless `AUTH_SHADOW_PDP` is true"
    go-api builds the shadow at start (`newShadow` in `internal/apiservice/shadow.go`) and reports its series under `service.name` `dev-health-api`. With `AUTH_SHADOW_PDP` unset or false the shadow is off: `dev_health_api_shadow_enabled` is 0 and the counters have no series.

## What is compared

For a go-api route whose profile row names a catalog action (see the [action map](../../reference/auth/action-map.md)), the guard calls `Observe` after the legacy decision and before it answers. `Observe` returns at once. The PDP call runs in its own goroutine under a timeout (2 s) and an in-flight bound (32 by default); a call over the bound is dropped, not queued.

A call is **skipped** (counted, never asked) when the caller cannot be judged: the token is not a user access token (a legacy HS256 token), the row is high risk while `AUTH_SHADOW_PDP_HIGH_RISK` is off, or the route is organization-scoped and the caller has no organization where enforcement asks nothing either (a delegated session's bearer, and on query-api every such caller). In go-api the shadow asks what enforcement asks: a token with no organization on an organization row is asked the one platform action the legacy guard admits it by (`admin.orgs`, or the wrap's gate action), and a request that serves another organization, or a value that is no UUID, is asked about the token organization. Routes with no catalog action are not counted at all.

A call is counted **impersonation_refused** (never asked) when a live go-api impersonation row is in force for the caller's own bearer and enforcement refuses it (a write, or a platform wrap); its read that the legacy check admits is counted **skipped**, as enforcement keeps the legacy answer; the stop route is asked. A delegated session's own bearer is asked like any other caller.

A call is counted **decided_no_question** (never asked) when a token with no organization reaches an `AdminOrg` row: enforcement refuses it with no question, with the legacy `403` `Organization context required`, so there is nothing to ask. The shadow venue judge passes such a slot only when its legacy answer is that refusal, byte for byte.

## Switches

| Variable | Meaning | Default |
| --- | --- | --- |
| `AUTH_SHADOW_PDP` | `true` turns the shadow on. | unset (off) |
| `AUTH_SHADOW_PDP_HIGH_RISK` | `true` lets high-risk rows ask the PDP. The PDP writes one audit row per high-risk decision, so this adds audit volume. | `false` |
| `AUTH_SHADOW_PDP_SAMPLE` | Share of covered requests that ask, 1 to 100. | 100 |
| `AUTH_SHADOW_PDP_MAX_IN_FLIGHT` | Calls in flight, 1 or more. | 32 |

A bad value never stops the service. The shadow stays off, the log has an ERROR line that names the variable (never its value), and `dev_health_api_shadow_config_invalid` is 1.

## Check that it is armed

1. `dev_health_api_shadow_enabled` is 1 and `dev_health_api_shadow_config_invalid` is 0 on every go-api replica. An applied alert rule is not proof; this check is.
2. `dev_health_api_shadow_results_total` rises. The counters are lazy: no series means no event yet, not zero risk.

## Read the results

| `result` | Meaning | Act when |
| --- | --- | --- |
| `match` | PDP and legacy agree. | never |
| `mismatch` | They differ. Also counted in `dev_health_api_shadow_mismatch_total{namespace,caller,class}`. | see the triage loop |
| `error` | The PDP gave no verdict (a local deny: unreachable, timeout, refused, rate limited and so on). Nothing is compared. | a rising share: check auth-service readiness and the `POST /v1/authorize` route; for the enforced side, see [Auth PDP outage and rollback](auth-pdp-outage-and-rollback.md) |
| `dropped` | The in-flight bound was full. | a rising share: raise `AUTH_SHADOW_PDP_MAX_IN_FLIGHT` or lower `AUTH_SHADOW_PDP_SAMPLE` |
| `skipped` | A covered request the shadow cannot ask. | only to judge coverage |

Zero mismatches is not "good" on its own. Read the results panel first: `error`, `dropped` and `skipped` calls produce no mismatch.

For a count per route, read the go-api INFO log `api shadow pdp: result`. Each shadowed request writes one line with `route` (the row key, `METHOD /route`), `namespace`, `caller` and `result`. The line also carries `decision_id` when the decision point answered. A `match` with no `decision_id` was decided without a decision point call (the request names another organization), so it is not PDP evidence for the row.

The `AuthShadowMismatchUnresolved` alert (`alerts/rules.yml`, group `auth_shadow`) fires on any increase of `class="unresolved"` within one hour. The dashboard "Dev Health - Auth Shadow Mismatch (production)" (`deploy/signoz/dashboards/auth-shadow-mismatch.json`) shows mismatches by namespace, caller and class. Setup and labels are in [Auth shadow mismatch](../observe/dashboards-and-alerts.md#auth-shadow-mismatch).

## Triage loop

1. Open the dashboard. Note the `namespace` (the first segment of the catalog action, not a Kubernetes namespace), the `caller` (member, admin, superuser) and the `class`.
2. Read samples from the go-api WARN log `api shadow pdp: mismatch with the legacy guard`: `action`, `route`, `legacy`, `shadow`, `reason_code`, `decision_id`. The log allows one line per second per class, so use it for samples, never for counts.
3. Decide which side is wrong. Two directions exist:
    - `legacy=allow`, `shadow=deny`: the PDP is stricter. Either the legacy guard lets too much through (`legacy_bug`), or the PDP or its policy data is wrong (`new_bug`). Use the `reason_code`: for example `role_lacks_action` or `no_membership` against a route that still serves the caller points to the legacy guard or a missing role row; `unknown_action` or `org_mismatch` points to the profile row or the observation.
    - `legacy=deny`, `shadow=allow`: the PDP would let through what legacy refuses. Treat as a possible `new_bug` first. It is the direction that decides whether enforcement is safe.
4. Fix the cause in the owning place: the legacy guard, the catalog or role row ([contributor guide](../../contribute/architecture/auth-pdp-contributor-guide.md)), the endpoint profile row, or the PDP. Do not hide a mismatch by editing the class table.
5. Add a class row only when the difference is understood and reviewed (next section). Anything else stays `unresolved`, which is the default and keeps the alert armed.

| Class | Use for |
| --- | --- |
| `legacy_bug` | The legacy guard's answer is wrong; the PDP is right. Fix legacy before enforcement. |
| `new_bug` | The PDP's answer is wrong. Fix the PDP or its data before enforcement. |
| `intentional` | The difference is wanted and reviewed. |
| `unresolved` | No reviewed row covers the pair. The default. |

The table on `main` has one row: legacy `allow`, PDP `deny`, reason `delegation_invalid`, class `intentional` (the PDP refuses a delegated session whose actor lost the power or is deactivated; the legacy guard does not read the actor).

## Add a class row

The table is Go source (`internal/api/policy/shadow_classes.go`), so a change is a reviewed code change. Rules the loader enforces (`LoadShadowClasses`):

- The first line is the exact header `legacy<TAB>pdp<TAB>reason<TAB>namespace<TAB>class<TAB>note`.
- One row per line, six tab-separated fields. Blank lines and lines starting with `#` are skipped.
- `legacy` and `pdp` are `allow` or `deny`.
- `reason` is `*` or a PDP reason code from `decision_reason_code` in `contracts/auth/v1/vocabulary/reason-codes.json`.
- `namespace` is `*` or one of admin, billing, integrations, license, members, settings, teams.
- `class` is one of the four above, and the note is not empty.
- The first row that matches wins. A pair no row matches is `unresolved`.

A bad table makes `NewShadow` fail, which leaves the shadow off with `dev_health_api_shadow_config_invalid` = 1, so a typo cannot silently widen a class. Name the evidence (decision ids, the mismatch count and window) in the pull request.

## Switch it off

Unset `AUTH_SHADOW_PDP` or set it `false`, then roll go-api. The shadow is a nil value in the guard; no response, policy row or audit row is involved, so there is nothing to undo. Read back `dev_health_api_shadow_enabled` = 0.
