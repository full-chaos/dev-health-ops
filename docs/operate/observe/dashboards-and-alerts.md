---
page_id: op-alerts
summary: Build actionable dashboards and alerts from service objectives and recovery ownership.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - alerts/rules.yml
applicability: current
lifecycle: active
---

# Dashboards and alerts

A dashboard should connect user impact with service, queue, provider, store, and data-progress signals.

An alert should include:

- affected service and environment;
- impact or violated objective;
- current value and threshold window;
- organization or provider scope when safe;
- runbook and owner;
- suppression or grouping behavior;
- evidence required to close.

Alert on sustained impact or recovery risk, not every transient retry. Test routing and runbook links regularly.

## Recovery seams need a PAIR of signals, not one

A recovery seam that reports what it *selected* separately from what it
*acted on* cannot be watched by either half alone, and `alerts/rules.yml`
carries three rules that exist because of one incident where it was.

Sync run `115e6246` held 17 provider units in `dispatching` for thirteen hours.
The unreclaimable sweep selected all 17 every second and terminalized none —
its default mode is `shadow`, no deploy shape had ever set
`SYNC_UNRECLAIMABLE_SWEEP`, and every tick logged `candidates=17
terminalized=0` at WARN with nothing reading it.

- `SyncDispatchUnreclaimableCandidatesNotTerminalizing` reads
  `sync_dispatch_unreclaimable_candidates` **against**
  `increase(sync_dispatch_unreclaimable_terminalized_total)`. The gauge alone is
  not a fault: a healthy sweep also shows candidates on the pass that finds
  work. What separates recovering from watching is whether the counter moves.
- `SyncDispatchUnreclaimableSweepFailing` is separate on purpose. On a failed
  pass the candidate gauge reads zero — identical to a healthy idle system — so
  the first rule is structurally unable to fire while the sweep is broken.
- `ProviderUnitStrandRearmsClimbing` watches
  `worker_outbox_reconciler_provider_unit_strands_rearmed_total`, the recovery
  side of the same strand.

**The delivery-attempt evidence is in logs, not in a metric.**
`worker_job_outbox.attempt_count` is a column and the reconciler exports
per-pass deltas, so there is no delivery-attempt gauge; run `115e6246` reached
624 delivery attempts on one row with nothing to alert on. Three log lines carry
the identifiers instead, and each rule's description names the one it needs:

| Line | Emitted by | Carries |
| --- | --- | --- |
| `syncreconciler.unreclaimable_sweep_mode_resolved` | reconciler startup | the resolved sweep `mode`, and `source` = `configured` or `default` |
| `provider unit strand rearmed after a terminal river delivery` | outbox reconciler, one per rearm | outbox id, dedupe key, dead River job id |
| `dispatch_sync_run.publish_hit_terminal_delivery` | dispatcher, one per unit | sync run id, unit id, provider, dataset |

Writing a rule against a metric nobody emits is worse than having no rule: it
never fires and it *looks* covered. the Go tests in `internal/syncreconciler/unreclaimable_sweep_test.go`
pins every metric name these rules read against the Go `WritePrometheus` bodies
that actually render them.

## Auth service rules

`alerts/rules.yml` carries an `auth_service` group over the auth service's operator `/metrics`: target down, scrape absent, a sustained rate-limit rejection rate, and a 5xx ratio above 5% that only fires with real traffic. Rate-limit store errors are alerted in the `auth_service_rate_limit` group (a warning at any error, and a critical tier above 20 in ten minutes kept beside it). Every rule of the `auth_service` group selects `job="auth-service"`; the critical store-error tier reads the same `limit=~"auth_svc_.*"` series as the warning rule beside it. The only label these rules group by is the `limit` id.

The `valkey` group watches the in-chart Valkey through the collector's redis receiver: any key eviction in ten minutes, and used memory above 80% of the planned maxmemory 192mb for ten minutes. The import JSON and the checks are in the auth-service scrape and alert import runbook.

The file is the text of record: no Prometheus loads it in production. The production rules are created in SigNoz from this text. A test pins every `dev_health_api_` metric name a rule reads against the series the service really exposes, so a rule on a name nobody emits fails the build instead of looking covered. The scrape series `up` (and `absent(up)`) are scrape metadata, not exposed by the service: the test checks their rule shape, and they depend on the collector's job name `auth-service`. The test also pins each rule's threshold shape and `for:` duration, that no rule result carries the scrape instance, and that the 5xx rule keeps its traffic floor.

## Issue-PR fallback invariant rule

`alerts/rules.yml` carries an `issue_pr_fallback_invariant` group with one rule, `IssuePRFallbackInvariantBreached`: any increase of `workgraph_issue_pr_fallback_beside_native_rows_total` in 24 hours (`increase(...[24h]) > 0`, no `for`). After every work-graph build the pre-step `issue_pr_fallback_invariant` adds the number of `explicit_text` and `heuristic` rows the native-first rule forbids to that counter, 0 included, so the first breach of a worker's life is an increase. The counters carry no label; the organization is in the WARN log line `issue-pr fallback invariant breached`. The three per-side counters (`..._beside_native_pull_request_rows_total`, `..._beside_native_issue_rows_total`, `..._pull_request_issue_side_rows_total`) say which side breached. Labels: `team = platform`, `severity = warning`. The check reports and repairs nothing: a breach is a link writer regression. Background: [data and storage](../../contribute/architecture/data-and-storage.md). PromQL rules do not evaluate on this SigNoz, so the production rule is a builder threshold rule made by hand from this text.

## Sync plan executed-proof rule

`alerts/rules.yml` carries a `sync_plan_gate` group with one rule, `SyncPlanExecutedProofUnsatisfied`: any increase of `sync_plan_gate_total{dataset="work-items",outcome="executed_proof_unsatisfied"}` in two hours, per `provider` (`sum by (provider) (increase(...[2h])) > 0`, no `for`). The scheduler counts every planner decision on its metrics endpoint. This outcome means the executed-proof gate refused a pair that was attempted before and never persisted a row, so the planner plans nothing for it. The canonical `work-items` claim of GitHub, GitLab, Jira and Linear carries one shared waiver, so a refusal there means the waiver is gone or broken and work-item data stops. The rule matches `dataset="work-items"` only: other pairs without executed proof yet (the PagerDuty datasets today) count on the same series and must not hold the rule red. The WARN log line `sync.plan.executed_proof_unsatisfied` names the provider, dataset, organization and source. Labels: `team = platform`, `severity = warning`. PromQL rules do not evaluate on this SigNoz, so the production rule is a builder threshold rule made by hand from this text.

## Org activity rules

`alerts/rules.yml` carries an `org_activity` group over the metrics worker's OTLP push. The worker's daily finalize writes `public.org_activity`, the table auth login reads to choose the active organization; a stale row makes login fall back to the earliest join. Two rules:

- `OrgActivityRefreshFailed`: any increase of `daily_finalize_org_activity_refresh_failed_total` in 24 hours. The counter is created and seeded at 0 when the finalize handler gets its refresher, so the first failure of a worker's life is an increase and not a first sample. A failure inside the first export interval (60 seconds after the worker starts) is still folded into the first exported value.
- `OrgActivityStale`: `dev_health_org_activity_refresh_age_seconds`, the age of the newest row (`now() - max(refreshed_at)`), above two days for fifteen minutes. SigNoz cannot read Postgres, so the worker exports the number: one database read per minute (two second timeout), no sample and never 0 when the read fails or the table is empty. `dev_health_org_activity_refresh_age_read_failed_total` counts the failed reads, so a gauge that stopped reporting can be told from a fresh table.

PromQL rules do not evaluate on this SigNoz, so the production rules are builder threshold rules. Both name the `accounts-email` channel explicitly and do not use the notification policy (`usePolicy` is false), as the production rule `OrgActivityRefreshFailed` does; the labels are `team = platform` and `severity = warning`.

```json
{
  "alert": "OrgActivityRefreshFailed",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "threshold_rule",
  "schemaVersion": "v2alpha1",
  "condition": {
    "compositeQuery": {
      "queryType": "builder",
      "panelType": "graph",
      "unit": "",
      "queries": [
        {
          "type": "builder_query",
          "spec": {
            "name": "A",
            "signal": "metrics",
            "stepInterval": 300,
            "aggregations": [
              {
                "metricName": "daily_finalize_org_activity_refresh_failed_total",
                "timeAggregation": "increase",
                "spaceAggregation": "sum"
              }
            ],
            "disabled": false
          }
        }
      ]
    },
    "selectedQueryName": "A",
    "thresholds": {
      "kind": "basic",
      "spec": [
        {
          "name": "warning",
          "target": 0,
          "op": "above",
          "matchType": "at_least_once",
          "channels": ["accounts-email"]
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "24h",
      "frequency": "5m"
    }
  },
  "notificationSettings": {
    "usePolicy": false
  },
  "labels": {
    "severity": "warning",
    "team": "platform"
  },
  "annotations": {
    "summary": "org_activity refresh failed in the daily finalize",
    "description": "The daily finalize could not refresh public.org_activity in the last day (ClickHouse read error, panic, or organizations left without a row). Login falls back to the earliest-joined organization for stale rows. Next: the worker log line 'the org activity projection was not refreshed' (run_id, organization_id)."
  }
}
```

```json
{
  "alert": "OrgActivityStale",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "threshold_rule",
  "schemaVersion": "v2alpha1",
  "condition": {
    "compositeQuery": {
      "queryType": "builder",
      "panelType": "graph",
      "unit": "s",
      "queries": [
        {
          "type": "builder_query",
          "spec": {
            "name": "A",
            "signal": "metrics",
            "stepInterval": 60,
            "aggregations": [
              {
                "metricName": "dev_health_org_activity_refresh_age_seconds",
                "timeAggregation": "latest",
                "spaceAggregation": "max"
              }
            ],
            "disabled": false
          }
        }
      ]
    },
    "selectedQueryName": "A",
    "thresholds": {
      "kind": "basic",
      "spec": [
        {
          "name": "warning",
          "target": 172800,
          "op": "above",
          "matchType": "all_the_times",
          "channels": ["accounts-email"]
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "15m",
      "frequency": "1m"
    }
  },
  "notificationSettings": {
    "usePolicy": false
  },
  "labels": {
    "severity": "warning",
    "team": "platform"
  },
  "annotations": {
    "summary": "org_activity not refreshed for two days",
    "description": "No organization's activity row was refreshed for two days: the daily finalize is not running or every refresh fails. Login falls back to the earliest join. Next: rule OrgActivityRefreshFailed, the worker finalize logs, and dev_health_org_activity_refresh_age_read_failed_total."
  }
}
```

Checks after import: query `dev_health_org_activity_refresh_age_seconds` and `daily_finalize_org_activity_refresh_failed_total` over the last hour. The age is below 172800 and the counter reads 0 on a healthy worker. "No data" for the age means the table is empty or the reads fail (see the read-failed counter); it does not mean fresh.

## Auth shadow mismatch

The go-api shadow harness runs the new policy decision next to the legacy guard and counts every difference. The dashboard source is `deploy/signoz/dashboards/auth-shadow-mismatch.json` ("Dev Health - Auth Shadow Mismatch (production)"); the alert is `AuthShadowMismatchUnresolved` in `alerts/rules.yml`, group `auth_shadow`.

**The signal is not live yet.** The mismatch and result panels show no data and the rule is inert until the shadow is wired into go-api (the pull request that adds the `AUTH_SHADOW_PDP` switch) and `AUTH_SHADOW_PDP` is on. The two status panels differ: once the shadow is wired, `dev_health_api_shadow_enabled` and `dev_health_api_shadow_config_invalid` report data even with the switch off (0 and 0 when off; 0 and 1 when the shadow settings are invalid). The counters are lazy: an OpenTelemetry counter has no series until its first `Add`, so an absent series means nothing happened. The rule has no absent-data alert. Do not read an applied rule as proof that the shadow is armed; the check is `dev_health_api_shadow_enabled` = 1 in SigNoz.

Labels are a closed set: `namespace`, `caller`, `class` on `dev_health_api_shadow_mismatch_total`, and `result`, `reason` on `dev_health_api_shadow_results_total`. `reason` is none, hs256, envelope, no_user_bearer, cap, unmapped or op_refused. `namespace` is the action namespace (admin, billing, integrations, license, members, settings, teams), not the Kubernetes namespace. `caller` is member, admin or superuser. `class` is legacy_bug, new_bug, unresolved or intentional. Filters use only `service.name` and `deployment.environment` besides these. No organization, user, route, action or decision id is ever a label.

What to read:

- `AuthShadowMismatchUnresolved`: any increase of `dev_health_api_shadow_mismatch_total` with `class = 'unresolved'` in one hour, evaluated every 5 minutes, severity warning, channel `accounts-email`, no renotify (one email per transition; the dashboard holds the detail). `unresolved` is the default class, so any difference that no reviewed row of the class table covers lands here.
- Dashboard: unresolved total; mismatches by action namespace, by caller and by class; a namespace by caller table; results by `result` (match, mismatch, error, dropped, skipped, impersonation_refused); `dev_health_api_shadow_enabled` and `dev_health_api_shadow_config_invalid`.
- Zero mismatches is not "good" by itself: read the results panel, because error, dropped or skipped calls produce no mismatch.
- The metric says where; the go-api WARN log "api shadow pdp: mismatch with the legacy guard" says what (action, route, reason_code, decision_id). The log is limited to one line per second per class: use it for samples, never for counts.

PromQL rules do not evaluate on this SigNoz, so the production rule is a builder threshold rule that names the `accounts-email` channel and does not use the notification policy (`usePolicy` is false):

```json
{
  "alert": "AuthShadowMismatchUnresolved",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "threshold_rule",
  "schemaVersion": "v2alpha1",
  "condition": {
    "compositeQuery": {
      "queryType": "builder",
      "panelType": "graph",
      "unit": "",
      "queries": [
        {
          "type": "builder_query",
          "spec": {
            "name": "A",
            "signal": "metrics",
            "stepInterval": 60,
            "aggregations": [
              {
                "metricName": "dev_health_api_shadow_mismatch_total",
                "timeAggregation": "increase",
                "spaceAggregation": "sum"
              }
            ],
            "filter": {
              "expression": "class = 'unresolved' AND deployment.environment = 'production'"
            },
            "disabled": false
          }
        }
      ]
    },
    "selectedQueryName": "A",
    "thresholds": {
      "kind": "basic",
      "spec": [
        {
          "name": "warning",
          "target": 0,
          "op": "above",
          "matchType": "at_least_once",
          "channels": [
            "accounts-email"
          ]
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "1h",
      "frequency": "5m"
    }
  },
  "notificationSettings": {
    "usePolicy": false
  },
  "labels": {
    "severity": "warning",
    "team": "platform",
    "environment": "production",
    "service": "go-api"
  },
  "annotations": {
    "summary": "auth shadow PDP: an unresolved mismatch with the legacy guard",
    "description": "The shadow policy decision differed from the legacy guard and no reviewed class covers the difference. Next: the dashboard 'Dev Health - Auth Shadow Mismatch (production)' for the action namespace and caller, then the go-api WARN log 'api shadow pdp: mismatch with the legacy guard' (action, route, reason_code, decision_id; one line per second per class, so read samples, not counts)."
  }
}
```

## Auth service SLO alerts

The table of targets, windows and sources is in the [auth readiness and SLO reference](../../reference/auth/readiness-and-slo.md). Four rules in `alerts/rules.yml`, group `auth_slo`, evaluate it: PDP p95 above 25 ms, PDP p99 above 75 ms, session issue or refresh p95 above 200 ms, and the oldest pending outbox event above 300 seconds. Each fires when the condition holds for five minutes. The latency rules read `dev_health_api_http_request_duration_seconds` filtered by `route`; 25 ms, 75 ms and 200 ms are bucket edges of that histogram, so the quantile is not rounded up to the next edge. Availability is not a rule of this group: `AuthService5xx` (any 5xx in five minutes, live) and `AuthServiceHighServerErrorRate` (5xx ratio above 5% for ten minutes, in `alerts/rules.yml`, not yet created in SigNoz) cover it, and no burn-rate rule exists.

These four blocks are derived from `alerts/rules.yml` and are not yet imported. Before you save a rule, run its query over the last hour in SigNoz: "no data" (and not 0) means the series does not reach SigNoz yet (the auth-service series is empty until go-api forwards routes to it), so stop and report. The notification channel is `accounts-email`; the policy is not used.

```json
{
  "alert": "AuthzLatencyP95High",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "threshold_rule",
  "schemaVersion": "v2alpha1",
  "condition": {
    "compositeQuery": {
      "queryType": "builder",
      "panelType": "graph",
      "unit": "s",
      "queries": [
        {
          "type": "builder_query",
          "spec": {
            "name": "A",
            "signal": "metrics",
            "stepInterval": 60,
            "aggregations": [
              {
                "metricName": "dev_health_api_http_request_duration_seconds",
                "timeAggregation": "rate",
                "spaceAggregation": "p95"
              }
            ],
            "filter": {
              "expression": "service.name = 'auth-service' AND route IN ('/v1/authorize', '/v1/authorize:batch') AND deployment.environment = 'production'"
            },
            "disabled": false
          }
        }
      ]
    },
    "selectedQueryName": "A",
    "thresholds": {
      "kind": "basic",
      "spec": [
        {
          "name": "warning",
          "target": 0.025,
          "targetUnit": "s",
          "op": "above",
          "matchType": "all_the_times",
          "channels": [
            "accounts-email"
          ]
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "5m",
      "frequency": "1m"
    }
  },
  "notificationSettings": {
    "usePolicy": false
  },
  "labels": {
    "severity": "warning",
    "team": "platform",
    "environment": "production",
    "service": "auth-service"
  },
  "annotations": {
    "summary": "Auth PDP p95 latency above 25 ms",
    "description": "The p95 of POST /v1/authorize and /v1/authorize:batch on the auth service was above 25 ms for five minutes (target p95 < 25 ms)."
  }
}
```

```json
{
  "alert": "AuthzLatencyP99High",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "threshold_rule",
  "schemaVersion": "v2alpha1",
  "condition": {
    "compositeQuery": {
      "queryType": "builder",
      "panelType": "graph",
      "unit": "s",
      "queries": [
        {
          "type": "builder_query",
          "spec": {
            "name": "A",
            "signal": "metrics",
            "stepInterval": 60,
            "aggregations": [
              {
                "metricName": "dev_health_api_http_request_duration_seconds",
                "timeAggregation": "rate",
                "spaceAggregation": "p99"
              }
            ],
            "filter": {
              "expression": "service.name = 'auth-service' AND route IN ('/v1/authorize', '/v1/authorize:batch') AND deployment.environment = 'production'"
            },
            "disabled": false
          }
        }
      ]
    },
    "selectedQueryName": "A",
    "thresholds": {
      "kind": "basic",
      "spec": [
        {
          "name": "warning",
          "target": 0.075,
          "targetUnit": "s",
          "op": "above",
          "matchType": "all_the_times",
          "channels": [
            "accounts-email"
          ]
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "5m",
      "frequency": "1m"
    }
  },
  "notificationSettings": {
    "usePolicy": false
  },
  "labels": {
    "severity": "warning",
    "team": "platform",
    "environment": "production",
    "service": "auth-service"
  },
  "annotations": {
    "summary": "Auth PDP p99 latency above 75 ms",
    "description": "The p99 of POST /v1/authorize and /v1/authorize:batch on the auth service was above 75 ms for five minutes (target p99 < 75 ms)."
  }
}
```

```json
{
  "alert": "AuthTokenLatencyP95High",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "threshold_rule",
  "schemaVersion": "v2alpha1",
  "condition": {
    "compositeQuery": {
      "queryType": "builder",
      "panelType": "graph",
      "unit": "s",
      "queries": [
        {
          "type": "builder_query",
          "spec": {
            "name": "A",
            "signal": "metrics",
            "stepInterval": 60,
            "aggregations": [
              {
                "metricName": "dev_health_api_http_request_duration_seconds",
                "timeAggregation": "rate",
                "spaceAggregation": "p95"
              }
            ],
            "filter": {
              "expression": "service.name = 'auth-service' AND route IN ('/internal/v1/sessions', '/v1/token/refresh') AND deployment.environment = 'production'"
            },
            "disabled": false
          }
        }
      ]
    },
    "selectedQueryName": "A",
    "thresholds": {
      "kind": "basic",
      "spec": [
        {
          "name": "warning",
          "target": 0.2,
          "targetUnit": "s",
          "op": "above",
          "matchType": "all_the_times",
          "channels": [
            "accounts-email"
          ]
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "5m",
      "frequency": "1m"
    }
  },
  "notificationSettings": {
    "usePolicy": false
  },
  "labels": {
    "severity": "warning",
    "team": "platform",
    "environment": "production",
    "service": "auth-service"
  },
  "annotations": {
    "summary": "Auth token issue or refresh p95 latency above 200 ms",
    "description": "The p95 of session issue and refresh on the auth service was above 200 ms for five minutes (target p95 < 200 ms)."
  }
}
```

```json
{
  "alert": "AuthOutboxOldestPendingHigh",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "threshold_rule",
  "schemaVersion": "v2alpha1",
  "condition": {
    "compositeQuery": {
      "queryType": "builder",
      "panelType": "graph",
      "unit": "s",
      "queries": [
        {
          "type": "builder_query",
          "spec": {
            "name": "A",
            "signal": "metrics",
            "stepInterval": 60,
            "aggregations": [
              {
                "metricName": "devhealth_auth_outbox_oldest_pending_age_seconds",
                "timeAggregation": "latest",
                "spaceAggregation": "max"
              }
            ],
            "filter": {
              "expression": "service.name = 'auth-service' AND deployment.environment = 'production'"
            },
            "disabled": false
          }
        }
      ]
    },
    "selectedQueryName": "A",
    "thresholds": {
      "kind": "basic",
      "spec": [
        {
          "name": "warning",
          "target": 300,
          "targetUnit": "s",
          "op": "above",
          "matchType": "all_the_times",
          "channels": [
            "accounts-email"
          ]
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "5m",
      "frequency": "1m"
    }
  },
  "notificationSettings": {
    "usePolicy": false
  },
  "labels": {
    "severity": "warning",
    "team": "platform",
    "environment": "production",
    "service": "auth-service"
  },
  "annotations": {
    "summary": "Auth outbox oldest pending event older than 5 minutes",
    "description": "The oldest unpublished auth outbox event was older than 300 seconds for five minutes (target: under 5 minutes)."
  }
}
```

## Auth service SLO dashboard

The dashboard source is `deploy/signoz/dashboards/auth-slo.json` ("Dev Health - Auth Service SLO (production)"). It has panels only. The alert rules and the SLO numbers are in the SLO table change (`docs/reference/auth/readiness-and-slo.md`, group `auth_slo` in `alerts/rules.yml`); the dashboard draws their lines and holds no second copy of a rule. A signal with no set number has a panel and no alert.

**No data yet.** The auth-service series is empty until the auth service takes traffic (phase 2); until then go-api serves the auth routes. The panels show no data and the rules stay inactive, as for the shadow dashboard.

What it shows:

- Requests by `status_class`, the 5xx count, and the p95 and p99 of the routes `/v1/authorize` and `/v1/authorize:batch` (lines at 25 ms and 75 ms), the p95 of `/internal/v1/sessions` and `/v1/token/refresh` (line at 200 ms), JWKS and the exchange route (rate and p95; no target is set). All read `dev_health_api_http_requests_total` and `dev_health_api_http_request_duration_seconds` filtered to `listener = 'auth-api-http'` and the `route` set of the alert it draws (the `route` label is the registered path pattern, with no method). Each line is a bucket edge of the histogram, so a percentile resolves to its edge. A guard pins each line, route set and metric to its alert.
- Workload exchanges by `outcome` and `reason` (`auth.workload.exchanges`; `subject` and `audience` are never a group-by), refresh exchanges by `reason_code`, access tokens refused by `reason` and `profile`, decision cache lookups by `result`.
- Outbox: oldest pending age (line at 300 s), delivered rate, delivery failures by `reason`, and `publisher_enabled` (0 means no sink).
- Revision feed events by `disposition`, resyncs by `cause`, subscription up, and push projection lag, parked and final rows. The push projection lag counts outbox ids, not seconds, and stays above 0 for up to one hour (the reader horizon) after a decided event, so no alert reads it. These are proxies for revocation propagation, not seconds of effect: the revocation lag metric does not exist yet, so there is no panel and no alert for it.
- Legacy ingest audit failures by `reason`.

Not covered, because the service emits no metric for it: the JWKS cache age at the verifiers, a PDP outage seen at the caller, and revocation lag in seconds.

The labels are the closed set in `internal/platform/otlpmetrics/labels.tsv`; the dashboard filters on `deployment.environment`, `service.name`, `listener` and `route` and groups only by labels listed there. `auth.*` metric names can land underscored in SigNoz: the owner checks the names at import (auth-service scrape runbook, Step 3). The import is a separate step from the merged JSON.

## query-api PDP gate

query-api builds one PDP gate from `QUERY_API_PDP_MODE` and decides the routes and GraphQL root fields of the namespaces in `QUERY_API_AUTHZ_ENFORCE` and `QUERY_API_HEADERS_AUTHZ_ENFORCE`. The runbook is [Auth PDP outage and rollback](../runbooks/auth-pdp-outage-and-rollback.md#query-api). Two rules in `alerts/rules.yml`, group `query_api_pdp`, read it. No dashboard source file exists for this gate yet; the rows below are the panels to build, and each reads a series named here.

The series are lazy: an OpenTelemetry counter has no series until its first `Add`, so an absent series means nothing happened. Both rules are inert while the gate is off and no namespace is on. They are PromQL rules in `alerts/rules.yml` and are not yet created in SigNoz (PromQL does not evaluate there; a builder threshold rule is a separate step).

Labels are a closed set: `mode` (`enforce`, `invalid`), `result` and `action` on `dev_health_query_api_pdp_results_total`; `class` and `action` on `dev_health_query_api_pdp_exempt_total`. `result` is allow, deny, unavailable, not_configured, not_carried_out, no_decision_point or no_org. `action` is a catalog action, bounded by the catalog. No organization, user, repository or route is ever a label.

What to read:

- `QueryAPIPDPConfigInvalid`: `dev_health_query_api_pdp_config_invalid` is above 0 on any pod for five minutes, severity critical. The reasons are a bad `QUERY_API_PDP_MODE`, a mode that is on with a missing input, and a namespace listed with no PDP client.
- `QueryAPIPDPRefusing`: any increase in fifteen minutes of `dev_health_query_api_pdp_results_total` with `result` unavailable, no_decision_point, not_carried_out or not_configured, held for five minutes, grouped by `result`, severity warning. A deny is a `403`, not an outage, and is not counted. The five minutes are a noise window for a restart or one PDP blip. They are not a wait before a flip.
- Row: `dev_health_query_api_pdp_enabled` (1 while the gate is built in shadow or enforce) and `dev_health_query_api_pdp_config_invalid`, as status panels.
- Row: results by `result`, then by `action`, to see which namespace refuses.
- Row: `dev_health_query_api_pdp_exempt_total` by `class`, the callers that never ask the PDP.
- Row: `dev_health_authz_enforcement_enabled{service="query-api"}` by `namespace`, the cells that are on.
- In shadow mode the PDP asks count on `dev_health_api_shadow_results_total` and `dev_health_api_shadow_mismatch_total`, not on the `pdp_results_total` counter.
