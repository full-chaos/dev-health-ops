---
page_id: op-rb-auth-push-projection-drift-alerts
summary: Load the five push credential projection alert rules into SigNoz and act on each when it fires.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Push projection drift alerts

The group `auth_push_projection` in `alerts/rules.yml` holds five rules on the push credential projection. No Prometheus loads that file in prod: it is the text of record, and the owner creates the rules in SigNoz from the builder JSON below at turn-on. Nothing here is run by a script. Do not print a Secret, an environment or a process argument list in any step.

## Before you start

- The rules `PushProjectionParkedGap` and `PushProjectionFinalRows` read the gauges `devhealth_auth_push_projection_parked_rows` and `devhealth_auth_push_projection_final_rows`. Only an auth-service replica with `AUTH_PUSH_PROJECTION=on` runs the projection reader and publishes them (the default is off). Until that flag is on, these two rules read no data and stay inactive. The rules `PushCredentialProjectionDrift`, `PushCredentialProjectionDriftLive` and `PushReconcileNotRunning` read series of the reconcile probe (`devhealth_auth_push_reconcile_drift` and `devhealth_auth_push_reconcile_runs_total`). The probe is off until `PUSH_RECONCILE_PROBE=on` is set on the worker. Until then these three rules read no data and stay inactive.
- The notification policy of the SigNoz org must send alerts with the label `team = platform` to the `accounts-email` channel. The JSON names no channel.

## Step 1: load the five rules

Create each rule in SigNoz (Alerts, New alert, import JSON). PromQL rules do not evaluate on this SigNoz, so each block is a builder threshold rule. Only `PushCredentialProjectionDriftLive` is critical; the other four are warnings. Notes on the derivation:

- `PushCredentialProjectionDrift`: `max` of the drift gauge per `class`, above 0 for the 10 minute window with "all the times" matching. The probe runs every 5 minutes and keeps a finding only when two passes agree, so 10 minutes means two confirmed passes.
- `PushCredentialProjectionDriftLive`: the same query with `class` limited to `status` and `scopes`, for 5 minutes. The projection reads a credential as live that the domain ended, or holds wider scopes.
- `PushProjectionParkedGap`: the SigNoz builder has no `min_over_time`, so the time aggregation is `min` and the window is 1 hour. The reader horizon is 1 hour and its retry backoff tops out at 5 minutes: a transient gap clears well inside the hour.
- `PushProjectionFinalRows`: final rows (malformed, unknown organization, conflict) are never retried and need a person.
- `PushReconcileNotRunning`: `increase` of the run counter with `outcome = ok`, equal to 0 over 3 hours. A reconcile that stops cannot raise the drift rules, and no sample is not zero, so this rule is separate. The counter is published at 0 from the start.

```json
{
  "alert": "PushCredentialProjectionDrift",
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
                "metricName": "devhealth_auth_push_reconcile_drift",
                "timeAggregation": "max",
                "spaceAggregation": "max"
              }
            ],
            "groupBy": [
              {
                "name": "class"
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
          "matchType": "all_the_times"
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "10m",
      "frequency": "1m"
    }
  },
  "notificationSettings": {
    "usePolicy": true
  },
  "labels": {
    "severity": "warning",
    "team": "platform"
  },
  "annotations": {
    "summary": "Push credential projection differs from the domain",
    "description": "Class {{$labels.class}}: two confirmed reconcile passes ten minutes apart saw the push projection differ from the credential domain. Next: Step 3 in docs/operate/runbooks/auth-push-projection-drift-alerts.md."
  }
}
```

```json
{
  "alert": "PushCredentialProjectionDriftLive",
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
                "metricName": "devhealth_auth_push_reconcile_drift",
                "timeAggregation": "max",
                "spaceAggregation": "max"
              }
            ],
            "filter": {
              "expression": "class REGEXP 'status|scopes'"
            },
            "groupBy": [
              {
                "name": "class"
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
          "name": "critical",
          "target": 0,
          "op": "above",
          "matchType": "all_the_times"
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
    "usePolicy": true
  },
  "labels": {
    "severity": "critical",
    "team": "platform"
  },
  "annotations": {
    "summary": "Push projection holds a credential live or wider than the domain",
    "description": "Class {{$labels.class}}: the projection reads a credential as live that the domain ended, or with wider scopes. Next: Step 3 in docs/operate/runbooks/auth-push-projection-drift-alerts.md."
  }
}
```

```json
{
  "alert": "PushProjectionParkedGap",
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
                "metricName": "devhealth_auth_push_projection_parked_rows",
                "timeAggregation": "min",
                "spaceAggregation": "min"
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
          "matchType": "at_least_once"
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "1h",
      "frequency": "1m"
    }
  },
  "notificationSettings": {
    "usePolicy": true
  },
  "labels": {
    "severity": "warning",
    "team": "platform"
  },
  "annotations": {
    "summary": "Push projection rows stayed parked for an hour",
    "description": "Parked projection rows did not clear for an hour, longer than the retry backoff allows. Next: Step 4 in docs/operate/runbooks/auth-push-projection-drift-alerts.md."
  }
}
```

```json
{
  "alert": "PushProjectionFinalRows",
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
                "metricName": "devhealth_auth_push_projection_final_rows",
                "timeAggregation": "max",
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
          "target": 0,
          "op": "above",
          "matchType": "all_the_times"
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
    "usePolicy": true
  },
  "labels": {
    "severity": "warning",
    "team": "platform"
  },
  "annotations": {
    "summary": "Push projection has final rows that need a person",
    "description": "Final projection rows (malformed, unknown organization or conflict) stay for five minutes and are never retried. Next: Step 4 in docs/operate/runbooks/auth-push-projection-drift-alerts.md."
  }
}
```

```json
{
  "alert": "PushReconcileNotRunning",
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
                "metricName": "devhealth_auth_push_reconcile_runs_total",
                "timeAggregation": "increase",
                "spaceAggregation": "sum"
              }
            ],
            "filter": {
              "expression": "outcome = 'ok'"
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
          "op": "equal",
          "matchType": "at_least_once"
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "3h",
      "frequency": "1m"
    }
  },
  "notificationSettings": {
    "usePolicy": true
  },
  "labels": {
    "severity": "warning",
    "team": "platform"
  },
  "annotations": {
    "summary": "Push projection reconcile has not completed for three hours",
    "description": "No reconcile pass finished ok in three hours, so the drift rules cannot fire. Next: Step 5 in docs/operate/runbooks/auth-push-projection-drift-alerts.md."
  }
}
```

## Step 2: check the series

1. In the metrics explorer, query each of the four instruments over the last hour: `devhealth_auth_push_reconcile_drift`, `devhealth_auth_push_reconcile_runs_total`, `devhealth_auth_push_projection_parked_rows`, `devhealth_auth_push_projection_final_rows`.
2. Expected with the probe on and no drift: the drift gauge has no sample, the run counter has `outcome = ok` rising, parked and final rows are 0.
3. An absent drift gauge is expected when the run counter is advancing (`outcome = ok` rising). It is the healthy no-drift state: do not stop for it.
4. No data for the run counter, the parked rows or the final rows, or an absent drift gauge while the run counter is not advancing, means that the metric is not scraped or pushed, the probe is off, or the collector is down. Stop and report; do not save the rule.

## Step 3: drift rule fires

1. Read the `class` of the alert: `missing`, `extra`, `status`, `scopes`, `expiry`, `binding` or `unwireable`.
2. Run `dho push-lifecycle reconcile -list` on a worker. It prints one `class token_id` pair per line. It never prints a token value, scope, organization or source.
3. `status` or `scopes` (critical): the projection reads a credential as live or wider than the domain. Treat as an access issue: find the token in the credential domain and fix the projection by the push lifecycle procedure; do not widen anything by hand.
4. Other classes: the projection is behind or wrong but not live. Fix by the same procedure and run the verb again; expect `drift total=0`.

## Step 4: parked or final rows

1. Parked rows that stay for an hour mean a gap after the fence that never filled. Read the projection reader log for the parked rows and the event they wait for.
2. Final rows need a person: read the reader log for the reason (malformed, unknown organization, conflict), fix the source row, and do not delete the row by hand.

## Step 5: reconcile not running

1. Check that `PUSH_RECONCILE_PROBE=on` is set on the worker and that the worker pods run the image that holds the probe.
2. Read the worker log for the probe line that says the pass failed. The run counter has `outcome = failed` for a failed pass.
3. Run `dho push-lifecycle reconcile` once by hand. If it fails, the error is the fixed message of the verb; follow the database runbook.

## Rollback

- A rule that pages wrongly: disable that rule in SigNoz. Do not delete it. The file `alerts/rules.yml` stays the text of record.
- The probe harms a worker: set `PUSH_RECONCILE_PROBE` off and roll. The three probe rules then read no data and stay inactive.
