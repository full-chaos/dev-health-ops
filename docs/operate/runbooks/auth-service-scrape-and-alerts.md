---
page_id: op-rb-auth-service-scrape-alerts
summary: Check that the auth-service operator /metrics is scraped in prod and load the three rate-limit alert rules into SigNoz.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth-service scrape check and alert import

This runbook is the login-flip gate for monitoring: the auth-service operator `/metrics` is scraped, and the three `auth_service_rate_limit` rules from `alerts/rules.yml` are loaded in SigNoz. Both steps are done by the owner. Nothing here is run by a lane or a script. No Prometheus loads `alerts/rules.yml` in prod: the file is the text of record, and the rules are created in SigNoz from it in the builder form given below.

## Before you start

- The deploy change that names the collector's scrape job `auth-service` has rolled. The rules `AuthServiceRateLimitStoreErrors` and `AuthServiceRateLimitStoreErrorsHigh` read the counter by its `limit` label and need no job. The other `auth_service` group rules select `job="auth-service"`; before the rename the collector job was `auth-service-operator`, so those rules would have read nothing.
- The auth-service Service publishes the `operator` port and its NetworkPolicy admits the collector pods (`ops.authService.networkPolicy.metricsFrom` in the prod values). A target that is not admitted shows no series at all.
- Do not print a Secret, an environment or a process argument list in any step.

## Step 1: one scrape carries both series

1. In SigNoz, open the metrics explorer and query `dev_health_api_rate_limit_store_errors_total` over the last 15 minutes.
2. Expected: one series per limiter id, every `limit` value starts with `auth_svc_`, and the value is 0 while the store is healthy. The series is present at zero once the limited routes are mounted.
3. Query `dev_health_runtime_check_failed` with `check = valkey`.
4. Expected: one series per auth-service replica, value 0.
5. Query `dev_health_runtime_info` with `service = auth-service`. Expected: one series per replica.
6. Check that the `instance` label on the check series and the info series carry the same value. The not-ready rule joins them `on (instance)`. If the labels differ or are missing in SigNoz, stop and report: the rule text needs a different join key.

If a series is missing: the collector scrape did not reach the port. Check the collector pod log for `auth-service` scrape errors, then the Service port and the NetworkPolicy. Do not change a limit or remove a route.

## Step 2: load the three rules

Create each rule in SigNoz (Alerts, New alert, import JSON). The JSON below is the builder threshold rule form of the three rules in `alerts/rules.yml`. PromQL rules do not evaluate on this SigNoz (D6417: its ClickHouse has no `timeSeriesLastToGrid`, so every PromQL query returns HTTP 400), so none of the rules in this runbook is a PromQL rule. These three blocks are derived from `alerts/rules.yml` and are not yet imported: run Step 3 on each rule before you save it. The notification channel is left empty on purpose: the rules route by notification policy (`usePolicy`). Field names follow the SigNoz rule schema; if your SigNoz version rejects a field, keep the metric, the filter, the group-by key, the threshold, the window, the severity and the labels, and set the rest in the form.

Notes on the derivation:

- `AuthServiceRateLimitStoreErrors` is `rate` of the counter, summed per `limit`, above 0 for the 5 minute window with "all the times" matching: this is the `for: 5m` of the file.
- `AuthServiceRateLimitStoreErrorsHigh` is `increase` of the same counter, summed per `limit`, above 20. The window is 10 minutes because a builder rule takes its aggregation range from its window.
- `AuthServiceRateLimitStoreNotReady` is one query on `dev_health_runtime_check_failed` with `check = 'valkey'` and `service.name = 'auth-service'` (an exact match, so `auth-service-operator` is left out), max per replica, equal to 1. A builder rule has no join, so the filter on `service.name` replaces the join with `dev_health_runtime_info` in the file. It is grouped by `service.instance.id`, the resource attribute that the scrape's `instance` label comes from. The values of the `check` attribute were not visible in the SigNoz field lookup when this was written: Step 3 checks them.

```json
{
  "alert": "AuthServiceRateLimitStoreErrors",
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
                "metricName": "dev_health_api_rate_limit_store_errors_total",
                "timeAggregation": "rate",
                "spaceAggregation": "sum"
              }
            ],
            "filter": {
              "expression": "limit REGEXP 'auth_svc_.*'"
            },
            "groupBy": [
              {
                "name": "limit"
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
      "frequency": "30s"
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
    "summary": "auth-service rate-limit store errors on {{$labels.limit}}",
    "description": "The shared rate-limit store failed for limiter {{$labels.limit}}. Limited auth routes answer a closed 500 while it fails, by design. Next: Rate-limit store down in docs/reference/auth/readiness-and-slo.md."
  }
}
```

```json
{
  "alert": "AuthServiceRateLimitStoreErrorsHigh",
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
                "metricName": "dev_health_api_rate_limit_store_errors_total",
                "timeAggregation": "increase",
                "spaceAggregation": "sum"
              }
            ],
            "filter": {
              "expression": "limit REGEXP 'auth_svc_.*'"
            },
            "groupBy": [
              {
                "name": "limit"
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
          "target": 20,
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
      "frequency": "30s"
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
    "summary": "Auth rate-limit store is failing at volume",
    "description": "Limit {{$labels.limit}}: more than 20 store errors in ten minutes; callers on that route are being refused."
  }
}
```

```json
{
  "alert": "AuthServiceRateLimitStoreNotReady",
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
                "metricName": "dev_health_runtime_check_failed",
                "timeAggregation": "max",
                "spaceAggregation": "max"
              }
            ],
            "filter": {
              "expression": "check = 'valkey' AND service.name = 'auth-service'"
            },
            "groupBy": [
              {
                "name": "service.instance.id"
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
          "target": 1,
          "op": "equal",
          "matchType": "all_the_times"
        }
      ]
    }
  },
  "evaluation": {
    "kind": "rolling",
    "spec": {
      "evalWindow": "5m",
      "frequency": "30s"
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
    "summary": "auth-service cannot reach its rate-limit store",
    "description": "The required readiness check valkey has failed for five minutes on a replica: /readyz answers 503 and limited auth routes answer a closed 500. The alert is grouped by service.instance.id. Next: Rate-limit store down in docs/reference/auth/readiness-and-slo.md."
  }
}
```

## Step 3: prove the rules read live series

1. Open each rule and run its builder query in the editor over the last hour.
2. Expected: `AuthServiceRateLimitStoreErrors` and `AuthServiceRateLimitStoreErrorsHigh` return series with value 0 (or no data points above 0). `AuthServiceRateLimitStoreNotReady` returns no series above 0 while the store is healthy.
3. A query that returns "no data" and not zero means that the filter or the metric name does not match what SigNoz stores, the series is not scraped, or the collector is down: go back to Step 1. For `AuthServiceRateLimitStoreNotReady`, also look at the values of the `check` and `service.name` attributes before you change the filter. Stop and report; do not save the rule.

No test of a firing alert is run in prod. The rule shapes are checked in CI against the series the service really writes.

## Valkey memory and eviction rules

The `valkey` group in `alerts/rules.yml` watches the in-chart Valkey that holds the auth-service rate-limit counters. The series come from the collector's redis receiver (`redis.keys.evicted`, `redis.memory.used`). Its rule text is the text of record, and the two rules are saved in SigNoz as builder threshold rules, which are the JSON below. PromQL rules do not evaluate on this SigNoz (D6417, see Step 2), so these two rules are saved in the builder form too. Import the two rules in the same way as Step 2, with the JSON below. The rules carry no channel: they route by notification policy (`usePolicy`).

The memory threshold is 161061274 bytes: 80% of the maxmemory 192mb (201326592 bytes) that the roll step for CHAOS-11630 sets. Change the rule and `valkey.maxmemory` together. Until that roll step runs, the live maxmemory is larger (256 MiB), so the memory rule is only stricter.

```json
{
  "alert": "ValkeyKeysEvicted",
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
                "metricName": "redis.keys.evicted",
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
          "matchType": "at_least_once"
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
    "summary": "Valkey evicted keys at maxmemory",
    "description": "The in-chart Valkey evicted keys because it reached maxmemory. Next: redis.memory.used and redis.db.keys, then valkey.maxmemory."
  }
}
```

```json
{
  "alert": "ValkeyMemoryNearMaxmemory",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "threshold_rule",
  "schemaVersion": "v2alpha1",
  "condition": {
    "compositeQuery": {
      "queryType": "builder",
      "panelType": "graph",
      "unit": "bytes",
      "queries": [
        {
          "type": "builder_query",
          "spec": {
            "name": "A",
            "signal": "metrics",
            "stepInterval": 60,
            "aggregations": [
              {
                "metricName": "redis.memory.used",
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
          "target": 161061274,
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
    "summary": "Valkey used memory above 80% of maxmemory for ten minutes",
    "description": "redis.memory.used is above 161061274 bytes (80% of maxmemory 192mb) for ten minutes. Next: redis.db.keys and redis.keys.evicted, then valkey.maxmemory (container limit 256Mi)."
  }
}
```

Checks after import:

1. Run each builder query over the last hour. A result of "no data" (and not 0) means that the metric name does not match what SigNoz stores, the metric is not scraped, or the collector is down. Stop and report; do not save the rule. Expected: `redis.memory.used` (max) near 2 MiB, and `redis.keys.evicted` (increase, sum) 0.
2. Planted threshold, memory rule only: create a temporary copy of the memory rule with the target set to 1000000 bytes, which is below the normal used memory of about 2 MiB. Wait one evaluation window and check that the copy fires. Then delete the copy. Do not edit the real rule. The eviction rule is not planted in prod, because a real eviction is the only way to make it fire.
3. Expected after the check: both rules are "inactive".

## Rollback

- A rule that pages wrongly: disable that rule in SigNoz. Do not delete it. The file `alerts/rules.yml` stays the text of record.
- A scrape change that harms the collector: revert the deploy change and roll. The rules then read no data and stay inactive.
