---
page_id: op-rb-auth-service-scrape-alerts
summary: Check that the auth-service operator /metrics is scraped in prod and load the three rate-limit alert rules into SigNoz.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth-service scrape check and alert import

This runbook is the login-flip gate for monitoring: the auth-service operator `/metrics` is scraped, and the three `auth_service_rate_limit` rules from `alerts/rules.yml` are loaded in SigNoz. Both steps are done by the owner. Nothing here is run by a lane or a script. No Prometheus loads `alerts/rules.yml` in prod: the file is the text of record, and the rules are created in SigNoz from it.

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

Create each rule in SigNoz (Alerts, New alert, import JSON). The JSON below is the text of the three rules in `alerts/rules.yml`. The evaluation window is 5 minutes with "all the times" matching: this is the `for: 5m` of the file. The notification channel is left empty on purpose: pick the platform channel at import. Field names follow the SigNoz rule schema; if your SigNoz version rejects a field, keep the query, the 5 minute window, the severity and the labels, and set the rest in the form.

### AuthServiceRateLimitStoreErrors

```json
{
  "alert": "AuthServiceRateLimitStoreErrors",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "promql_rule",
  "version": "v4",
  "evalWindow": "5m0s",
  "frequency": "30s",
  "condition": {
    "compositeQuery": {
      "queryType": "promql",
      "panelType": "graph",
      "unit": "",
      "promQueries": {
        "A": {
          "name": "A",
          "query": "sum by (limit) (rate(dev_health_api_rate_limit_store_errors_total{limit=~\"auth_svc_.*\"}[5m])) > 0",
          "legend": "{{limit}}",
          "disabled": false
        }
      }
    },
    "op": "1",
    "target": 0,
    "matchType": "2",
    "selectedQueryName": "A"
  },
  "labels": {
    "severity": "warning",
    "team": "platform"
  },
  "annotations": {
    "summary": "auth-service rate-limit store errors on {{$labels.limit}}",
    "description": "The shared rate-limit store failed for limiter {{$labels.limit}}. Limited auth routes answer a closed 500 while it fails, by design. Next: Rate-limit store down in docs/reference/auth/readiness-and-slo.md."
  },
  "preferredChannels": []
}
```

### AuthServiceRateLimitStoreErrorsHigh

```json
{
  "alert": "AuthServiceRateLimitStoreErrorsHigh",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "promql_rule",
  "version": "v4",
  "evalWindow": "5m0s",
  "frequency": "30s",
  "condition": {
    "compositeQuery": {
      "queryType": "promql",
      "panelType": "graph",
      "unit": "",
      "promQueries": {
        "A": {
          "name": "A",
          "query": "sum by (limit) (increase(dev_health_api_rate_limit_store_errors_total{limit=~\"auth_svc_.*\"}[10m])) > 20",
          "legend": "{{limit}}",
          "disabled": false
        }
      }
    },
    "op": "1",
    "target": 0,
    "matchType": "2",
    "selectedQueryName": "A"
  },
  "labels": {
    "severity": "critical",
    "team": "platform"
  },
  "annotations": {
    "summary": "Auth rate-limit store is failing at volume",
    "description": "Limit {{$labels.limit}}: more than 20 store errors in ten minutes; callers on that route are being refused."
  },
  "preferredChannels": []
}
```

### AuthServiceRateLimitStoreNotReady

```json
{
  "alert": "AuthServiceRateLimitStoreNotReady",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "promql_rule",
  "version": "v4",
  "evalWindow": "5m0s",
  "frequency": "30s",
  "condition": {
    "compositeQuery": {
      "queryType": "promql",
      "panelType": "graph",
      "unit": "",
      "promQueries": {
        "A": {
          "name": "A",
          "query": "max by (instance) (dev_health_runtime_check_failed{check=\"valkey\"} and on (instance) dev_health_runtime_info{service=\"auth-service\"}) == 1",
          "legend": "{{instance}}",
          "disabled": false
        }
      }
    },
    "op": "1",
    "target": 0,
    "matchType": "2",
    "selectedQueryName": "A"
  },
  "labels": {
    "severity": "critical",
    "team": "platform"
  },
  "annotations": {
    "summary": "auth-service cannot reach its rate-limit store ({{$labels.instance}})",
    "description": "The required readiness check valkey has failed for five minutes on {{$labels.instance}}: /readyz answers 503 and limited auth routes answer a closed 500. Next: Rate-limit store down in docs/reference/auth/readiness-and-slo.md."
  },
  "preferredChannels": []
}
```

Expected after import: three rules in the alert list, state "inactive" (not firing), with the severities above.

## Step 3: prove the rules read live series

1. Open each rule and run its query in the editor over the last hour.
2. Expected: `AuthServiceRateLimitStoreErrors` and `AuthServiceRateLimitStoreErrorsHigh` return series with value 0 (or no data points above 0). `AuthServiceRateLimitStoreNotReady` returns no series above 0 while the store is healthy.
3. A query that returns "no data" and not zero means the series is not scraped: go back to Step 1.

No test of a firing alert is run in prod. The rule shapes are checked in CI against the series the service really writes.

## Valkey memory and eviction rules

The `valkey` group in `alerts/rules.yml` watches the in-chart Valkey that holds the auth-service rate-limit counters. The series come from the collector's redis receiver (`redis.keys.evicted`, `redis.memory.used`). The receiver writes dotted names only, so the queries select them by `__name__`. Import the two rules in the same way as Step 2, with the platform channel picked at import.

The memory threshold is 161061274 bytes: 80% of the maxmemory 192mb (201326592 bytes) that the roll step for CHAOS-11630 sets. Change the rule and `valkey.maxmemory` together. Until that roll step runs, the live maxmemory is larger (256 MiB), so the memory rule is only stricter.

```json
{
  "alert": "ValkeyKeysEvicted",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "promql_rule",
  "version": "v4",
  "evalWindow": "10m0s",
  "frequency": "1m0s",
  "condition": {
    "compositeQuery": {
      "queryType": "promql",
      "panelType": "graph",
      "unit": "",
      "promQueries": {
        "A": {
          "name": "A",
          "query": "sum(increase({__name__=\"redis.keys.evicted\"}[10m])) > 0",
          "legend": "evicted keys (10m)",
          "disabled": false
        }
      }
    },
    "op": "1",
    "target": 0,
    "matchType": "1",
    "selectedQueryName": "A"
  },
  "labels": {
    "severity": "warning",
    "team": "platform"
  },
  "annotations": {
    "summary": "Valkey evicted keys at maxmemory",
    "description": "The in-chart Valkey evicted keys because it reached maxmemory. Next: redis.memory.used and redis.db.keys, then valkey.maxmemory."
  },
  "preferredChannels": []
}
```

```json
{
  "alert": "ValkeyMemoryNearMaxmemory",
  "alertType": "METRIC_BASED_ALERT",
  "ruleType": "promql_rule",
  "version": "v4",
  "evalWindow": "10m0s",
  "frequency": "1m0s",
  "condition": {
    "compositeQuery": {
      "queryType": "promql",
      "panelType": "graph",
      "unit": "bytes",
      "promQueries": {
        "A": {
          "name": "A",
          "query": "max({__name__=\"redis.memory.used\"}) > 161061274",
          "legend": "used_memory",
          "disabled": false
        }
      }
    },
    "op": "1",
    "target": 161061274,
    "matchType": "2",
    "selectedQueryName": "A"
  },
  "labels": {
    "severity": "warning",
    "team": "platform"
  },
  "annotations": {
    "summary": "Valkey used memory above 80% of maxmemory for ten minutes",
    "description": "redis.memory.used is above 161061274 bytes (80% of maxmemory 192mb) for ten minutes. Next: redis.db.keys and redis.keys.evicted, then valkey.maxmemory (container limit 256Mi)."
  },
  "preferredChannels": []
}
```

Checks after import:

1. Run each query in the editor over the last hour. A result of "no data" (and not 0) means that the `__name__` form or the metric name does not match what SigNoz stores. Stop and report; do not save the rule.
2. Planted threshold, memory rule only: set the target and the query threshold to 1000000 bytes, which is below the normal used memory of about 2 MiB. Wait one evaluation window and check that the rule fires. Then set both back to 161061274. The eviction rule is not planted in prod, because a real eviction is the only way to make it fire.
3. Expected after the restore: both rules are "inactive".

## Rollback

- A rule that pages wrongly: disable that rule in SigNoz. Do not delete it. The file `alerts/rules.yml` stays the text of record.
- A scrape change that harms the collector: revert the deploy change and roll. The rules then read no data and stay inactive.
