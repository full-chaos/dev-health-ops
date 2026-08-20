# Service Level Objectives (SLOs) and Service Level Indicators (SLIs)

This document defines the SLOs and SLIs for the dev-health-ops platform.
These targets govern the reliability contract for the analytics API, data
pipeline, and background workers.

---

## 1. API Availability

### SLI
Proportion of HTTP requests that return a non-5xx response, measured over
a rolling 30-day window.

```
SLI = (requests with status < 500) / (total requests)
```

**Prometheus query:**
```promql
1 - (
  sum(rate(http_requests_total{status=~"5.."}[30d]))
  /
  sum(rate(http_requests_total[30d]))
)
```

### SLO
| Tier         | Target   | Error Budget (30d) |
|--------------|----------|--------------------|
| Production   | 99.5%    | ~3.6 hours         |
| Staging      | 95.0%    | ~36 hours          |

---

## 2. API Latency

### SLI
Proportion of requests that complete within the latency threshold.

```
SLI (P95) = fraction of requests with duration < 2.0s
SLI (P99) = fraction of requests with duration < 5.0s
```

**Prometheus query (P95):**
```promql
histogram_quantile(
  0.95,
  sum(rate(http_request_duration_seconds_bucket[5m])) by (le)
)
```

### SLO
| Percentile | Target Latency | SLO (% of window within target) |
|------------|---------------|----------------------------------|
| P50        | < 500ms       | 99%                              |
| P95        | < 2.0s        | 99%                              |
| P99        | < 5.0s        | 95%                              |

---

## 3. Analytics Data Freshness

### SLI
Time elapsed since the most recent successful metrics ingestion for any
active repository, measured as the age of the latest record in ClickHouse.

```
SLI = age_of_latest_record < freshness_threshold
```

### SLO
| Metric                    | Threshold | Target  |
|---------------------------|-----------|---------|
| Daily rollup freshness    | < 26h     | 99.5%   |
| Commit data freshness     | < 4h      | 99%     |
| Work item freshness       | < 6h      | 99%     |

**Alert:** See `alerts/rules.yml` — future `DataStaleness` alert group.

---

## 4. Celery Worker Reliability

### SLI
Proportion of Celery task executions that complete successfully (not
`FAILURE` or `REVOKED`), measured over 24 hours.

```
SLI = devhealth_celery_tasks_total{state="success"} / devhealth_celery_tasks_total
```

**Prometheus query:**
```promql
sum(rate(devhealth_celery_tasks_total{state="success"}[24h]))
/
sum(rate(devhealth_celery_tasks_total[24h]))
```

### SLO
| Queue        | Target Success Rate |
|--------------|---------------------|
| metrics      | 99%                 |
| sync         | 99%                 |
| webhooks     | 99.5%               |
| default      | 95%                 |

---

## 5. LLM Categorisation Reliability

### SLI
Proportion of LLM categorisation requests that return a valid result
(not an error or repair-path fallback).

```
SLI = devhealth_llm_requests_total{status="success"} / devhealth_llm_requests_total
```

### SLO
| Target Success Rate | Latency P95 |
|---------------------|-------------|
| 95%                 | < 30s       |

---

## 6. ClickHouse Query Latency

### SLI
P95 latency of analytical queries executed against ClickHouse.

**Prometheus query:**
```promql
histogram_quantile(
  0.95,
  sum(rate(devhealth_clickhouse_query_duration_seconds_bucket[5m])) by (le)
)
```

### SLO
| Percentile | Target  |
|------------|---------|
| P50        | < 500ms |
| P95        | < 2.0s  |
| P99        | < 5.0s  |

---

## Error Budget Policy

- Error budget is calculated per 30-day rolling window.
- When error budget drops below **50%**, engineering is notified.
- When error budget drops below **20%**, all non-critical feature work is
  paused until reliability is restored.
- Post-mortems are mandatory for any incident that consumes > 10% of the
  monthly error budget.

---

## Review Cadence

SLOs are reviewed quarterly by the platform team. Targets are adjusted
based on observed reliability trends and business requirements.

*Last updated: 2026-02-27 (CHAOS-677)*
