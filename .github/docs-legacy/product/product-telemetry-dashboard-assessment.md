# Product Telemetry Dashboard Assessment

Status: recommended path for CHAOS-1785

## Summary

Use first-party ClickHouse dashboards as the near-term product telemetry analytics
path. Keep SigNoz, PostHog, Plausible, and Kafka as optional follow-ups with
narrow entry criteria.

The current product telemetry pipeline already treats
`product_telemetry_events` as the source of truth, retains raw events for 180
days, and keeps product usage events separate from voluntary instance telemetry
and feature/release telemetry. That makes a first-party dashboard the smallest
correct next step: it can query persisted events directly, preserve the privacy
contract, and avoid vendor semantics in product code.

External tools should mirror only selected, sanitized events after a concrete
need appears. They must not become the canonical source of product telemetry.

## Current source of truth

Product telemetry is persisted in ClickHouse table
`product_telemetry_events` with these analytics fields:

- `org_id_hash`
- `event_id`
- `name`
- `schema_version`
- `session_id`
- `anonymous_user_id`
- `route_pattern`
- `payload_json`
- `occurred_at`
- `ingested_at`
- `source`

Raw event retention is 180 days. Longer-term product analytics should come from
future rollups, not from exporting raw events to vendor tools as the durable
record.

## Requirements checked

The dashboard path needs to answer these questions from early ClickHouse data:

1. How many anonymous users are active each day?
2. Which route patterns are used most?
3. Which stable feature IDs are viewed most?
4. Which filters change most often by view and filter key?
5. Which chart actions are used by chart type and surface?
6. Which client errors appear by route pattern, boundary, and error class?
7. What do session duration, pages viewed, and interaction counts look like?

These are product-usage questions, not observability questions. They should be
answered from persisted product events first.

## Example ClickHouse query sketches

These sketches assume `payload_json` stores sanitized JSON and that the event
catalog remains stable at `2026-05-telemetry-v1`.

### Daily active anonymous users

```sql
SELECT
    toDate(occurred_at) AS day,
    uniqExact(anonymous_user_id) AS active_anonymous_users
FROM product_telemetry_events
WHERE occurred_at >= now() - INTERVAL 30 DAY
GROUP BY day
ORDER BY day;
```

### Top route patterns

```sql
SELECT
    route_pattern,
    count() AS events,
    uniqExact(session_id) AS sessions,
    uniqExact(anonymous_user_id) AS anonymous_users
FROM product_telemetry_events
WHERE name = 'page_viewed'
  AND occurred_at >= now() - INTERVAL 30 DAY
GROUP BY route_pattern
ORDER BY events DESC
LIMIT 25;
```

### Feature usage

```sql
SELECT
    JSONExtractString(payload_json, 'feature') AS feature,
    JSONExtractString(payload_json, 'surface') AS surface,
    count() AS views,
    uniqExact(anonymous_user_id) AS anonymous_users
FROM product_telemetry_events
WHERE name = 'feature_viewed'
  AND occurred_at >= now() - INTERVAL 30 DAY
GROUP BY feature, surface
ORDER BY views DESC;
```

### Filter changes

```sql
SELECT
    JSONExtractString(payload_json, 'view') AS view,
    JSONExtractString(payload_json, 'filterKey') AS filter_key,
    count() AS changes,
    avg(JSONExtractInt(payload_json, 'valueCount')) AS avg_value_count
FROM product_telemetry_events
WHERE name = 'filter_changed'
  AND occurred_at >= now() - INTERVAL 30 DAY
GROUP BY view, filter_key
ORDER BY changes DESC;
```

### Chart interactions

```sql
SELECT
    JSONExtractString(payload_json, 'chart') AS chart,
    JSONExtractString(payload_json, 'action') AS action,
    JSONExtractString(payload_json, 'surface') AS surface,
    count() AS interactions,
    uniqExact(session_id) AS sessions
FROM product_telemetry_events
WHERE name = 'chart_interacted'
  AND occurred_at >= now() - INTERVAL 30 DAY
GROUP BY chart, action, surface
ORDER BY interactions DESC;
```

### Client errors

```sql
SELECT
    route_pattern,
    JSONExtractString(payload_json, 'boundary') AS boundary,
    JSONExtractString(payload_json, 'errorClass') AS error_class,
    count() AS errors,
    uniqExact(anonymous_user_id) AS affected_anonymous_users
FROM product_telemetry_events
WHERE name = 'client_error'
  AND occurred_at >= now() - INTERVAL 30 DAY
GROUP BY route_pattern, boundary, error_class
ORDER BY errors DESC;
```

### Session duration distribution

```sql
SELECT
    quantiles(0.5, 0.75, 0.9, 0.95)(JSONExtractInt(payload_json, 'durationMs')) AS duration_ms_quantiles,
    avg(JSONExtractInt(payload_json, 'pagesViewed')) AS avg_pages_viewed,
    avg(JSONExtractInt(payload_json, 'interactions')) AS avg_interactions
FROM product_telemetry_events
WHERE name = 'session_ended'
  AND occurred_at >= now() - INTERVAL 30 DAY;
```

## Decision matrix

| Option | Source of truth fit | Privacy and consent risk | Operational burden | Funnel/cohort support | Observability correlation | Replay/fanout support | Implementation effort | Recommendation |
|--------|---------------------|--------------------------|--------------------|-----------------------|---------------------------|----------------------|-----------------------|----------------|
| First-party ClickHouse dashboard | Strong. Reads canonical `product_telemetry_events` directly. | Low. Reuses current sanitized event contract and 180-day retention. | Low. Uses existing ClickHouse and app/API surface. | Basic initially; can improve with rollups. | Limited unless joined with error/trace data later. | Limited to retained ClickHouse rows. | Low to medium. | Build first. |
| SigNoz observability dashboards | Weak for product usage source of truth; strong as observability mirror. | Medium. Must avoid raw URLs, query strings, user IDs, and payload expansion. | Medium. SigNoz is already the preferred observability direction but adds dashboard upkeep. | Weak. Not a product analytics tool. | Strong for traces, metrics, logs, errors, and performance correlation. | Weak for product replay. | Medium. | Use for error/performance correlation after first-party dashboard. |
| PostHog funnels/cohorts | Weak as source of truth; strong for dedicated product analytics workflows. | High unless consent, event allowlists, and autocapture/session replay disablement are enforced. | Medium. Adds SDK/config/vendor account or self-hosting path. | Strong. | Limited unless separately connected to observability data. | Moderate for product analytics retention, but not canonical. | Medium to high. | Defer until funnels/cohorts become a real product requirement. |
| Plausible aggregate analytics | Weak. Aggregate web analytics only. | Medium. Safer than broad product analytics, but still external and not canonical. | Low to medium. | Weak. | None. | None. | Low. | Use only for marketing-style aggregate page analytics, not app product analytics. |
| Kafka event fanout/replay | Strong as an event bus, not as a dashboard. | Medium. Requires schema and retention controls to avoid expanding raw event exposure. | High. Adds new infrastructure and schema discipline. | Depends on downstream consumers. | Depends on consumers. | Strong. | High. | Defer until Redis/Celery and ClickHouse retention are insufficient. |

## Recommended path

### 1. Build the first-party dashboard first

Create a small product telemetry dashboard backed by ClickHouse queries or
materialized rollups. The first version should show:

- Daily active anonymous users
- Top route patterns
- Feature views
- Filter changes by view/filter key
- Chart interactions by chart/action/surface
- Client errors by route pattern/boundary/error class
- Session duration distribution

This keeps the product analytics loop inspectable and aligned with the platform
principles: trends over absolutes, evidence over judgment, and persisted data as
the only source of rendered analytics.

### 2. Add SigNoz only for observability correlation

Use SigNoz when the question is about runtime health: errors, latency, traces,
logs, worker failures, or correlation between a client error and backend/API
behavior. Do not treat SigNoz dashboards as the product usage source of truth.

If product events are mirrored to SigNoz later, mirror only coarse counters or
selected sanitized events through a controlled OpenTelemetry collector/proxy.

### 3. Defer PostHog until funnels/cohorts are necessary

PostHog is a good fit only when the team needs product analytics workflows that
would be expensive to rebuild first-party: funnel drop-off, cohort retention,
experimentation, or lifecycle analysis.

Before adopting PostHog, define consent and data processing boundaries. Use
manual capture only. Autocapture, session replay, raw URLs, query strings,
names, emails, raw user IDs, raw org IDs, and selected filter values must remain
disabled or absent.

### 4. Use Plausible only for aggregate web analytics

Plausible fits marketing-style aggregate analytics: public landing pages,
referrer trends, and high-level page popularity. It does not fit authenticated
product telemetry where the platform needs feature usage, filters, charts,
sessions, and error dimensions from persisted product events.

### 5. Defer Kafka until fanout/replay needs are proven

Kafka should be introduced only if Redis Streams/Celery and ClickHouse retention
are not enough. Valid triggers include multiple independent downstream
consumers, replay requirements beyond ClickHouse retention, high-volume event
fanout, or a schema-registry-backed product data platform.

Kafka should not be added just to build dashboards.

## Adapter acceptance criteria

No external adapter should ship unless all of these are true:

1. The first-party ClickHouse pipeline remains the source of truth.
2. The adapter is optional and disabled by default unless explicitly configured.
3. Component code imports only the provider-agnostic telemetry interface.
4. Unit tests cover enabled and disabled adapter paths.
5. Manual browser network QA proves disabled configuration sends no requests to
   the vendor or collector.
6. Manual browser network QA proves enabled configuration sends only sanitized,
   allowed event fields.
7. Autocapture, session replay, raw URLs, query strings, names, emails, raw user
   IDs, raw org IDs, stack traces, issue titles, PR titles, and selected filter
   values are not sent.

## Follow-up issues to create when selected

- First-party dashboard: create an implementation issue for ClickHouse-backed
  dashboard queries, optional rollups, GraphQL/API exposure, and UI rendering.
- SigNoz mirror: create an issue for controlled OpenTelemetry product counters
  or sanitized selected-event export through an internal collector/proxy.
- PostHog mirror: create an issue for manual-only PostHog adapter tests,
  consent/config gates, disabled-network QA, and a data processing review.
- Plausible mirror: create an issue for aggregate public-page analytics only,
  explicitly excluding authenticated product event payloads.
- Kafka/event bus: create an architecture issue covering schema registry,
  replay retention, consumer ownership, DLQ policy, and ClickHouse sink
  consistency.

## Boundaries

In scope for CHAOS-1785:

- Assessment document
- Query sketches
- Decision matrix
- Recommendation
- Adapter acceptance criteria
- Follow-up issue list

Out of scope for CHAOS-1785:

- Building the dashboard
- Implementing Kafka
- Adding or enabling vendor adapters
- Enabling autocapture or session replay
- Changing the product telemetry event schema
- Moving source-of-truth status away from ClickHouse

## Final recommendation

Build the first-party ClickHouse dashboard next. It is the only option that
directly satisfies the current product telemetry contract without introducing a
new source of truth or widening the privacy surface.

Use SigNoz for observability correlation, PostHog for future funnels/cohorts,
Plausible for aggregate public-page analytics, and Kafka only for proven
fanout/replay needs.
