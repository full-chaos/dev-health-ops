---
page_id: op-rb-perf
summary: Recover saturation, provider throttling, or performance degradation without creating retry amplification.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Performance, rate limit, or saturation

1. Identify the constrained service, queue, provider bucket, store, or external service.
2. Check latency, error, saturation, queue age, deferrals, retry rate, and provider response headers.
3. Reduce or defer optional workload before increasing concurrency.
4. Confirm cache, query, batch, and connection behavior.
5. Recover a bounded workload and watch the limiting signal.
6. Restore normal budgets gradually.

Do not bypass provider budgets or rate limits in a way that hides delayed coverage or creates an outage loop.

## Ask Dev scope catalog

Ask Dev scope resolution reads only the tenant-scoped `repos`, `projects`,
`work_unit_investments`, `work_items`, `git_pull_requests`, and `teams` catalogs.
Each entity search is capped at 25 candidates; repository sets are capped at 20.
Candidate order is deterministic, so changing order between identical requests
usually indicates catalog or watermark drift rather than a client-side ranking
change.

The resolver uses a bounded request-local cache. Its key includes organization,
the effective permission fingerprint, normalized input, query version, and the
latest relevant source watermark. It has no distributed cache to flush. A role,
tenant, query-version, input, or watermark change produces a different key, and
an authorized GraphQL context rebind clears the request cache.

For a `catalog_unavailable` warning or a failed `devScopeSearch` query:

1. identify the entity kinds in the request without logging user text or IDs;
2. verify the corresponding ClickHouse tables and migrations are healthy;
3. check the latest source watermark and tenant-scoped query latency;
4. confirm every query includes `org_id` and a bounded `LIMIT`;
5. retry one bounded request after storage health recovers.

Do not replace a failed explicit reference with organization scope, increase the
25-candidate limit, or add a shared cache while troubleshooting.

## Ask Dev metric queries

Ask Dev V1 accepts only the eight registered metric IDs. Metric ranges are
limited to 366 days, repository and team sets to 20 IDs, returned dimension rows
to 12, series to 366 points per row, and serialized metric results to 64 KiB.
Every ClickHouse metric read has a 15-second hard deadline and uses
parameterized, allowlisted SQL with a tenant predicate.

Metric caching is request-local and capped at 128 entries. The cache key includes
organization, effective permission fingerprint, normalized resolved scope,
window, metric/definition/query/source versions, requested options, and the
source watermark. There is no cross-request metric cache to flush. A source
watermark, permission, tenant, version, or normalized-input change bypasses the
old entry.

When investigating latency, preserve the metric ID and source table, inspect the
bounded ClickHouse query and watermark read, and confirm comparison did not turn
one requested metric into an unexpected unbounded scan. Do not widen the range,
row, series, timeout, or request-cache limits to mask a slow source query.
