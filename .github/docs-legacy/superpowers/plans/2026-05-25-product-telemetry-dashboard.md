# Product Telemetry Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first-party ClickHouse-backed product telemetry dashboard recommended by `docs/product/product-telemetry-dashboard-assessment.md`.

**Architecture:** Keep `product_telemetry_events` as the canonical source of truth. Add a backend read model and GraphQL query that aggregates persisted ClickHouse rows, then render the result in `dev-health-web` with existing chart/table patterns. CHAOS-1868 remains responsible for ingestion; this plan covers the read/API/UI path.

**Tech Stack:** Python, FastAPI, Strawberry GraphQL, ClickHouse, pytest, Next.js, TypeScript, urql, Vitest, Playwright.

---

## Linear Tracking

- Parent: CHAOS-1869 — First-party product telemetry dashboard
- Backend read model: CHAOS-1870
- API/GraphQL exposure: CHAOS-1871
- Web dashboard: CHAOS-1872
- QA/privacy pass: CHAOS-1873
- Related ingestion dependency: CHAOS-1868

## File Structure

### Ops backend

- Create `src/dev_health_ops/api/product_telemetry/dashboard.py` for ClickHouse dashboard query SQL and row mapping.
- Create `tests/api/test_product_telemetry_dashboard.py` for backend read-model tests using fake ClickHouse clients.
- Modify `src/dev_health_ops/api/graphql/models/outputs.py` to add typed Strawberry output objects for dashboard sections.
- Modify `src/dev_health_ops/api/graphql/models/inputs.py` to add a date-range input if no reusable date range fits the dashboard query.
- Create `src/dev_health_ops/api/graphql/resolvers/product_telemetry.py` for auth-scoped resolver orchestration.
- Modify `src/dev_health_ops/api/graphql/schema.py` to expose `productTelemetryDashboard`.
- Add or update GraphQL resolver tests under `tests/api/graphql/`.
- Run `python -m dev_health_ops.api.graphql.export_schema` if schema export changes `dev-health-web` contracts.

### Web frontend

- Modify generated GraphQL schema/types after ops schema export, following existing `src/lib/graphql/__generated__/` workflow.
- Create `src/lib/graphql/productTelemetryFetchers.ts` for server-side dashboard query fetching.
- Create `src/lib/graphql/hooks/useProductTelemetryDashboard.ts` only if the page needs client-side refresh; otherwise prefer server-side fetching.
- Create `src/components/product-telemetry/ProductTelemetryDashboard.tsx` for dashboard composition.
- Create focused section components under `src/components/product-telemetry/` for route usage, feature usage, filter changes, chart interactions, client errors, and session summary if the main component grows beyond a readable single page.
- Create `src/app/(app)/admin/product-telemetry/page.tsx` for the admin dashboard route.
- Add tests next to new frontend modules and Playwright coverage for the route.

## Task 1: Backend read model over ClickHouse (CHAOS-1870)

**Files:**
- Create: `src/dev_health_ops/api/product_telemetry/dashboard.py`
- Create: `tests/api/test_product_telemetry_dashboard.py`

- [ ] **Step 1: Write failing tests for dashboard query orchestration**

Create `tests/api/test_product_telemetry_dashboard.py` with a fake client that records SQL calls and returns section-specific rows:

```python
from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from dev_health_ops.api.product_telemetry.dashboard import (
    ProductTelemetryDashboardRange,
    load_product_telemetry_dashboard,
)


class FakeQueryClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []


async def fake_query_dicts(
    client: FakeQueryClient, sql: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    client.calls.append((sql, params))
    if "active_anonymous_users" in sql:
        return [{"day": date(2026, 5, 24), "active_anonymous_users": 7}]
    if "FROM product_telemetry_events" in sql and "route_pattern" in sql and "page_viewed" in sql:
        return [{"route_pattern": "/metrics", "events": 9, "sessions": 3, "anonymous_users": 2}]
    if "feature_viewed" in sql:
        return [{"feature": "investment", "surface": "dashboard", "views": 5, "anonymous_users": 2}]
    if "filter_changed" in sql:
        return [{"view": "metrics", "filter_key": "team", "changes": 4, "avg_value_count": 1.5}]
    if "chart_interacted" in sql:
        return [{"chart": "quadrant", "action": "hover", "surface": "metrics", "interactions": 8, "sessions": 2}]
    if "client_error" in sql:
        return [{"route_pattern": "/metrics", "boundary": "chart", "error_class": "RenderError", "errors": 2, "affected_anonymous_users": 1}]
    if "session_ended" in sql:
        return [{"p50_duration_ms": 1000, "p75_duration_ms": 1500, "p90_duration_ms": 2500, "p95_duration_ms": 3000, "avg_pages_viewed": 4.0, "avg_interactions": 11.0}]
    return []


@pytest.mark.asyncio
async def test_load_product_telemetry_dashboard_queries_all_sections(monkeypatch) -> None:
    client = FakeQueryClient()
    monkeypatch.setattr(
        "dev_health_ops.api.product_telemetry.dashboard.query_dicts",
        fake_query_dicts,
    )

    result = await load_product_telemetry_dashboard(
        client,
        org_id_hash="org_hash_123",
        date_range=ProductTelemetryDashboardRange(
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 25)
        ),
    )

    assert result.daily_active_users[0].active_anonymous_users == 7
    assert result.top_routes[0].route_pattern == "/metrics"
    assert result.feature_views[0].feature == "investment"
    assert result.filter_changes[0].filter_key == "team"
    assert result.chart_interactions[0].chart == "quadrant"
    assert result.client_errors[0].error_class == "RenderError"
    assert result.session_summary.p95_duration_ms == 3000
    assert len(client.calls) == 7
    assert all(call[1]["org_id_hash"] == "org_hash_123" for call in client.calls)
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
pytest tests/api/test_product_telemetry_dashboard.py -q
```

Expected: fail because `dev_health_ops.api.product_telemetry.dashboard` does not exist.

- [ ] **Step 3: Implement typed dashboard read model and queries**

Create `src/dev_health_ops/api/product_telemetry/dashboard.py` with dataclasses for each dashboard section and a `load_product_telemetry_dashboard()` function. Use `dev_health_ops.api.queries.client.query_dicts` for execution. Every query must include `org_id_hash = %(org_id_hash)s`, `occurred_at >= %(start)s`, and `occurred_at < %(end)s`.

The SQL sections should implement the sketches from `docs/product/product-telemetry-dashboard-assessment.md` with stable aliases:

```sql
SELECT toDate(occurred_at) AS day, uniqExact(anonymous_user_id) AS active_anonymous_users
FROM product_telemetry_events
WHERE org_id_hash = %(org_id_hash)s
  AND occurred_at >= %(start)s
  AND occurred_at < %(end)s
GROUP BY day
ORDER BY day
```

Repeat for `page_viewed`, `feature_viewed`, `filter_changed`, `chart_interacted`, `client_error`, and `session_ended` using the assessment’s dimensions and aliases.

- [ ] **Step 4: Run backend read-model tests**

Run:

```bash
pytest tests/api/test_product_telemetry_dashboard.py -q
```

Expected: pass.

## Task 2: GraphQL dashboard API (CHAOS-1871)

**Files:**
- Modify: `src/dev_health_ops/api/graphql/models/inputs.py`
- Modify: `src/dev_health_ops/api/graphql/models/outputs.py`
- Create: `src/dev_health_ops/api/graphql/resolvers/product_telemetry.py`
- Modify: `src/dev_health_ops/api/graphql/schema.py`
- Create: `tests/api/graphql/test_product_telemetry_dashboard_resolver.py`

- [ ] **Step 1: Write failing resolver test**

Create a test that builds a `GraphQLContext` with `org_id="org_hash_123"` and fake client, monkeypatches `load_product_telemetry_dashboard`, calls `resolve_product_telemetry_dashboard`, and asserts the output shape includes all seven sections.

```python
from __future__ import annotations

from datetime import date

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import ProductTelemetryDashboardInput
from dev_health_ops.api.graphql.resolvers.product_telemetry import (
    resolve_product_telemetry_dashboard,
)
from dev_health_ops.api.product_telemetry.dashboard import (
    ProductTelemetryDashboard,
    ProductTelemetryDailyActiveUsers,
    ProductTelemetrySessionSummary,
)


@pytest.mark.asyncio
async def test_resolve_product_telemetry_dashboard_requires_org_and_maps_sections(monkeypatch) -> None:
    async def fake_loader(client, org_id_hash, date_range):
        assert org_id_hash == "org_hash_123"
        return ProductTelemetryDashboard(
            daily_active_users=[ProductTelemetryDailyActiveUsers(day=date(2026, 5, 24), active_anonymous_users=7)],
            top_routes=[],
            feature_views=[],
            filter_changes=[],
            chart_interactions=[],
            client_errors=[],
            session_summary=ProductTelemetrySessionSummary(),
        )

    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.product_telemetry.load_product_telemetry_dashboard",
        fake_loader,
    )

    context = GraphQLContext(client=object(), org_id="org_hash_123")
    result = await resolve_product_telemetry_dashboard(
        context,
        ProductTelemetryDashboardInput(start_date=date(2026, 5, 1), end_date=date(2026, 5, 25)),
    )

    assert result.daily_active_users[0].active_anonymous_users == 7
    assert result.session_summary.avg_pages_viewed is None
```

- [ ] **Step 2: Run resolver test to verify red**

Run:

```bash
pytest tests/api/graphql/test_product_telemetry_dashboard_resolver.py -q
```

Expected: fail because GraphQL input/output/resolver types are missing.

- [ ] **Step 3: Add GraphQL input and output contracts**

Add `ProductTelemetryDashboardInput` to `models/inputs.py` with `start_date: date` and `end_date: date`. Add Strawberry output types to `models/outputs.py` matching the backend dataclasses:

- `ProductTelemetryDailyActiveUsersType`
- `ProductTelemetryRouteUsageType`
- `ProductTelemetryFeatureViewType`
- `ProductTelemetryFilterChangeType`
- `ProductTelemetryChartInteractionType`
- `ProductTelemetryClientErrorType`
- `ProductTelemetrySessionSummaryType`
- `ProductTelemetryDashboardType`

Use numeric fields as `int` or `float | None`, and expose `day` as `date`.

- [ ] **Step 4: Add resolver**

Create `src/dev_health_ops/api/graphql/resolvers/product_telemetry.py`. Use `require_org_id(context)`, ensure `context.client` exists, validate `start_date <= end_date`, call `load_product_telemetry_dashboard()`, and map dataclasses to Strawberry output types.

- [ ] **Step 5: Expose schema field**

In `schema.py`, import the input/output/resolver and add:

```python
@strawberry.field(description="Get first-party product telemetry dashboard metrics")
async def product_telemetry_dashboard(
    self,
    info: Info,
    org_id: str,
    input: ProductTelemetryDashboardInput,
) -> ProductTelemetryDashboardType:
    context = get_context(info)
    return await resolve_product_telemetry_dashboard(context, input)
```

- [ ] **Step 6: Run GraphQL tests**

Run:

```bash
pytest tests/api/graphql/test_product_telemetry_dashboard_resolver.py tests/api/test_product_telemetry_dashboard.py -q
```

Expected: pass.

## Task 3: Web dashboard data contract and route (CHAOS-1872)

**Files:**
- Modify: `web/src/lib/graphql/schema.graphql`
- Modify generated files under `web/src/lib/graphql/__generated__/`
- Create: `web/src/lib/graphql/productTelemetryFetchers.ts`
- Create: `web/src/components/product-telemetry/ProductTelemetryDashboard.tsx`
- Create: `web/src/app/(app)/admin/product-telemetry/page.tsx`
- Create tests next to new frontend files.

- [ ] **Step 1: Export and sync GraphQL schema**

From ops, run the schema export command used by `src/dev_health_ops/api/graphql/export_schema.py`, then copy or sync the schema into `web/src/lib/graphql/schema.graphql` following the existing repo workflow. Regenerate frontend GraphQL types with the package script defined in `web/package.json`.

- [ ] **Step 2: Write failing fetcher test**

Add a test for `productTelemetryFetchers.ts` that verifies the GraphQL document requests `productTelemetryDashboard` with `dailyActiveUsers`, `topRoutes`, `featureViews`, `filterChanges`, `chartInteractions`, `clientErrors`, and `sessionSummary`.

- [ ] **Step 3: Implement fetcher**

Create a server-side fetcher that accepts `{ orgId, startDate, endDate }`, calls the generated GraphQL document, and returns a normalized dashboard object. Follow the existing server GraphQL fetcher patterns in `src/lib/graphql/*Fetchers.ts`.

- [ ] **Step 4: Write failing component test**

Add a Vitest/Testing Library test for `ProductTelemetryDashboard` with representative data. Assert the page renders:

- daily active anonymous users trend heading
- top route `/metrics`
- feature `investment`
- filter key `team`
- chart `quadrant`
- error class `RenderError`
- session p95 duration

- [ ] **Step 5: Implement dashboard component and route**

Create the dashboard component using existing chart/table components from `src/components/charts/` and metric cards from `src/components/metrics/`. Add `src/app/(app)/admin/product-telemetry/page.tsx` with an authenticated admin page that fetches the last 30 days and renders the component.

- [ ] **Step 6: Run web tests**

Run from `web/`:

```bash
pnpm test ProductTelemetryDashboard productTelemetryFetchers
pnpm typecheck
```

Expected: pass.

## Task 4: End-to-end QA and privacy verification (CHAOS-1873)

**Files:**
- Add or modify Playwright test under `web/tests/`.
- Update PR/Linear evidence with screenshots when implementation lands.

- [ ] **Step 1: Seed representative product telemetry events**

Use the existing product telemetry ingestion fixtures or insert representative rows into local ClickHouse for these event names: `page_viewed`, `feature_viewed`, `filter_changed`, `chart_interacted`, `client_error`, and `session_ended`.

- [ ] **Step 2: Verify API surface manually**

Run a GraphQL query against the local ops API for `productTelemetryDashboard` with the seeded org and a 30-day range. Expected: all seven sections return seeded aggregates and no raw blocked fields appear.

- [ ] **Step 3: Verify browser surface manually**

Use the Playwright skill and a real browser to open `/admin/product-telemetry`. Expected: dashboard renders populated sections, empty-state behavior works with no seeded rows, and console has no runtime errors.

- [ ] **Step 4: Verify privacy/network boundaries**

In the browser network panel or Playwright request log, confirm the dashboard does not send product telemetry events to SigNoz, PostHog, Plausible, Kafka, or any external collector. Expected: only first-party app/API requests appear.

- [ ] **Step 5: Capture screenshots**

Capture dashboard screenshots and attach them to the implementation PR and CHAOS-1869/CHAOS-1872 when UI work lands.

## Verification Commands

Run from `ops/`:

```bash
pytest tests/api/test_product_telemetry_dashboard.py tests/api/graphql/test_product_telemetry_dashboard_resolver.py -q
ruff format --check src/dev_health_ops/api/product_telemetry src/dev_health_ops/api/graphql tests/api/test_product_telemetry_dashboard.py tests/api/graphql/test_product_telemetry_dashboard_resolver.py
ruff check src/dev_health_ops/api/product_telemetry src/dev_health_ops/api/graphql tests/api/test_product_telemetry_dashboard.py tests/api/graphql/test_product_telemetry_dashboard_resolver.py
```

Run from `web/`:

```bash
pnpm test ProductTelemetryDashboard productTelemetryFetchers
pnpm typecheck
```

Manual QA gate:

```text
1. Start the local stack.
2. Seed product_telemetry_events rows.
3. Query productTelemetryDashboard via GraphQL.
4. Open /admin/product-telemetry in a browser.
5. Capture screenshot evidence.
6. Confirm no external vendor/collector network requests.
```

## Self-Review

- Assessment coverage: the plan covers daily active anonymous users, top route patterns, feature views, filter changes, chart interactions, client errors, and session duration distribution.
- Boundary coverage: ClickHouse remains source of truth; no vendor adapter, Kafka, autocapture, session replay, or schema-source migration is included.
- Tracking coverage: Linear parent and subissues exist for backend, API, web, and QA work.
- Type consistency: backend dataclasses map to GraphQL output types, then to generated frontend GraphQL types.
- Placeholder scan: no task intentionally defers undefined behavior; follow-up adapters remain out of scope per assessment.
