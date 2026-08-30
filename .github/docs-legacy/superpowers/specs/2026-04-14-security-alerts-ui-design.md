# Security Alerts UI — Design

**Date:** 2026-04-14
**Linear:** [CHAOS-66](https://linear.app/fullchaos/issue/CHAOS-66) (parent — backend sync)
**Scope:** Web frontend + GraphQL exposure for the security alerts synced by CHAOS-66
**Repos touched:** `dev-health-ops` (GraphQL layer), `dev-health-web` (UI)
**Branch (ops):** `feat/security-alert-sync`

## Goal

Surface synced security alerts (GitHub Dependabot, GitHub code scanning, GitHub advisories, GitLab vulnerability findings, GitLab dependency scanning) in the web app as a first-class area. Users get a dashboard of org-wide posture plus a triage queue, and can drill from a chart into a repo-scoped "evidence" page listing alerts for that repo.

## Context

CHAOS-66 landed the ingest half: `SecurityAlert` SQLAlchemy model (`ops/src/dev_health_ops/models/git.py:700-736`), connectors, processors, sink, and the ClickHouse table (`ops/src/dev_health_ops/migrations/clickhouse/032_security_alerts.sql`). No GraphQL exposure exists; the data is reachable only via direct DB queries.

The web app is Next.js 16 (App Router), React 19, urql, Tailwind 4. List-page conventions are set by `web/src/app/(app)/opportunities/page.tsx`: RSC page reads `searchParams`, decodes an encoded filter param (`?f=...`), and renders a `<FilterBar>` plus a data-table-style body. Dashboard conventions come from the existing analytics pages using `useOrgId()` from the `GraphQLProvider`.

## Non-goals

- Mutations (dismiss / assign / mark-fixed) — the upstream system (GitHub/GitLab) stays the source of truth. Rows link out.
- Local notes or annotations on alerts.
- Custom date range picker beyond 7 / 30 / 90 presets.
- Heatmap or Sankey visualizations.
- Notifications (Slack/email) on new Critical alerts.

These are explicitly parked; follow-up work only if asked.

## Architecture

Two resolvers on ops, two routes on web.

```
ops/                                  web/
┌─────────────────────────┐           ┌───────────────────────────────────┐
│ Strawberry GraphQL      │           │ Next.js App Router                │
│                         │           │                                   │
│ security_overview(...)  │ ◄──────── │ /security (dashboard + queue)     │
│ security_alerts(...)    │ ◄──────── │ /security/repos/[repoId] (evidence)│
└────────────┬────────────┘           └───────────────────────────────────┘
             │
             ▼
       ClickHouse (security_alerts JOIN repos)
```

Two resolvers (not one) because the existing codebase has two shapes:
- `work_graph_edges` is the per-row connection. Our `security_alerts` mirrors it.
- `analytics(batch)` is the dashboard roll-up. Our `security_overview` mirrors that idea with four aggregate buckets.

Evidence page reuses `security_alerts` with `repoIds: [id]` — no extra resolver needed.

## Backend additions (`ops`)

### Inputs — `ops/src/dev_health_ops/api/graphql/models/inputs.py`

Appended. String enums for stable wire format matching existing patterns (`WorkGraphEdgeTypeInput`, `inputs.py:227-243`):

```python
@strawberry.enum
class SecuritySeverityInput(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    UNKNOWN = "unknown"

@strawberry.enum
class SecuritySourceInput(Enum):
    DEPENDABOT = "dependabot"
    CODE_SCANNING = "code_scanning"
    ADVISORY = "advisory"
    GITLAB_VULNERABILITY = "gitlab_vulnerability"
    GITLAB_DEPENDENCY = "gitlab_dependency"

@strawberry.enum
class SecurityStateInput(Enum):
    OPEN = "open"
    FIXED = "fixed"
    DISMISSED = "dismissed"
    DETECTED = "detected"
    CONFIRMED = "confirmed"
    RESOLVED = "resolved"

@strawberry.input
class SecurityAlertFilterInput:
    repo_ids: list[str] | None = None
    severities: list[SecuritySeverityInput] | None = None
    sources: list[SecuritySourceInput] | None = None
    states: list[SecurityStateInput] | None = None
    since: datetime | None = None  # created_at >= since
    until: datetime | None = None  # created_at <= until
    open_only: bool = False        # shorthand for states=OPEN,DETECTED,CONFIRMED
    search: str | None = None      # ILIKE on title + package_name + cve_id

@strawberry.input
class SecurityPaginationInput:
    first: int = 50
    after: str | None = None       # cursor; format matches work_graph_edges
```

"Open" is defined once: `{open, detected, confirmed}`. `open_only=True` overrides `states` when both are provided (resolver coerces).

### Outputs — `ops/src/dev_health_ops/api/graphql/models/outputs.py`

Appended. Connection shape reuses the existing `PageInfo` (`outputs.py:142-189`).

```python
@strawberry.type
class SecurityAlertNode:
    alert_id: str
    repo_id: str
    repo_name: str            # joined from repos
    repo_url: str | None
    source: str               # lowercase string matching enum values
    severity: str
    state: str
    package_name: str | None
    cve_id: str | None
    url: str | None           # upstream link (GitHub/GitLab alert page)
    title: str | None
    description: str | None
    created_at: datetime
    fixed_at: datetime | None
    dismissed_at: datetime | None

@strawberry.type
class SecurityAlertEdge:
    node: SecurityAlertNode
    cursor: str

@strawberry.type
class SecurityAlertConnection:
    edges: list[SecurityAlertEdge]
    total_count: int
    page_info: PageInfo

@strawberry.type
class SecurityKpis:
    open_total: int
    critical: int
    high: int
    mean_days_to_fix_30d: float | None   # null if no alerts fixed in window
    open_delta_30d: int                   # net change in open over last 30d

@strawberry.type
class SeverityBucket:
    severity: str   # one of low/medium/high/critical/unknown
    count: int

@strawberry.type
class RepoAlertCount:
    repo_id: str
    repo_name: str
    repo_url: str | None
    count: int

@strawberry.type
class TrendPoint:
    day: date
    opened: int
    fixed: int

@strawberry.type
class SecurityOverview:
    kpis: SecurityKpis
    severity_breakdown: list[SeverityBucket]
    top_repos: list[RepoAlertCount]   # LIMIT 10, count DESC
    trend: list[TrendPoint]           # last 30d, one point per day
```

### Resolvers — `ops/src/dev_health_ops/api/graphql/resolvers/security.py` (new)

Mirrors `resolvers/work_graph.py` structure. Two resolver functions.

`resolve_security_alerts(context, org_id, filters, pagination) -> SecurityAlertConnection`
- Builds `WHERE` fragments from filters (org scoping via `repos.org_id`).
- JOINs `security_alerts sa` with `repos r` on `sa.repo_id = r.id`.
- Cursor format matches whatever `work_graph_edges` emits — to be confirmed in implementation (offset or composite `created_at|alert_id`).
- Sort: `severity_rank DESC, created_at DESC` where `severity_rank = map(severity, {critical:4, high:3, medium:2, low:1, unknown:0})`.
- Search: `ILIKE '%{search}%' ON (title, package_name, cve_id)` with parameterization (no string concatenation).

`resolve_security_overview(context, org_id, filters) -> SecurityOverview`
- Four parallel CH queries via `asyncio.gather`:
  1. KPIs: `COUNT(*) FILTER (WHERE state IN open_states)`, same for critical and high. Mean days to fix: `avg(dateDiff('day', created_at, fixed_at))` where `fixed_at` in last 30d. Delta: open count now minus open count at t-30d.
  2. Severity breakdown: `GROUP BY severity`.
  3. Top repos: `GROUP BY repo_id, repo_name ORDER BY count DESC LIMIT 10`.
  4. Trend: `GROUP BY toDate(day)` over last 30 days; `opened = count(created_at on day)`, `fixed = count(fixed_at on day)`.
- Filters apply consistently: the dashboard reflects whatever the user has narrowed by.

### Schema wiring — `ops/src/dev_health_ops/api/graphql/schema.py`

Two new fields on `Query` (around line 160, next to `work_graph_edges`):

```python
@strawberry.field(description="Paginated list of security alerts")
async def security_alerts(
    self,
    info: Info,
    org_id: str,
    filters: SecurityAlertFilterInput | None = None,
    pagination: SecurityPaginationInput | None = None,
) -> SecurityAlertConnection: ...

@strawberry.field(description="Aggregated security posture for the dashboard")
async def security_overview(
    self,
    info: Info,
    org_id: str,
    filters: SecurityAlertFilterInput | None = None,
) -> SecurityOverview: ...
```

### Backend tests — `ops/tests/graphql/test_security.py` (new)

Mirrors `tests/graphql/test_work_graph.py` conventions (mock `query_dicts`, call resolver directly, assert on shape). Coverage:
- `open_only=True` coerces to the open state set.
- Filter encoding for severities / sources / states produces the right `WHERE` fragments.
- Connection shape returns `edges`, `totalCount`, `pageInfo` correctly.
- Cursor round-trip: `after=<cursor from previous page>` returns the next page.
- Top-repos query returns `repo_name` (not just `repo_id`).
- Empty result returns an empty connection, not an error.

Sync-side tests in `ops/tests/test_security_alerts.py` already exist and stay green.

## Frontend additions (`web`)

### Routes

- `web/src/app/(app)/security/page.tsx` — RSC. Decodes `searchParams.f` into a `SecurityFilter` with `decodeFilter`. Renders `<SecurityDashboard filter={...} />` + `<SecurityAlertQueue filter={...} />`. Follows the shape of `opportunities/page.tsx:15-107`.
- `web/src/app/(app)/security/repos/[repoId]/page.tsx` — RSC. Merges `{ repoIds: [params.repoId] }` into the decoded filter. Renders only `<SecurityAlertQueue filter={...} lockedRepoId={params.repoId} />` (no dashboard).
- `web/src/app/(app)/security/loading.tsx` — page-level skeleton.
- `web/src/app/(app)/security/error.tsx` — page-level error with `<ErrorCard />`.

### GraphQL layer

- `web/src/lib/graphql/queries.ts` — append `SECURITY_OVERVIEW_QUERY` and `SECURITY_ALERTS_QUERY` string constants.
- `web/src/lib/graphql/hooks/useSecurity.ts` (new) — two urql hooks:
  - `useSecurityOverview(filter)` → `{ data, fetching, error }`.
  - `useSecurityAlerts(filter, pagination)` → `{ data, fetching, error, fetchMore }`.
  - Both pull `orgId` via `useOrgId()`, translate `SecurityFilter` into the GraphQL input.
- Codegen (`npm run codegen`) regenerates types into `src/lib/graphql/__generated__/` against the new ops schema.

### Components — `web/src/components/security/`

All new, grouped under one folder.

- `SecurityDashboard.tsx` (client) — grid layout:
  - Row 1: four `<KpiTile>` across — *Open alerts*, *Critical*, *High*, *Mean days to fix (30d)*.
  - Row 2: `<SeverityStackedBar>` (left) + `<TopReposChart>` (right).
  - Row 3: `<TrendChart>` full-width.
- `KpiTile.tsx` — `{ label, value, delta?, tone }`. Tones: `default | warn | danger`. Colors draw from existing `StatusBadge` tokens.
- `SeverityStackedBar.tsx` — wraps `components/charts/HorizontalBarChart`. Colors:
  - `critical: red-600`, `high: orange-500`, `medium: amber-400`, `low: slate-400`, `unknown: slate-300`.
- `TopReposChart.tsx` — wraps `HorizontalBarChart`. Each bar is a Next `<Link>` to `/security/repos/[repoId]`. Reads repo name + id from the overview data.
- `TrendChart.tsx` — simple line/area chart, `opened` vs `fixed` over last 30 days.
- `SecurityAlertQueue.tsx` (client) — hosts `<FilterBar view="security" />`, table of alerts, pagination controls ("Load more" style matching existing urql fetchMore pattern).
- `SecurityAlertRow.tsx` — one row: `SeverityBadge`, `SourceBadge`, title (truncated), package / CVE chip, repo chip (links to evidence page), `StateBadge`, relative age, external-link icon. Whole row is an anchor to `alert.url` with `target="_blank" rel="noopener noreferrer"`. Fallback when `url` is null: non-clickable row.
- `SeverityBadge.tsx`, `SourceBadge.tsx`, `StateBadge.tsx` — follow the `components/reports/StatusBadge.tsx` pill style. Source labels are human: "Dependabot", "Code Scanning", "Advisory", "GitLab Vuln", "GitLab Deps".

### Filters and URL state

- `web/src/lib/filters/security.ts` (new) — defines `SecurityFilter`:
  ```ts
  type SecurityFilter = {
    severities?: ('low'|'medium'|'high'|'critical'|'unknown')[];
    sources?: ('dependabot'|'code_scanning'|'advisory'|'gitlab_vulnerability'|'gitlab_dependency')[];
    states?: ('open'|'fixed'|'dismissed'|'detected'|'confirmed'|'resolved')[];
    repoIds?: string[];
    since?: string;   // ISO date
    until?: string;
    openOnly?: boolean;
    search?: string;
  };
  ```
  Default behavior: if `searchParams.f` is absent (first visit), the page injects `{ openOnly: true }` before rendering. Once the user touches the filter bar, all filter state — including `openOnly` — is written back to the URL and becomes the source of truth.
- Plugs into the existing `encodeFilter` / `decodeFilter` helpers (`web/src/lib/filters/encode.ts`).
- `FilterBar` gets a `view="security"` visibility config listing which chips to render.
- On the evidence page, the `repoIds` chip is rendered as a **locked pill** (no remove button) and excluded from `encodeFilter` output so the URL stays clean.

### Loading / error / empty

- Loading: per-widget `<Skeleton>` tiles; per-row skeleton in the table while `fetching` is true on first load.
- Error: page-level `error.tsx` catches render-time throws; per-widget `<ErrorCard />` when a single urql query errors so the rest of the page renders.
- Empty: `<EmptyState title="No alerts match these filters" cta="Clear filters" />` when the queue has zero edges.

### Navigation

- `PrimaryNav` gains one entry: icon (shield), label "Security", href `/security`. Role-gating (if any) follows whatever pattern the existing Reports or Admin nav entries use.

### Frontend tests

- Unit (vitest):
  - `SecurityFilter` encode / decode round-trip.
  - KPI mapper (overview response → props).
  - Severity sort order.
- E2E (Playwright) — one happy-path spec `tests/security.spec.ts`:
  1. Navigate to `/security`. Assert 4 KPI tiles visible.
  2. Assert top-repos chart has at least one bar (seeded fixture data).
  3. Click the first bar. Assert URL is `/security/repos/<id>`, assert queue is rendered with the locked `repoIds` filter pill.
  4. Click the first alert row. Assert external-link behavior (anchor's `href` + `target="_blank"`).

## Cross-repo coordination

1. **PR 1 (ops) — "feat(graphql): expose security alerts via GraphQL"**
   Adds inputs / outputs / resolvers / schema fields / backend tests. Merges onto `feat/security-alert-sync`. Regenerates any committed schema snapshot the web codegen consumes.
2. **PR 2 (web) — "feat(security): add /security dashboard and evidence pages"**
   Adds routes / components / hooks / filter encoding / tests. `npm run codegen` regenerates types. Opens after PR 1 is merged (or the exported schema is available).
3. **Nav wiring** lands inside PR 2.

## Quality gates

**ops**
- `pytest tests/graphql/test_security.py` — new test file passes.
- `pytest tests/test_security_alerts.py` — existing sync tests stay green.
- `ruff check` and `mypy` clean.

**web**
- `npm run test:unit` — filter encode/decode + mapper tests pass.
- `npm run test:e2e -- security.spec.ts` — happy-path spec passes.
- `npm run typecheck` — clean after codegen.

**Manual verification**
- `npm run dev` (web), visit `/security`.
- Exercise severity / source / state filters; confirm URL updates.
- Click a bar in top-repos chart; confirm navigation + locked pill.
- Click an alert row; confirm upstream link opens in a new tab.

## Open points (resolve in implementation, not design)

- **Cursor format** — match whatever `work_graph_edges` emits. If it's offset-based, use offset; if base64'd composite, use that. No new cursor convention.
- **`repos` in ClickHouse** — if the `repos` dimension isn't natively present in CH, either (a) query Postgres-side materialized view, (b) add a minimal `repos_lookup` CH table fed by the sync job. Decision falls out once the CH query is exercised.
- **`PrimaryNav` role-gating** — reuse whichever pattern Reports / Admin already use. Not a design decision.

## Success criteria

- `/security` renders 4 KPIs, severity breakdown, top-repos chart, trend, and a filterable queue of alerts.
- Clicking a bar in top-repos lands on `/security/repos/[repoId]` with the queue filtered to that repo (locked pill).
- Alert rows link out to the upstream GitHub/GitLab URL.
- URL-encoded filters round-trip across refresh.
- Codegen produces types; typecheck passes; unit and E2E suites pass.
