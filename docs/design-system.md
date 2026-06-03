# Design & Style Framework — Dev Health Web

> **Status: binding.** Every UI change in `dev-health-web` must conform to this framework.
> "Done" requires conformance **plus** visual verification (see Part F).
>
> This file is the canonical source of truth. It is mirrored in Linear and referenced by
> the *Market-ready Frontend UX* milestone (CHAOS-2055 interaction/design normalization,
> CHAOS-2031 style rollout, and the Phase-1 trust issues). When this file and Linear
> diverge, **this file governs**. Reference it from the repo agent config (`CLAUDE.md` /
> `AGENTS.md`) so coding agents auto-load it.

## Why this exists

The UI inconsistencies were **not** caused by agents ignoring issues — the issues shipped
with real PRs. They were caused by *explicit, per-defect* issues producing isolated
point-fixes with no shared framework to conform to. One chart says "Unattributed", another
dumps raw JSON; one page resolves names, another shows `#cf3d56b4`; CTAs drift to
"Re-orient in Cockpit". The fix is systemic: canonical rules + tokens + shared primitives,
**enforced**, so every change uses them instead of inventing one-offs.

---

## Part A — Interaction & content rules (design)

- **A1 Sidebar** — major product areas only. No one-off metric pages without product approval.
- **A2 Tabs** — sibling views within one area only (Work: Investment / Flow / Landscape /
  Heatmap / Capacity / Evidence; AI Workflows: Impact / Attribution / Review Load /
  Test Gaps / Governance Risk / Evidence). Never style a navigation exit as a tab.
- **A3 Pills** — filters, scope, status, and segmented view controls only. Never for
  navigation, back, or primary CTAs.
- **A4 Buttons** — actions only, drawn from the CTA registry (Part D). Do not invent verbs.
- **A5 Back links** — one pattern: `Back to Cockpit` or `Back to {parent area}`. Never
  styled as pills/filters. One return path per screen — remove redundant ones (e.g.
  `Back to Metrics View` **and** `Back to Cockpit` together).
- **A6 Page-name agreement** — sidebar label = page title = breadcrumb = route metadata.
  Normalize: Delivery Risk vs Risk & Quality Drag; AI Workflows vs AI Workflow Intelligence;
  Coverage vs Coverage Delta.
- **A7 Identity labels — full-stack contract (not a render-only rule).** Never show a raw
  ID/hash as a primary user-facing label. The data layer must do the resolving:
  **GraphQL queries/resolvers must return a resolved display name alongside any id**
  (e.g. `compoundingRisk` must return `scope { id, displayName }`, not a bare `scope_id`),
  and the frontend renders it via `EntityLabel`. Fallback order, *only when the server
  genuinely cannot resolve*: (1) display name → (2) repo/name slug → (3) provider key with
  prefix → (4) shortened ID with an explicit `Unresolved` badge. A bare `#cf3d56b4` is
  non-compliant.
  - **Root-cause example (cockpit conclusion):** "Compounding risk appears elevated for
    {scope} across {scope}" is driven by `api/graphql?query=compoundingRisk&scope_id=<uuid>`.
    That query does **not** return a resolved scope display name, and it binds the **same
    `scope_id` into both the subject and the scope slot** — hence the UUID rendered twice
    ("for X across X"). The fix lives in the resolver: return `scope.displayName`, and make
    subject vs scope distinct fields. `EntityLabel` can only render a name the API returns —
    so this work spans the **GraphQL backend (`dev_health_ops`)** and `dev-health-web`, not
    the web repo alone. (Tracked: CHAOS-2064.)
- **A8 No internal/impl leakage** — never render in customer copy: raw IDs, API/GraphQL
  paths (`/api/v1/...`, `api/graphql?query=...&scope_id=...`), graph edge names (`DEPLOYS`,
  `LINKED_INCIDENT`), detector/telemetry jargon, version tags (`V1 SPARKLINE`), or Linear
  IDs (`CHAOS-1757`). Dev-only details go behind debug mode. Remove `Debug Filters` from
  customer/explore views.
- **A9 Never render raw data structures** — no JSON/object dumps in user-facing tables. The
  Evidence Table currently renders `{"repo_id":"…","number":1,…}`; map to typed, labeled
  fields.
- **A10 Active navigation** — exactly one selected destination at a time; hover/focus must
  be visually distinct from selected. No two outlined/selected items at once (observed:
  Coverage + Delivery Risk both selected; Bottlenecks highlighted on the Coverage page).
- **A11 Empty / unavailable / error states** — use the `DataState` component with
  customer-safe copy ("No prior period", "Ownership data not connected yet"), never bare
  `--`, blank panels, or raw red error blocks (CHAOS-2054). Evidence drawers use the
  `EvidencePanel` contract or a controlled empty state.
- **A12 Evidence vs recommendation** — an "Evidence" section contains artifacts
  (PR / commit / review / pipeline / incident / test / deployment) with a human summary +
  timestamp; recommendations live in a separate "Recommended next step" slot
  (EvidencePanel contract).

## Part B — Number, metric & data-integrity rules

- **B1 Formatting** — all numbers via `formatNumber` / `formatPercent`. No raw floats as
  labels (`74.74071428571429` → `74.7%`). Sensible precision, thousands separators,
  explicit units.
- **B2 Signed zero** — never render `-0%`; show `0%` or `No change`.
- **B3 Extremes** — extreme values (e.g. WIP 939%) must carry a baseline/band or
  interpretation; if a value looks impossible, validate the computation.
- **B4 Coherence** — numbers on one page reconcile or state their relationship (Pipelines
  already does this for success/failure — reuse the pattern).
- **B5 Distribution / forecast integrity** — percentiles must be monotonic. P50/P75/P90 all
  reading "4 weeks" is a collapse bug; assert `P50 ≤ P75 ≤ P90`.
- **B6 Threshold sanity** — default thresholds must be calibrated (Incident load 17.1/wk vs
  `Threshold 1.0/wk` reads as a wrong default).
- **B7 Resolver completeness** — a card/conclusion is not "done" if its backing query
  returns an unresolved id, an empty/flat result, or a missing field that forces a
  placeholder. The query is part of the deliverable (see A7 root-cause example). If a query
  is missing or returns nothing, fix the query — do not paper over it with a UUID or
  "appears flat".

## Part C — Style tokens

Use tokens only. **No hardcoded hex or px in components** — if a value is missing, add a
token, don't inline.

- **C1 Typography** — one locked scale (size/line-height): `display` 32/40, `h1` 24/32,
  `h2` 18/26, `h3` 15/22, `body` 14/22, `label-caps` 11/16 (+tracking), `mono` for code/IDs
  in dev only. Page title = `h1`; section header = `h2`; the uppercase descriptor pattern
  ("WIP SATURATION") = `label-caps`. Finalize values, then lock; this replaces the ad-hoc
  per-page sizing.
- **C2 Color roles** — semantic tokens, not raw hex: `bg`, `surface`, `surface-raised`,
  `border`, `text-primary`, `text-secondary`, `text-muted`, `accent` (orange), and status
  `positive` / `caution` / `negative` / `info`. Map status badges consistently
  (WATCH / ELEVATED → caution / negative; NORMAL → positive).
- **C3 Spacing** — 4px base scale (4 / 8 / 12 / 16 / 24 / 32 / 48). Standardize card padding
  and section gaps.
- **C4 Radius & elevation** — one radius scale (`sm` 6 / `md` 10 / `lg` 16 / `pill` 999) and
  one elevation set for cards/drawers.
- **C5 Charts** — one palette + conventions: sequential scale for heatmaps that **must map
  data variance** (the Churn heatmap renders uniform cyan today), categorical palette for
  series, consistent axis / gridline / tooltip styling, **styled tooltips** (Pipelines shows
  a blank white tooltip box), and a `ChartFrame` wrapper exposing title / interpretation /
  threshold slots.
- **C6 Density & alignment** — consistent KPI card layout, card heights, and sparkline
  treatment across TestOps / Work / Coverage.

## Part D — CTA vocabulary registry (typed constants)

Approved actions (add new ones here before use):
`Open evidence`, `Inspect associations`, `Open artifact`, `Export report`, `Apply filters`,
`Reset filters`, `Copy`.
Navigation: `Back to Cockpit`, `Back to {area}`.

Migrations:

| Found | Use |
| --- | --- |
| `Re-orient in Cockpit` | `Back to Cockpit` |
| `Back to Metrics View` | `Back to {area}` |
| `Open Landscapes` / `Explore Work` | a tab, or `Back to {area}` |
| `Open Flame` | `Open artifact` (or register `Open flame graph`) |
| standalone `EVIDENCE` label | `Open evidence` |

## Part E — Shared primitives (build once, use everywhere)

The only sanctioned implementations:

| Primitive | Enforces |
| --- | --- |
| `EntityLabel` | A7 |
| `DataState` | A11 |
| `EvidencePanel` | A12 |
| `MetricDelta` | B1 / B2 |
| `formatNumber` / `formatPercent` | B1 |
| `BackLink` | A5 |
| `ModeTabs` | A2 |
| `FilterPills` | A3 |
| `ChartFrame` + chart theme | C5 |
| CTA constants | D |

New surfaces **compose** these — do not re-implement. The `render-safe entity-label helper`
shipped in PR #555 (CHAOS-2034) is the seed of `EntityLabel`; consolidate everything onto
it. **Note:** `EntityLabel` can only render a name the API returns — the backing
query/resolver must supply `displayName` (A7 / B7), so the primitive is incomplete without
the matching GraphQL contract.

## Part F — Enforcement (so it is adhered to)

- **Definition of Done** (add to the issue template):
  > Conforms to the Design & Style Framework; uses the shared primitives; 
  > backing queries return resolved names (no unresolved ids surfaced); `npm run design-lint` passes; 
  >an after-screenshot or visual-regression assertion is attached.
- **`design-lint`** (ESLint / custom): ban raw UUID/hash regex in JSX text and label props;
  ban hardcoded hex/px in components; ban non-registry CTA strings; ban `/api/`,
  `api/graphql`, `CHAOS-\d+`, edge-name and detector/telemetry tokens in user-facing strings;
  require `formatNumber` on chart value labels.
- **Dev guard** — throw in dev if an unresolved hash reaches a primary label slot.
- **Visual gate** — tie "Done" to the *Visual User Journey Evidence & UX Acceptance
  Coverage* milestone: an after-screenshot or visual-regression assertion per fix, so "Done"
  means *verified*, not just merged.
