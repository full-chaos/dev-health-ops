---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views-index/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Investigate investment
  url: user-guide/views/investment-mix/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# Views and Charts

Each view answers a specific operational question. Views are explanations with drill-down, not a wall of widgets.

## Core views
- Investment Mix (treemap/sunburst)
- Investment Flows (sankey)
- Investment Expense (stacked area)
- Code Hotspots (heatmap/treemap/sunburst)
- PR Flow (stage breakdown)
- Quadrants (raw-value state classification)
- Flame diagrams (single-point decomposition)
- Work Graph (entity relationships, related entities)

## AI workflow views

- [AI Impact](views/ai-impact.md) — compare AI-associated delivery and drag signals in a shared scope.
- [AI Review Load](views/ai-review-load.md) — inspect aggregate review pressure without ranking people.
- [AI Risk](views/ai-risk.md) — follow rework, reverts, test gaps, and incident evidence with caveats.
- [AI Attribution](views/ai-attribution.md) — inspect the saved attribution evidence behind AI workflow signals.

## Interpretation rules
- Trends over snapshots.
- Compare within the same window + filter context.
- Treat “unknown/unclassified” as a pipeline bug, not an output.
