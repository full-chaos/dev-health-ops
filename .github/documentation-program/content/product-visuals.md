# Product visual capture plan

The public documentation needs a small, deliberate set of sanitized product captures. Images should explain page structure, controls, visual encodings, and states; they should not decorate prose or repeat what the text already makes obvious.

## Capture standard

For every image:

- use a fixture or demo workspace with no customer or employee data;
- capture the current supported product route and visible UI labels;
- record the source revision, route, fixture, theme, viewport, redaction status, and capture date;
- include useful alt text and a concise caption that explains what the reader should notice;
- crop browser chrome unless it provides necessary route or environment context;
- avoid tiny full-dashboard screenshots when a focused crop communicates the control or state better;
- use annotations sparingly and never rely on color alone;
- replace the image when a changed control, label, chart encoding, or state would make the existing capture misleading.

## Launch-priority captures

| Documentation page | Capture | What the image must explain |
| --- | --- | --- |
| `/use/investment/investment-mix/` | Full Investment Mix view plus one focused drill-down | Scope and period controls, treemap or sunburst hierarchy, theme and subcategory selection, evidence-quality treatment, and the path to supporting work. |
| `/use/delivery-flow/pr-flow/` | Current Flow destination | State labels, transition widths, scope and period context, and where the reader opens supporting work. |
| `/use/delivery-flow/quadrants/` | One current quadrant pair | Axis labels and units, threshold or zone treatment, item population, and the evidence action. |
| `/use/code-and-relationships/code-hotspots/` | Repository hotspot surface | Active metric, path hierarchy, node size or intensity, period, and source drill-down. |
| `/use/code-and-relationships/work-graph/` | Focused relationship slice | Start node, edge labels, relationship expansion, coverage state, and source-artifact action. |
| `/use/plan-and-improve/capacity-planning/` | Completion Forecast | Backlog and historical-period inputs, P50/P85/P95 outputs, refresh action, and uncertainty presentation. |
| `/use/reports/` | Report Center landing and one report detail | Saved definition, Run Now, run status/history, rendered output, and provenance context. |
| `/use/troubleshooting/no-or-incomplete-data/` | Representative unavailable or incomplete state | Exact visible state label, retained filters, freshness or coverage indicator, and the next diagnostic path. |
| `/admin/sync-and-coverage/status-and-freshness/` | Administrator status surface | Provider/source status, last successful synchronization, coverage boundary, and retry or escalation affordance. |

## Secondary captures

Add only after the launch-priority images are accurate:

- AI Impact, Review Load, and Governance Risk, using fixture-backed coverage and no prompt or user data;
- provider connection and credential lifecycle pages;
- local installation and health verification output;
- Customer Push schema discovery, validation, and delivery status;
- dense reference pages where a rendered response or CLI output is materially easier to understand than prose alone.

## Placement

Use the shared `fc-product-figure` class:

```html
<figure class="fc-product-figure">
  <img src="../../assets/product/investment-mix.png" alt="Investment Mix showing scope and period controls above an effort-weighted theme treemap, with evidence quality and drill-down actions visible.">
  <figcaption>Investment Mix preserves scope and period while the reader moves from the full theme distribution to subcategories and supporting work.</figcaption>
</figure>
```

Do not publish an empty image placeholder. A page remains text-only until a truthful capture and metadata record are available.
