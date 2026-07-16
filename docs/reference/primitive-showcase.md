# Primitive showcase

This page is the visual contract for the public Dev Health manual. It demonstrates
the reusable documentation primitives before audience pages compose them.

## Navigation and search

The Material header provides the actual keyboard-reachable search surface. This
navigation sample demonstrates the editorial destination pattern: a clear task,
not a repository filename.

<nav class="fc-primitive" aria-label="Manual paths">
  <p class="fc-eyebrow">Manual paths</p>
  <p><a href="../getting-started/">Start with the operating model</a></p>
  <p><a href="../product/concepts/">Read the evidence standard</a></p>
  <p><a href="../ops/workers/">Run an operational task</a></p>
</nav>

## Evidence trail

<aside class="fc-callout" aria-label="Evidence trail example">
  <p class="fc-eyebrow">Evidence before inference</p>
  <p>A claim names its source, its caveat, and the next responsible action. The rail
  on wide pages repeats this compact sequence without competing with the article.</p>
  <a href="../product/concepts/">Inspect product concepts and guardrails</a>
</aside>

## Callouts and states

!!! note "Evidence"
    This interpretation is useful only while its source, date range, and confidence
    remain visible to the reader.

!!! warning "Caveat"
    A trend is a prompt to inspect evidence. It is not a ranking or a conclusion
    about an individual.

<div class="fc-callout fc-callout--caution" role="note">
  <p class="fc-eyebrow">Operational pause</p>
  <p>When a source is incomplete, name what is missing before choosing a next step.</p>
</div>

## Code, tables, and diagrams

```bash
CLICKHOUSE_URI="clickhouse://analytics.example/dev-health" \
  dev-hops metrics daily
```

| Evidence | Interpretation | Next step |
| --- | --- | --- |
| Source timestamp | Context may be stale | Check the refresh boundary |
| Confidence note | Classification has limits | Open the supporting work items |
| Trend over time | A pattern needs context | Compare the same period |

<section class="fc-flow-diagram" aria-label="Evidence flow: source material, interpretation with caveats, responsible action">
  <p class="fc-flow-diagram__node">Source material</p>
  <p class="fc-flow-diagram__node">Interpret with caveats</p>
  <p class="fc-flow-diagram__node">Responsible action</p>
</section>

## Screenshot frame

<section class="fc-screenshot-frame" aria-label="Product screenshot capture rule">
  <p class="fc-screenshot-frame__caption">Fixture-backed product capture</p>
  <p>Product screenshots are captured from a verified fixture state with source metadata
  and descriptive alternative text. A decorative mock is never substituted for a live
  product surface.</p>
</section>

## Action hierarchy

<div class="fc-showcase-grid">
  <section class="fc-primitive">
    <p class="fc-eyebrow">Primary action</p>
    <p>Use the orange action only to advance a clear, accountable task.</p>
    <a class="fc-action" href="../getting-started/">Open the quickstart</a>
  </section>
  <section class="fc-primitive">
    <p class="fc-eyebrow">Evidence link</p>
    <p>Use underlined links to inspect supporting material without implying a workflow transition.</p>
    <a href="../product/investment-taxonomy/">Read the Investment taxonomy</a>
  </section>
</div>
