---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views/ai-impact/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Compare AI review load
  url: user-guide/views/ai-review-load/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# AI Impact

AI Impact is a team and repository context view for persisted workflow signals. It
helps a group ask whether AI-associated work **appears** to change a delivery pattern;
it does not evaluate people. AI-derived signals are **estimates** from the available
persisted workflow data and source coverage.

## Purpose
Use the view to frame an evidence-led team conversation about delivery, review, and
quality signals in the selected scope and time window.

## What it measures
The current dashboard shows **AI-assisted work share**, **Agent-created work share**,
**Unknown attribution**, **Net delivery lift**, review amplification, rework drag, test
gap rate, and revert plus incident drag. Net delivery lift presents the selected scope's
delivery and drag components together; it is not a forecast or a decision.

## How to read
Start with the share and its scope, then read the comparison panels together. A change
in review amplification or rework suggests a useful question about the flow; it does not
explain why that change happened. Use the evidence path before choosing a team response.

<aside class="fc-evidence-rail fc-evidence-rail--in-flow" aria-label="Evidence trail">
  <p class="fc-evidence-rail__label">Evidence trail</p>
  <ol class="fc-evidence-rail__steps">
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">01</span><span>Keep the scope and time window visible.</span></li>
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">02</span><span>Open the supporting work before interpreting a comparison.</span></li>
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">03</span><span>Use the trend to choose a team conversation, not a judgment.</span></li>
  </ol>
  <a class="fc-evidence-rail__link" href="../../how-to-read-dev-health/">Open the evidence model</a>
</aside>

## Confidence and provenance
The dashboard labels its freshness with **Last computed** and offers persisted evidence
where a drill-down is available. Unknown attribution stays visible so coverage gaps can
be inspected. Saved metric rows support the page, while the resolver calculates the
selected scope's comparisons and deltas at query time; opening the page does not create a
new classification. Each comparison **leans** on the available source coverage.

## Empty and error states
If AI workflow data has not populated, confirm the selected context and available
source coverage. An unavailable share or comparison means there is not enough usable
information for that view; it does not mean that the value is zero.

## Caveats and limits
Read trends over a longer period and keep the same scope when comparing values. The
view provides system context, not a productivity label, person-level rollup, or prompt
content. Team and repository context can **appear** different because their evidence
coverage differs.

## Next step
- [AI Review Load](ai-review-load.md) helps explore review pressure.
- [AI Risk](ai-risk.md) helps explore quality signals.
- [AI Attribution](ai-attribution.md) explains the persisted evidence view.
- [How to read Dev Health](../how-to-read-dev-health.md) explains the shared interpretation model.
- [Glossary](../glossary.md) defines the terms used here.
