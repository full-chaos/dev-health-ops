---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views/ai-risk/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Inspect AI attribution
  url: user-guide/views/ai-attribution/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# AI Risk

AI Risk brings persisted quality and governance signals into one team-level reading.
It helps a group see where AI-associated work **appears** to need more evidence or a
closer operating conversation. AI-derived signals are **estimates** from the available
persisted quality facts and source coverage.

## Purpose
Use the view to investigate rework, reverts, test gaps, incidents, and governance
context in one selected scope and time window.

## What it measures
The current comparison cards show **Rework rate**, **Revert rate**, **Test gap rate**, and
**Incident rate**. Supporting context can show hotspot-file overlap, high-complexity-file
overlap, **Linked incidents**, and governance findings when their persisted inputs are
available. For every displayed AI bucket, `incidentsCount` repeats all incidents that
started in the selected window for the same repository. It is not a Work Graph
linked-incident rollup. The Incident rate panel uses this repository-level context as an
estimate that may suggest a closer evidence review.

## How to read
Read several signals together and compare the same window before acting. A rising test
gap with rework suggests a follow-up investigation, while an isolated change may lean on
one unusual release. Open the available evidence before forming a response.

<aside class="fc-evidence-rail fc-evidence-rail--in-flow" aria-label="Evidence trail">
  <p class="fc-evidence-rail__label">Evidence trail</p>
  <ol class="fc-evidence-rail__steps">
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">01</span><span>Compare rates in the same period and selected scope.</span></li>
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">02</span><span>Open linked work and incident context before responding.</span></li>
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">03</span><span>Use missing coverage as a question, not a zero value.</span></li>
  </ol>
  <a class="fc-evidence-rail__link" href="../../how-to-read-dev-health/">Open the evidence model</a>
</aside>

## Confidence and provenance
Saved counts and related evidence support the selected scope. The resolver calculates
per-bucket rates when the view is queried, while governance findings keep the related
subject and observation context visible. The page **leans** on those records and does not
create a new attribution or incident link while you read it.

## Empty and error states
Overlap panels show an explicit unavailable state when their input is absent. A missing
panel or value means the relevant coverage is not ready in that context, not that there
is no risk or that the value equals zero.

## Caveats and limits
These signals provide evidence for a team conversation, not a quality label for a
person. An incident edge or a revert can **appear** alongside a pull request without
explaining cause. Keep the source, period, and linked work in view.

## Next step
- [AI Impact](ai-impact.md) provides the wider delivery context.
- [AI Review Load](ai-review-load.md) helps read review pressure.
- [AI Attribution](ai-attribution.md) shows the saved signal evidence.
- [How to read Dev Health](../how-to-read-dev-health.md) explains the shared interpretation model.
- [Glossary](../glossary.md) defines the terms used here.
