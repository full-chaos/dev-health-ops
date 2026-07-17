---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views/ai-attribution/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Return to views and charts
  url: user-guide/views-index/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# AI Attribution

AI Attribution explains the saved signal evidence behind the AI views. It helps a team
see how work **appears** to split across the available attribution kinds without turning
that context into a statement about a person. AI-derived signals are **estimates** from
the available persisted evidence and source coverage.

## Purpose
Use this view when an AI Impact, Review Load, or Risk pattern needs its stored evidence
and source context.

## What it measures
The current page has an **Attribution mix** and an **Attribution evidence** table. The mix
groups the highest-precedence, non-superseded signal for each subject: AI-assisted,
Agent-created, AI review, or **Unknown attribution**. Unknown means attribution remains
unresolved and is never guessed. The mix intentionally excludes a synthesized human bucket;
use AI Impact for an AI-versus-human split. The table shows **Subject**, Attribution, Source,
**Provider**, Team, and **Observed** fields for the saved evidence rows in the selected
window. The implemented precedence is `MANUAL > PR_LABEL > BOT_AUTHOR > COMMIT_TRAILER >
CI_ANNOTATION > BRANCH_NAME > PR_BODY`. The Subject field suggests which saved work item
should be opened for context.

## How to read
Start with the mix, then open the evidence rows for the work that matters to the
question. A source and confidence label may **suggest** how much care to take with the
signal; it does not settle a causal explanation.

<aside class="fc-evidence-rail fc-evidence-rail--in-flow" aria-label="Evidence trail">
  <p class="fc-evidence-rail__label">Evidence trail</p>
  <ol class="fc-evidence-rail__steps">
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">01</span><span>Read the attribution kind with its source and confidence.</span></li>
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">02</span><span>Open the subject to inspect the saved evidence.</span></li>
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">03</span><span>Keep Unknown visible when coverage is unresolved.</span></li>
  </ol>
  <a class="fc-evidence-rail__link" href="../../how-to-read-dev-health/">Open the evidence model</a>
</aside>

## Confidence and provenance
Each evidence row carries its persisted source, confidence, provider, and observation
date. Confidence is the detector's source-specific confidence from `0.0` to `1.0` saved
with the signal; it is not an outcome, quality, or person-performance score. The resolver
enriches the `team_id` context when the view is queried. The table **leans** on those saved
rows and does not derive a new attribution in the browser. The mix covers resolved signals
in the chosen window; use AI Impact when a human baseline is needed for a comparison.

## Empty and error states
No AI attribution data yet means the selected window has no available persisted signal.
Check source coverage and widen the context only when that matches the question.

## Caveats and limits
Attribution evidence can **appear** incomplete when a source has limited coverage. The
view does not expose prompt or session content and does not support a person-level
usage comparison. Keep the source, confidence, and observed date beside every reading.

## Next step
- [AI Impact](ai-impact.md) provides the delivery context.
- [AI Review Load](ai-review-load.md) helps read review pressure.
- [AI Risk](ai-risk.md) helps read quality signals.
- [How to read Dev Health](../how-to-read-dev-health.md) explains the shared interpretation model.
- [Glossary](../glossary.md) defines the terms used here.
