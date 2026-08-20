---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/views/ai-review-load/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Review AI risk
  url: user-guide/views/ai-risk/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# AI Review Load

AI Review Load helps a team understand how AI-associated pull requests **appear** in
review activity. It gives system context for a selected scope and period, never a
scoreboard for named reviewers. AI-derived signals are **estimates** from the available
persisted review facts and source coverage.

## Purpose
Use this view to ask where review work may be changing, then investigate the linked
work and its timing with the team.

## What it measures
The current cards include **Pickup latency**, **Review comments per LOC**, **Change request
rate**, **Approval friction**, push iterations after first review, and **Review amplification**.
Reviewer concentration, when available, remains aggregate-only. The Review amplification
panel suggests whether review volume needs a wider trend check.

## How to read
Read every comparison against the same baseline, scope, and window. A higher value
suggests more back-and-forth, but a short window can lean on unusual work. Use the
evidence trail before deciding what to investigate with the team.

<aside class="fc-evidence-rail fc-evidence-rail--in-flow" aria-label="Evidence trail">
  <p class="fc-evidence-rail__label">Evidence trail</p>
  <ol class="fc-evidence-rail__steps">
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">01</span><span>Keep the baseline, scope, and window together.</span></li>
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">02</span><span>Open the related work before interpreting review pressure.</span></li>
    <li class="fc-evidence-rail__step"><span class="fc-evidence-rail__number">03</span><span>Use aggregate context to plan a team follow-up.</span></li>
  </ol>
  <a class="fc-evidence-rail__link" href="../../how-to-read-dev-health/">Open the evidence model</a>
</aside>

## Confidence and provenance
Saved review facts supply the selected context, while the resolver calculates engagement
metrics and rates when the view is queried. The review drill-down opens supporting work
rather than making a new attribution. Reviewer concentration **appears** only as a
distribution-level value, without reviewer names or person-level counts.

## Empty and error states
The view names missing review activity explicitly. An unavailable comparison does not
mean no review occurred; broaden the scope or period and check source coverage first.

## Caveats and limits
Approval friction is `changesRequestedPerPr / reviewsPerPr` when `reviewsPerPr` is
nonzero; it and Change request rate are ratios, not quality labels. Review comments per
LOC and follow-up pushes need the same change context to be useful. The view **leans** on
available review facts and does not provide a person-level performance comparison.

## Next step
- [AI Impact](ai-impact.md) provides the wider delivery context.
- [AI Risk](ai-risk.md) helps read related quality signals.
- [How to read Dev Health](../how-to-read-dev-health.md) explains the shared interpretation model.
- [Glossary](../glossary.md) defines the terms used here.
