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

The AI Impact view summarizes how AI-assisted and agent-created work
**appear** to influence delivery, review pressure, quality, and operational
drag at the **org level**. It is the top-level dashboard for the AI Workflow
Intelligence feature (CHAOS-1578).

> **Purpose:** Help leadership see whether AI-assisted workflows are
> improving flow, shifting work into review or rework, or quietly increasing
> operational risk. This is a _system-health_ lens, not a productivity
> ranking.

---

## What this view shows

The dashboard renders org-scoped panels with three filter dimensions —
**team**, **repo**, **work type** — plus a **date range**. All panels share
the same scope at all times.

### Headline cards

| Card                     | What it answers                                              |
| ------------------------ | ------------------------------------------------------------ |
| AI-assisted work share   | What fraction of PRs lean AI-assisted in the selected scope? |
| Agent-created work share | How many fully agent-authored PRs landed?                    |
| Unknown attribution      | Where attribution remains undetermined and _stays unknown_.  |

### Diagnostic panels

| Panel                             | Reads from                                                 |
| --------------------------------- | ---------------------------------------------------------- |
| AI-assisted work share (donut)    | `aiImpactSummary.byBucket`                                 |
| Agent-created work share (trend)  | `aiImpactSummary.byBucket` + `.daily`                      |
| Net delivery lift                 | `aiImpactSummary.byBucket[AI_ASSISTED].leverage`           |
| Review amplification              | `aiComparison.delta.reviewsPerPrDelta`                     |
| Rework drag                       | `aiComparison.delta.reworkRateDelta`                       |
| Test gap rate                     | `aiComparison.delta.testGapRateDelta`                      |
| Revert + incident drag            | `aiComparison.delta.revertRateDelta` + `incidentRateDelta` |
| Top affected repos and teams      | Placeholder until repo/team rollups ship                   |
| Best-fit automation opportunities | `aiOpportunities.recommendations`                          |

Net delivery lift is rendered as **decomposable components** (PR volume,
cycle time, review, rework, test, incident). The aggregate score is never
shown as a black-box number — see the
[AI Flow Metrics computation reference](../../computations/ai-opportunity-detector.md)
and the [GraphQL contract](../../api/graphql-ai.md) for the underlying math.

### AI Operating Leverage components

Use the leverage breakdown as an investigation path, not a verdict:

| Component                      | What it suggests                                              | Next place to look                                                 |
| ------------------------------ | ------------------------------------------------------------- | ------------------------------------------------------------------ |
| Delivery lift                  | AI-attributed work may be improving throughput or cycle time. | Validate the drag components before expanding the pattern.         |
| Review amplification           | Drafting cost may be shifting into reviewer effort.           | Open [AI Review Load](ai-review-load.md).                          |
| Rework drag                    | First-pass speed may be offset by post-review iteration.      | Inspect PR evidence and churn/reopen signals.                      |
| Test, revert, or incident drag | Delivery confidence may be falling after merge.               | Open [AI Risk](ai-risk.md).                                        |
| Unknown attribution            | Detection coverage is incomplete and should remain visible.   | Improve labels, trailers, bot identity mapping, or CI annotations. |
| Governance coverage            | Controls may be missing for AI-attributed workflows.          | Review policy violations and coverage gaps.                        |

For demo talk tracks and buyer-facing copy, see the
[AI Operating Leverage demo narrative](https://github.com/full-chaos/dev-health-ops/blob/main/docs/product/ai-assisted/AI%20Operating%20Leverage%20Demo%20Narrative.md).

---

## How to read it

1. **Start with share, not delta.** A 60% AI-assisted share with a small
   negative delta is a different signal than a 5% share with the same
   delta.
2. **Read deltas as direction, not verdict.** Deltas compare AI side to the
   _human-only baseline_ on the same scope and time window. A positive
   review-amplification delta says "AI-attributed PRs appear to attract
   more review comments per PR than human PRs _in this scope_", not "AI is
   bad".
3. **Keep the unknown bucket in view.** When the unknown count grows, your
   coverage shrinks. Treat that as a data-quality signal first, a pattern
   signal second.
4. **Open the leverage breakdown.** If "Net delivery lift" trends negative,
   the breakdown tells you whether the drag came from cycle time, review,
   rework, test gap, or incident drag.
5. **Filter, then drill.** Use team/repo/work-type filters to localize
   patterns before clicking through to the underlying PR evidence.

---

## What this view does **not** do

This view is intentionally framed for system observation, not individual
evaluation. The product contract explicitly **forbids** the following uses:

- ❌ **Individual AI usage surveillance.** No per-author, per-login, or
  per-developer panels exist. There is no per-user filter and no API path
  that returns per-individual AI attribution rollups.
- ❌ **Productivity scoring or rankings.** Leverage is exposed only as
  decomposable components. There is no "AI productivity score" exposed
  through any panel or API.
- ❌ **Cross-person comparison.** Filters cap at team granularity.
- ❌ **Raw prompt/session capture.** Attribution is inferred from publicly
  observable provider signals (labels, trailers, bot accounts). Prompt
  content is never ingested or rendered.

If you are looking for a way to "see which developer uses AI most", **this
is the wrong tool**, and the design will keep being the wrong tool.

---

## Interpretation guardrails

| Signal                                                 | Useful framing                                                        | Misuse                                                     |
| ------------------------------------------------------ | --------------------------------------------------------------------- | ---------------------------------------------------------- |
| Rising AI-assisted share + rising review amplification | "Are we moving work into review?"                                     | "Reviewers are slow."                                      |
| Rising AI-assisted share + rising rework drag          | "Are merges incurring more iteration?"                                | "AI is bad" or "Author X is bad."                          |
| High unknown attribution                               | "Attribution coverage may be missing labels / trailers / bot config." | Ignore it; treat the dashboard as if unknown didn't exist. |
| Negative net delivery lift                             | "Drag exceeds delivery on this scope; inspect the breakdown."         | Treat as a verdict on a team's competence.                 |

---

## Data sources and freshness

- All AI metrics are read from `ai_impact_metrics_daily` (ClickHouse).
- The resolver is **read-only**; metrics never compute at request time.
- `computedAt` is surfaced on every response — older `computedAt` means
  the rollup hasn't refreshed yet, not that the metric is wrong.
- See [AI Attribution](ai-attribution.md) for what each bucket represents
  and how confidence is recorded.

---

## Related

- [AI Review Load](ai-review-load.md) — diagnostic view for review pressure.
- [AI Risk](ai-risk.md) — diagnostic view for quality risk.
- [AI Attribution](ai-attribution.md) — what counts as AI-assisted, how
  confidence works, what stays "unknown", and what we will not do with it.
- [AI Workflow Analytics — GraphQL Contracts](../../api/graphql-ai.md)
