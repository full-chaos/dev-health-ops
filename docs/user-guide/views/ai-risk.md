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

The AI Risk view is the diagnostic surface for **quality risk associated
with AI-attributed work**. It compares AI-attributed PRs against the
human-only baseline on the same scope and time window and surfaces
governance violations that fire on AI workflows.

> **Purpose:** Surface where AI-assisted work appears to land with lower
> quality signals (rework, reverts, test gaps, incidents, governance
> violations). This is a *system-quality* lens. It is **not** a per-
> author quality grading tool.

---

## What this view shows

Four metric cards comparing AI-side to baseline, three risk-overlap panels,
one rollup card, and the governance violations list.

### Risk metrics (vs human baseline)

| Card           | What it answers                                                  |
| -------------- | ---------------------------------------------------------------- |
| Rework rate    | Share of AI-attributed PRs that appear to require iteration.     |
| Revert rate    | Share of AI-attributed PRs associated with a subsequent revert.  |
| Test gap rate  | Share of AI-attributed PRs landing without matching test deltas. |
| Incident rate  | Share of AI-attributed PRs linked to incident rollups.           |

### Risk overlap panels

Two file-overlap panels render as **missing-data cards** today because the
underlying detectors do not yet expose per-file overlap with AI-attributed
PR changed-file sets:

| Missing card                       | What it will show when it lands                                              |
| ---------------------------------- | ---------------------------------------------------------------------------- |
| Hotspot file overlap               | AI-attributed PRs that touched detector-flagged hotspot files.               |
| High-complexity file overlap       | AI-attributed PRs that touched complexity-flagged files.                     |

### Linked incidents

A single rollup card surfacing the count of incidents associated with
AI-attributed PR edges in the window. PR-level drilldown will arrive when
a specific PR is selected — the aggregate card does *not* fabricate PR-level
edges from rollups.

### Governance violations

The AI Governance summary feed is rendered in the same view to make policy
breaches visible where the quality conversation happens. Each row carries:

- the rule ID
- severity
- subject type and subject ID (e.g. `PR pr-7001`)
- team and repo
- evidence string
- observation timestamp

Violations are surfaced by **ruleId + subjectId**, not by author handle.

---

## How to read it

1. **Read deltas, not absolutes.** Every risk card shows the AI-side value
   alongside the delta vs human-only baseline on the same scope.
2. **One signal at a time.** A small positive delta on a single metric is
   noise. Multiple deltas trending the same direction across rework, test
   gap, and incident is a real pattern.
3. **Missing-data cards are honest.** When the detector is not yet wired,
   the card stays present with a "data source needed" note rather than
   silently disappearing.
4. **Governance violations are evidence.** Each violation row links to the
   subject (PR or repo) — drill into the artifact, not the author.

---

## What this view does **not** do

- ❌ **No author quality grading.** No card maps a quality metric to an
  individual author or reviewer. There is no per-author filter and no
  resolver path that exposes per-author quality rollups.
- ❌ **No "AI risk score".** Risk is exposed as named, decomposable
  metrics, not a synthetic composite.
- ❌ **No incident attribution to people.** Linked incidents trace to PRs
  and repos, never to individuals.
- ❌ **No predictive blocking.** This view explains observed patterns; it
  does not gate merges, reviews, or deployments.

---

## Interpretation guardrails

| Signal                                                       | Useful framing                                                          | Misuse                                                       |
| ------------------------------------------------------------ | ----------------------------------------------------------------------- | ------------------------------------------------------------ |
| Rework rate elevated on AI side                              | "Are we landing AI-attributed work that needs follow-up iterations?"    | "Author X writes worse code."                                |
| Test gap rate elevated on AI side                            | "Is test coverage lagging behind AI-assisted change?"                   | "AI = no tests, ban it."                                     |
| Incident rate elevated on AI side                            | "Is AI-attributed work clustering near incident-linked PRs?"            | Conclude causality from a single window.                     |
| Governance violations rising                                 | "Is policy enforcement keeping up with workflow change?"                | Use as a list of contributors to discipline.                 |
| All cards quiet, missing-data cards loud                     | "Detectors haven't ramped here yet — invest in detection coverage."     | Conclude there is no risk.                                   |

---

## Data sources and freshness

- Risk cards read from `aiRiskBreakdown`.
- Deltas read from `aiComparison`.
- Governance violations read from `aiGovernanceSummary.recentViolations`.
- All three queries return `dataAvailable: Boolean!` — `false` triggers the
  missing-data UX rather than a silently empty dashboard.
- Schema details: [graphql-ai.md](../../api/graphql-ai.md).

---

## Related

- [AI Impact](ai-impact.md) — top-level summary view.
- [AI Review Load](ai-review-load.md) — diagnostic view for review pressure.
- [AI Attribution](ai-attribution.md) — how AI-assisted is detected and the
  anti-surveillance posture this view inherits.
