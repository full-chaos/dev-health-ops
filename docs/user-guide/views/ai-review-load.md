# AI Review Load

The AI Review Load view is the diagnostic surface for **how AI-attributed
work shows up in review**. It compares AI-attributed PRs against the
human-only baseline on the same scope and time window.

> **Purpose:** Surface review pressure that may be created (or absorbed) by
> AI-assisted workflows. This is a *team flow* lens. It is **not** a per-
> reviewer scoreboard.

---

## What this view shows

Six metric cards plus one trend, all bucketed by `AI_ASSISTED` vs `HUMAN`:

| Card                       | What it answers                                                             |
| -------------------------- | --------------------------------------------------------------------------- |
| Pickup latency             | How long, on average, before review activity begins on a PR. *(Cycle-time proxy until pickup-event ingestion ships.)* |
| Review comments per PR     | Average review comments per merged PR.                                      |
| Change request rate        | Average change-request comments per PR.                                     |
| Approval friction          | Derived: `changesRequestedPerPr / reviewsPerPr` per bucket.                 |
| Review amplification       | Review volume on AI-attributed PRs vs the baseline.                         |
| Push iterations after first review | Persisted follow-up push count after review activity starts.         |
| Review amplification trend | Daily series showing whether amplification is trending up or down.          |

### Intentional gaps

One panel remains explicitly **aggregate-only** and renders as a missing-data
card until the distribution summary is available:

| Missing card                         | Why it is missing                                                                                |
| ------------------------------------ | ------------------------------------------------------------------------------------------------ |
| Reviewer concentration               | Render only aggregate reviewer distribution (`reviewerGini`, `reviewerCount`), never names or rankings. |

Reviewer concentration is intentionally constrained. The API may use reviewer
identities inside the aggregation boundary, but it exposes only distribution
values. It does **not** ship individual-leveled metrics, leaderboard rows, or
reviewer names.

---

## How to read it

1. **Always read deltas against the baseline.** Every card shows the AI-side
   value with a delta vs the human-only baseline on the same scope.
2. **Approval friction beats reviews-per-PR alone.** A high reviews-per-PR
   value with low change-request rate is healthy back-and-forth. A high
   change-request rate is friction.
3. **Trends matter more than absolutes.** Look at the amplification trend
   before reacting to a single day's value.
4. **Filter first.** The same metric can mean different things in
   "platform" vs "product" repos. Use team/repo/work-type filters.

---

## What this view does **not** do

- ❌ **No reviewer leaderboards, ever.** No card surfaces "who reviewed
  the most" or "who approved the fastest". The product contract treats
  reviewer concentration as a *system-distribution* metric, not an
  individual rating.
- ❌ **No author/login surfacing.** PR detail drill-downs surface the PR
  itself; no per-author rollups exist or are exposed.
- ❌ **No "AI overhead score".** Friction and amplification are exposed as
  individual numbers with named definitions, not as a synthetic composite.

If a use case requires "rank reviewers by speed", the answer is the same as
elsewhere in Dev Health: **don't**. Build a reviewer-load conversation,
not a reviewer scoreboard.

---

## Interpretation guardrails

| Signal                                              | Useful framing                                                       | Misuse                                                  |
| --------------------------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------------- |
| AI-side reviews-per-PR clearly above baseline       | "AI-assisted PRs may be triggering more back-and-forth here."        | "AI-assisted authors are sloppy."                       |
| AI-side change-request rate above baseline          | "Reviewers may be catching more issues on AI-attributed PRs."        | "Reviewer X is too strict."                             |
| Rising amplification trend                          | "Are AI-assisted PRs adding review load over time?"                  | Treat as a verdict on a single contributor.             |
| Both cards available but no baseline available      | "Scope likely has no human PRs in the window — broaden the scope."   | Treat AI-side absolute value as comparable to anything. |

---

## Data sources and freshness

- Cards read from `aiReviewLoad` (per-bucket review rollups plus aggregate-only reviewer concentration).
- Deltas come from `aiComparison` (same scope, same window).
- Both queries return `dataAvailable: Boolean!` — `false` triggers the
  missing-data UX rather than a silently empty dashboard.
- Schema details: [`aiReviewLoad` in graphql-ai.md](../../api/graphql-ai.md).

---

## Related

- [AI Impact](ai-impact.md) — top-level summary view.
- [AI Risk](ai-risk.md) — diagnostic view for quality risk.
- [AI Attribution](ai-attribution.md) — how AI-assisted is detected, with
  full anti-surveillance posture.
