# AI Attribution

This page is the **interpretation reference** for the attribution signals
that power every AI Workflow Intelligence view. It explains what counts as
AI-assisted, how confidence is recorded, what stays *unknown*, and — most
importantly — the explicit guardrails on how this data may and may not be
used.

> If you read only one page in the AI section, read this one. The
> dashboards make sense only when the attribution rules are understood.

---

## Buckets

Every PR (and every reviewed artifact) lands in exactly one attribution
bucket. The buckets are stable across resolvers, metrics, and storage:

| Bucket          | Meaning                                                          |
| --------------- | ---------------------------------------------------------------- |
| `ai_assisted`   | Human-authored with explicit AI assistance.                      |
| `agent_created` | Autonomous agent produced the artifact end-to-end.               |
| `ai_review`     | AI performed the review.                                         |
| `human`         | Human-only baseline; no detected AI involvement.                 |
| `unknown`       | Attribution unresolved. **Never guessed.**                       |

Buckets are mutually exclusive at the artifact level. Aggregates sum to
the total artifact count on every resolver.

---

## P0 detection sources (CHAOS-1580)

The ingestion path detects assistance from these provider signals, in
**precedence order**:

1. **Explicit PR labels.** `ai-assisted`, `agent-created`, `ai-review`,
   and similar canonical labels override every weaker signal.
2. **Bot / app authors.** Known agent identities (e.g. `app/devin-ai-…`,
   `bot/copilot-…`) attribute the PR as `agent_created`.
3. **Commit trailers.** `AI-Assisted-By:` and equivalent trailers attribute
   the commits (and the PR through commit roll-up).
4. **Branch naming conventions.** Branch prefixes like `agent/…`,
   `copilot/…` contribute as a *secondary* signal.
5. **PR description patterns** and **CI annotations** — secondary signals.

Manual attribution (operator override) is preserved and always wins over
auto-detection.

---

## Confidence

Every inferred attribution carries a `confidence` value and a `source`
field describing the strongest signal that produced the classification.

| Source class      | Typical confidence | Interpretation                                       |
| ----------------- | ------------------ | ---------------------------------------------------- |
| `explicit_label`  | 1.0                | Operator or org has tagged this artifact directly.   |
| `bot_author`      | 0.95               | Identified bot/app account is the author.            |
| `commit_trailer`  | 0.85 – 0.9         | Trailer parsed cleanly from commit message.          |
| `branch_pattern`  | 0.5 – 0.7          | Branch name matches a known agent prefix.            |
| `pr_description`  | 0.5                | PR body contains assistance markers.                 |
| `heuristic`       | 0.3                | Time-window / co-occurrence matching only.           |
| `manual_override` | 1.0                | Operator set this attribution explicitly.            |

If the strongest signal is weaker than the ingestion threshold, the
artifact remains in `unknown` — it does **not** get demoted into a "maybe
AI-assisted" bucket. Unknown is a first-class state, not a guess.

---

## What "unknown" really means

`unknown` is preserved deliberately:

- **In the API**: every resolver exposes the unknown count and ratio.
- **In the UI**: the AI Impact dashboard surfaces an "Unknown attribution"
  card so coverage gaps stay visible.
- **In aggregates**: unknown contributes to denominators where appropriate
  so AI-share percentages don't silently inflate.

A high unknown rate is a **data-coverage signal first**, a pattern signal
second. If unknown is climbing, the answer is usually missing labels,
missing trailers, or a bot account that hasn't been added to the identity
registry — not a usage trend.

---

## What this attribution model **explicitly does not do**

The product contract (CHAOS-1578) bans the following uses. These bans are
enforced by what the data model exposes — not just by policy:

- ❌ **No individual surveillance.** There is no per-author AI usage
  rollup. No resolver returns "AI assistance by login". No filter narrows
  any AI view to a single person.
- ❌ **No "AI productivity score" per person.** AI Operating Leverage is
  org-scoped and decomposable; there is no per-person variant.
- ❌ **No ranking of developers by AI use, AI-attributed quality, or any
  AI-derived metric.** Aggregations stop at team granularity.
- ❌ **No raw prompt or session capture.** Attribution uses only the
  provider signals listed above. Prompt content is never ingested,
  rendered, or persisted.
- ❌ **No invasive IDE telemetry.** This milestone covers PR/commit/issue/
  workflow-run signals, not editor session data.
- ❌ **No "did you use AI?" surveys surfaced through this view.**
  Self-attestation, if added later, will be a separate audited surface.

These limits are encoded in the model — the [AI Governance audit](../../audit/ai_governance/)
includes a `test_surveillance_posture` test suite that *fails* if a
per-individual AI usage rollup is ever introduced.

---

## How to use this data well

| ✅ Use for                                                          | ❌ Do not use for                                              |
| ------------------------------------------------------------------- | -------------------------------------------------------------- |
| Understanding org-wide adoption trends.                             | Ranking developers by AI use.                                  |
| Spotting review-load or quality patterns at team / repo scope.      | Performance reviews, calibration, or HR processes.             |
| Sizing the impact of an agent rollout.                              | Justifying replacing specific individuals.                     |
| Identifying coverage gaps to improve detection.                     | Naming-and-shaming PRs or contributors.                        |
| Informing automation opportunity prioritization.                    | Gating individual merges based on AI attribution.              |

If a stakeholder asks for "the list of who is using AI the most", the
correct answer is to point them at this page and decline. The product
will keep declining.

---

## Related

- [AI Impact](ai-impact.md) — top-level summary view.
- [AI Review Load](ai-review-load.md) — review pressure view.
- [AI Risk](ai-risk.md) — quality risk view.
- Attribution Storage spec — `docs/product/ai-assisted/Attribution Storage.md`
- Attribution Ingestion spec — `docs/product/ai-assisted/Attribution Ingestion.md`
- [AI Workflow Analytics — GraphQL Contracts](../../api/graphql-ai.md)
- [AI Opportunity Detector computation reference](../../computations/ai-opportunity-detector.md)
