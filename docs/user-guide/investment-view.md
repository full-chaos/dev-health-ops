# Investment View

The Investment View answers one question for leaders and individual contributors:

> **Where does our effort appear to be going — and what mix of work is dominating?**

You do not tag any of this by hand. The platform reads the work your team already
produces (issues, pull requests, commits) and presents a **picture of the mix**. This
page explains how to read that picture without needing to know how it is computed.

For the exact vocabulary used below, see the
[Investment Taxonomy](../product/investment-taxonomy.md) — the single shared list of the
five themes and fifteen subcategories.

---

## What you are looking at: a distribution, not a label

Nobody on the team assigns a category to a ticket. Instead, the platform clusters related
work together and produces a **distribution** — a set of percentages that add up to about
100%. So a view might read:

> This work **appears ~60% Feature Delivery**, ~25% Quality / Reliability, and ~15%
> Maintenance / Tech Debt.

Read that as *"the mix leans heavily toward shipping new capability, with a meaningful
slice of quality work."* It is a **lean**, not a verdict. A single piece of work is never
stamped "this is a feature." It is always expressed as a blend.

Because of this, the language throughout the product is deliberately tentative — you will
see **appears**, **leans**, and **suggests**, never "is", "was", or "detected". The view
is a hypothesis starter for a conversation, not a measurement of fact.

---

## The percentage is effort-weighted (this is the key idea)

The most common misreading is *"60% Feature Delivery means 60% of our tickets were
features."* **It does not.**

The percentages are **effort-weighted**. Each cluster of work carries an **effort value**,
and the mix is weighted by that effort before the percentages are computed. Effort is
drawn from, in order of preference:

1. **Code churn** — lines added + deleted in the related commits, or
2. **Code churn** from the related pull requests, or
3. **Active hours** logged on the related issues, or
4. nothing (an effort value of zero, which contributes nothing to the mix).

So a single large refactor that touched thousands of lines can outweigh a dozen tiny
tickets. **"60% Feature Delivery" means 60% of the weighted effort appears to lean Feature
Delivery — not 60% of the ticket count.** A handful of high-effort items can dominate the
picture even if most tickets sit in another theme.

> Why effort-weighting? Counting tickets would treat a one-line typo fix and a multi-week
> platform build as equal. Weighting by churn / hours keeps the picture anchored to where
> human effort actually went. The exact formula lives in the
> [Investment API](../api/investment-api.md).

---

## Worked example: "Why does this appear ~60% Feature Delivery?"

Imagine the Investment View shows a cluster of linked work that reads **~60% Feature
Delivery, ~25% Quality / Reliability, ~15% Maintenance / Tech Debt.** Here is the kind of
underlying work that produces that mix.

**The linked work in this cluster:**

| Item | Type | What it touched | Effort (churn / hours) |
| ---- | ---- | --------------- | ---------------------- |
| "Add saved-search filters" | Issue → PR → 4 commits | New filtering feature | **1,800 lines** churn |
| "Filter results pagination" | Issue → PR → 3 commits | More of the same feature | **900 lines** churn |
| "Fix off-by-one in filter count" | Issue → PR → 1 commit | Bug in the new filter | **220 lines** churn |
| "Add tests for filters" | PR → 2 commits | Test coverage | **400 lines** churn |
| "Bump query library to 3.x" | PR → 1 commit | Dependency upgrade | **300 lines** churn |

**How that becomes the mix:**

- The two feature items carry the most churn (1,800 + 900 = **2,700 lines**), so the
  cluster's effort leans heavily toward building new capability — these push toward
  **Feature Delivery**.
- The bug fix and the test work (220 + 400 = **620 lines**) suggest a real but smaller
  slice of **Quality / Reliability**.
- The dependency upgrade (300 lines) contributes a modest **Maintenance / Tech Debt**
  share.

Weighted by effort, the blend settles at roughly **60% Feature Delivery / 25% Quality /
15% Maintenance**. Notice that *by ticket count* feature work is only 2 of 5 items (40%) —
but because those two carried far more churn, the **effort-weighted** picture leans 60%
Feature Delivery. That gap between "count" and "effort" is exactly what this view is
designed to surface.

If you swapped the big feature PRs for a single large refactor with the same churn, the
same arithmetic would tip the cluster toward Maintenance instead — the mix follows the
effort, not the labels people typed.

---

## Confidence: evidence quality and what "low confidence" means

Every cluster also carries an **evidence quality** score, presented as a band:
**high**, **moderate**, **low**, or **very low**. It reflects how much trustworthy signal
was available to form the mix — roughly:

- **How much text** there was to read (rich issue descriptions and PR bodies vs. a bare
  commit subject),
- **Whether multiple kinds of source agreed** (issues *and* PRs *and* commits, rather than
  commits alone),
- **How densely the work was linked together.**

**Read the confidence band before you read the percentages.**

- A **high** or **moderate** band means the mix rests on substantial, agreeing evidence —
  it is reasonable to lean on it as a conversation starter.
- A **low** or **very low** band means the picture is thin. The platform still produces a
  distribution (it never returns "unknown"), but when evidence is too sparse it falls back
  to a **neutral prior** — an even spread that essentially says *"there was not enough
  validated evidence to suggest a confident mix here."* That is **not** a finding; treat it
  as a prompt to look at the underlying items yourself rather than as a signal about the
  work.

In short: the percentages tell you which way the work **appears** to lean; the confidence
band tells you **how much weight to put on that lean.** A confident-looking 60% built on
very-low-quality evidence deserves far more skepticism than a 45% built on high-quality
evidence.

For how the score is computed and banded, see the
[Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md#step-7-evidence-quality-and-effort-value).

---

## "unassigned" is a scope state, not a category

When you drill into where effort is flowing (by repository or team), you may see an
**`unassigned`** bucket. This is **not** an investment category and it never appears inside
a theme or subcategory mix. It simply means **a piece of work could not be tied back to a
known repository or team** — its scope attribution is missing.

So `unassigned` answers *"whose / which repo?"* with "we don't know," not *"what kind of
work?"*. If the `unassigned` bucket is large, it is a data-coverage gap to chase (missing
repo or team mapping), not a type of work to interpret. See the
[Investment API note on scope labels](../api/investment-api.md#unassigned-means-missing-scope-not-a-category).

---

## How to drill in

The view is designed to be read top-down, the same way you would brief a leader and then
dig in:

1. **Themes first** — the five-way split (Feature Delivery, Operational / Support,
   Maintenance / Tech Debt, Quality / Reliability, Risk / Security). This is the
   leadership-readable summary.
2. **Subcategories** — open a theme to see the finer mix (e.g. Feature Delivery → customer
   / roadmap / enablement). Same effort-weighted logic, more resolution.
3. **Evidence** — open a subcategory to see the actual linked issues, PRs, and commits
   that shaped the mix, so any number traces back to real work.

Always check the **confidence band** at whatever level you are reading.

---

## What this view does *not* do

- It does **not** tag individual issues, PRs, or commits with a category.
- It does **not** let you define your own categories — the
  [taxonomy](../product/investment-taxonomy.md) is fixed and shared by everyone.
- It does **not** rank or compare people. It describes *work*, not individuals.
- It does **not** return "unknown" — when evidence is thin it leans on a neutral prior and
  lowers the confidence band instead.

---

## Related docs

- [Investment Taxonomy](../product/investment-taxonomy.md) — the fixed list of themes and subcategories (shared source)
- [Investment Categorization Pipeline](../architecture/investment-categorization-pipeline.md) — how the distribution is computed
- [Investment Data Model](../architecture/investment-data-model.md) — how it is persisted
- [Investment API](../api/investment-api.md) — how it is aggregated and effort-weighted
- [Investment Materialization](../ops/investment-materialization.md) — the CLI that produces it
- [LLM Categorization Contract](../llm/categorization-contract.md) — the AI guardrails (compute-time only)
- [Work Graph](work-graph.md) — relationships and materialization
