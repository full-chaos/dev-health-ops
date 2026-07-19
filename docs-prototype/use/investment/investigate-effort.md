---
page_id: use-investment-investigate-effort
summary: Move from a question about engineering effort to the Investment theme mix, subcategories, evidence quality, and supporting work.
content_type: task-guide
owner: product-analytics
source_of_truth:
  - src/dev_health_ops/api/services/investment.py
  - src/dev_health_ops/api/queries/investment.py
  - src/components/work/investment/charts/InvestmentMixSection.tsx
  - docs/user-guide/journeys/investment-view.md
applicability: current
lifecycle: active
---

# Investigate where effort appears to be going

Use this workflow when the question is about the **composition of work** in a team, repository, or other supported scope: how much of the selected effort appears associated with feature delivery, operational support, maintenance, quality, risk, and their subcategories.
{: .fc-page-lede }

Investment is a distribution over supported WorkUnit evidence. It is not a ticket taxonomy, a count of records, or a person-level assessment. The workflow begins with the full mix and ends with the source artifacts needed to explain it.

## Before you begin

Confirm that:

- you have access to the Investment destination for the workspace;
- the intended team, repository, or other scope is available;
- the selected period matches the question;
- the expected providers and repositories are synchronized;
- the result is not visibly stale or partially covered.

Keep the scope, period, and filters stable while moving between the mix and its evidence. A changed context produces a different distribution.

## 1. Start with the full theme mix

Open Investment Mix and read the complete distribution before selecting a segment. The canonical themes are fixed across teams and providers; source issue types and labels are normalized inputs rather than the displayed truth.

Read the largest shapes as the largest **effort-weighted contributions** to the current positive total. A 30% share does not mean 30% of tickets or people. [Investment Mix](investment-mix.md) explains the visual encodings and exact reading order.

## 2. Check evidence quality and data state

Before interpreting a difference, identify whether the result is complete enough for the question:

- **Evidence quality** describes the strength of corroboration behind the categorization.
- **Coverage** describes whether the expected source population is represented.
- **Freshness** describes whether the available records are recent enough.
- **Measured zero**, **empty**, **partial**, and **unavailable** are different states.

A low evidence-quality result can still be visible; it requires more cautious language and a closer review of the linked work. An incomplete or stale result should not be compared as though it were complete.

## 3. Open a theme and its subcategories

Choose a theme that is material to the question, then inspect the subcategories inside it. Themes answer “What broad kind of investment is this?” Subcategories answer “What more specific kind of work contributes inside that theme?”

The hierarchy is:

```text
Theme → Subcategory → WorkUnit evidence
```

WorkUnits do not appear as categories. A WorkUnit can contribute to more than one subcategory because Investment stores distributions rather than one permanent label.

## 4. Follow the selection to supporting work

Open the available evidence action for the selected theme or subcategory. Inspect the WorkUnits and their supported issues, pull requests, commits, incidents, dates, and source context.

This step distinguishes explanations the chart cannot choose between. A larger maintenance share, for example, can reflect an intentional refactor, a migration, recurring repair, or another kind of work. The distribution locates the question; the source artifacts explain the context.

Use [Work Graph](../code-and-relationships/work-graph.md) when the evidence requires a relationship path across issues, pull requests, commits, files, or other entities.

## 5. State the result with its limits

A useful summary keeps the context and uncertainty visible:

> In the selected platform-team scope and four-week period, the effort-weighted mix appears to lean more toward Maintenance / Tech Debt than in the equivalent prior period. The largest contributing subcategory is refactoring. Coverage is comparable, and the linked work shows a planned service-boundary migration.

Avoid statements such as “the team spent 40% of its time on bad code” or “these people caused maintenance work.” The product does not measure individual time, value, or causation.

## Compare periods or scopes

Compare only when the following are equivalent or explicitly accounted for:

- scope and repository population;
- time-window length;
- filters;
- source coverage and freshness;
- calculation and taxonomy version;
- available evidence quality.

A difference can be caused by changed coverage or context, not only changed work. Use [Compare trends responsibly](../plan-and-improve/compare-trends.md) for the comparison discipline.

## When the result is not usable

| State | Interpretation | Response |
| --- | --- | --- |
| Measured positive contribution | Supported inputs produced a positive effort-weighted result | Read the mix, quality, and evidence. |
| Measured zero | The supported calculation produced zero | Confirm the exact contract and source population. |
| Empty response | No usable rows or required input is available | Diagnose scope, filters, availability, and coverage. |
| Incomplete or stale | Expected source data is missing or older than required | Resolve coverage or freshness before comparing. |
| Low or unknown evidence quality | Categorization is weakly corroborated | Inspect more supporting work and use cautious language. |

Use [No or incomplete data](../troubleshooting/no-or-incomplete-data.md) when the page cannot support the question.

## Continue

- [Investment Mix](investment-mix.md)
- [Follow investment evidence](follow-evidence.md)
- [Investment taxonomy](../../reference/taxonomies/investment.md)
- [Weighting and aggregation](../../reference/metrics/weighting-and-aggregation.md)
