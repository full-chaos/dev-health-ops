---
page_id: use
summary: Understand Dev Health views, choose the right workflow, and follow results to supporting evidence.
content_type: landing
owner: documentation
source_of_truth:
  - .github/documentation-program/ia/use.tsv
  - docs/user-guide/views-index.md
  - docs/user-guide/how-to-read-dev-health.md
applicability: current
lifecycle: active
hide:
  - toc
---

# Use Dev Health

Dev Health views answer different questions about a selected body of work. They are explanations with drill-down—not a wall of independent widgets. Start by choosing a scope and period, read the visible state and units, then follow a result to the work that supports it before choosing an action.
{: .fc-page-lede }

## Start with the shared context

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

### [Set scope and time](navigate/scope-and-time.md)

Choose the workspace, team, repository, and period that define the question. A value without its scope and window is not comparable.

</article>

<article class="fc-topic-card" markdown>

### [Use filters and comparisons](navigate/filters-and-comparisons.md)

Understand which filters narrow the source data and which comparison choices change the question being asked.

</article>

<article class="fc-topic-card" markdown>

### [Understand data states](navigate/data-states.md)

Distinguish loading, measured zero, empty, unavailable, stale, delayed, and partially covered results before interpreting a chart.

</article>

</div>

## Choose the view that matches the question

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

Product investment
{: .fc-topic-card__label }

### [Investment](investment/index.md)

Ask where effort appears to be going, how the mix changes over time, and which work supports a theme or subcategory.

</article>

<article class="fc-topic-card" markdown>

Movement and waiting
{: .fc-topic-card__label }

### [Delivery flow](delivery-flow/index.md)

Examine state transitions, review pressure, throughput, waiting, and repeated movement through the delivery system.

</article>

<article class="fc-topic-card" markdown>

Code and evidence
{: .fc-topic-card__label }

### [Code and relationships](code-and-relationships/index.md)

Locate persistent code-area patterns and follow issues, pull requests, commits, files, and other linked evidence.

</article>

<article class="fc-topic-card" markdown>

Forecasts and decisions
{: .fc-topic-card__label }

### [Planning](plan-and-improve/index.md)

Use completion ranges and trend comparisons as planning inputs without turning uncertainty into a commitment or target.

</article>

<article class="fc-topic-card" markdown>

Model-assisted analysis
{: .fc-topic-card__label }

### [AI workflows](ai-workflows/index.md)

Read AI-associated delivery, review, and risk signals with their attribution, coverage, and evidence limitations visible.

</article>

<article class="fc-topic-card" markdown>

Repeatable questions
{: .fc-topic-card__label }

### [Reports](reports/index.md)

Save a scope, period, and analytical question, run it repeatedly, and read the resulting narrative with its provenance.

</article>

</div>

## Read every result in the same order

1. Confirm the **scope**, **time window**, filters, and units.
2. Identify the visible **data state** and whether coverage is complete enough for the question.
3. Read the distribution, transition, trend, or relationship the view actually shows.
4. Inspect confidence, evidence quality, and caveats where they are available.
5. Open the related work before selecting an explanation or response.

Metrics and visualizations describe patterns in a selected context. They do not rank people, establish cause by themselves, or turn missing information into zero.

## Recover from a problem

Start from the symptom in [Troubleshoot product use](troubleshooting/index.md). User guidance explains checks available inside the product; workspace configuration belongs under [Administer Dev Health](../admin/index.md), and ingestion or runtime failures belong under [Install and operate](../operate/index.md).
