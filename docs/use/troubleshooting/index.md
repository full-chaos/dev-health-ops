---
page_id: use-troubleshooting
summary: Start from a visible product symptom, identify its boundary, and collect the right context before escalating.
content_type: troubleshooting-index
owner: documentation
applicability: current
lifecycle: active
hide:
  - toc
---

# Troubleshoot product use

Start with what the product is showing, not with a deployment command. These guides explain the checks available to a person using Dev Health and identify when the problem belongs to workspace administration or platform operations.
{: .fc-page-lede }

## Choose the visible symptom

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

### [No or incomplete data](no-or-incomplete-data.md)

The view is empty, partially populated, missing a source, or unexpectedly returns no usable rows.

</article>

<article class="fc-topic-card" markdown>

### [Unexpected scope or filters](scope-and-filters.md)

The result appears to include the wrong repository, team, period, comparison, or filtered population.

</article>

<article class="fc-topic-card" markdown>

### [Missing permission or unavailable view](permissions-and-availability.md)

A route, control, or result is absent, blocked, or not available to the current workspace or role.

</article>

<article class="fc-topic-card" markdown>

### [Stale or delayed results](stale-or-delayed-results.md)

A value exists but does not include recent expected work, or a sync, computation, or report is still processing.

</article>

<article class="fc-topic-card" markdown>

### [Report problems](reports.md)

A report cannot be created, run, scheduled, or read, or its run history and output do not match the saved context.

</article>

</div>

## Preserve the failing context

Before changing filters or asking for help, record:

- workspace;
- team, repository, or other scope;
- time window and comparison window;
- active filters;
- page and visible state;
- source/provider involved;
- approximate time the problem occurred;
- whether another equivalent scope works.

Use sanitized screenshots where they make the state clearer. Do not include credentials, access tokens, customer-sensitive source content, or unredacted logs.

## Know the escalation boundary

| Problem boundary | Where to continue |
| --- | --- |
| The selected scope, filter, comparison, or page state is unclear | Stay in these product troubleshooting guides. |
| A workspace role, team mapping, source connection, or coverage setting is wrong | Continue under [Administer Dev Health](../../admin/troubleshooting/index.md). |
| Ingestion, workers, queues, stores, migrations, or runtime health are failing | Continue under [Install and operate](../../operate/runbooks/index.md). |
| The product behavior, metric contract, or supported route appears incorrect | Preserve the context and escalate with the relevant source and reference links. |

Do not send ordinary product users directly to database repair, migration commands, or worker internals. The troubleshooting path should narrow the problem before crossing into administration or operations.
