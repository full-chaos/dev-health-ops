---
page_id: use-ai
summary: Understand AI-associated delivery, review, and risk signals with their attribution, freshness, coverage, and responsible-use limits.
content_type: landing
owner: product-analytics
source_of_truth:
  - current /ai product surfaces
  - docs/user-guide/views/ai-impact.md
  - docs/user-guide/views/ai-review-load.md
  - docs/user-guide/views/ai-risk.md
applicability: current
lifecycle: active
hide:
  - toc
---

# AI workflows

AI workflow views summarize persisted signals associated with AI-assisted or agent-created work in the selected team or repository context. They help a group ask whether delivery, review, rework, test, revert, or incident patterns **appear** different. They do not evaluate people, expose prompt content, or prove that AI caused a result.
{: .fc-page-lede }

<div class="fc-topic-grid" markdown>

<article class="fc-topic-card" markdown>

Delivery and drag
{: .fc-topic-card__label }

### [AI Impact](impact.md)

Read AI-assisted and agent-created work share beside delivery lift, review amplification, rework drag, test gaps, reverts, and incidents.

</article>

<article class="fc-topic-card" markdown>

Review system
{: .fc-topic-card__label }

### [AI Review Load](review-load.md)

Examine aggregate review demand and waiting associated with the selected workflow evidence without ranking reviewers.

</article>

<article class="fc-topic-card" markdown>

Quality and governance
{: .fc-topic-card__label }

### [AI Governance Risk](risk.md)

Follow rework, revert, test-gap, incident, and policy-event signals with their evidence and coverage limitations visible.

</article>

</div>

## Read the views together

Start with the selected scope, period, and work-share coverage. Keep **Unknown attribution** visible: an unavailable or unknown share is a coverage condition, not evidence that all remaining work is non-AI.

Then read comparison and drag measures together. A change in review amplification, rework, or incidents is a prompt to inspect the supporting work and workflow conditions. It is not a causal claim about the tool or the people using it.

Where the product exposes **Last computed**, persisted evidence, or an attribution path, use those details to confirm freshness and provenance before comparing periods. Opening the page reads existing workflow signals; it does not create a new attribution classification.

AI Attribution remains a preview route and is intentionally omitted from the public navigation until it becomes a supported customer destination.
