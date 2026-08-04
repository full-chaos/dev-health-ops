---
page_id: context-fabric
summary: See how Context Fabric connects engineering evidence so people and agents can understand the real state of projects, teams, and organizations.
content_type: marketing
owner: product
source_of_truth:
  - current Ask Dev window and /dev workspace
  - current Context Fabric Validation surface
  - Ask Dev Interaction Surface Amendment — Context Fabric relationship, chat window, and full-page workspace
  - "PRD v2.1: Dev Health Agent Context Runtime — Go service and MCP sidecar"
  - full-chaos/dev-health-acr README.md
applicability: current
lifecycle: active
hide:
  - navigation
  - toc
---

<section class="fc-cf-hero" aria-labelledby="fc-cf-title" markdown>
<div class="fc-cf-hero__copy" markdown>

Context Fabric
{: .fc-cf-kicker }

# Know what’s actually happening—not just what the tracker says. { #fc-cf-title }

Context Fabric connects the work, code, delivery, reliability, and source-health evidence already flowing through Dev Health. It gives people and agents a shared, evidence-backed view of the real state of a project, team, or organization.
{: .fc-cf-lede }

<div class="fc-cf-actions" markdown>

[Explore Ask Dev](../use/ai-workflows/index.md#use-ask-dev-for-a-human-investigation){ .md-button .md-button--primary }
[Use Context Fabric with an agent](https://github.com/full-chaos/dev-health-acr/blob/main/docs/mcp-sidecar.md){ .md-button }

</div>
</div>

<aside class="fc-cf-hero__visual" aria-label="Context Fabric connects engineering signals to Ask Dev and compatible agents">
  <div class="fc-cf-sources" aria-label="Connected engineering evidence">
    <span>Planning</span>
    <span>Code and review</span>
    <span>CI and delivery</span>
    <span>Incidents and reliability</span>
  </div>
  <div class="fc-cf-core">
    <strong>Context Fabric</strong>
    <span>State → Pressure → Cause → Evidence → Action</span>
  </div>
  <div class="fc-cf-destinations">
    <div><strong>Ask Dev</strong><span>For people</span></div>
    <div><strong>MCP</strong><span>For developers and agents</span></div>
  </div>
</aside>
</section>

<section class="fc-cf-section fc-cf-section--intro" markdown>

## Project status is a relationship, not a field

A tracking system records what someone declared. The rest of the engineering ecosystem records what actually happened.

A pull request may be merged while the ticket still says **In Progress**. A feature may be deployed behind a flag, active for users, and followed by an incident or additional rollout work. Each system contains part of the story, but no single status field contains the whole thing.

Context Fabric connects those signals so teams do not have to treat one tracker—or one teammate’s memory—as the only source of truth.

</section>

<section class="fc-cf-example" aria-labelledby="fc-cf-example-title" markdown>
<div class="fc-cf-example__heading" markdown>

Real-world example
{: .fc-cf-kicker }

## The ticket says “In Progress.” What is the actual state? { #fc-cf-example-title }

</div>

<div class="fc-cf-example__grid">
  <div class="fc-cf-example__declared">
    <span class="fc-cf-example__label">Tracking system</span>
    <strong>In Progress</strong>
    <span>Last updated four days ago</span>
  </div>

  <div class="fc-cf-evidence" aria-label="Observed evidence">
    <span class="fc-cf-evidence__item is-complete">Pull request merged</span>
    <span class="fc-cf-evidence__item is-complete">CI passed</span>
    <span class="fc-cf-evidence__item is-complete">Deployment succeeded</span>
    <span class="fc-cf-evidence__item is-complete">Feature flag enabled</span>
    <span class="fc-cf-evidence__item is-current">Available to users</span>
  </div>

  <div class="fc-cf-example__answer">
    <span class="fc-cf-example__label">Context Fabric</span>
    <strong>Implemented, deployed, and available</strong>
    <p>The delivery evidence shows the feature is live. The tracking record is stale, and any remaining follow-up is reported separately.</p>
  </div>
</div>
</section>

<section class="fc-cf-section" markdown>

## Come to the conversation prepared

Context Fabric is not meant to replace talking with your teammates. It helps you ask better questions and arrive with the relevant evidence already connected—like the A+ student who actually did the reading.

That gives engineering teams, leadership, developers, and agents a common starting point for discussing status, blockers, health, decisions, and next actions.

</section>

<section class="fc-cf-use-cases" aria-labelledby="fc-cf-use-cases-title" markdown>

## Two ways to use Context Fabric { #fc-cf-use-cases-title }

<div class="fc-cf-use-grid" markdown>

<article class="fc-cf-use-card fc-cf-use-card--ask" markdown>

For teams and leaders
{: .fc-cf-card-kicker }

### Ask Dev

Dev is embedded in the Dev Health application and can answer questions about project, team, and organizational health across the connected ecosystem.

<div class="fc-cf-question-list" markdown>

- Is this project actually done?
- What is blocking delivery?
- Which teams need attention?
- What do this team’s DORA metrics look like over the last 90 days?

</div>

[See how Ask Dev works →](../use/ai-workflows/index.md#use-ask-dev-for-a-human-investigation){ .fc-cf-card-link }

</article>

<article class="fc-cf-use-card fc-cf-use-card--mcp" markdown>

For developers and agents
{: .fc-cf-card-kicker }

### MCP for agents

Compatible coding, review, documentation, and automation agents can receive scoped context and supporting evidence before they begin work instead of starting cold.

<div class="fc-cf-question-list" markdown>

- What decisions already govern this work?
- Which related changes or failures matter?
- What evidence should be verified before editing?
- What risks and required checks are already known?

</div>

[Configure the ACR MCP sidecar →](https://github.com/full-chaos/dev-health-acr/blob/main/docs/mcp-sidecar.md){ .fc-cf-card-link }

</article>

</div>
</section>

<section class="fc-cf-trust" aria-labelledby="fc-cf-trust-title" markdown>
<div class="fc-cf-trust__copy" markdown>

Evidence before confidence
{: .fc-cf-kicker }

## An answer should show its work. { #fc-cf-trust-title }

Context Fabric does not quietly turn incomplete data into certainty. It keeps the scope, as-of time, freshness, coverage, conflicts, unavailable sources, and supporting evidence visible.

What it can answer depends on the sources, permissions, freshness, and capabilities available to the organization.

</div>

<div class="fc-cf-trust__points" markdown>

- Observed facts stay separate from inferences and recommendations.
- Missing, stale, or unavailable sources are disclosed—not represented as zero.
- Evidence is authorized again when it is opened.
- Conflicting systems remain visible instead of being silently overwritten.

</div>
</section>

<section class="fc-cf-final" aria-labelledby="fc-cf-final-title" markdown>

## See the whole picture before deciding what happens next. { #fc-cf-final-title }

Use Ask Dev when a person needs an evidence-backed answer. Use the ACR MCP path when an agent needs context before it works.

<div class="fc-cf-actions" markdown>

[Start with Ask Dev](../use/ai-workflows/index.md#use-ask-dev-for-a-human-investigation){ .md-button .md-button--primary }
[Read the technical architecture](../contribute/architecture/platform.md#ask-dev-and-context-fabric-runtime){ .md-button }

</div>
</section>
