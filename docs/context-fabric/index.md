---
page_id: context-fabric
summary: See how Context Fabric gives people and agents evidence-backed context about the work, systems, relationships, decisions, and conditions across an engineering organization.
content_type: marketing
owner: product
source_of_truth:
  - current Ask Dev window and /dev workspace
  - current Context Fabric Validation surface
  - Ask Dev Interaction Surface Amendment — Context Fabric relationship, chat window, and full-page workspace
  - Ask Dev Wave 3.1 — Evidence-backed engineering intelligence
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

# Give people and agents the context behind the work. { #fc-cf-title }

Context Fabric turns the engineering evidence already flowing through Dev Health into shared operating context: who owns and is affected by the work, what is actually happening, why it matters, how the systems and decisions relate, and what evidence should guide the next action.
{: .fc-cf-lede }

<div class="fc-cf-actions" markdown>

[Explore Ask Dev](../use/ai-workflows/index.md#use-ask-dev-for-a-human-investigation){ .md-button .md-button--primary }
[Connect an agent](https://github.com/full-chaos/dev-health-acr/blob/main/docs/mcp-sidecar.md){ .md-button }

</div>
</div>

<aside class="fc-cf-hero__visual" aria-label="Context Fabric connects the engineering ecosystem to Ask Dev and ACR MCP consumers">
  <div class="fc-cf-sources" aria-label="Connected engineering evidence">
    <span>Work and priorities</span>
    <span>Code and delivery</span>
    <span>Teams and ownership</span>
    <span>Reliability and operations</span>
  </div>
  <div class="fc-cf-core">
    <strong>Context Fabric</strong>
    <span>State → Pressure → Cause → Evidence → Action</span>
  </div>
  <div class="fc-cf-destinations">
    <div><strong>Ask Dev</strong><span>For people</span></div>
    <div><strong>ACR / MCP</strong><span>For developers and agents</span></div>
  </div>
</aside>
</section>

<section class="fc-cf-section fc-cf-section--intro" markdown>

## Understand the engineering ecosystem around the work

Planning systems, repositories, reviews, delivery pipelines, incidents, architecture, ownership, investment, dependencies, and source health each describe a different part of engineering reality. On their own, they leave people and agents to reconstruct the surrounding context every time they need to decide or act.

Context Fabric connects those authorized signals without flattening them into one status field or opaque score. It creates a bounded, evidence-backed view of the relationships, conditions, history, and constraints that explain what is happening across a project, team, portfolio, or organization.

</section>

<section class="fc-cf-use-cases" aria-labelledby="fc-cf-context-title" markdown>

## Context that answers who, what, why, and how { #fc-cf-context-title }

<div class="fc-cf-use-grid" markdown>

<article class="fc-cf-use-card fc-cf-use-card--ask" markdown>

Connected scope
{: .fc-cf-card-kicker }

### Who

The teams, owners, reviewers, repositories, services, projects, and stakeholders that are authorized, related, responsible, or affected.

</article>

<article class="fc-cf-use-card fc-cf-use-card--mcp" markdown>

Observed state
{: .fc-cf-card-kicker }

### What

The work, changes, delivery state, incidents, metrics, investment mix, source coverage, and other evidence that describe current reality.

</article>

<article class="fc-cf-use-card fc-cf-use-card--mcp" markdown>

Relationships and causes
{: .fc-cf-card-kicker }

### Why

The decisions, dependencies, prior attempts, failures, constraints, and sustained pressures that explain why the current state exists.

</article>

<article class="fc-cf-use-card fc-cf-use-card--ask" markdown>

Ways of working
{: .fc-cf-card-kicker }

### How

The architecture, workflows, required checks, evidence paths, operating boundaries, and next actions that should shape what happens next.

</article>

</div>
</section>

<section class="fc-cf-example" aria-labelledby="fc-cf-example-title" markdown>
<div class="fc-cf-example__heading" markdown>

One day-to-day use case
{: .fc-cf-kicker }

## The ticket says “In Progress.” What is the actual state? { #fc-cf-example-title }

Project status is one familiar place where connected context matters. A tracking system records what someone declared; the rest of the engineering ecosystem records what actually happened.

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

## Unbounded Insights

The same fabric supports broader operating questions: project and portfolio health, team conditions and workload pressure, investment balance, operational deficiencies, ownership and dependency risk, changes in delivery or reliability, prior decisions that govern new work, and the quality or freshness of the available evidence.

It gives a leader a connected view before deciding where to intervene, a team a better starting point for a conversation, and an agent the relevant history and constraints before it edits code, reviews a change, updates documentation, investigates a failure, or runs automation.

</section>

<section class="fc-cf-use-cases" aria-labelledby="fc-cf-use-cases-title" markdown>

## One fabric, built for people and agents { #fc-cf-use-cases-title }

<div class="fc-cf-use-grid" markdown>

<article class="fc-cf-use-card fc-cf-use-card--ask" markdown>

For teams and leaders
{: .fc-cf-card-kicker }

### Ask Dev

Ask Dev is the people-facing conversational layer in Dev Health. It brings project, team, portfolio, delivery, reliability, investment, operational, and data-trust evidence into one investigation instead of making someone navigate and reconcile every surface manually.

<div class="fc-cf-question-list" markdown>

- What needs attention across this portfolio?
- Which teams show sustained pressure, and why?
- Where is engineering investment going?
- What operational deficiencies or source gaps matter first?

</div>

[See how Ask Dev works →](../use/ai-workflows/index.md#use-ask-dev-for-a-human-investigation){ .fc-cf-card-link }

</article>

<article class="fc-cf-use-card fc-cf-use-card--mcp" markdown>

For developers and agents
{: .fc-cf-card-kicker }

### ACR and MCP

Compatible coding, review, documentation, CI, research, and automation agents can receive scoped context and supporting evidence before they begin work instead of starting cold or rediscovering the ecosystem from scratch.

<div class="fc-cf-question-list" markdown>

- Who owns or is affected by this change?
- What decisions and dependencies already govern it?
- Which related code, reviews, incidents, or failures matter?
- What constraints, risks, evidence, and checks should shape the plan?

</div>

[Configure the ACR MCP sidecar →](https://github.com/full-chaos/dev-health-acr/blob/main/docs/mcp-sidecar.md){ .fc-cf-card-link }

</article>

</div>
</section>

<section class="fc-cf-trust" aria-labelledby="fc-cf-trust-title" markdown>
<div class="fc-cf-trust__copy" markdown>

Evidence before confidence
{: .fc-cf-kicker }

## Understanding should remain inspectable. { #fc-cf-trust-title }

Context Fabric does not quietly turn incomplete data into certainty. It keeps the committed scope, as-of time, freshness, coverage, conflicts, unavailable sources, relationships, and supporting evidence visible.

What it can explain depends on the sources, permissions, freshness, relationships, and capabilities available to the organization.

</div>

<div class="fc-cf-trust__points" markdown>

- Observed facts stay separate from inferences and recommendations.
- Missing, stale, or unavailable sources are disclosed—not represented as zero.
- Evidence is authorized again when it is opened.
- Conflicting systems remain visible instead of being silently overwritten.

</div>
</section>

<section class="fc-cf-final" aria-labelledby="fc-cf-final-title" markdown>

## Give every decision—and every agent—the context behind the work. { #fc-cf-final-title }

Use Ask Dev when people need shared, evidence-backed understanding. Use ACR and MCP when agents need the relevant scope, relationships, history, constraints, and evidence before they act.

<div class="fc-cf-actions" markdown>

[Start with Ask Dev](../use/ai-workflows/index.md#use-ask-dev-for-a-human-investigation){ .md-button .md-button--primary }
[Read the technical architecture](../contribute/architecture/platform.md#ask-dev-and-context-fabric-runtime){ .md-button }

</div>
</section>
