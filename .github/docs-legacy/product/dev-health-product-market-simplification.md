# Dev Health Ops Product Market Overview

## Executive Readout

Dev Health Ops is not short on metrics. The gap is packaging, opinionated workflows, buyer-facing narratives, and differentiated product surfaces that convert metrics into operational decisions.

The strongest product position is:

> Open, inspectable engineering intelligence for teams that distrust opaque developer scoring.

The market-grabbing version of the product should not present itself as another developer productivity analytics dashboard. That market is crowded and often mistrusted. The sharper category is:

> Developer Health Operations: inspectable engineering intelligence for delivery, risk, and team sustainability.

## Core Product Thesis

Most engineering analytics tools answer fragments of the system:

- How fast are PRs moving?
- How much work is in progress?
- Who is reviewing what?
- Where is deployment risk increasing?
- What work is consuming capacity?

Dev Health Ops should answer the operating question leaders actually care about:

> Where is the engineering system unhealthy, what is causing it, what evidence supports that, and what intervention should happen next?

The product should organize around this loop:

```text
State -> Pressure -> Cause -> Evidence -> Action
```

Not:

```text
Metric -> Metric -> Metric -> Metric
```

## Current Inventory

### Backend Strength

The backend already covers a broad analytics base:

- Git and PR metrics: cycle time, coding time, pickup time, review time, PR size, review load
- Work tracking: Jira, GitHub issues/Projects, GitLab issues, WIP, lead time, cycle time, predictability
- DORA: deployment frequency, lead time, MTTR, change failure rate
- CI/CD and TestOps: pipeline success, failure, rerun, queue time, duration, coverage, and test reliability
- Incidents and deployments: provider sync and daily rollups
- Code risk: churn, hotspots, ownership concentration, complexity
- Collaboration: review responsiveness, load, reciprocity, bus factor, knowledge distribution
- Well-being: after-hours activity, weekend activity, burnout risk
- Portfolio: investment allocation, KTLO, new value, security, infrastructure classification

### Frontend Status

The web app has a strong stack and demo posture, but the product surface appears behind the backend breadth:

- Next.js / React / GraphQL / REST fallback
- Demo mode and sample data
- Investment View GraphQL migration
- E2E live-backend testing
- Schema contract enforcement
- Work Graph UI still unfinished
- Related issue, PR, and entity surfaces still unfinished
- Integration tests still placeholder-level

### AI Workflow Intelligence Completion Readout

The AI Workflow Intelligence feature changes the product-market story. Dev Health Ops now has a concrete wedge for teams adopting AI coding agents without wanting a surveillance product or a black-box productivity score.

Shipped / documented surfaces now include:

- AI attribution architecture for `ai_assisted`, `agent_created`, `ai_review`, `human`, and `unknown` buckets
- Provider-side attribution ingestion from explicit labels, bot/app authors, commit trailers, branch names, PR descriptions, and CI annotations
- Attribution storage that preserves source, confidence, scope, provider metadata, and timestamps
- GraphQL read-side contracts for AI impact, attribution buckets, comparisons, review load, risk, governance, recommendations, and drilldowns
- AI Impact dashboard framing with decomposable operating leverage, unknown attribution, and opportunity recommendations
- AI Review Load and AI Risk diagnostic views that compare AI-attributed work against the human baseline without introducing reviewer or author leaderboards
- Governance visibility for declaration coverage, required human review, security/license controls, model/tool policy, and violations
- User-facing documentation that explicitly rejects individual AI productivity scoring and raw prompt/session capture

This means AI Workflow Intelligence should not be treated as a future optional add-on. It should become the clearest market entry point for the broader Dev Health Ops platform:

```text
AI adoption question -> flow impact -> review pressure -> quality risk -> governance evidence -> next intervention
```

Remaining gap: the feature has the right foundations and view contracts, but it still needs product packaging, guided interpretation, demo narrative, buyer-facing copy, and phased follow-through on the intentionally visible missing states.

## Major Gaps

### P0: Package AI Workflow Intelligence as the Wedge

AI Workflow Intelligence is the most timely buyer-facing entry point. The product should lead with the operational question every AI-adopting engineering leader is asking:

> Are AI-assisted workflows improving delivery, or are they shifting cost into review, rework, quality risk, and governance gaps?

The surface should answer that through the same inspectable loop as the rest of Dev Health Ops:

```text
AI attribution -> delivery lift -> review amplification -> rework / risk drag -> governance coverage -> evidence -> action
```

Missing product work:

- A guided AI Workflow Intelligence landing path from the home page
- Buyer-facing copy that explains AI Operating Leverage as decomposable system evidence, not a productivity score
- Demo data and narrative examples that show positive lift, hidden review drag, high unknown attribution, and policy gaps
- Explicit empty-state guidance for unknown attribution, missing hotspot overlap, missing complexity overlap, post-first-review pushes, and aggregate-only reviewer concentration
- A handoff from AI recommendations into the broader Work Graph evidence trail and Engineering Operating Review

Guardrail: do not add raw prompt/session capture, per-author AI scoring, or individual reviewer ranking to make the AI story feel more complete. The no-surveillance posture is the differentiator.

### P0: A Single Killer Workflow

The product needs one dominant path:

```text
Home -> Flow degradation -> Bottleneck -> Work Graph -> Evidence -> Recommendation
```

Users should not have to assemble the story manually from dashboards.

This workflow should answer:

1. What changed?
2. Why did it change?
3. Which teams, repos, services, or workflows are affected?
4. What evidence supports the diagnosis?
5. What is the next intervention?
6. How will we know whether the intervention worked?

### P0: Work Graph as the Product Spine

Work Graph should become the differentiator:

```text
Work item -> PR -> review -> commits -> files -> deployment -> incident -> investment area
```

This turns metrics into evidence.

High-value questions Work Graph should answer:

| Question | Buyer Value |
|---|---|
| What work caused this incident? | Reliability |
| Which projects are consuming review capacity? | Planning |
| Where is complexity slowing delivery? | Architecture |
| What work is invisible to planning? | Leadership |
| Which repos are becoming ownership risks? | Risk management |

### P0: Backend Breadth Is Under-Surfaced

The backend has enough raw capability to impress a technical reviewer. Buyers, however, do not buy inventories. They buy faster answers to painful operating questions.

The web app should lead users through:

```text
State -> Pressure -> Cause -> Evidence -> Action
```

The risk is becoming a broad analytics platform that feels like a collection of smart panels instead of a product that tells a coherent operating story.

### P1: Operational Recommendations

Add rule-based interpretations before adding AI-generated summaries.

Examples:

| Signal | Product Recommendation |
|---|---|
| Rising WIP + flat throughput | The team is saturating. Reduce active work before adding scope. |
| High review latency + concentrated reviewers | Review dependency risk. Add reviewers or rotate ownership. |
| High churn + low delivery | Thrash likely. Inspect hotspots and rework loops. |
| High after-hours + rising cycle time | Sustainability risk. Delivery may be propped up by time debt. |
| Complexity rising in hotspots | Code risk is compounding where change pressure is highest. |

### P1: Capacity Planning

Forecast completion using historical throughput, confidence bands, WIP, review bottlenecks, and backlog size.

Minimum viable forecast:

```text
Given current backlog and rolling throughput, expected completion is:
P50: X weeks
P75: Y weeks
P90: Z weeks
Primary risk: WIP congestion / review latency / incident load
```

This should be positioned as forecast, not commitment.

### P1: Data Health and Trust Layer

Users will not trust analytics if setup is fragile or data gaps are silent.

Expose:

- Last sync by connector
- Rows ingested
- Failed sources
- Missing review facts
- Missing deployment mappings
- Unmapped identities
- Backfill completeness
- Metric lineage

This aligns with the inspectability principle and avoids becoming another opaque scoring system.

### P2: Privacy-First Cognitive Load

Do not start with invasive IDE telemetry.

Start with low-risk signals:

- PR interruption load
- Context spread across repos, issues, and PRs
- Meeting fragmentation if optional calendar support is later added
- After-hours trends from existing commit data

The positioning should be:

```text
Focus fragmentation, not surveillance.
```

## Market-Grabbing Opportunities

### 1. Explainable Engineering Intelligence

Every major metric should drill down as:

```text
Metric changed -> likely cause -> supporting artifacts -> suggested next action
```

This is the most defensible version of inspectability.

### 2. Weekly Engineering Operating Review

Create a weekly operating-review mode that generates a repeatable leadership cadence:

1. What changed this week
2. What improved
3. What worsened
4. Biggest bottleneck
5. Highest-risk hotspot
6. Work-in / work-out imbalance
7. Recommended intervention

Leaders do not need another dashboard. They need a repeatable operating system.

### 2a. AI Workflow Intelligence Review

Create a focused operating-review mode for AI adoption:

1. What share of work is AI-assisted, agent-created, AI-reviewed, human, or unknown
2. Whether AI-attributed work appears to improve cycle time or throughput
3. Where review amplification is rising
4. Where rework, revert, test-gap, or incident drag is appearing
5. Which repos or teams have governance coverage gaps
6. Which automation opportunities are evidence-backed enough to try next
7. What intervention should be tested and how success will be measured

This gives the completed AI Workflow Intelligence foundation a weekly buyer-facing ritual instead of leaving it as a set of standalone dashboards.

### 3. No-Surveillance Positioning

Make this visible in-product:

- No leaderboards
- Team/repo first
- Individual views only for single-person reflection or coaching
- Trends over absolutes
- Evidence over scores
- Metrics as system signals, not performance ratings

This is not just ethics. It is differentiation.

### 4. Compounding Risk View

Create a risk surface combining:

```text
High churn + high complexity + low ownership distribution + slow review
```

This is more valuable than a generic cycle-time dashboard because it points to the architectural and operational risk behind the delivery signal.

Possible names:

- Compounding Risk
- Change Risk Hotspots
- Delivery Drag Hotspots

### 5. Work Graph Evidence Trail

Every major chart should support drilldown into evidence artifacts:

- Work item
- PR
- Review
- Commit
- File
- Deployment
- Incident
- Investment area

This converts trust from an abstract promise into a visible product behavior.

## Competitive Positioning

### Against LinearB-Style Flow Tools

Dev Health Ops can compete on raw flow metrics, but the opportunity is not to clone flow dashboards.

Differentiation:

- Inspectable computation
- Open data model
- Evidence-linked recommendations
- No individual leaderboards
- Work Graph as diagnostic layer
- AI Workflow Intelligence for adoption impact, review pressure, quality risk, and governance without per-person AI scoring
- AI Workflow Intelligence for adoption impact, review pressure, quality risk, and governance without per-person AI scoring

Needed additions:

- Guided bottleneck diagnosis
- Capacity forecasting
- Executive-ready intervention view
- Historical benchmark against the team’s own baseline
- Guided AI adoption review with unknown-attribution and policy-coverage callouts
- Guided AI adoption review with unknown-attribution and policy-coverage callouts

### Against Pluralsight Flow / GitPrime-Style Tools

Dev Health Ops should avoid the trap of individual productivity scoring.

Differentiation:

- Single-person reflection only, not peer ranking
- Team and system interpretation first
- Explicit misuse guardrails
- Evidence-based coaching surfaces

Needed additions:

- Single-person reflection view
- Manager coaching playbooks
- Identity confidence and alias-resolution UX

### Against GitLab Analytics / DORA Tools

Dev Health Ops already has a strong DORA and CI/CD base.

Differentiation:

- Cross-provider analytics
- Work Graph linkage
- Investment and portfolio classification
- TestOps plus delivery-risk interpretation

Needed additions:

- Deployment mapping quality
- Incident correlation to changes
- Change-failure root-cause drilldown
- Release and environment views

### Against Cognitive Load Tools

The current gap is IDE-level and cognitive-load telemetry.

Do not chase this first. It is high risk because it can feel invasive.

Better path:

1. Derive focus-fragmentation signals from existing operational data.
2. Keep aggregation team/repo-first.
3. Add optional local-first IDE telemetry later.
4. Never expose raw keystroke-like telemetry.

## Packaging Strategy

Avoid per-seat pricing if accessibility and anti-surveillance are core principles.

Suggested packaging:

| Tier | Buyer Logic |
|---|---|
| Free / OSS | Self-hosted, single org, limited retention |
| Team SaaS | Hosted ingestion, standard retention, core dashboards |
| Scale SaaS | Multiple orgs, SSO, advanced retention, forecasts |
| Enterprise | Private deployment, audit, custom connectors, compliance |

Positioning line:

> Engineering intelligence without per-seat surveillance pricing.

## Buyer-Specific Narratives

### VP Engineering

> Know where delivery is constrained before it becomes a miss.

Primary surfaces:

- Engineering Operating Review
- Capacity forecast
- Delivery bottleneck summary
- Investment allocation
- Reliability and incident linkage

### Platform / DevEx

> Find systemic friction across repos, CI, reviews, and deployments.

Primary surfaces:

- CI/CD and TestOps
- Review load
- Work Graph
- Hotspots
- Connector/data quality
- AI Workflow Intelligence for automation opportunities and review/risk drag

### AI / Engineering Operations

> Adopt AI coding agents with evidence, controls, and system-health feedback loops.

Primary surfaces:

- AI Impact
- AI Review Load
- AI Risk
- AI Governance
- AI Attribution coverage
- AI Opportunity Detection
- Work Graph evidence trail

### Engineering Manager

> Coach teams with evidence, not rankings.

Primary surfaces:

- Team flow
- WIP and review bottlenecks
- Single-person reflection
- Sustainability signals
- Evidence-backed recommendations

### CTO / Architecture

> See where change pressure is compounding architectural risk.

Primary surfaces:

- Compounding Risk
- Complexity trends
- Ownership concentration
- Hotspots
- Incident correlation

## Prioritized Product Plan

### P0: Package AI Workflow Intelligence for Market Entry

Goal: make the completed AI Workflow Intelligence feature the first clear wedge for buyers evaluating AI engineering operations.

Build one end-to-end path:

```text
Home -> AI Impact -> Review Load / Risk -> Governance gaps -> Evidence -> Recommended intervention
```

Tasks:

- Add a top-level AI Workflow Intelligence entry point and guided empty states
- Write product copy for AI Operating Leverage that explains component math and rejects productivity-score framing
- Add demo scenarios for lift, review amplification, rework drag, incident drag, unknown attribution, and governance violations
- Connect AI Impact recommendations to Work Graph evidence and the weekly operating review
- Track intentionally missing states as visible follow-up work rather than silent omissions

### P0: Finish the Story Loop

Goal: turn metrics into diagnosis.

Build one end-to-end path:

```text
Home -> Flow degradation -> Bottleneck -> Work Graph -> Artifact evidence -> Recommendation
```

Tasks:

- Finish Work Graph frontend types and query integration
- Build Issue and PR related-entity panels
- Build Work Graph Explorer
- Add chart drilldowns from Flow, Hotspots, and Investment views into Work Graph entities
- Add evidence panels under every major metric

### P1: Add Engineering Operating Review Mode

Build a weekly review page with opinionated sections:

| Section | Inputs |
|---|---|
| Delivery movement | Cycle time, throughput, WIP |
| Bottleneck | State duration, review latency, WIP age |
| Risk | Hotspots, ownership, complexity |
| Reliability | Deployments, incidents, change failure |
| Investment | KTLO, new value, security, infrastructure |
| Recommendation | Rule-based interpretation |

Start rule-based. Do not wait for AI.

Include an AI Workflow Intelligence section once the generic review page exists:

| Section | Inputs |
|---|---|
| AI adoption mix | AI attribution buckets, unknown attribution |
| Delivery impact | AI cycle-time delta, throughput lift, operating leverage components |
| Review pressure | Review amplification, pickup latency, change request rate |
| Risk drag | Rework, reverts, test gaps, linked incidents |
| Governance | Declaration, review, scan, allowlist, and violation coverage |
| Opportunity | Rule-based automation opportunities with evidence |

### P1: Ship Capacity Planning

Implement throughput-based forecasting:

- Rolling 4, 8, and 12 week throughput
- Confidence bands
- Scope risk
- WIP congestion overlay
- Review bottleneck overlay
- Forecast-impact explanations

### P1: Add Connector and Data-Quality Health

Add a dedicated Data Health area:

- Connector status
- Last successful sync
- Ingestion volume
- Missing source coverage
- Identity mapping gaps
- Deployment mapping gaps
- Backfill completeness
- Metric lineage

### P2: Build Privacy-First Cognitive Load Wedge

Initial signals:

- Context spread
- PR interruption load
- Review request load
- After-hours trend
- Weekend trend
- Optional meeting fragmentation later

### P2: Create Buyer-Specific Landing Pages

Recommended site structure:

- For Engineering Leaders
- For Platform / DevEx
- For Engineering Managers
- For Architecture / Reliability
- For AI Engineering Operations

## Direct Critique

The main risk is confusing metric breadth with product strength.

The project already has enough raw capability to be compelling. The next level is not more metrics. The next level is a product that says:

```text
Here is where the system is unhealthy.
Here is the evidence.
Here is the next intervention.
Here is how we will know it worked.
```

That is the market-grabbing version.
