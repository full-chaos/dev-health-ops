---
audience: Start here
canonical: https://docs.fullchaos.dev/user-guide/glossary/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: reference.html
next:
  label: Find the right view
  url: user-guide/views-index/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# Glossary

Use these definitions while reading the manual. They describe the product's displayed
concepts, not a way to score individuals.

## Work and evidence

**WorkUnit**
: An evidence container that groups related work, such as an issue, pull request,
  commit, or incident. A WorkUnit is not a category.

**Theme**
: One of the fixed, broad Investment groupings: Feature Delivery, Operational / Support,
  Maintenance / Tech Debt, Quality / Reliability, or Risk / Security.

**Subcategory**
: A more specific part of a theme. Subcategories are displayed as a distribution within
  the fixed taxonomy, not as a permanent label on a person or work item.

**Evidence quality**
: A band that describes how much useful material supports a displayed distribution or
  relationship. Lower evidence quality means read the linked work and caveats more
  closely.

**Source label**
: A connected source may supply a label in its original language, such as **진행 중인 검토**.
  Keep that label readable with its source reference—<code>sourceEventRef_<wbr>2026Q3EngineeringEnablementMigration</code>—instead of widening the page or replacing the source text.

## Flow and capacity

**Cycle time**
: Time from active work to completion. Read it with waiting and review states.

**Lead time**
: Time from request to completion. It includes time before active work begins.

**Throughput**
: Completed work in the selected period. Compare it with the same scope and time window.

**WIP**
: Work in progress: work that is active but not yet complete.

**Capacity**
: A planning view of what recent delivery history may support. It is a scenario aid, not
  a promise.

## Sustainability and resilience

**After-hours ratio**
: The share of observed work activity outside the configured usual working window. Use it
  as a team-level pattern, not a judgment about an individual.

**Bus factor**
: A concentration signal for recent change knowledge. Read the evidence sample and work
  context before choosing a response.

**Churn**
: Repeated code change in a selected period. Churn can accompany healthy delivery,
  maintenance, or a difficult area; it needs context.

## Continue

- [How to read Dev Health](how-to-read-dev-health.md) shows how the terms work together.
- [Views and Charts](views-index.md) links each question to a suitable view.
