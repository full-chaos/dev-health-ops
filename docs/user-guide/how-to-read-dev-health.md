---
audience: Start here
canonical: https://docs.fullchaos.dev/user-guide/how-to-read-dev-health/
owner: Dev Health documentation
last-reviewed: 2026-07-16
template: guide.html
next:
  label: Start the Investment journey
  url: user-guide/journeys/investment-view/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# How to read Dev Health

Read every view in the same order: set the context, read the trend, inspect confidence
and caveats, then open the evidence. The result is a question worth discussing, not a
ranking of people or a final explanation.

## Keep the context visible

Before comparing values, confirm the team or repository scope and the time window. A
value is meaningful only beside its unit, period, and coverage. A blank or unavailable
value means the page lacks usable information for that context; it does not mean zero.

## Read common measures together

| Measure | A useful question | Read with |
| --- | --- | --- |
| **Churn** | Where is code changing repeatedly? | Complexity and the related work. |
| **Cycle time** | How long does work take once it starts? | **Lead time**, WIP, and review latency. |
| **Lead time** | How long does work take from request to completion? | The states and waiting time along the way. |
| **Throughput** | How much completed work moved in the period? | WIP and the same time window. |
| **WIP** | How much work is active at once? | Throughput and blocked or waiting states. |
| **After-hours ratio** | Is work clustering outside the usual working window? | Team context and a longer trend. |
| **Bus factor** | How concentrated is recent change knowledge? | The evidence sample and ownership context. |

Metrics show patterns in a selected context. They do not compare people with one
another, and they should not be used to judge an individual.

## Follow the evidence model

A **WorkUnit** is an evidence container: it gathers related work such as an issue, pull
request, commit, or incident. It is not a category. In the Investment journey, a
**theme** is a broad, fixed kind of work and a **subcategory** is a more specific part of
that theme. The view shows distributions, so a WorkUnit can contribute to several
subcategories rather than receive one permanent label.

**Evidence quality** tells you how well the available material supports the displayed
distribution. Read the evidence quality before relying on a percentage. Sparse evidence
calls for a closer look at the linked work and more caution in the conversation.

## Calibrate model-derived language

Where a view includes a model-derived estimate, its language should say **appears**,
**leans**, or **suggests**. It should not turn an estimate into a conclusion. The model
helps summarize evidence at compute time; reading a page does not recalculate the work
categories or relationship weights.

## Continue

- [Glossary](glossary.md) defines the terms used across the manual.
- [Your first 10 minutes](first-10-minutes.md) returns to the starting path.
- [Investment: follow the evidence](journeys/investment-view.md) puts the model into a
  practical journey.
