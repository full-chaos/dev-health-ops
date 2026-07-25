---
audience: Use Dev Health
canonical: https://docs.fullchaos.dev/user-guide/metrics-interpretation/
owner: Dev Health documentation
last-reviewed: 2026-07-17
template: guide.html
next:
  label: Review reports
  url: user-guide/reports/
troubleshooting: customer-push-ingestion/troubleshooting/
---

# Interpret shared metrics

Metrics help a team notice patterns in work, not judge people. Keep the scope, time
window, unit, and available evidence beside every reading.

## Purpose

Use this guide before comparing a report or chart. It gives each common measure a plain
meaning and a question that keeps the conversation focused on work rather than people.

## Read the trend before the latest value

Start with the trend in one selected scope and time window. Then open the related work,
states, and source coverage. A single point can be unusual; several points moving together
are a better reason to ask a team question.

## Common measures

| Measure | Plain meaning | Read it with |
| --- | --- | --- |
| **Cycle time** | Time from work starting to completion. | Lead time, WIP, and waiting or review states. |
| **Lead time** | Time from a request being created to completion. | Cycle time and the period before active work begins. |
| **Throughput** | Completed work in the selected period. | WIP and the same scope and time window. |
| **WIP** | Work in progress: active work that is not complete. | Throughput, blocked work, and a longer trend. |
| **After-hours ratio** | The share of observed weekday activity outside the configured working window. | The team's business timezone and a longer trend. |
| **Weekend ratio** | The share of observed activity on weekends. | The same scope, business timezone, and a longer trend. |
| **Bus factor** | A concentration signal: the smallest number of contributors behind at least 80% of recent code-change churn. | The evidence sample, ownership context, and related work. |

## Interpret without ranking

These measures are **not a ranking**. They do not compare people or set individual
targets. A higher or lower value can describe the mix of work, waiting, incidents, or
source coverage in one context; it does not explain why the pattern exists.

For example, rising WIP with flat throughput can suggest a question about blocked work or
scope. A higher after-hours ratio can suggest a team conversation about delivery pressure.
Neither observation names a person or assigns a cause.

## Missing values are not zero

An unavailable, blank, or **null** value means there is not enough usable information for
that measure in the selected context. It **does not mean zero**. Check the date range,
scope, connection coverage, and nearby evidence before widening the question.

## Caveats

Source coverage and work types vary across teams and repositories. Compare like with like,
read trends over snapshots, and avoid treating a ratio or concentration signal as a quality
label. Bus factor is a resilience question about the evidence sample, not a request to rank
or evaluate contributors.

## Next step

- [Review reports](reports.md) when a repeatable narrative would help the conversation.
- [How to read Dev Health](how-to-read-dev-health.md) explains the shared evidence model.
- [Glossary](glossary.md) defines the terms used across the manual.
