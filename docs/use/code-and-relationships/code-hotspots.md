---
page_id: use-hotspots
summary: Understand the Code Hotspots page, the change and complexity signals it combines, and how to inspect a concentrated area safely.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - current Code and Complexity product surfaces
  - docs/user-guide/views/code-hotspots.md
applicability: current
lifecycle: active
---

# Code Hotspots

Code Hotspots highlights files, paths, or modules where **change concentration** and **complexity** deserve inspection in the selected repository scope and period. It turns a broad durability question into a path toward source and related work. It does not label code as defective or assign blame to the people who changed it.
{: .fc-page-lede }

## When to use this page

Use Code Hotspots when another signal points toward a code area, or when you need to ask where repeated change and structural difficulty overlap. Typical starting questions include:

- Which areas have changed repeatedly across several windows?
- Where is complexity concentrated alongside active change?
- Is a delivery or maintenance pattern associated with a particular path?
- Which source areas should be inspected before planning refactoring or test work?

Do not use the page to compare contributors or infer that a frequently changed area is unhealthy by definition.

## What the page shows

The exact layout depends on the current Code and Complexity surface, but read these elements together:

1. **Repository scope and period** define the source population.
2. **Visualization form** can present the hierarchy as a heatmap, tree, treemap, or related code-area view.
3. **Path hierarchy** organizes repositories, directories, modules, and files where the source supports them.
4. **Selected metric and unit** explain whether size, intensity, color, or another channel represents churn, complexity, or a combined risk signal.
5. **Details and related-work actions** provide the source and evidence path for a selected area.

Read the legend and units before comparing nodes. A large node can represent file size, changed lines, activity, or another current measure; do not assume the visual encoding.

## Interpret a hotspot

A concentrated area can have several legitimate explanations:

- active feature delivery in a central component;
- a planned migration or refactor;
- repeated repair around an unstable boundary;
- generated, vendored, migration, fixture, or test output;
- a large file whose size dominates the selected measure;
- incomplete repository or branch coverage.

Look for persistence across equivalent windows. A one-period burst can be important, but it is not yet a durable hotspot pattern.

## Worked example

Suppose a path shows repeated churn across several periods while its complexity measure also rises. A supported reading is:

> This area has experienced sustained change and increasing structural complexity in the selected repository and period.

The page does not tell you whether that is planned evolution or accumulating design cost. Open the source and related pull requests, issues, commits, tests, and dependencies. The response might be to continue a planned migration, improve tests around a boundary, split a module, or take no action after the context is understood.

## Check common distortions

Before recommending work, check whether the result is dominated by:

- generated or vendored files;
- dependency lockfiles;
- database or schema migrations;
- snapshots and fixtures;
- large test data;
- file moves or mechanical formatting;
- a branch, repository, or period that differs from the intended question.

Where the product exposes exclusions or filters, keep them visible in the interpretation.

## Follow the evidence

Open a heatmap cell, tree branch, or selected node when an evidence action is available. Inspect:

- the exact path and metric values;
- the periods in which the pattern appears;
- linked pull requests, commits, and issues;
- affected tests and dependencies;
- the business or operational work that motivated the changes.

Use [Work Graph](work-graph.md) when the question becomes how the selected code area relates to work artifacts or other entities.

## Empty and partial states

| State | Meaning | Next step |
| --- | --- | --- |
| Empty view | No usable code activity or complexity rows are available for the context | Confirm repository, branch, period, and source coverage. |
| Partial hierarchy | Some paths or repositories are not represented | Avoid whole-system conclusions until coverage is understood. |
| Stale result | Recent expected changes have not arrived | Check synchronization freshness. |
| Unsupported metric | The current source or workspace does not provide the selected measure | Use an available measure or check prerequisites. |

Use [Diagnose no or incomplete data](../troubleshooting/no-or-incomplete-data.md) when the view is empty or inconsistent with the selected context.

## What not to conclude

- A hotspot is not proof that the code is bad.
- High churn is not automatically rework.
- Complexity without the surrounding work does not determine a remediation.
- A code-area pattern is not an assessment of its authors.

## Continue

- [Follow relationships in Work Graph](work-graph.md)
- [Read Quadrants](../delivery-flow/quadrants.md)
- [Compare trends responsibly](../plan-and-improve/compare-trends.md)
