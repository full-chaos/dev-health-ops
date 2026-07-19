---
page_id: use-work-graph
summary: Understand the Work Graph Explorer, choose a relationship slice, and follow supported links among work and source artifacts.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - current /diagnose/work-graph product surface
  - current Work Graph GraphQL and relationship contracts
  - docs/user-guide/views/work-graph.md
applicability: current
lifecycle: active
---

# Work Graph

Work Graph is the evidence-navigation surface for questions that require relationships rather than a summary value. It connects supported entities such as issues, pull requests, commits, files, repositories, WorkUnits, and Investment context so you can inspect how the available records relate.
{: .fc-page-lede }

The graph supports navigation and evidence review. Proximity does not prove ownership, intent, responsibility, or causation, and a missing edge can mean the source or current synchronization does not provide that relationship.

## When to use this page

Use Work Graph when:

- an Investment theme or subcategory needs supporting-work context;
- a Code Hotspot needs links to the work that changed the area;
- a PR Flow pattern needs issue, pull-request, commit, or dependency context;
- you already have one artifact and need to follow a supported relationship to another.

Use a summary view first when the question is still “where is the pattern?” Work Graph is most useful after you have a result or artifact to investigate.

## What the page contains

The current **Work Graph Explorer** exposes the relationships available for the selected workspace and connected sources. Read these parts together:

1. **Entry context** identifies the originating entity, category, or workflow when you arrived from another page.
2. **Connection type** selects the relationship slice to inspect.
3. **Theme and subcategory scope**, where available, narrows Investment-related evidence.
4. **Nodes** represent entities defined by the current graph contract.
5. **Edges** represent supported relationship types, not generic visual similarity.
6. **Details and source links** provide identifiers, dates, labels, or a route to the original artifact where supported.

The product may add richer relationship exploration over time. Treat only nodes, edges, filters, and actions visible in the current workspace as available.

## Follow a relationship

1. Preserve the scope, period, and filters from the workflow that brought you to the graph.
2. Identify the starting node and the question you are trying to answer.
3. Select the smallest relevant connection type rather than expanding every relationship.
4. Read the node and edge labels exactly as the current contract defines them.
5. Open the linked source artifact or details panel where available.
6. Compare identifiers and dates to confirm the relationship applies to the selected work.
7. Return to the originating workflow with the evidence, not just the graph shape.

In an Investment investigation, a common path is:

```text
Theme → Subcategory → WorkUnit → issue / pull request / commit / incident
```

The WorkUnit is the evidence container. It is not an extra Investment category.

## Worked example

Suppose an Investment subcategory links to a WorkUnit, which links to an issue, a pull request, several commits, and the files changed by those commits. The graph establishes that the platform has a supported relationship path among those records.

A useful review then opens the issue and pull request, checks their dates and scope, and reads the actual change. The graph alone does not prove that the issue caused every commit, that the files share one owner, or that the relationship explains the entire Investment result.

## Understand missing and sparse relationships

A missing edge can mean:

- the provider does not expose the relationship;
- the relevant source has not synchronized or is stale;
- identifiers did not reconcile across sources;
- the selected scope or period excludes one side of the relationship;
- the relationship type is not supported by the current graph contract;
- the underlying records genuinely have no known link.

Check coverage and the source artifacts before treating an absent relationship as evidence that no relationship exists.

## Empty and error states

| State | Meaning | Next step |
| --- | --- | --- |
| Empty graph | No supported nodes or edges are available for the selected context | Confirm the starting entity, scope, period, and source coverage. |
| Sparse graph | Only part of the expected relationship chain is available | Inspect the available source artifacts and check synchronization. |
| Unresolved identity | Records exist but cannot be reconciled into the expected entity | Check provider identifiers and the canonical data model. |
| Unavailable connection type | The current product or source does not support that relationship | Use a supported slice or consult the reference model. |

Use [Diagnose no or incomplete data](../troubleshooting/no-or-incomplete-data.md) when the graph is empty, stale, or unexpectedly sparse.

## What not to conclude

- Graph proximity is not causation.
- A missing edge is not proof that two records are unrelated.
- A relationship path is not a person-level ownership or performance statement.
- The graph does not replace reading the linked issue, pull request, commit, file, or incident.

## Continue

- [Read the Work Graph data model](../../reference/data-models/work-graph.md)
- [Read Code Hotspots](code-hotspots.md)
- [Follow investment evidence](../investment/follow-evidence.md)
