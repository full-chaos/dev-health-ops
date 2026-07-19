---
page_id: gs-prereq
summary: Confirm access, source data, scope, and processing state before diagnosing a product workflow.
content_type: task-guide
owner: documentation
source_of_truth:
  - current workspace access and synchronization behavior
applicability: current
lifecycle: provisional
---

# Check product access and prerequisites

Before changing a filter or interpreting an empty result, confirm that the product can answer the question for the selected context.

## Minimum checks

1. **Access:** you can sign in to the intended workspace and open the relevant product area.
2. **Source connection:** the workspace has at least one supported source connected for the workflow.
3. **Scope:** the selected repository or team is included in the connected source and workspace configuration.
4. **Time window:** the selected period overlaps collected and processed data.
5. **Processing:** synchronization or ingestion has completed far enough to populate the workflow.
6. **Availability:** the feature is enabled for the workspace and your role can open it.

## Do not convert missing data into a conclusion

A blank page can mean no matching work, missing scope, incomplete synchronization, delayed processing, unavailable support, or a real measured zero. These states require different actions.

Use [Diagnose no or incomplete data](../use/troubleshooting/no-or-incomplete-data.md) to distinguish them. A workspace administrator can use [Check synchronization status and freshness](../admin/sync-and-coverage/status-and-freshness.md) when the problem is outside the ordinary product workflow.
