---
page_id: use-work-graph
summary: Follow supported relationships among work units, issues, pull requests, commits, repositories, and evidence.
content_type: workflow-guide
owner: product-analytics
source_of_truth:
  - current /diagnose/work-graph product surface
  - current Work Graph GraphQL and relationship contracts
applicability: current
lifecycle: active
---

# Follow relationships in Work Graph

Work Graph is the evidence destination for questions that require relationships rather than a summary value.

1. Open `/diagnose/work-graph` directly or follow a contextual action from a supported workflow.
2. Preserve the originating scope, time window, and category filters.
3. Read node and edge labels from the current graph contract.
4. Expand only the relationship needed for the question.
5. Open the source artifact where available.
6. Treat a missing edge as missing or unsupported evidence until coverage is checked.

Do not infer ownership, intent, or causation from graph proximity alone.
