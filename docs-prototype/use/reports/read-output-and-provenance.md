---
page_id: use-report-read
summary: Read rendered report output with its definition, run status, trigger, and source context.
content_type: task-guide
owner: product-analytics
source_of_truth:
  - current SavedReport and ReportRun GraphQL contracts
applicability: current
lifecycle: active
---

# Read output and provenance

A report detail page can show rendered Markdown from the latest successful run and a history of prior runs.

1. Confirm which saved definition produced the output.
2. Check the run status, trigger type, duration, and completion time.
3. Review the scope, date range, metrics, and plan stored with the report.
4. Distinguish generated narrative from measured or derived source values.
5. Treat missing or failed output as a run state, not a measured zero.

Do not share customer-sensitive output outside its approved audience. A future artifact URL field does not guarantee that downloadable output is currently supported.
