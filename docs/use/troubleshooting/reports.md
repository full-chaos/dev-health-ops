---
page_id: use-report-problems
summary: Diagnose report creation, triggering, run-history, and rendered-output problems.
content_type: troubleshooting
owner: product-analytics
applicability: current
lifecycle: active
---

# Report generation problems

## Report cannot be saved

Check required form fields, scope availability, and permission. Retain the validation message.

## Run remains queued or running

Check whether status and duration are advancing. Avoid triggering duplicate runs until the current run state is understood.

## Run failed

Record the report ID, run ID, trigger type, time, and sanitized error. Preserve the report definition.

## Run succeeded but output is missing

Confirm you opened the latest successful run and that rendered output exists. A successful status without usable content requires escalation; it is not a zero result.
