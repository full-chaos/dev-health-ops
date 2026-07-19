---
page_id: con-fixtures
summary: Use deterministic isolated fixtures for broad coverage and live-like validation for external behavior that fixtures cannot prove.
content_type: task-guide
owner: engineering
applicability: current
lifecycle: active
---

# Fixtures and production-like validation

Use synthetic fixtures in a dedicated organization. The fixture generator refuses to mix with recognized live connector data unless an explicit override is supplied; preserve that safeguard.

A fixture can prove shape, state transition, and deterministic calculation. It cannot prove current provider permission, rate, callback, network, or hosted-service behavior.

For live-like validation:

1. use a dedicated test organization and minimum-permission source;
2. bound scope and time;
3. redact credentials and customer data;
4. retain identifiers and status evidence;
5. clean up or revoke test resources;
6. never write to production without an approved test plan.
