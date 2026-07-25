---
page_id: ref-metric-defs
summary: Contract for generated metric definitions and the source fields every entry must expose.
content_type: generated-reference
owner: product-analytics
source_of_truth:
  - current metrics schema and computation code
applicability: current
lifecycle: active
---

# Canonical metric definitions

Generate one entry per currently supported metric with:

- stable key and display label;
- question answered;
- unit and value domain;
- included population and exclusions;
- event time and window semantics;
- exact formula and aggregation;
- allowed scope and filters;
- measured-zero, null, unavailable, partial, and stale behavior;
- source tables, fields, and computation code;
- version or applicability;
- interpretation limits.

Do not publish a metric from an old planning document when no current computation or product surface supports it.
