---
page_id: admin-privacy
summary: Review product telemetry and data-handling choices at the workspace boundary.
content_type: concept
owner: platform-product
source_of_truth:
  - docs/user-guide/product-telemetry.md
  - current telemetry and privacy controls
applicability: current
lifecycle: active
---

# Privacy and data-handling responsibilities

Before enabling telemetry, model-assisted features, or a new data source, identify:

- data categories collected or transmitted;
- workspace and provider boundaries;
- retention and deletion behavior;
- access roles and intended audience;
- external processors or model providers;
- whether prompt, source, or customer-sensitive content is included;
- the supported control for disabling or changing the behavior.

Document the current product behavior, not an aspirational policy. Escalate deployment-level encryption, logging, backup, and incident questions to platform operations and security.

## Ask Dev data boundary

Ask Dev stores questions and validated `dev_answer.v1` artifacts only when the
selected retention is 30 days. The alternative is exactly zero days: content
is removed after the request completes. The product does not offer 7-day or
90-day choices.

Safe audit rows may include run state, durations, token and cost counts,
version identifiers, input/result digests, result counts, and opaque evidence
references. They must not contain prompts, provider request or response
payloads, chain-of-thought, raw source evidence, query results, or credentials.
Conversation content is not used for model training.

Users may delete their own conversations. An organization administrator may
purge a target user's Ask Dev content, and user or organization deletion
cascades through all conversation content. Cleanup leaves only a minimal
tombstone containing tenant/user identifiers, the selected retention, reason,
and lifecycle timestamps; it contains no question, answer, feedback, or tool
content.
