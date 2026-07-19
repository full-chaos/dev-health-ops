---
page_id: con-contracts
summary: Rules for canonical provider, identity, schema, API, metric, taxonomy, feature, and documentation contracts.
content_type: architecture
owner: engineering
source_of_truth:
  - docs/contributing/platform-contract.md
  - current ADRs and canonical registries
applicability: current
lifecycle: active
---

# Stable contracts and source-of-truth rules

A stable contract has one authoritative source and explicit compatibility behavior.

- Provider adapters normalize into canonical models; they do not redefine the product model.
- Public schemas and GraphQL fields are generated from code and versioned when incompatible.
- Canonical IDs include tenant and provider/source boundaries where collision is possible.
- Metrics and taxonomies have one registry or computation source.
- Feature availability comes from the current feature and entitlement source.
- Documentation IA owns one canonical public page and URL per reader outcome.

ADRs can explain a durable decision, but implementation history and rollout evidence stay internal unless they help a supported reader operate the current system.
