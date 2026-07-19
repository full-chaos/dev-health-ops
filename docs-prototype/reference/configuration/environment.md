---
page_id: ref-config-env
summary: Rules and source categories for the generated environment-variable reference.
content_type: configuration-reference
owner: platform-operations
source_of_truth:
  - current settings modules
  - deploy/ manifests and compose files
applicability: current
lifecycle: active
---

# Environment variables

Generate entries for these groups from source:

- application and environment mode;
- Postgres, ClickHouse, Redis or queue stores;
- authentication, signing, sessions, and trusted proxies;
- provider, GitHub App, GitLab, Jira, Linear, and webhook credentials;
- worker routing, concurrency, leases, retries, and schedules;
- synchronization windows and provider budgets;
- GraphQL cost and timeout controls;
- model, email, billing, telemetry, and external services;
- observability and logging.

Secret values never belong in generated output. A deployment example is not the authority for a default unless runtime source agrees.
