---
page_id: con-platform
summary: Current service and repository responsibilities from product route through API, workers, stores, and external providers.
content_type: architecture
owner: engineering
source_of_truth:
  - docs/architecture.md
  - current deployment and service entry points
applicability: current
lifecycle: active
---

# Platform architecture

A typical request or data path spans:

```text
web route → API/GraphQL → service/query layer → Postgres or ClickHouse
provider/source → sync planner or external-ingest API → queue/worker → normalized sinks → metrics/product
```

Primary boundaries:

- web owns current route, interaction, and rendering behavior;
- API owns public request, authentication, authorization, and schema contracts;
- services own business orchestration;
- queries and compilers own bounded storage access;
- workers own asynchronous processing, retries, and schedules;
- providers/connectors own source-specific acquisition;
- canonical models and normalization own cross-provider meaning;
- deployment and operations own runtime configuration and recovery.

A frontend label is not a backend enum unless the contract explicitly maps it.
