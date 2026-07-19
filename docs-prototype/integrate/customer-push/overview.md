---
page_id: int-push-overview
summary: Decide whether Customer Push or a managed connector owns a source.
content_type: concept
owner: platform-api
source_of_truth:
  - docs/customer-push-ingestion/overview.md
applicability: current
lifecycle: active
---

# Supported record families and prerequisites

Customer Push accepts normalized records over REST and makes accepted data available to the same downstream product and analytics surfaces as managed connector data.

## Boundary

- Customer Push endpoints: `/api/v1/external-ingest/*`
- Authentication: dedicated bearer token with an `fcpush_` prefix
- Querying accepted data: ordinary product and GraphQL surfaces
- Provider webhooks: separate `/api/v1/webhooks/*` managed-sync path

## Lifecycle

```text
optional validate → durable batch acceptance → stream → worker → normalized sinks
                                                    └→ bounded recompute → status
```

`POST /batches` returns after durable acceptance, not after processing. Poll the batch status until it reaches a terminal state.

Use a native connector when the source is reachable and managed synchronization is desired. Use Customer Push when the customer owns export timing or the source is otherwise unavailable to managed synchronization.
