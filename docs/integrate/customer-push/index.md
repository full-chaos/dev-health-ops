---
page_id: int-push
summary: Push normalized records from a customer-controlled pipeline over the versioned external-ingest REST API.
content_type: landing
owner: platform-api
source_of_truth:
  - current external-ingest API, schemas, worker, and status implementation
applicability: current
lifecycle: active
---

# Send data with Customer Push

Customer Push is for customer-controlled CI, ETL, or internal integrations that submit normalized records to `/api/v1/external-ingest/*`.

Use it when native managed synchronization is unsuitable—for example, the source is inaccessible to Full Chaos, unsupported by a native connector, or already exported through a customer-owned pipeline.

- [Understand the boundary](overview.md)
- [Register a source and credential](register-source.md)
- [Discover the supported schema](schema-discovery.md)
- [Submit records](submit-records.md)
- [Use idempotency and retries](idempotency-and-retries.md)
- [Handle validation and delivery errors](errors.md)
- [Observe source delivery](observability.md)

One active ingestion owner is allowed for a `(system, instance)` pair. A managed connector and Customer Push must not write the same source concurrently.
