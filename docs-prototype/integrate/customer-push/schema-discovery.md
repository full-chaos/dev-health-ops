---
page_id: int-schema
summary: Fetch the server-generated JSON Schema and examples for the supported external-ingest version.
content_type: task-guide
owner: platform-api
source_of_truth:
  - current external-ingest Pydantic models and schema routes
applicability: current
lifecycle: active
---

# Discover the supported schema

1. Query the schema index or `GET /api/v1/external-ingest/schemas/external-ingest.v1` with a token that has schema access.
2. Cache the schema with the integration build artifact, not indefinitely.
3. Validate generated batches against the returned schema in CI.
4. Use the server-shipped examples for each record kind as the payload baseline.
5. Fail the integration when the server reports an unsupported version or unknown record kind.

The current wire version is `external-ingest.v1`. The schema is generated from server models; do not maintain a competing hand-written contract.
