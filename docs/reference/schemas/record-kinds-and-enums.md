---
page_id: ref-record-kinds
summary: Customer Push `external-ingest.v1` record kinds, status enums, and source-system compatibility.
content_type: generated-reference
owner: platform-api
source_of_truth:
  - current external-ingest Pydantic models
  - server-shipped JSON Schema and examples
applicability: current
lifecycle: active
---

# Record kinds and enums

The current Customer Push schema version is `external-ingest.v1`.

## Record kinds

- `repository.v1`
- `identity.v1`
- `team.v1`
- `work_item.v1`
- `work_item_transition.v1`
- `work_item_dependency.v1`
- `pull_request.v1`
- `review.v1`
- `commit.v1`

The server-generated schema owns required fields, optional fields, enums, and examples. Unknown fields are rejected because the versioned models forbid extras.

Current normalized work-item statuses are `backlog`, `todo`, `in_progress`, `in_review`, `blocked`, `done`, `canceled`, and `unknown`. Pull-request and review states follow their schema enums.

Not every kind is supported by every source system. Use the schema and current kind/system matrix before submitting.
