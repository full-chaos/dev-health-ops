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

A `team.v1` record's `id` is stored with the source system's prefix: `gh:` for `github`, `gl:` for `gitlab`, and `jira:` for `jira` and `atlassian` (a pushed Atlassian team is the native Atlassian team, and `atlassian:<id>` folds into `jira:<id>`), and `<system>:` for every other system (for example `linear:ENG`). An `id` that already carries any known provider key (`gh:`, `gl:`, `linear:`, `jira:`, `pagerduty:`, `custom:`, `ms-teams:`) is stored as sent, whatever the system. A `team.v1` `parentTeamId` and the `teamIds` of an `identity.v1` record get the same prefix. A `team.v1` record without `nativeTeamKey` stores its `id` without the system prefix in `native_team_key` (NULL when the `id` holds another provider's key). An `id` that is only a known prefix is refused.

Not every kind is supported by every source system. Use the schema and current kind/system matrix before submitting.
