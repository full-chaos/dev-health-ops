# Customer Push Ingestion: Schemas & Idempotency

## Schema versioning

The wire contract is versioned as a whole: `schemaVersion: "external-ingest.v1"`. There is
currently exactly one supported version. A batch or validate request with any other
`schemaVersion` value is rejected with `400 unsupported_schema_version` (or `404` from
`GET /schemas/{version}` if you ask for an unknown version by URL).

`GET /api/v1/external-ingest/schemas/{schema_version}` returns the full JSON Schema for this
version — generated directly from the server's Pydantic models (never hand-written), so it can
never drift from what the server actually accepts. Validate your integration against it in CI.

## The batch envelope

```json
{
  "schemaVersion": "external-ingest.v1",
  "idempotencyKey": "acme-github-prs-2026-06-26T00:00:00Z",
  "source": {
    "type": "customer_push",
    "system": "github",
    "instance": "acme/api",
    "producer": "dev-hops-cli",
    "producerVersion": "0.12.0"
  },
  "window": {
    "startedAt": "2026-06-25T00:00:00Z",
    "endedAt": "2026-06-26T00:00:00Z"
  },
  "records": [
    {"kind": "pull_request.v1", "externalId": "acme/api#482", "payload": { "...": "..." }}
  ]
}
```

| Field | Notes |
|---|---|
| `schemaVersion` | Must be `external-ingest.v1`. |
| `idempotencyKey` | 1-255 chars. Unique **forever** per `(org, source.system, source.instance, idempotencyKey)` — no TTL. See [Idempotency](#idempotency). |
| `source.type` | Always `customer_push`. |
| `source.system` | One of `github`, `gitlab`, `jira`, `linear`, `custom`. |
| `source.instance` | The provider instance identifier — for git systems this must equal each record's `repositoryExternalId`/`externalId` (provider full name, e.g. `owner/repo`, not a URL). Matching against your token's bound source and the source-registration record is **case-insensitive**. |
| `source.producer` / `source.producerVersion` | Optional, free-text — identifies the client (e.g. `dev-hops-cli` / `0.12.0`). |
| `window` | Optional. `startedAt`/`endedAt` — `endedAt` must be `>= startedAt`. |
| `records` | 1-1000 entries (see [Batch limits](#batch-limits)). |

### The record envelope

Every entry in `records[]` is a generic wrapper:

```json
{"kind": "pull_request.v1", "externalId": "acme/api#482", "payload": { }}
```

| Field | Notes |
|---|---|
| `kind` | A versioned record kind, e.g. `pull_request.v1` — see the 9 kinds below. An unknown `kind` fails the *whole batch* with `400 unknown_record_kind` at `POST /batches` (no partial acceptance); `POST /validate` instead reports it as a per-record `unknown_kind` error and continues checking the rest. |
| `externalId` | 1-512 chars. A correlation id used purely for error reporting/diagnostics — it is **not** validated against the payload's own natural key. |
| `payload` | The kind-specific record body — validated per-kind (see below). |

## The 9 record kinds

Each `payload` is validated against a versioned Pydantic model with `extra="forbid"` — this is
a customer-facing, versioned contract, so an unrecognized field is a loud validation error, not
silently dropped data. Field names below are the wire (camelCase) names.

| Kind | Required fields | Notable optional fields |
|---|---|---|
| `repository.v1` | `externalId`, `sourceSystem` | `defaultRef`, `tags[]`, `settings{}` |
| `identity.v1` | `canonicalId`, `updatedAt` | `displayName`, `email`, `providerIdentities{}`, `teamIds[]`, `isActive` |
| `team.v1` | `id`, `name`, `updatedAt` | `description`, `members[]`, `projectKeys[]`, `repoPatterns[]`, `nativeTeamKey`, `parentTeamId` |
| `work_item.v1` | `externalKey`, `provider`, `title`, `status`, `createdAt` | `type`, `statusRaw`, `repositoryExternalId`, `projectKey`, `assignees[]`, `labels[]`, `storyPoints`, `sprintId`, `parentId`, `epicId`, `url` |
| `work_item_transition.v1` | `externalKey`, `provider`, `occurredAt`, `fromStatus`, `toStatus` | `workItemType`, `fromStatusRaw`, `toStatusRaw`, `actor` |
| `work_item_dependency.v1` | `sourceExternalKey`, `targetExternalKey`, `relationshipType` | `sourceWorkItemType`, `targetWorkItemType`, `relationshipTypeRaw` |
| `pull_request.v1` | `repositoryExternalId`, `number`, `state`, `createdAt` | `title`, `body`, `authorName`, `authorEmail`, `mergedAt`, `closedAt`, `headBranch`, `baseBranch`, `additions`, `deletions`, `changedFiles`, `reviewsCount`, `commentsCount`, `url` |
| `review.v1` | `repositoryExternalId`, `pullRequestNumber`, `reviewId`, `reviewer`, `state`, `submittedAt` | — |
| `commit.v1` | `repositoryExternalId`, `hash`, `authorWhen` | `message`, `authorName`, `authorEmail`, `committerName`, `committerEmail`, `committerWhen`, `parents` |

`status` (`work_item.v1`) and `fromStatus`/`toStatus` (`work_item_transition.v1`) are one of:
`backlog | todo | in_progress | in_review | blocked | done | canceled | unknown`.
`review.v1`'s `state` is one of `APPROVED | CHANGES_REQUESTED | COMMENTED | DISMISSED |
PENDING`. `pull_request.v1`'s `state` is one of `open | closed | merged`.

### Canonical example payloads

These are the exact, server-shipped example payloads (`src/dev_health_ops/api/external_ingest/examples/*.json`) — the same files served in each record kind's `examples[]` in `GET /schemas/{version}`, and what `dev-hops push sample --kind <kind>` prints. They are the source of truth for valid payload shape.

**`repository.v1`**

```json
--8<-- "repository.v1.json"
```

**`identity.v1`**

```json
--8<-- "identity.v1.json"
```

**`team.v1`**

```json
--8<-- "team.v1.json"
```

**`work_item.v1`**

```json
--8<-- "work_item.v1.json"
```

**`work_item_transition.v1`**

```json
--8<-- "work_item_transition.v1.json"
```

**`work_item_dependency.v1`**

```json
--8<-- "work_item_dependency.v1.json"
```

**`pull_request.v1`**

```json
--8<-- "pull_request.v1.json"
```

**`review.v1`**

```json
--8<-- "review.v1.json"
```

**`commit.v1`**

```json
--8<-- "commit.v1.json"
```

## Idempotency

Every batch is identified by `(org_id, source.system, source.instance, idempotencyKey)` —
unique **forever** (no TTL, unlike the legacy `/api/v1/ingest` router's 24h cache). Resubmitting
the same key is always safe. The payload hash used for comparison is a SHA-256 digest of the
canonicalized (sorted keys, normalized timestamps), schema-validated envelope — so resending
byte-identical JSON with different field order or `Z` vs `+00:00` timestamps still hashes the
same.

| Outcome | When | What happens |
|---|---|---|
| **New** | No row exists yet for this key. | A new batch is accepted (`202`) and enqueued normally. |
| **Replay** | Same key, same payload hash, and the batch isn't in a retryable state. | Returns **`200`** with the batch's *current* full status envelope (same shape as `GET /batches/{id}`) — nothing is re-enqueued. |
| **Conflict** | Same key, but a **different** payload hash. | `409 idempotency_conflict`. The original batch is never overwritten — use a new `idempotencyKey`, or resend the exact original payload to get the replayed status. |
| **Retry** | Same key, same hash, and the existing batch is in a recoverable state: `stream_unavailable`, `failed`, or `accepted` for longer than `EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES` (default 15 minutes — covers a crash between the Postgres commit and the stream enqueue). | The **same** `ingestion_id` is re-accepted (`attempts` incremented, prior outcome cleared) and re-enqueued. |

A concurrent request racing for the same idempotency key (two requests landing before either
commits) gets `503 ingest_temporarily_unavailable` — retry with the same key; it resolves to
replay/retry on the next attempt.

## Batch limits

| Limit | Default | Override |
|---|---|---|
| Max records per batch | 1000 | `EXTERNAL_INGEST_MAX_RECORDS` |
| Max request body size | 10,000,000 bytes (10MB) | `EXTERNAL_INGEST_MAX_BODY_BYTES` |

Both are also reported live in `GET /schemas` / `GET /schemas/{version}`'s `limits` block, and
`dev-hops push batch` fetches them before submitting (unless `--skip-limits-check` is passed) so
the CLI enforces the server's actual current limits rather than hardcoded defaults. Exceeding
either limit is `400 batch_too_large` (record count) or `413 payload_too_large` (body size).

See [Troubleshooting](troubleshooting.md) for how these show up as HTTP errors and how to
remediate, and [Setup Guide](setup-guide.md) for a first end-to-end walkthrough.
