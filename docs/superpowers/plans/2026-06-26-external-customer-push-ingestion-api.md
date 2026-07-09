# External Customer Push Ingestion API

## Decision

Use REST for customer data ingestion and operational batch/status flows. Keep GraphQL for read/query/exploration surfaces.

Rationale:

- The FastAPI app already exposes OpenAPI at `/openapi.json` and mounts GraphQL separately under `/graphql`.
- Customer ingestion is command-style, side-effecting, idempotent, rate-limited, async, and operationally observable. REST gives better fit for HTTP status codes, idempotency keys, streaming/backpressure boundaries, payload size limits, and generated customer SDKs.
- GraphQL should remain a query layer for analytics exploration, not a mutation layer for customer-owned raw data. Ingestion mutations through GraphQL would blur operational boundaries and make retry/idempotency semantics harder to reason about.

## Product goal

Allow customers to push developer-health source facts into the platform instead of requiring FullChaos to sync every provider directly. The API should support customers that cannot grant long-lived provider credentials, need data-residency control, or already have internal ETL that can emit normalized delivery data.

The pushed data must feed the same product model as native syncs:

- Delivery and velocity
- Durability and risk
- Collaboration and team dynamics
- Developer health / load signals
- Systemic process health

## Architecture

Do not create a parallel analytics path.

```text
Customer API / dev-hops push CLI
→ external ingest schemas
→ async ingest stream
→ external ingest worker
→ provider-neutral internal models
→ existing sinks
→ existing metrics jobs
→ existing REST/GraphQL/web UX
```

This preserves the existing pipeline boundary:

```text
Connectors → Processors → Sinks → Metrics → Visualization
```

For customer push, the external ingest API becomes a connector-equivalent boundary. It accepts customer-owned source facts, validates them, and hands them to processors. It must not write final metric tables directly.

## API surface

Base path: `/api/v1/external-ingest`

### `POST /batches`

Accept a batch for async processing.

Request envelope:

```json
{
  "schemaVersion": "external-ingest.v1",
  "idempotencyKey": "acme-github-prs-2026-06-26T00:00:00Z",
  "source": {
    "type": "customer_push",
    "system": "github",
    "instance": "github.com/acme",
    "producer": "dev-hops-cli",
    "producerVersion": "0.12.0"
  },
  "window": {
    "startedAt": "2026-06-25T00:00:00Z",
    "endedAt": "2026-06-26T00:00:00Z"
  },
  "records": []
}
```

Response:

```json
{
  "ingestionId": "uuid",
  "status": "accepted",
  "itemsReceived": 500,
  "stream": "external-ingest:<org_id>:events"
}
```

Behavior:

- `202 Accepted` for valid accepted batches.
- `400` for malformed envelopes or unsupported schema versions.
- `401` for missing/invalid token.
- `403` for disabled source, wrong org, or insufficient scope.
- `409` for conflicting idempotency key reuse.
- `413` for too-large payloads.
- `429` for rate limit.
- `503` if the ingest stream is unavailable. Do not silently accept customer data when the durable ingest path is unavailable.

### `POST /validate`

Validate a batch without enqueueing it.

Response:

```json
{
  "valid": false,
  "itemsAccepted": 487,
  "itemsRejected": 13,
  "errors": [
    {
      "index": 12,
      "kind": "pull_request",
      "code": "missing_external_id",
      "message": "externalId is required"
    }
  ]
}
```

### `GET /batches/{ingestion_id}`

Return operational status and rejected-record diagnostics.

Response:

```json
{
  "ingestionId": "uuid",
  "status": "processing",
  "itemsReceived": 500,
  "itemsAccepted": 492,
  "itemsRejected": 8,
  "source": {
    "system": "github",
    "instance": "github.com/acme"
  },
  "window": {
    "startedAt": "2026-06-25T00:00:00Z",
    "endedAt": "2026-06-26T00:00:00Z"
  },
  "errors": []
}
```

### `GET /schemas`

List supported schema versions and record kinds.

### `GET /schemas/{schema_version}`

Return JSON Schema for SDK/customer validation.

## Record kinds for v1

Start narrow. These are enough to power the core developer-health surfaces without jumping into IDE telemetry or custom wellness events.

- `repository.v1`
- `identity.v1`
- `team.v1`
- `work_item.v1`
- `work_item_transition.v1`
- `work_item_dependency.v1`
- `pull_request.v1`
- `review.v1`
- `commit.v1`

Defer:

- `deployment.v1`
- `incident.v1`
- `test_run.v1`
- `security_finding.v1`
- IDE/editor activity

## Internal modules

```text
src/dev_health_ops/api/external_ingest/
  __init__.py
  router.py
  schemas.py
  streams.py
  status.py
  auth.py
  errors.py

src/dev_health_ops/external_ingest/
  normalize.py
  validate.py
  processor.py
  idempotency.py
  mappings.py
```

Use direct SQL for API persistence/status queries where database access is needed. Keep SQL authoritative and avoid adding ORM-only paths.

## Storage and processing

### Status store

Persist ingestion status with enough detail for customer support and CLI polling:

- `org_id`
- `ingestion_id`
- `idempotency_key`
- `source_system`
- `source_instance`
- `schema_version`
- `window_started_at`
- `window_ended_at`
- `status`
- `items_received`
- `items_accepted`
- `items_rejected`
- `created_at`
- `updated_at`
- `completed_at`
- `error_summary`

### Error store

Store bounded rejected-record diagnostics:

- `org_id`
- `ingestion_id`
- `record_index`
- `record_kind`
- `external_id`
- `code`
- `message`
- `path`

### Stream

Use Redis Streams / Valkey following the product telemetry ingestion precedent, but stricter:

- Product telemetry can accept-and-warn in local/dev.
- Customer push ingestion must fail with `503` when durable enqueue is unavailable.

### Worker

The worker should:

1. Read the accepted batch from the stream.
2. Run full validation.
3. Normalize records into provider-neutral internal dataclasses.
4. Stamp `org_id` on every record.
5. Write raw facts through existing sink methods.
6. Update ingest status and rejected-record errors.
7. Enqueue bounded metric recomputation for affected org/source/repo/team/window.

## Idempotency

Batch identity:

```text
org_id + source_system + source_instance + idempotency_key
```

Record identity:

```text
org_id + source_system + source_instance + record_kind + external_id + updated_at/hash
```

Rules:

- Same idempotency key and same payload hash returns the existing ingestion status.
- Same idempotency key and different payload hash returns `409`.
- Reprocessing must be safe.
- Final sink writes must preserve current append/versioning semantics and avoid duplicate current-state rows.

## Source registration and token scopes

Register customer-push sources per org:

```json
{
  "sourceId": "uuid",
  "orgId": "uuid",
  "system": "github",
  "instance": "github.com/acme",
  "mode": "customer_push",
  "enabled": true
}
```

Token scopes:

- `schema:read`
- `ingest:write`
- `ingest:status`

Optional provider-specific scopes later:

- `ingest:github`
- `ingest:gitlab`
- `ingest:jira`
- `ingest:linear`

## Conflict policy

For v1, a source instance has exactly one active ingestion mode:

- `fullchaos_sync`
- `customer_push`
- `disabled`

Do not allow FullChaos sync and customer push to both own the same `source_system + source_instance` at the same time. Mixed-mode will create untrustworthy metrics.

## dev-hops CLI

Add command group:

```bash
dev-hops push
```

Commands:

```bash
dev-hops push validate payload.json --schema external-ingest.v1

dev-hops push batch payload.json \
  --api-url https://api.fullchaos.dev \
  --token $FULLCHAOS_API_TOKEN \
  --org $ORG_ID

cat payload.json | dev-hops push batch - \
  --api-url https://api.fullchaos.dev \
  --token $FULLCHAOS_API_TOKEN

dev-hops push sample --kind pull_request

dev-hops push sample --all
```

Provider export helpers are useful but should not block v1:

```bash
dev-hops push export github --repo acme/api --since 2026-06-01 --until 2026-06-26
```

The export output is a customer transport payload, not an internal persistence/export path.

## Metric recomputation

Do not recompute the whole org by default.

Track affected scope from the accepted batch:

- org
- source systems
- source instances
- repos
- teams
- min timestamp
- max timestamp
- record kinds

Queue bounded recompute jobs for affected windows only.

## Testing

Minimum test coverage:

- API schema validation
- unsupported schema version
- max batch size
- idempotency same payload
- idempotency conflict
- disabled source rejection
- stream unavailable returns `503`
- worker normalizes each v1 record kind
- worker writes through sink helpers
- rejected-record diagnostics are queryable
- CLI validates local payloads
- CLI sends batch and polls status

## Implementation phases

### Phase 1: Contract and acceptance path

- REST router
- Pydantic schemas
- schema discovery
- validation endpoint
- accepted batch endpoint
- stream helper
- API tests

### Phase 2: Durable processing

- status store
- idempotency
- worker
- normalization
- sink writes
- rejected-record diagnostics

### Phase 3: Customer tooling

- dev-hops push validate
- dev-hops push batch
- dev-hops push sample
- docs and examples

### Phase 4: Bounded recomputation

- affected-scope extraction
- recompute enqueue
- status visibility
- operational metrics

## Non-goals for v1

- GraphQL ingestion mutations
- Kafka
- Temporal
- IDE/editor telemetry
- custom arbitrary event blob ingestion
- direct writes to metric tables
- mixed FullChaos-sync and customer-push ownership for the same source instance
