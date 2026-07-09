# Customer Push Ingestion: Overview

Customer-push ingestion lets a customer's own systems (CI pipelines, ETL jobs, custom
integrations) push repository, work-item, and review data directly into FullChaos over a
versioned REST API, instead of FullChaos pulling it via a managed GitHub/GitLab/Jira/Linear
connector.

!!! note "Not the same thing as provider webhooks"
    This is a different ingestion path from [Webhooks](../webhooks.md). Provider webhooks
    (`/api/v1/webhooks/*`) are FullChaos-managed sync reacting to GitHub/GitLab/Jira events in
    real time. Customer-push ingestion (`/api/v1/external-ingest/*`) is the customer's own
    system actively submitting batches of normalized records, on its own schedule. See
    [Webhooks](../webhooks.md) for the provider-webhook path.

## When to use customer-push vs. FullChaos-managed sync

| Use FullChaos-managed sync (native connectors) when... | Use customer-push ingestion when... |
|---|---|
| Your GitHub/GitLab/Jira/Linear instance is directly reachable and you're comfortable granting FullChaos an integration/OAuth connection. | Your source systems are firewalled, air-gapped, or you don't want to grant a third-party integration. |
| You want zero-maintenance, fully automatic sync. | You already run an ETL/CI pipeline and want to push data on your own schedule. |
| Your provider is one of the natively supported connectors. | You have a `custom` internal system with no native FullChaos connector. |

A given `(system, instance)` pair (e.g. `github` / `acme/api`) can only be owned by **one**
active ingestion path at a time — see [Schemas & Idempotency](schemas-and-idempotency.md) and
[Setup Guide](setup-guide.md) for the one-active-owner registration rules. If FullChaos-managed
sync is actively connected to the same instance, customer-push writes for that instance are
rejected with `403 source_owned_by_fullchaos_sync`.

## REST, not GraphQL

Customer-push ingestion is a **REST** API (`/api/v1/external-ingest/*`), authenticated with a
dedicated bearer token (`fcpush_...`), completely separate from the GraphQL API used for
analytics exploration and the web UI. Once customer-pushed data lands and is normalized, it is
readable through the same GraphQL API / UI / metrics views as data from any other connector —
ingestion is REST-only, but querying the results stays GraphQL/API/UI as usual. See
[GraphQL Overview](../api/graphql-overview.md) for the analytics side.

## Lifecycle

A batch of records goes through the following stages:

1. **Validate** (optional, recommended) — `POST /api/v1/external-ingest/validate` checks
   envelope shape and per-record payload validity against the versioned schema, without
   writing anything. Use this in CI before submitting a real batch.
2. **Batch accept** — `POST /api/v1/external-ingest/batches` runs the full accept sequence:
   auth/scope check, source-ownership check (one-active-owner), idempotency resolution
   (new/replay/conflict/retry), a durable Postgres write of the raw payload, and a commit —
   *before* the batch is queued for processing. The endpoint returns `202 Accepted` with an
   `ingestionId` as soon as the batch is durably recorded; it does not wait for processing.
3. **Stream** — once the payload is durable, the batch is enqueued as a pointer (not the
   payload itself) onto a per-org Valkey/Redis stream (`external-ingest:<org_id>:batches`). If
   the stream is unavailable, the API fails closed with `503 stream_unavailable` rather than
   silently dropping the batch — the durable Postgres row lets a same-key retry recover.
4. **Worker** — a dedicated external-ingest worker consumes the stream, re-validates each
   record (the same validation logic `POST /validate` uses), and normalizes accepted records.
5. **Sinks** — normalized records are written to the same ClickHouse/Postgres tables used by
   FullChaos-managed sync (repositories, pull requests, reviews, commits, identities, teams,
   work items, work-item transitions, work-item dependencies).
6. **Status** — `GET /api/v1/external-ingest/batches/{ingestion_id}` reports the batch's
   lifecycle status (`accepted → processing → completed|partial|failed`, with
   `stream_unavailable` as a recoverable interim state), per-record rejection diagnostics, and
   an `errorSummary`. See [Troubleshooting](troubleshooting.md).
7. **Bounded recompute** — once the worker completes a batch, affected metrics are recomputed
   for the scope the batch actually touched (its repos/teams/window), not a full-org
   recompute. `GET /batches/{id}` surfaces this as a `recompute` block
   (`not_applicable|pending|dispatched|skipped_no_scope|failed`) with the dispatched Celery
   job ids.

```text
validate ──▶ batches (202 + ingestionId) ──▶ stream ──▶ worker ──▶ sinks
                                                            │
                                                            ▼
                                        GET /batches/{id} ◀── bounded recompute
```

## Record kinds

A batch's `records[]` array carries a mix of the 9 canonical record kinds
(`repository.v1`, `identity.v1`, `team.v1`, `work_item.v1`, `work_item_transition.v1`,
`work_item_dependency.v1`, `pull_request.v1`, `review.v1`, `commit.v1`). See
[Schemas & Idempotency](schemas-and-idempotency.md) for the full field reference and canonical
example payloads.

## Where to go next

- [API Reference](api-reference.md) — every REST endpoint, request/response shapes, status
  codes.
- [Schemas & Idempotency](schemas-and-idempotency.md) — the record envelope, the 9 record
  kinds, idempotency semantics, batch limits.
- [Troubleshooting](troubleshooting.md) — rejected-record diagnostics, status polling, common
  failure modes and remediation.
- [Setup Guide](setup-guide.md) — register a source, create/rotate a credential, submit your
  first batch.
- CI/CD-runnable pipeline examples (GitHub Actions, GitLab CI) are tracked separately —
  see CHAOS-2713.
