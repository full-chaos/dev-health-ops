# Examples & quickstart

A copy-paste path from a **sample payload** to a **persisted, verifiable batch**. It uses
the `dev-hops push` CLI and the equivalent raw REST calls. For the full per-kind payload
catalog see [Schemas & Idempotency](schemas-and-idempotency.md); for credential setup see
the [Setup Guide](setup-guide.md).

!!! note "REST, not GraphQL"
    Ingestion is REST-only — there is no GraphQL ingestion mutation in v1 (see the
    [Overview](overview.md#rest-not-graphql)). GraphQL/API/UI remain for *querying* the
    results once they land.

## 1. Generate a sample batch envelope

`dev-hops push sample` prints one of the canonical, server-shipped example payloads (the
same files served in each kind's `examples[]` from `GET /schemas/{version}`). Pick a single
kind, or `--all` for a combined envelope with one record of every kind:

```bash
# one record kind
dev-hops push sample --kind pull_request > sample-batch.json

# one record of every v1 kind
dev-hops push sample --all > sample-batch.json
```

A single-record envelope looks like this (the `pull_request.v1` record body is the exact
package example, snippet-included here so it can never drift from what the server accepts):

```json
{
  "schemaVersion": "external-ingest.v1",
  "idempotencyKey": "sample-pull_request.v1",
  "source": {
    "type": "customer_push",
    "system": "github",
    "instance": "acme/api",
    "producer": "dev-hops-cli",
    "producerVersion": "1.0.0"
  },
  "window": { "startedAt": "2026-06-20T00:00:00Z", "endedAt": "2026-06-26T00:00:00Z" },
  "records": [
    {
      "kind": "pull_request.v1",
      "externalId": "acme/api#482",
      "payload":
--8<-- "pull_request.v1.json"
    }
  ]
}
```

!!! warning "`source.instance` scopes git-family records"
    Note `source.instance` is `acme/api` — the **same** repository identifier as the
    record's `repositoryExternalId`. The worker rejects a git-family record
    (`repository`/`pull_request`/`review`/`commit`) whose repo falls outside the batch's
    `source.instance` with `record_outside_source_instance`, and `source.instance` must
    also match your registered source's casing exactly (else `403 source_mismatch`). Push
    each repository's git records under a source registered for that repository.

## 2. Validate before you push

Validation is local and makes **no network call** — run it in CI to catch shape problems
(`unknown_kind`, `missing_required_field`, `invalid_literal`, `invalid_field`) before
spending an ingest call. Read a file, or `-` for stdin:

```bash
dev-hops push validate sample-batch.json
# or piped straight from sample:
dev-hops push sample --kind pull_request | dev-hops push validate -
```

The REST equivalent (`POST /validate`, scope `schema:read`) returns `200` with
`{"valid": true|false, "itemsRejected": N, "errors": [...]}` and never enqueues:

```bash
curl -sS -X POST "$FULLCHAOS_API_URL/api/v1/external-ingest/validate" \
  -H "Authorization: Bearer $FULLCHAOS_INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  --data @sample-batch.json
```

!!! warning "`/validate` is shape-only"
    It does **not** enforce the source-system↔kind matrix or `source.instance` scoping —
    the worker does. A record can pass `/validate` and still be rejected at processing with
    `unsupported_kind_for_system` or `record_outside_source_instance`. See
    [Troubleshooting](troubleshooting.md).

## 3. Push the batch

`push batch` submits to `POST /api/v1/external-ingest/batches` (scope `ingest:write`) and,
with `--poll`, blocks until a terminal status. Credentials come from flags or the
`FULLCHAOS_API_URL` / `FULLCHAOS_INGEST_TOKEN` / `FULLCHAOS_ORG_ID` env vars:

```bash
export FULLCHAOS_API_URL="https://app.fullchaos.example"
export FULLCHAOS_INGEST_TOKEN="fcpush_…"        # from your secret store (scopes below)
export FULLCHAOS_ORG_ID="…"

dev-hops push batch --poll sample-batch.json
```

!!! important "Scope the token for the whole flow"
    This quickstart's single token drives three routes with **three different** scopes:
    `POST /validate` needs `schema:read`, `POST /batches` needs `ingest:write`, and the
    `GET /batches/{id}` polling (`--poll`, `push status`) needs `ingest:status`. Mint the
    token with **all three** (`schema:read`, `ingest:write`, `ingest:status`) or a
    write-only token will submit the batch and then `403` on the status poll.

Raw REST — `202 Accepted` returns `{"ingestionId": "…", "status": "accepted", …}`:

```bash
curl -sS -X POST "$FULLCHAOS_API_URL/api/v1/external-ingest/batches" \
  -H "Authorization: Bearer $FULLCHAOS_INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  --data @sample-batch.json
```

!!! note "Idempotency & source ownership"
    Re-posting the same `idempotencyKey` + payload returns `200` with the same
    `ingestionId` (replay); a different payload under the same key returns `409`. A source
    instance has exactly one active owner — a second registration of the same instance
    (case-insensitive) is rejected `409`, and at push time `source.instance` must match the
    registered casing exactly. See the [Setup Guide](setup-guide.md#1-register-a-source).

## 4. Poll status & verify it landed

```bash
# push batch --poll already does this; to check later:
dev-hops push status --poll <ingestion_id>
```

Raw REST (`GET /batches/{ingestion_id}`, scope `ingest:status`) reports
`itemsAccepted` / `itemsRejected` and, once terminal (`completed` / `partial` / `failed`),
per-record `errors[]`:

```bash
curl -sS "$FULLCHAOS_API_URL/api/v1/external-ingest/batches/<ingestion_id>" \
  -H "Authorization: Bearer $FULLCHAOS_INGEST_TOKEN"
```

A `completed` (or `partial`) status with `itemsAccepted >= 1` means the records are
persisted and will surface in the product once the debounced metric recompute runs. The
same validate → push → poll sequence is what you wire into a GitHub Actions / GitLab CI /
generic runner job to push on a schedule.
