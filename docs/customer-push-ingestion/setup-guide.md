# Customer Push Ingestion: Setup Guide

This walks through registering a source, minting a credential, validating a payload, and
submitting your first batch. See [Overview](overview.md) for when customer-push is the right
choice, and [API Reference](api-reference.md) / [Schemas & Idempotency](schemas-and-idempotency.md)
for the full contract.

## 1. Register a source

Every `(system, instance)` pair your org pushes data for must be registered first — this is
what enforces the one-active-owner rule (a source can't be simultaneously customer-push and
FullChaos-managed sync) and what an `ingest:write` token binds to.

Source registration and token management are **admin-role, session-authenticated** endpoints
(`/api/v1/admin/customer-push/*`) — normally driven through the FullChaos web console. If you
need to script it (e.g. infra-as-code), authenticate as an org admin and call the API directly:

```bash
curl -X POST "https://your-fullchaos-instance/api/v1/admin/customer-push/sources" \
  -H "Authorization: Bearer <admin-session-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "system": "github",
    "instance": "acme/api",
    "display_name": "Acme API repo",
    "mode": "customer_push"
  }'
```

If a FullChaos-managed sync source already actively owns `github`/`acme/api` in your org, this
returns `409` with `code: "source_owned_by_fullchaos_sync"` — disable managed sync for that
instance first. A registered-but-not-actively-owned managed source instead surfaces as a
non-blocking `warnings[]` entry in the response.

Registering under a system/instance that only differs by case from an existing registration
(e.g. `Acme/API` vs `acme/api`) is also rejected with `409` — instance matching is
case-insensitive.

## 2. Create an ingest token

Tokens are scoped and (for write access) bound to a single source:

```bash
curl -X POST "https://your-fullchaos-instance/api/v1/admin/customer-push/sources/<source_id>/tokens" \
  -H "Authorization: Bearer <admin-session-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ci-pipeline-token",
    "scopes": ["ingest:write", "ingest:status"]
  }'
```

The response's `token` field is the **only time the plaintext token is returned** —
`fcpush_<random>` — store it in your CI secret store immediately. Later reads
(`GET /customer-push/tokens`) only show `token_prefix` (first 12 characters) for identification.

An org-wide token (not bound to any source, for read-only use across all your registered
sources) can only use `schema:read`/`ingest:status` scopes — `ingest:write` always requires a
source-bound token:

```bash
curl -X POST "https://your-fullchaos-instance/api/v1/admin/customer-push/tokens" \
  -H "Authorization: Bearer <admin-session-token>" \
  -H "Content-Type: application/json" \
  -d '{"name": "read-only-monitor", "scopes": ["ingest:status"]}'
```

### Rotating a token

Rotation is a **hard, immediate cutover** — the old token stops working the instant the new one
is issued (no grace window):

```bash
curl -X POST "https://your-fullchaos-instance/api/v1/admin/customer-push/tokens/<token_id>/rotate" \
  -H "Authorization: Bearer <admin-session-token>"
```

The response is a fresh `IngestTokenCreateResponse` (new plaintext `token`, same name/scopes/
source binding, same expiry offset if the original had one). Update your CI secret immediately
after rotating. To revoke without rotating: `POST .../tokens/{token_id}/revoke`.

Export the new token for the CLI examples below:

```bash
export FULLCHAOS_API_URL="https://your-fullchaos-instance"
export FULLCHAOS_INGEST_TOKEN="fcpush_..."
export FULLCHAOS_ORG_ID="<your-org-id>"
```

## 3. Test before production: validate

Validate locally (no network call — checks shape/fields against the schema):

```bash
dev-hops push sample --kind pull_request > sample-batch.json
dev-hops push validate sample-batch.json
```

```text
valid: 1 record(s) accepted
```

Or validate against the live server (also checks scope/auth, still doesn't write anything):

```bash
curl -X POST "$FULLCHAOS_API_URL/api/v1/external-ingest/validate" \
  -H "Authorization: Bearer $FULLCHAOS_INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary @sample-batch.json
```

Wire your own export into the same shape (see
[Schemas & Idempotency](schemas-and-idempotency.md) for the record kinds and canonical example
payloads) and validate it the same way before ever calling `push batch`.

## 4. First successful batch

Using the CLI (submits, then polls until the batch reaches a terminal status):

```bash
dev-hops push batch sample-batch.json --poll
```

```text
ingestion_id: b6c1e6b0-...-uuid
status: completed
items_received: 1
items_accepted: 1
items_rejected: 0
```

Or with cURL (submit only — poll separately with `GET /batches/{id}`):

```bash
curl -X POST "$FULLCHAOS_API_URL/api/v1/external-ingest/batches" \
  -H "Authorization: Bearer $FULLCHAOS_INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary @sample-batch.json
```

```json
{"ingestionId": "b6c1e6b0-...-uuid", "status": "accepted", "itemsReceived": 1,
 "stream": "external-ingest:<org_id>:batches"}
```

Resubmitting the exact same file again is always safe — same `idempotencyKey` + identical
payload resolves as a **replay** (`200`, not a duplicate `202`). See
[Idempotency](schemas-and-idempotency.md#idempotency).

## 5. Verify data landed

Poll status by `ingestion_id`:

```bash
dev-hops push status b6c1e6b0-...-uuid --poll
```

```text
ingestion_id: b6c1e6b0-...-uuid
status: completed
items_received: 1
items_accepted: 1
items_rejected: 0
```

Or with cURL:

```bash
curl "$FULLCHAOS_API_URL/api/v1/external-ingest/batches/b6c1e6b0-...-uuid" \
  -H "Authorization: Bearer $FULLCHAOS_INGEST_TOKEN"
```

A `partial` status with `itemsRejected > 0` means some records were rejected — see
`errors[]`/`errorSummary` in the response, and
[Troubleshooting](troubleshooting.md#rejected-record-diagnostics) for the error-code
vocabulary and fixes.

Once `status` is `completed` (or `partial`, for the accepted records), the normalized data is
queryable through the regular FullChaos GraphQL API / web UI / metrics views — same as data
from any native connector. See [GraphQL Overview](../api/graphql-overview.md).

## Next steps

- [API Reference](api-reference.md) — every endpoint in detail.
- [Schemas & Idempotency](schemas-and-idempotency.md) — all 9 record kinds, canonical
  examples, idempotency rules, batch limits.
- [Troubleshooting](troubleshooting.md) — status polling, rejected-record diagnostics, and
  remediation for every failure mode.
- [CLI Reference](../ops/cli-reference.md#push) — the full `dev-hops push` command reference.
- CI/CD-runnable pipeline examples (GitHub Actions, GitLab CI) are tracked separately — see
  CHAOS-2713.
