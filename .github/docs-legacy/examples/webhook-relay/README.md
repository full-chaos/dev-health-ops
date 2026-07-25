# Webhook relay example (illustrative only)

`github_relay.py` in this directory is an illustrative example of the customer-owned
webhook relay pattern described in
[`adr-004-webhook-assisted-customer-push-ingestion.md`](../../architecture/adr-004-webhook-assisted-customer-push-ingestion.md).
It receives a GitHub `pull_request` webhook, verifies the signature, normalizes the event
into an `external-ingest.v1` record, and forwards it as a one-record batch to
`POST /api/v1/external-ingest/batches`.

**This is NOT production code.** It is not imported by `dev_health_ops`, has no
`pyproject.toml` entry, and is not covered by CI or the test suite — it exists purely to
prove the envelope and idempotency-key shape a real relay needs to reproduce. It handles
exactly one GitHub event type and has no retry, backoff, or dead-letter handling. It is not
a distributable relay package.

## Running it

```bash
export GITHUB_WEBHOOK_SECRET=...   # the secret you configure on your GitHub webhook
export FULLCHAOS_API_URL=https://api.fullchaos.dev
export FULLCHAOS_INGEST_TOKEN=...  # an ingest:write-scoped token bound to the source

pip install fastapi httpx uvicorn
uvicorn github_relay:app --port 8080
```

Point a GitHub repository or org webhook (Settings → Webhooks) at
`https://your-relay-host/github/webhook` with content type `application/json` and the same
secret as `GITHUB_WEBHOOK_SECRET`.

## Extending it

A real relay would, at minimum:

- dispatch on `X-GitHub-Event` and add normalizers for `push`, `pull_request_review`,
  `check_run`/`check_suite`, and `deployment`/`deployment_status` per the ADR's
  provider-feasibility table,
- retry on `503`/`429` responses with backoff and persist a local dead-letter queue for
  anything that keeps failing,
- still run a periodic reconciliation batch (e.g. via `dev-hops push batch`) — see the
  ADR's "Reconciliation schedule" section. Webhooks are lossy by design (GitHub payloads
  are capped at 25 MB; deliveries can fail) and are never a complete source of truth on
  their own.

For GitLab and Jira relays, follow the same shape: verify the provider's
signature/token, normalize into `external-ingest.v1` records, and derive the idempotency
key per the ADR's per-provider derivation table (GitLab and Jira have no single delivery
GUID, unlike GitHub's `X-GitHub-Delivery`, so the key must be derived from event fields).
