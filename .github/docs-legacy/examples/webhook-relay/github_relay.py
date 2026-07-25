"""Illustrative customer-relay example for GitHub webhooks -> FullChaos external-ingest.

NOT production code. NOT imported by dev_health_ops. Demonstrates the minimum shape
a customer-owned relay needs: verify GitHub's signature, normalize a subset of events
into external-ingest.v1 records, derive a stable idempotency key, and POST a batch.

See adr-004-webhook-assisted-customer-push-ingestion.md for the full recommendation
this sketch implements, and README.md in this directory for run instructions and
disclaimers.

Only the `pull_request` event is normalized here, to keep the sketch minimal and
focused on proving the envelope/idempotency-key shape. A production relay would
dispatch on `x_github_event` and add normalizers for the other events the ADR's
provider-feasibility table recommends (`push`, `pull_request_review`,
`check_run`/`check_suite`, `deployment`/`deployment_status`), and would still need a
periodic reconciliation batch job per the ADR's "Reconciliation schedule" section --
webhooks alone are never a complete source of truth.

Run: FULLCHAOS_INGEST_TOKEN=... FULLCHAOS_API_URL=... GITHUB_WEBHOOK_SECRET=... \
     uvicorn github_relay:app --port 8080
"""

import hashlib
import hmac
import os
import time
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Request

app = FastAPI()

GITHUB_WEBHOOK_SECRET = os.environ["GITHUB_WEBHOOK_SECRET"]
FULLCHAOS_API_URL = os.environ["FULLCHAOS_API_URL"]
FULLCHAOS_INGEST_TOKEN = os.environ["FULLCHAOS_INGEST_TOKEN"]
# No FULLCHAOS_ORG_ID env var: the ingest token is source-bound, so org and source
# instance are resolved server-side from the token -- they are never sent in the body.


def _verify_signature(raw_body: bytes, signature_header: str | None) -> None:
    if not signature_header or not signature_header.startswith("sha256="):
        raise HTTPException(status_code=401, detail="missing signature")
    expected = hmac.new(
        GITHUB_WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(f"sha256={expected}", signature_header):
        raise HTTPException(status_code=401, detail="bad signature")


def _normalize_pull_request(payload: dict[str, Any]) -> dict[str, Any]:
    """Build one external-ingest.v1 record from a GitHub `pull_request` event.

    Record shape follows the core plan's wrapper contract: `kind` + `externalId` at
    the top level, kind-specific fields nested under `payload`. `repositoryExternalId`
    must equal the batch's `source.instance` (repo full name) for git-family records.
    """
    pr = payload["pull_request"]
    repo_full_name = payload["repository"]["full_name"]
    return {
        "kind": "pull_request.v1",
        "externalId": str(pr["id"]),
        "payload": {
            "repositoryExternalId": repo_full_name,
            "number": pr["number"],
            "title": pr["title"],
            "state": pr["state"],
            "authorExternalId": str(pr["user"]["id"]),
            "createdAt": pr["created_at"],
            "updatedAt": pr["updated_at"],
            "mergedAt": pr.get("merged_at"),
            "closedAt": pr.get("closed_at"),
        },
    }


@app.post("/github/webhook")
async def github_webhook(
    request: Request,
    x_hub_signature_256: str | None = Header(default=None),
    x_github_delivery: str | None = Header(default=None),
    x_github_event: str | None = Header(default=None),
) -> dict[str, str]:
    raw_body = await request.body()
    _verify_signature(raw_body, x_hub_signature_256)
    if not x_github_delivery:
        raise HTTPException(status_code=400, detail="missing X-GitHub-Delivery")

    payload = await request.json()
    if x_github_event != "pull_request":
        # Illustrative relay only handles one event type; a real relay would
        # dispatch on x_github_event and normalize each supported kind.
        return {"status": "ignored", "event": x_github_event or "unknown"}

    record = _normalize_pull_request(payload)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    batch = {
        "schemaVersion": "external-ingest.v1",
        # GitHub delivery IDs are stable across retries, so they are usable directly
        # as the batch-level idempotency key for a one-event-per-call relay. A relay
        # that buffers multiple events into one batch must derive a batch-level key
        # instead -- see the ADR's idempotency-key derivation table.
        "idempotencyKey": x_github_delivery,
        "source": {
            "type": "customer_push",
            "system": "github",
            "instance": payload["repository"]["full_name"],
            "producer": "webhook-relay",
            "producerVersion": "0.1.0",
        },
        "window": {"startedAt": now, "endedAt": now},
        "records": [record],
    }

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{FULLCHAOS_API_URL}/api/v1/external-ingest/batches",
            headers={"Authorization": f"Bearer {FULLCHAOS_INGEST_TOKEN}"},
            json=batch,
        )
    # A production relay must retry on 503/429 with backoff and persist a local
    # dead-letter queue for anything that keeps failing -- omitted here for brevity.
    resp.raise_for_status()
    return {"status": "forwarded", "ingestionId": resp.json()["ingestionId"]}
