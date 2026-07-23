---
page_id: int-wh-config
summary: Configure a supported provider webhook only after its source, credential, event allowlist, binding authority, and synchronization fallback are understood.
content_type: task-guide
owner: platform-api
source_of_truth:
  - current provider webhook handlers
  - docs/architecture/pagerduty-contract.md
applicability: current
lifecycle: active
---

# Configure a webhook

A webhook accelerates delivery for supported provider events; it does not replace provider authentication, source discovery, canonical identity, bounded backfill, or reconciliation. Connect and verify the provider through workspace administration before creating a webhook subscription.
{: .fc-page-lede }

## Identify the provider contract

Before configuring the provider, record:

- Dev Health organization and integration source;
- provider account, host, installation, region, or subscription identity;
- exact public HTTPS receiver route;
- supported event allowlist;
- signature or secret mechanism;
- canonical entity and ordering behavior;
- replay and deduplication key;
- REST or managed-sync path used for backfill and reconciliation.

Do not apply one provider's webhook contract to another provider.

## Generic setup sequence

1. Complete provider authentication and source discovery.
2. Create the webhook through the supported Dev Health administrator path when one exists; do not invent route keys manually.
3. Store the signing secret in the supported encrypted binding or deployment secret boundary.
4. Register the exact HTTPS receiver URL in the provider.
5. Select only events implemented by the current handler.
6. Send the provider's official test or health event.
7. Verify signature acceptance, binding identity, durable admission, queueing, and downstream progress.
8. Run a bounded reconciliation or sync to prove webhook and managed-ingest results converge.

## PagerDuty V3

PagerDuty webhook setup uses a persisted opaque binding. The canonical receiver route is:

```text
POST /api/v1/webhooks/pagerduty/{binding_id}
```

The binding ID is the only application route key. Organization, source, credential, provider subscription, and encrypted signing-secret authority are resolved from PostgreSQL. Do not put organization or credential identifiers in query parameters or trust them from the JSON body.

PagerDuty setup follows this cutover order:

1. Create a candidate binding and encrypted signing secret.
2. Create or update the PagerDuty V3 subscription to use the binding route.
3. Send PagerDuty's official `pagey.ping` event.
4. Mark the binding ready only after the authenticated ping succeeds.
5. Activate the ready binding.
6. Revoke the superseded binding after delivery is confirmed.

`pagey.ping` verifies the subscription and produces no canonical incident write.

## Supported PagerDuty events

The receiver accepts an explicit incident and service event allowlist. Unknown or future event names fail closed until the contract is updated. Do not enable V1/V2 webhook extensions or treat an unsupported payload as a canonical incident.

## Network and secret requirements

- Use the exact public application origin and valid TLS path.
- Keep signing secrets out of URLs, screenshots, logs, metrics, and examples.
- Protect administrator creation, rotation, activation, and revocation controls.
- Ensure API and worker processes can reload binding and credential authority from PostgreSQL.
- Keep a managed synchronization path for historical backfill and missed-event reconciliation.

## Verify readiness

A webhook is ready only when:

- the source and credential are active;
- the provider subscription identity matches the persisted binding;
- the official test event passes signature verification;
- no domain row is written for a health-only event;
- one supported event is durably accepted and processed;
- replay produces no duplicate domain effect;
- the corresponding source freshness advances;
- rotation and revocation fail closed for the old secret.

Continue with [Verify webhook signatures](verify-signatures.md) and [Handle retries and replay](retries-and-replay.md).
