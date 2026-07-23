# PagerDuty provider and webhook contract

This document is the backend contract for the `canonical_incident_ingestion`
PagerDuty integration. It separates PagerDuty REST API V1 reads from PagerDuty
Webhooks V3 delivery. The two interfaces have different authentication,
versioning, and replay rules.

## Authoritative PagerDuty references

The implementation follows PagerDuty's official documentation:

- [REST API introduction](https://developer.pagerduty.com/docs/ZG9jOjQ2NDA2-introduction)
- [REST API reference](https://developer.pagerduty.com/api-reference/)
- [Webhooks](https://support.pagerduty.com/main/docs/webhooks)
- [V3 webhook overview](https://developer.pagerduty.com/docs/webhooks/v3-overview)
- [Verifying webhook signatures](https://developer.pagerduty.com/docs/ZG9jOjExMDI5NTkz-verifying-signatures)

PagerDuty documents `x-webhook-subscription` and
`x-pagerduty-signature` as V3 request headers. The signature is an HMAC-SHA256
digest over the exact raw request body. A signature header can contain more
than one `v1=` value during secret rotation, and each valid value is checked.
The receiver must read the body once, before parsing or re-serializing JSON,
and verify every accepted signature against those original bytes.

PagerDuty's V3 test event is `pagey.ping`. It is a health check, not an
operational event. A valid ping returns success without a canonical write or
queue entry. PagerDuty does not define an application timestamp header for
this contract. In particular, this service does not require or invent
`X-PagerDuty-Timestamp` or a nonstandard five-minute timestamp rule.

## REST API V1 target

The provider registry exposes one target named `operational`. It expands to
exactly eleven datasets:

1. `services`
2. `business-services`
3. `escalation-policies`
4. `schedules`
5. `on-calls`
6. `users`
7. `teams`
8. `incidents`
9. `incident-alerts`
10. `incident-log-entries`
11. `incident-notes`

`incidents` is the parent dataset required for alert, log-entry, and note
enrichment. The registry has eight OAuth scope families but seven distinct
scope strings:

| Family | Dataset keys | Scope |
| --- | --- | --- |
| incidents | `incidents`, `incident-alerts`, `incident-log-entries`, `incident-notes` | `incidents.read` |
| services | `services` | `services.read` |
| business services | `business-services` | `services.read` |
| escalation policies | `escalation-policies` | `escalation_policies.read` |
| schedules | `schedules` | `schedules.read` |
| on calls | `on-calls` | `oncalls.read` |
| users | `users` | `users.read` |
| teams | `teams` | `teams.read` |

The hyphenated registry key `business-services` maps explicitly to the
`business_services` family. No caller may guess a dataset key or scope.
REST reads remain read-only, paginated, and dataset-specific when a plan or
permission does not expose every family.

API token and client-credentials setup must make a live PagerDuty request
before a credential is reported as connected or ready. The check uses the
selected region and requested dataset families, reports missing permissions
per dataset, and never persists or logs the secret. OAuth readiness remains
dataset-specific and is not inferred from the manual credential check.

## Webhooks V3 allowlist

The canonical receiver accepts exactly these seventeen V3 events:

```text
incident.triggered
incident.acknowledged
incident.unacknowledged
incident.escalated
incident.reassigned
incident.delegated
incident.priority_updated
incident.resolved
incident.reopened
incident.annotated
incident.responder.added
incident.responder.replied
incident.service_updated
incident.status_update_published
service.created
service.deleted
service.updated
```

`pagey.ping` is recognized separately for subscription health checks. It is
successful, has no write side effect, and is not part of the seventeen-event
allowlist. Unknown events fail closed. No V1/V2 extension or future event is
implicitly accepted.

## Binding, ownership, and route

Each configured V3 subscription has one semantic-control
`PagerDutyWebhookBinding` row in PostgreSQL:

| Field | Ownership and use |
| --- | --- |
| `id` | Opaque, unguessable UUID and the only application route key |
| `org_id` | Server-owned organization authority |
| `integration_source_id` | Server-owned PagerDuty source authority |
| `credential_id` | Server-owned credential authority |
| `provider_subscription_id` | PagerDuty subscription identity from `x-webhook-subscription` |
| `signing_secret_encrypted`, `signing_secret_key_version` | Encrypted at rest through the existing credential encryption boundary |
| `status` | `candidate`, `ready`, `active`, or `inactive` |
| `created_at`, `rotated_at`, `revoked_at` | Immutable lifecycle history |

There is at most one active binding per `(org_id, integration_source_id)` and
per `(org_id, provider_subscription_id)`. The only receiver route is:

```text
POST /api/v1/webhooks/pagerduty/{binding_id}
```

The route UUID is the sole binding lookup key. The receiver first resolves the
binding, organization, source, credential, and encrypted secret from
PostgreSQL, then compares the request's trusted `x-webhook-subscription` value
with the persisted `provider_subscription_id` before HMAC/queue authority
proceeds. URL, query, JSON, and environment values cannot provide organization
or source authority. The
stream name is derived only from the opaque binding UUID. Redis stream fields
and Celery arguments carry the binding UUID and stream entry ID, never a
secret, token, organization authority, or credential authority. The worker
reloads all trusted state, checks the feature and active integration, source,
credential, and binding status, then processes the raw event.

Candidate and ready bindings accept only an authenticated official `pagey.ping`;
readiness is marked only by that verified ping. Binding create, rotate,
activate, and readiness setup endpoints are feature-gated. Revoke and
disconnect cleanup endpoints remain available when the feature is off.

## Signature, replay, and ordering

Signature verification uses the exact raw body and every `v1=` value in
`x-pagerduty-signature`. There is no timestamp header requirement. After
successful signature and event validation, durable deduplication uses the
provider subscription and PagerDuty event ID, with the raw-body hash retained
for payload identity:

```text
(binding UUID, bounded-hash(event identity))
```

Here the binding UUID is canonical route authority, and event identity includes
the trusted persisted provider subscription plus event ID. Blank event IDs fall
back to a bounded raw-body identity hash. A new claim stores
`pending:<raw-body-sha256>`; successful enqueue/processing promotes it to
`accepted:<raw-body-sha256>`. Pending claims retry safely, accepted same-body
replay returns 202 without enqueue, and different-body reuse is audited and
returns 409. Both states have finite 30-day retention.

An identical replay is idempotent and returns HTTP 202 without enqueueing.
Reuse of an event identity with a different body is audited, returns HTTP 409,
and is not processed as a second event. This durable dedupe is separate from
the queue claim and survives process, queue, and worker restarts. A duplicate
or replay cannot create a second canonical write. Out-of-order events use the
shared canonical ordering builder and current-row reader. PagerDuty webhook
code does not issue a correctness `SELECT` before writing and does not
maintain a second ordering protocol.

## Subscription cutover and rotation

The legacy environment URL and environment secret are bootstrap and diagnostic
inputs only. They cannot authorize canonical writes. A route cutover follows
this order:

1. After the feature gate, create and persist a candidate binding and encrypted
   secret.
2. Create or update the PagerDuty V3 subscription to the binding route.
3. Verify the subscription with an authenticated `pagey.ping`; only verified
   ping marks the candidate ready.
4. Activate the ready binding, then revoke the legacy binding.

Rotation is immutable. Create and verify the replacement binding first, switch
the PagerDuty subscription, and revoke the old binding only after the switch.
Disconnect and credential deletion revoke bindings, detach nullable credentials
before deleting encrypted secrets, and remove their ability to enqueue or
write. Old secrets, inactive bindings, and queued events after revoke fail
closed. No in-place mutation can make an old secret authoritative. Revoked
history remains nullable and preserves the first `revoked_at`. Candidate
revocation and lock acquisition use deterministic UUID order. OAuth
reconnect commits the new grant and binding before revoking the superseded
grant; if the new grant transaction rolls back, only the newly exchanged grant
is revoked. Migration 0044 persists encrypted OAuth revocation outbox entries
with retry metadata and locked delivery. Client credentials validate every
`READ_SCOPES` permission without mutating prior state on failure; OAuth also
proves live account identity, full scopes, and concurrent replacement safety.

Malformed binding or subscription configuration is repaired by creating a new
valid binding and disabling the malformed one. Legacy dataset/source repair is
idempotent and disables malformed legacy targets without changing zero-unit
integrations. Canonical source identity comes only from the verified PagerDuty
subdomain/account identity; inconsistent identity is rejected. Malformed repair
is encoded as a terminal zero-unit `SyncRunPlan` with persisted evidence and no
enqueue or outbox work. It is never
repaired by trusting an environment URL, payload organization, or guessed
subscription identity.

## Rollout and release posture

`canonical_incident_ingestion` is enabled by default for every license tier after
the canonical cutover. The global feature row remains a kill switch, and an
organization may carry an explicit false override. Feature-off blocks new canonical
webhook enqueue, processing, and writes while status, inspection, disconnect,
deletion, and secret cleanup remain available.

The PostgreSQL binding migration and any shared ordering migration must be
safe for mixed versions. Rollout uses a dual-schema bridge, inventory of every
API, scheduler, worker, ingest, webhook, backfill, and reader replica, a
maintenance barrier that quiesces writers, bounded raw-row copying, explicit
idempotency counters, and resumable shadow exchange. A restart resumes from
the recorded checkpoint without duplicating rows. The migration records an
explicit **no production downgrade** decision: old binaries cannot re-enter
after contract 2 is admitted, and rollback uses the same contract-2 bridge.
Binding persistence uses one lock order through the persistence boundary:
binding, source, integration, credential; revoke candidates use deterministic
UUID order. The worker hydrates credentials through the async-native path,
revalidates that graph immediately before the ClickHouse write, and retains a
completed receipt for 604800 seconds (seven days). Migration head is 0048.
There is no production downgrade path to the legacy writer or reader.
