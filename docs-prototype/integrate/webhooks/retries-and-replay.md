---
page_id: int-wh-retry
summary: Handle provider redelivery, durable deduplication, internal retries, conflicts, reconciliation, and secret rotation without duplicate canonical effects.
content_type: task-guide
owner: platform-api
source_of_truth:
  - current provider webhook handlers and idempotency stores
  - docs/architecture/pagerduty-contract.md
applicability: current
lifecycle: active
---

# Handle retries and replay

Provider redelivery is normal. The receiver must authenticate every attempt, identify the provider event through its own contract, durably admit it once, and make repeated delivery safe without hiding conflicting payload reuse.
{: .fc-page-lede }

## Delivery sequence

1. Preserve the provider subscription or delivery identity, event ID, event type, and original body hash.
2. Verify provider authentication on every delivery, including retry and replay.
3. Resolve organization, source, and credential authority from server-owned state.
4. Claim the provider-specific deduplication key before queue admission.
5. Return success only after the contract's durable acceptance boundary.
6. Retry transient internal failures with bounded backoff.
7. Reconcile missing or failed event delivery through the provider's managed synchronization path.

Do not reuse Customer Push idempotency semantics for provider webhooks. Each surface has its own identity and acceptance contract.

## PagerDuty V3 replay contract

PagerDuty deduplication is scoped to the persisted binding and event identity. The implementation retains the raw-body SHA-256 so it can distinguish identical replay from conflicting reuse.

| Condition | Outcome |
| --- | --- |
| New valid event identity | Claim, enqueue, and process once |
| Same identity and same body after acceptance | Return HTTP 202 without a second enqueue or canonical write |
| Pending same-body delivery | Retry safely through the pending claim path |
| Same identity with a different body | Audit the conflict, return HTTP 409, and do not process it as a second event |
| Missing event ID | Use the bounded raw-body identity fallback defined by the contract |

Pending and accepted deduplication state has finite retention. Queue claims and durable provider-event deduplication are separate; a process or queue restart must not permit a second canonical write.

## Out-of-order events

Provider delivery order is not canonical ordering. The PagerDuty worker uses the shared canonical operational ordering builder and current-row reader. It does not issue a source-specific correctness read or maintain a parallel ordering protocol.

When a later-observed event describes an earlier provider transition, preserve provider timestamps and apply the current canonical ordering rules. Do not overwrite newer canonical state merely because the queue processed the event later.

## Internal retry

Retry only after identifying the failed boundary:

- signature or binding failures are not transient and must fail closed;
- unknown event types require a contract update, not automatic retry;
- provider identity/body conflicts are terminal;
- queue, database, or downstream transport failures may be transient;
- ambiguous commit outcomes require inspection before another mutation;
- revoked credentials, bindings, and secrets remain invalid for queued work.

Keep backoff bounded and observable. Repeated retries must not bypass provider budgets, create lockout risk, or conceal a persistent schema or authorization problem.

## Reconciliation

Webhooks are not a historical source of truth by themselves. Use the provider's REST or managed-sync path to:

- backfill before webhook activation;
- recover missed delivery windows;
- compare canonical source identity and timestamps;
- verify that accepted events produced the expected domain state;
- resolve delivery gaps without fabricating deletion or absence.

For PagerDuty, REST synchronization and Webhooks V3 use different authentication and replay contracts but converge on the same canonical incident model.

## Rotation and revoke behavior

Queued or replayed events must reload the current binding and credential state before writing. An old signing secret, inactive binding, revoked OAuth grant, or deleted source must fail closed even if the event was enqueued before revocation.

Create and verify a replacement binding first, switch the provider subscription, then revoke the old binding. Do not mutate secret authority in place and assume prior claims now belong to the replacement.
