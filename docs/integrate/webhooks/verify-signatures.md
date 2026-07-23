---
page_id: int-wh-verify
summary: Verify a webhook signature for GitHub, GitLab, Jira, or PagerDuty against the provider's exact authentication contract and original request bytes.
content_type: task-guide
owner: platform-api
source_of_truth:
  - current provider webhook handlers
  - docs/architecture/pagerduty-contract.md
applicability: current
lifecycle: active
---

# Verify a webhook signature

Use this guide when you need to verify a webhook signature or secret-token header before accepting provider delivery. Webhook authentication is provider-specific: validate the request at the receiver boundary before parsing or re-serializing the body, trusting source authority, admitting queue work, or writing canonical state.
{: .fc-page-lede }

## General rules

- Read the request body once and preserve the original bytes when the provider signs the body.
- Resolve the expected secret and source authority from server-owned configuration or a persisted binding.
- Reject missing, malformed, or invalid authentication before dispatch.
- Use constant-time comparison where the implementation compares digests or tokens.
- Do not log secrets, complete signature headers, authorization headers, raw credential-bearing URLs, or customer-sensitive payloads.
- Verify every delivery, including provider retry and manual replay.

## Provider differences

| Provider | Current authentication boundary |
| --- | --- |
| GitHub | HMAC signature using the configured webhook secret and original body |
| GitLab | Configured secret token header |
| Jira | Supported configured secret path where the current handler is enabled |
| PagerDuty V3 | HMAC-SHA256 `v1=` signatures in `x-pagerduty-signature`, over the exact raw body, using the encrypted secret from the persisted binding |

Do not invent a shared header or algorithm across providers.

## PagerDuty V3 verification

PagerDuty can include more than one `v1=` value during secret rotation. Parse every supported signature value and accept the request when at least one validates against an active or controlled-overlap secret.

The receiver must also:

1. Resolve the opaque route binding from PostgreSQL.
2. Confirm that `x-webhook-subscription` matches the persisted provider subscription identity.
3. Compute HMAC-SHA256 over the exact raw request bytes.
4. Compare against every accepted `v1=` signature.
5. Parse and validate the event only after authentication succeeds.

PagerDuty does not define an application timestamp header for this contract. Do not require or invent `X-PagerDuty-Timestamp` or a five-minute timestamp rule.

## Test event

PagerDuty's official test event is `pagey.ping`. A valid signed ping proves that the subscription can reach the binding and authenticate. It returns success but does not enqueue or write a canonical operational event.

A ping with an invalid signature, wrong subscription identity, inactive binding, or revoked secret fails closed.

## Rotation

Create and verify a replacement secret or binding before revoking the old one. During a controlled overlap, accept only the explicitly active rotation set. After cutover, old secrets and inactive bindings must fail authentication even when a queued or replayed event arrives later.

Continue with [Handle retries and replay](retries-and-replay.md).
