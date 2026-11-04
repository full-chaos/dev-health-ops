---
page_id: op-external-push-credentials
summary: List, create, rotate and revoke External Push credentials as an organization administrator, and read the credential kind label.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Manage External Push credentials

These steps need an authenticated organization administrator. Authority and settings: [External Push credential authority](../../reference/auth/external-push-authority.md).

## List

The administration UI lists the push tokens of a source and of the organization. Each token row shows an "External Push" badge, and each provider integration credential shows an "Integration" badge (web change `77e46db0`, dev-health-web main). The label comes from which list shows the row.

## Create

1. Register the source, then create a source-bound token with the least scopes it needs. See [Register a source and credential](../../integrate/customer-push/register-source.md).
2. Store the plaintext `fcpush_…` value at once. It is shown only at creation and at rotation.

## Rotate

1. Rotate the token in the UI, or `POST /api/v1/admin/customer-push/tokens/{token_id}/rotate`.
2. The old token stops working at once. Put the new value in the secret consumer and check one request.

## Revoke

1. Revoke in the UI, or `POST /api/v1/admin/customer-push/tokens/{token_id}/revoke`.
2. Rotate and revoke call only the customer-push routes. They never call the integration credential delete.
3. A revoked token is refused by go-api at once. The auth-service record follows from the `revoked` event when the lifecycle events are on.

## Do not

- Do not paste a token into a ticket, a log or a chat.
- Do not use rotate to extend a token that expired. Create a new token for the source.

Incidents and operator steps: [External Push lifecycle runbook](../runbooks/external-push-lifecycle.md).
