---
page_id: admin-external-push-credentials
summary: List, create, rotate and revoke External Push credentials as an organization administrator, and check the result of a rotation.
content_type: task-guide
owner: platform-product
source_of_truth:
  - internal/apiservice/responsemodel_admin_customer_push.go
  - internal/apiservice/customerpush/deps.go
  - internal/apiservice/customerpush/reads.go
  - internal/apiservice/customerpush/writes.go
applicability: current
lifecycle: active
---

# Manage External Push credentials

Use these steps as an authenticated organization administrator. External Push access
must be available to the organization. For the operating procedure, see
[Manage External Push credentials](../../operate/security/external-push-credentials.md).
For validation and lifecycle ownership, see
[External Push credential authority](../../reference/auth/external-push-authority.md).

## List and identify a credential

1. Select the intended organization in the administration UI.
2. Review the source's token list or the organization's token list. The API routes are
   `GET /api/v1/admin/customer-push/sources/{source_id}/tokens` and
   `GET /api/v1/admin/customer-push/tokens`.
3. Check the token name, source, scopes, expiry and revocation time before you change it.
   List responses include a token prefix, not the plaintext value.

The UI shows an **External Push** badge for a push token and an **Integration** badge
for a provider integration credential. The badge comes from the list that contains the
row. It identifies the credential kind; it does not prove that a request has the required
access. External Push rotation and revocation use the customer-push routes.

## Create a source credential

1. Follow [Register a source and credential](../../integrate/customer-push/register-source.md)
   to register the source and resolve any managed-sync ownership conflict.
2. Create a source-bound token in the UI or with
   `POST /api/v1/admin/customer-push/sources/{source_id}/tokens`. Choose only the scopes
   the consumer needs. A token with `ingest:write` must be bound to a source.
3. Store the returned `fcpush_…` value in the consumer's secret storage at once.
   The value is returned only at creation and rotation. Keep it out of logs, screenshots
   and administrative notes.

## Rotate and check the result

1. Identify the current token and its consumer. Prepare to replace the consumer's secret.
2. Rotate in the UI or with
   `POST /api/v1/admin/customer-push/tokens/{token_id}/rotate`.
   The old token stops working at once. There is no overlap.
3. Store the new value and update the consumer. Record the new token id without the value.
4. List the tokens again. Check that the old token has a revocation time and that the
   replacement has the intended source, scopes and expiry.
5. Check one request from the consumer with the new value before broad submission.

If the token has expired, create a new token for the source. Do not use rotation to
extend an expired token.

## Revoke

1. Identify the token that must stop.
2. Revoke in the UI or with
   `POST /api/v1/admin/customer-push/tokens/{token_id}/revoke`.
3. List the tokens again and check the revocation time. go-api refuses the revoked token
   at once. This operation does not delete an integration credential.

auth-service can hold a projection of these credentials. That projection does not decide
whether go-api accepts a token. For incidents, use the
[External Push lifecycle runbook](../../operate/runbooks/external-push-lifecycle.md).
