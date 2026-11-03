---
page_id: int-auth
summary: Authenticate an API client with the deployment-supported mechanism and explicit organization scope.
content_type: task-guide
owner: platform-api
applicability: current
lifecycle: active
---

# Authenticate API clients

1. Identify the API surface: GraphQL, Customer Push, provider webhook, or administration route.
2. Use the credential type intended for that surface.
3. Supply organization or workspace scope through the supported request contract.
4. Store credentials in a secret manager and redact request diagnostics.
5. Verify one least-privilege operation and one expected denial.
6. Rotate or revoke through the owning surface.

An `fcpush_` token is for Customer Push and must not be reused as a generic GraphQL or admin credential. See [Authentication reference](../../reference/api/authentication.md).

A service that receives the EdDSA user access token verifies it as described in [Verify access tokens in a Go service](verify-access-tokens-go.md) and [Access-token claims and validation](../../reference/auth/access-token-claims.md).
