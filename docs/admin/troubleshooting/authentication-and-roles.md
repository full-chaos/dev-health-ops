---
page_id: admin-auth
summary: Diagnose sign-in, SSO, membership, role, and authorization failures.
content_type: troubleshooting
owner: platform-product
applicability: current
lifecycle: active
---

# Authentication and role problems

1. Confirm the user is signing into the intended workspace and identity provider.
2. Check workspace membership and the current role.
3. For SSO, verify issuer, audience, callback, and group or role mapping.
   For a refused sign-in, match its reason code against the [identity linking reference](../../reference/auth/identity-linking.md).
4. Reproduce with a second account that has the expected role.
5. Distinguish authentication failure from authorization failure.
6. Retain the route, time, correlation or request identifier, and sanitized message.

Do not solve an authorization defect by granting owner access permanently.

A signed-in user who gets `401` on every request after a token class change: see [legacy compatibility and rollback](../../operate/runbooks/auth-legacy-compat-and-rollback.md).

## Failed logins or 5xx after a roll

A NetworkPolicy roll and the sessions flip are separate rolls. Check which one is live with `helm history`, then follow [Auth sessions flip and rollback](../../operate/runbooks/auth-sessions-flip-and-rollback.md). Users who report a verify or reset link that fails after the later e-mail link flip need a new link; links sent before that flip are not accepted.

## Read the status code first

| Answer | Most likely cause | Go to |
| --- | --- | --- |
| `401` on every request | The token is refused: expired, signed by a retired key, or for another issuer or audience. Or the session ended. | [Token verification problems](../../integrate/troubleshooting/token-verification.md) |
| `401` after a token class change | A token class the deployment no longer accepts. | [Legacy compatibility and rollback](../../operate/runbooks/auth-legacy-compat-and-rollback.md) |
| `403` "Not allowed in an impersonation session" | The request came from an impersonation session, which is read-only. | [Impersonation is read-only](../security-and-audit/impersonation.md#impersonation-is-read-only) |
| `403` "Admin access required", "Superuser access required" or "Access denied" | The policy decision point (PDP) answered deny for the user's role. Check the role first. | [Organization roles and members](../../reference/auth/org-member-roles.md), [reason codes](../../reference/auth/reason-codes.md) |
| `403` where the same request was allowed before an enforcement change | A parity gap between the PDP and the legacy answer. Do not grant a wider role to work around it. | [Auth shadow PDP triage](../../operate/runbooks/auth-shadow-pdp-triage.md) |
| `404` on an organization, member or invite | The organization, member or invite that the request names is not there. The user may not be a member of that organization. | [Organization roles and members](../../reference/auth/org-member-roles.md) |
| `503` "Authorization service temporarily unavailable" | go-api could not get a verdict from the PDP. It does not fall back to the legacy answer. | [Auth PDP outage and rollback](../../operate/runbooks/auth-pdp-outage-and-rollback.md) |
| `503` on a bearer token | The verifier could not read the signing keys (JWKS) or a store. The token may be good. | [JWKS or discovery outage](../../integrate/troubleshooting/token-verification.md#jwks-or-discovery-outage) |

### A change that is not visible yet

Some state reaches go-api through a projection or a reconcile pass, not at once. A recent change can look stale for a short time:

- An organization suspended or reactivated in the auth service usually reaches go-api within one reconcile pass (every 5 minutes) when organization provisioning goes through the auth service. See [auth service configuration](../../reference/auth/configuration.md).
- A revoked External Push token can still show as active in the auth service record for a short time. go-api already refuses the token. See [External Push lifecycle events](../../operate/runbooks/external-push-lifecycle.md).

If the state is still wrong after that time, treat it as a fault and collect the identifiers listed above.
