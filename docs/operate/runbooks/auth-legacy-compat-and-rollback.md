---
page_id: op-rb-auth-legacy-compat
summary: What each service accepts and mints now that the hs256_compat class is retired, the two preconditions of running eddsa, how to roll back (to the previous image, not a class flip), what a rollback does not undo, and how to check that HS256 tokens are still in use.
content_type: runbook
owner: platform-operations
source_of_truth:
  - internal/auth/authconfig/config.go
  - internal/auth/accessverify/accessverify.go
  - internal/apiservice/accessverify.go
  - contracts/auth/v1/endpoint-profiles.go-api.json
  - deploy/helm/dev-health/templates/_helpers.tpl
applicability: current
lifecycle: active
---

# Legacy compatibility and rollback of the access-token class

auth-service mints one class of access token: `eddsa`, the Ed25519 token of
[access-token.v1](../../reference/auth/access-token-claims.md). The `hs256_compat` class is retired: no Go service
mints an HS256 access token. `AUTH_ACCESS_TOKEN_CLASS` unset or empty is `eddsa`; the value `hs256_compat` is a start
error that names `eddsa`. This page says what that means for every service, what must be true before a venue runs
the class, and how to go back. This page gives no prod context, no Secret value and no values path.

## What the class means

| | `eddsa` |
| --- | --- |
| Token auth-service mints | Ed25519 with a `kid`, signed with the key file, 5 to 60 minutes (default 60) |
| `session_access_token` in sign-in, refresh and issue answers | present: the token of audience `auth-service` |
| go-api mints | nothing: it takes every session from auth-service's issue route. With `AUTH_PROXY_SESSIONS` off the sign-in routes answer `503` with `reason_code` `config_missing` |
| `AUTH_ACCESS_TOKEN_ISSUER` on go-api and query-api | rendered from `authService.accessToken.issuerURL` |

There is no fallback between classes. When auth-service cannot mint, the request fails. It never mints an HS256
token.

### Why a route still reads an HS256 token

go-api, query-api and auth-service pick the validator for a route from its endpoint-profile row and from the `alg`
of the token. In the contract files no row has `legacy_hs256` true: the class `ops_access_token_hs256` is
`retired`, so every row refuses an HS256 bearer with `alg_not_allowed`. No auth-service route verifies a go-api
HS256 refresh token: refresh answers it as a token that does not verify, and logout revokes nothing for it.

## Preconditions of running the class

Both are binding. A venue that renders auth-service without the class or the issuer URL does not start.

1. **`AUTH_PROXY_CREDENTIALS` is on.** The chart turns it on from `authService.phase` 4. Then go-api forwards
   `POST /api/v1/auth/change-password` to auth-service without reading the token, and auth-service
   authenticates it. With the flag off, go-api's own guard runs and refuses `session_access_token`, because its
   audience is `auth-service`.
2. **Web sends `session_access_token`, and that web is deployed.** The change-password route and the social
   link and unlink routes accept `session_access_token` only. A web that still sends `access_token` there fails
   those routes for every signed-in user.

`AUTH_TOKEN_ISSUER_URL` is required with the class. Also required: the key and the issuer pass the checks of
[Signing-key custody and environment](../configure/auth-signing-key-custody.md#check-before-the-flip).

## Roll back

A class flip is not a rollback any more: `hs256_compat` does not start. To undo a release that carries this
change, roll auth-service, go-api and query-api back to the previous image, in one release, and set the chart
values the previous image needs. The previous image still mints the HS256 token.

What a rollback does:

- A client that holds an EdDSA access token is refused on go-api and query-api with `401` until it signs in or
  refreshes, if the previous image does not verify the issuer. Such a token ends within its lifetime (60 minutes at
  most).

What a rollback does not do:

- It does not change a key state. A key that was `compromised` stays out of the JWKS and denied. A key that
  was `retired` stays retired. The key states and moves are in [planned rotation](auth-signing-key-planned-rotation.md) and [emergency rotation and rollback](auth-signing-key-emergency-rotation-and-rollback.md).
- It does not restore a revoked session. A session that was revoked stays refused.

## Check that HS256 tokens are in use

Read the counter by `class` and route:

- `devhealth_auth_access_token_accepted_total{class="ops_access_token_hs256"}`: HS256 tokens accepted. It does not
  rise on a build with this change, because no row accepts the class.
- `devhealth_auth_access_token_accepted_total{class="user_access_token"}`: EdDSA tokens accepted.
- `devhealth_auth_access_token_refused_total{reason="legacy_rejected"}` and `{reason="alg_not_allowed"}`: HS256
  tokens that a row refused.

## When things go wrong

See [Troubleshoot token verification](../../integrate/troubleshooting/token-verification.md) for a refusal by
reason, and [Auth sessions flip and rollback](auth-sessions-flip-and-rollback.md) for the roll of the phase.
