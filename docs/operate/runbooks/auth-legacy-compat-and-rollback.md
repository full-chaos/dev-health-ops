---
page_id: op-rb-auth-legacy-compat
summary: Which access tokens each service accepts under the hs256_compat and eddsa classes, the two preconditions of the eddsa flip, how to roll the class back, what a rollback does not undo, and how to check that HS256 tokens are still in use.
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

auth-service mints one class of access token at a time. `hs256_compat` is the default and keeps the HS256 token
that go-api minted before auth-service existed. `eddsa` mints the Ed25519 token of
[access-token.v1](../../reference/auth/access-token-claims.md). This page says what each class means for
every service, what must be true before the flip, and how to go back. The steps of the flip itself are in
[Auth sessions flip and rollback](auth-sessions-flip-and-rollback.md#access-token-class-eddsa-w3-d1). This page gives no prod context,
no Secret value and no values path.

## What each class means

| | `hs256_compat` | `eddsa` |
| --- | --- | --- |
| Token auth-service mints | HS256, signed with the shared secret (`AUTH_JWT_SECRET_KEY`), go-api's 60 minutes | Ed25519 with a `kid`, signed with the key file, 5 to 60 minutes (default 60) |
| `session_access_token` in sign-in, refresh and issue answers | absent | present: the token of audience `auth-service` |
| go-api and query-api verify | HS256 only | EdDSA (audience `dev-health-api`) and HS256 |
| auth-service session routes accept | HS256 (and EdDSA when `AUTH_TOKEN_ISSUER_URL` is set) | EdDSA audience `auth-service` and HS256 |
| `AUTH_ACCESS_TOKEN_ISSUER` on go-api and query-api | not rendered | rendered from `authService.accessToken.issuerURL` |

An issuer set under `hs256_compat` reaches auth-service alone. It mounts the discovery document and the JWKS,
and it changes nothing for go-api and query-api.

There is no fallback between classes. When the class of auth-service cannot mint, the request fails. It never
mints a token of the other class.

### Why a route accepts both

go-api, query-api and auth-service pick the validator for a route from its endpoint-profile row and from the
`alg` of the token. The row says whether the route accepts the EdDSA token (`user_access_token`) and whether
`legacy_hs256` is true. In the contract files today, every row that accepts `user_access_token` also has
`legacy_hs256` true: 196 go-api rows, 35 query-api rows and 6 auth-service rows. So an HS256 token that was
minted before the flip is still accepted after it, until it expires (60 minutes at most). No client has to
change its token handling at the moment of the flip, except the two preconditions below.

## Preconditions of the eddsa flip

Both are binding. Do not set `authService.accessToken.class=eddsa` in any environment before both are true.

1. **`AUTH_PROXY_CREDENTIALS` is on.** The chart turns it on from `authService.phase` 4. Then go-api forwards
   `POST /api/v1/auth/change-password` to auth-service without reading the token, and auth-service
   authenticates it. With the flag off, go-api's own guard runs and refuses `session_access_token`, because its
   audience is `auth-service`.
2. **Web sends `session_access_token`, and that web is deployed.** The change-password route and the social
   link and unlink routes accept `session_access_token` only under `eddsa`. A web that still sends
   `access_token` there fails those routes for every signed-in user.

The chart refuses the class below `authService.phase` 2. With the first precondition the flip in practice is
at phase 4 or more.

Also required before the flip: the key and the issuer pass the checks of
[Signing-key custody and environment](../configure/auth-signing-key-custody.md#check-before-the-flip).

## Roll the class back

Use this when the eddsa class must stop, for example when the key is lost or a verifier cannot reach the
issuer.

1. Set `authService.accessToken.class` to `hs256_compat`. Leave `issuerURL` as it is if auth-service keeps
   serving the JWKS and discovery. Unset `authService.accessToken.lifetime` if you set it: it needs `eddsa`.
2. Roll auth-service, go-api and query-api in one release. go-api and query-api read the issuer only under
   `eddsa`, so they stop verifying the EdDSA token with the roll.
3. Read back: new sign-ins return HS256 tokens and no `session_access_token`. The counter
   `devhealth_auth_access_token_accepted_total` shows `class="ops_access_token_hs256"` rising for the
   go-api and query-api routes.

What the rollback does:

- A client that still holds an EdDSA access token is refused on go-api and query-api with `401` until it
  signs in or refreshes. Such a token ends within its lifetime (60 minutes at most).
- New tokens are HS256 and are accepted on the routes whose row has `legacy_hs256`.

What the rollback does not do:

- It does not change a key state. A key that was `compromised` stays out of the JWKS and denied. A key that
  was `retired` stays retired.
- It does not restore a revoked session. A session that was revoked stays refused under both classes.
- It does not change the order of the flip. The way forward is the same roll back to `eddsa`, with the
  preconditions above checked again.

## Check that HS256 tokens are in use

Read the counter by `class` and route before any change that narrows the HS256 acceptance:

- `devhealth_auth_access_token_accepted_total{class="ops_access_token_hs256"}`: HS256 tokens accepted.
- `devhealth_auth_access_token_accepted_total{class="user_access_token"}`: EdDSA tokens accepted.
- `devhealth_auth_access_token_refused_total{reason="legacy_rejected"}` and `{reason="alg_not_allowed"}`: HS256
  tokens that a row refused.

The HS256 count does not reach 0 while any service still mints an HS256 token, and a token that was minted
just before a change lives up to 60 minutes. A decision to remove `legacy_hs256` from a row needs the count
for that route to stay at 0 for longer than that.

## When things go wrong

See [Troubleshoot token verification](../../integrate/troubleshooting/token-verification.md) for a refusal by
reason, and [Auth sessions flip and rollback](auth-sessions-flip-and-rollback.md) for the roll of the phase.
