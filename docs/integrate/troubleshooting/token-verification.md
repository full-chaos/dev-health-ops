---
page_id: int-token-verification
summary: Find the cause of a refused or unavailable EdDSA access token by its symptom - unknown kid, clock skew, issuer or audience mismatch, signature, a revoked session, and a JWKS or discovery outage.
content_type: troubleshooting
owner: platform-api
source_of_truth:
  - internal/auth/accessverify/accessverify.go
  - internal/api/policy/session.go
applicability: current
lifecycle: active
---

# Troubleshoot token verification

A verifier answers `401` for a refused token and `503` when it could not decide. Start with that split. A `401`
has a reason. A `503` means the key source or a store could not be read, and the token may be good.

Never paste a token into a ticket, a chat or a log. The header and the payload of a token are base64url
JSON: decode them in a local shell, and read only `kid`, `iss`, `aud`, `iat`, `nbf` and `exp`.

## Where the reason is

- The Go SDK gives the reason: `authverify.ReasonOf(err)`.
- go-api and query-api answer a generic `401`. The reason is in the counter
  `devhealth_auth_access_token_refused_total` (labels `service`, `profile`, `reason`) and in a debug log line.
  The server groups the reasons `issuer`, `audience` and `not_yet_valid` as `claims`, and `contract` as `malformed`.
  See [how a token is checked](../../reference/auth/access-token-claims.md#how-a-token-is-checked).

| Server `reason` | SDK reason | Go to |
| --- | --- | --- |
| `kid_unknown` | `unknown_kid` | [Unknown kid](#unknown-kid) |
| `signature` | `signature` | [Signature](#signature) |
| `expired`, `claims` (time) | `expired`, `not_yet_valid` | [Clock skew and expiry](#clock-skew-and-expiry) |
| `claims` (issuer, audience) | `issuer`, `audience` | [Issuer or audience mismatch](#issuer-or-audience-mismatch) |
| `malformed` | `contract` | [Malformed token](#malformed-token) |
| `alg_not_allowed` | | [Algorithm not allowed](#algorithm-not-allowed) |
| `unavailable`, status `503` | | [JWKS or discovery outage](#jwks-or-discovery-outage) |
| no reason, `401` after the signature passes | | [A valid token that is still refused](#a-valid-token-that-is-still-refused) |

## Unknown kid

**Symptom.** `401`, reason `kid_unknown` or `unknown_kid`.

**Causes.**

- The token was signed by a key that has left the JWKS: a `retired` or `compromised` key. Tokens of such a key
  are refused by design.
- The token comes from another deployment, whose issuer has other keys.
- The key is new and the verifier has not fetched it. A verifier fetches again for an unknown `kid`, but at
  most once in 30 seconds. A planned rotation publishes a key at least 5 minutes before it signs, so this
  should not happen in a rotation that follows the runbook.

**Check.**

1. Read `kid` from the token header.
2. `curl -sS https://ISSUER_URL/.well-known/jwks.json` and look for that `kid`.
3. If the `kid` is listed and the verifier still refuses it, wait 30 seconds and send the request again. If it
   still fails, restart the verifier and read its log for a refresh that fails. See
   [JWKS or discovery outage](#jwks-or-discovery-outage).
4. If the `kid` is not listed, the key was retired or dropped. The user signs in again to get a token of the
   active key.

## Signature

**Symptom.** `401`, reason `signature`.

**Causes.** The token was changed after signing, or the key under that `kid` is not the key that signed it, or
the verifier holds an old copy of the key set. A mismatch for a `kid` that the JWKS lists is not part of a normal
rotation: report it.

**Check.** Compare the `kid` with the JWKS as above. Confirm the verifier reads the same issuer as the one that
signed (`iss`). A token that was truncated by a proxy or a header limit also fails here or as `contract`.

## Clock skew and expiry

**Symptom.** `401`, reason `expired` or `not_yet_valid` (on the server: `expired`, or `claims`).

**Causes.**

- The token is expired. It lasts at most 60 minutes (`AUTH_ACCESS_TOKEN_LIFETIME`). The client must refresh.
- The clock of the verifier or of auth-service is wrong. A token that is not valid yet means the clock of the
  verifier is behind the clock of the signer. A token that expired too early means the verifier is ahead.
- go-api and query-api use no leeway. A difference of one second shows.

**Check.** Compare `iat`, `nbf` and `exp` with the clock of the verifier host. Fix time synchronization first. In
your own Go verifier, a `Leeway` of up to 60 seconds absorbs small differences. Do not use leeway to hide a
clock that is minutes wrong.

## Issuer or audience mismatch

**Symptom.** `401`, reason `issuer` or `audience` in the SDK. On go-api and query-api the label is `claims`.

**Causes, issuer.** The `iss` must equal the issuer of the verifier byte for byte. A trailing slash, `http`
for `https`, a capital letter in the host, or another host name is a mismatch. The values
`AUTH_TOKEN_ISSUER_URL` (auth-service) and `AUTH_ACCESS_TOKEN_ISSUER` (go-api, query-api) must be the same string.

**Causes, audience.** A service accepts one audience.

| Token `aud` | Where it works |
| --- | --- |
| `dev-health-api` | go-api and query-api |
| `auth-service` | the session routes of auth-service, and `change-password` and the social link routes |
| `acr-api`, `acr-mcp` | acr-api and acr-mcp only |

A common fault is `session_access_token` (audience `auth-service`) sent to a go-api data route, or `access_token`
sent to the change-password route. See
[Auth service configuration](../../reference/auth/configuration.md#session-route-bearer).

**Check.** Decode `iss` and `aud` and compare them with the settings of the service that refused.

## Malformed token

**Symptom.** `401`, reason `malformed` or `contract`.

**Causes.** The token is not a compact JWS of three parts, is over 8192 bytes, has surrounding whitespace, has a
header other than `alg` `EdDSA`, `typ` `at+jwt` and `kid`, has an unknown or duplicate claim, has a decimal
NumericDate, or has a lifetime over 60 minutes. A string that is not a token, such as `Bearer Bearer ...` or a
cookie value, ends here.

## Algorithm not allowed

**Symptom.** `401`, reason `alg_not_allowed`.

**Causes.** The route does not accept the class of the token. An HS256 token on a route whose row has
`legacy_hs256` false, or an EdDSA token on a route that does not accept `user_access_token`. An unsupported
`alg` always ends here. See [Legacy compatibility and rollback](../../operate/runbooks/auth-legacy-compat-and-rollback.md).

## JWKS or discovery outage

**Symptom.** `503`, reason `unavailable`, and in the log `access token validator unavailable`. In the Go SDK,
`*authverify.UnavailableError`. The tokens may be good.

**What the verifier does meanwhile.** It serves keys from memory for 5 minutes, then for 10 more minutes
(grace) while refreshes fail. Tokens of a key it already holds keep verifying in that time. A `kid` it does not
hold gets `503`. After the grace, every token that passes the contract check gets `503`. A token that fails the
contract check is checked before any key is read and keeps its `401` (`malformed`).

**Causes and checks.** The refresh fails if any of these is true. Test each from the verifier's network.

1. The issuer does not answer. `curl -sSi https://ISSUER_URL/.well-known/openid-configuration` and the JWKS URL.
   Both must answer `200`.
2. The certificate is not trusted by the verifier, or the host name does not resolve there.
3. The URL redirects. The client never follows a redirect: a redirecting proxy in front of the issuer fails the
   refresh.
4. The discovery document does not fit. It must have exactly `issuer`, `jwks_uri` and
   `id_token_signing_alg_values_supported`, with `issuer` equal to the issuer of the verifier, `jwks_uri` equal
   to the issuer plus `/.well-known/jwks.json`, and the algorithm list `["EdDSA"]`. If `AUTH_TOKEN_ISSUER_URL` is
   not set, the discovery route is not served, and the client cannot refresh.
5. The JWKS answers `503` with `Retry-After`. auth-service cannot read its key set. Check its `/readyz` and the
   [key custody checks](../../operate/configure/auth-signing-key-custody.md#the-key-file).
6. A response is over 64 KiB, or the JWKS holds more than 32 keys.
7. A network policy blocks the egress of the verifier to the issuer.

**Fix.** Repair the cause. The verifier recovers on its next refresh, at most 30 seconds later. No restart is needed.

## A valid token that is still refused

**Symptom.** The signature, the issuer and the audience are right, and go-api or query-api still answers `401`.

**Causes.** After the token passes, go-api and query-api check state. The log has a warning line that names the cause:

- `session revoked`: the session ended, for example by a key compromise that revoked the sessions of its
  tokens. The user signs in again.
- `session not found`, `session of another principal`, `unusable session claim`: the `sid` does not name a live
  session of the user.
- `session predates the token_version` or `session has no token_version pin`: the user's tokens were reset after
  the session began.
- `user not found or deactivated`: the user is gone or inactive.
- `for an org the user is not a member of`: the user left the organization in `org_id`.

A failed read of the session or user store answers `503`, not `401`.

## Related

- [Access-token claims and validation](../../reference/auth/access-token-claims.md)
- [JWKS and discovery](../../reference/auth/jwks-and-discovery.md)
- [Verify access tokens in a Go service](../api/verify-access-tokens-go.md)
