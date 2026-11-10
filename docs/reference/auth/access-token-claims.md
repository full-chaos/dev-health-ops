---
page_id: ref-auth-access-token-claims
summary: The access-token.v1 wire format of the EdDSA user access token (header, claims, audiences and scopes, lifetime) and the order in which a verifier checks a token, with the refusal reason of each check.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/jsonschema/access-token.v1.schema.json
  - internal/auth/contracts/access_token.go
  - internal/auth/accessverify/accessverify.go
applicability: current
lifecycle: active
---

# Access-token claims and validation

The EdDSA user access token (credential class `user_access_token`) is a compact JWS. auth-service signs it
when `AUTH_ACCESS_TOKEN_CLASS=eddsa`. Its wire contract is `access-token.v1`. A verifier checks the signature
with a key from the issuer's [JWKS](jwks-and-discovery.md) before it reads a claim.
{: .fc-page-lede }

## Header

The header has exactly three members, and nothing else is accepted.

| Member | Value | Rule |
| --- | --- | --- |
| `alg` | `EdDSA` | Ed25519 only. `none` and every symmetric algorithm are refused. |
| `typ` | `at+jwt` | Separates this token from the HS256 compatibility token (`typ` `JWT`) and from a workload token. |
| `kid` | text | 1 to 256 printable ASCII characters. Names the key in the JWKS. |

The header has no `jku`, `jwk`, `x5u`, `x5c` or `crit` member. A token cannot choose its own key.

## Claims

The claim set is closed: an unknown claim, a duplicate member at any depth, or a NumericDate with a decimal
form refuses the token. The claims carry identity and session only. They carry no role, e-mail or permission:
authorization is checked live for each request.

| Claim | Required | Value |
| --- | --- | --- |
| `iss` | yes | The issuer URL (`AUTH_TOKEN_ISSUER_URL`), `https`, lowercase host, no trailing slash, no query. |
| `aud` | yes | One string, never an array. One of the audiences below. |
| `sub` | yes | The principal id, `prn_` and base64url characters. |
| `principal_type` | yes | `human`. |
| `sid` | yes | The session id, `ses_` and 22 to 64 base64url characters. |
| `jti` | yes | A unique token id, 22 to 64 base64url characters. |
| `iat`, `nbf`, `exp` | yes | Integer seconds. `iat <= nbf < exp`. |
| `scope` | yes | One scope, bound to the audience. |
| `org_id` | no | The organization of the session, a lowercase UUID. Absent means no organization. A relying party checks membership; the claim is context, not proof. |
| `act` | no | Present only on a delegated token: an object with one member, `sub`, the real actor. `sub` stays the effective subject. One hop only. |

### Audiences and scopes

| `aud` | `scope` | Accepted by |
| --- | --- | --- |
| `dev-health-api` | `api:access` | go-api and query-api. |
| `auth-service` | `auth:session` | The session routes of auth-service. |
| `acr-api` | `acr:api` | acr-api only. |
| `acr-mcp` | `acr:mcp` | acr-mcp only. |

go-api, query-api and auth-service refuse `acr-api` and `acr-mcp`. A service accepts the one audience of
its own. Under the eddsa class one sign-in gives two tokens of the same session: `access_token` (audience
`dev-health-api`) and `session_access_token` (audience `auth-service`). See
[Auth service configuration](configuration.md#session-route-bearer).

### Lifetime

`exp - iat` is at most 60 minutes. `AUTH_ACCESS_TOKEN_LIFETIME` sets it from 5 to 60 whole minutes
(default 60). A token over the ceiling is refused. A token is at most 8192 bytes.

## How a token is checked

go-api and query-api choose the validator from the route's [endpoint profile](endpoint-profiles.md) and the
`alg` in the token header. They never try several validators.

| `alg` | Route row | Result |
| --- | --- | --- |
| `EdDSA` | accepts `user_access_token` | The EdDSA verifier checks the token. |
| `EdDSA` | does not accept it | Refused: `alg_not_allowed`. |
| `HS256` | `legacy_hs256` is true | The HS256 compatibility check runs. No row sets it: the class is retired. See [legacy compatibility](../../operate/runbooks/auth-legacy-compat-and-rollback.md). |
| `HS256` | `legacy_hs256` is false | Refused: `alg_not_allowed`. |
| any other | any | Refused: `alg_not_allowed`. |
| no readable `alg` | any | Refused: `malformed`. |

The EdDSA verifier decides in this order. The first failure ends the check.

| Step | Check | SDK reason | Refusal label on the server |
| --- | --- | --- | --- |
| 1 | Header and claims match the contract. No key is used. | `contract` | `malformed` |
| 2 | The `kid` is not denied and the key source holds it. | `unknown_kid` | `kid_unknown` |
| 3 | The Ed25519 signature is valid for that key. | `signature` | `signature` |
| 4 | `iss` equals the pinned issuer, byte for byte. | `issuer` | `claims` |
| 5 | `aud` equals the audience of the service. | `audience` | `claims` |
| 6 | `nbf` is not later than now plus the leeway. | `not_yet_valid` | `claims` |
| 7 | `exp` is later than now minus the leeway. | `expired` | `expired` |

go-api and query-api set no leeway: a clock difference shows at once. The SDK `Verifier` accepts a leeway of 0
to 60 seconds, in whole seconds.

The contract check (step 1) runs before the key lookup, so a token that fails it gets `401` even during an
outage. A token that passes the contract check and needs a key that cannot be read is not a refusal: the caller
gets `503`, not `401`, and the label is `unavailable`. Every refusal answers `401` with a generic body. The reason is in the log at debug level and in
the counter `devhealth_auth_access_token_refused_total` (labels `service`, `profile`, `reason`). The counter
`devhealth_auth_access_token_accepted_total` has the label `class`: `user_access_token` or
`ops_access_token_hs256`.

After the signature and claims pass, go-api and query-api check state that a token cannot carry. The user must
exist and be active. The `sid` must name a live session of that user, and the session must pin the user's
current token version. A revoked session is refused though the token has not expired. When `org_id` is set,
the user must be a member of that organization. A failed read of this state answers `503`, never a pass.

## Verified against

- `contracts/auth/v1/jsonschema/access-token.v1.schema.json`: header, claims, audiences, scopes, patterns.
- `internal/auth/contracts/access_token.go`: audience and scope constants, `AccessTokenLifetimeCeiling`, `ParseAccessToken` rules.
- `internal/auth/accessverify/accessverify.go`: `Validate` (the choice by `alg`), the reason labels, the metrics.
- `internal/api/policy/accessroute_user.go` and `internal/api/policy/session.go`: `accessTokenUser`, `checkSession`.
- `internal/apiservice/accessverify.go` and `internal/queryapi/server/access_verify.go`: the verifier of go-api and query-api (audience, no leeway).
- `github.com/full-chaos/dev-health-go` v0.37.0, `authverify/accesstoken.go` and `accesstoken_wire.go`: `Verifier.Verify`, `MaxLeeway`, claim patterns.
