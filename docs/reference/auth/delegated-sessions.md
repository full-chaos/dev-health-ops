---
page_id: ref-auth-delegated-sessions
summary: How auth-service starts and ends a delegated session, what its tokens carry, which routes its bearer reaches, and how it ends.
content_type: reference
owner: engineering
source_of_truth:
  - internal/auth/authapi/delegated.go
  - internal/auth/authapi/routes.go
  - internal/auth/session/delegation.go
  - internal/auth/delegationend/delegationend.go
  - contracts/auth/v1/endpoint-profiles.auth-service.json
applicability: current
lifecycle: active
---

# Delegated sessions

A delegated session lets a platform administrator act as an organization
member. It is one row of `auth.sessions`: `principal_id` is the target, and
`actor_principal_id` is the real actor. No caller uses these routes yet: go-api
moves to them in a later change.

## Start

`POST /internal/v1/sessions/delegated` takes the session issuer credential or
the go-api workload token. The body is exactly `base_session_id`,
`target_principal_id`, `organization_id` and `reason`. It names no actor: the
actor is the principal of the base session.

The answer is `201` with exactly these members:

| Member | Value |
| --- | --- |
| `session` | `session.v1`, with its `delegation` object |
| `access_token` | The token of the target, for go-api and query-api |
| `session_access_token` | The token of audience `auth-service`, under the eddsa class only |
| `token_type` | `bearer` |
| `expires_in` | `exp - iat` of the tokens |

There is no refresh member, as the session has no refresh credential. If a mint
fails after the row commits, the route ends the row and answers `503`.

A refusal uses the existing reason codes. The exact cause is only in the log.

| Refusal | Answer |
| --- | --- |
| Ownership, authority, target, membership, role, organization, nesting | `403 not_entitled` |
| The base session is not live | `401 session_revoked` |
| A bad body or reason | `400 malformed` |
| A busy row or a store failure | `503 store_unavailable` |

## End

`POST /internal/v1/sessions/delegated/end` takes the same callers and a body of
exactly `session_id`. It answers `204`, and the base session stays live. A plain
session is `404 session_not_found`, and nothing changes.

## Life

A delegated session ends at the first of 15 minutes, the base session's
effective expiry and the actor's role assignment. An actor has one unrevoked
delegated session: a new start ends the prior one.

A delegated session also ends with its base session, with the target's own
session ends, and when the actor loses the role.

An actor disabled through a go-api write is not a session end path of
auth-service. Their delegated session stays live until its `expires_at`, so the
bound is 15 minutes. A new start is refused at once.

## Tokens

`exp` is the earlier of the token lifetime and the session's `expires_at`. Under
`hs256_compat` the token carries `impersonating_user_id`, the actor's user id.
Under eddsa the access token and the session-route token carry
`act: {"sub": <actor principal>}`. A plain grant carries neither claim.

## What the bearer reaches

`authenticateWith` reads the session row, not a claim. On a delegated row only
`GET /v1/sessions/current` and `POST /v1/sessions/revoke` pass. Any other route
it gates answers `403`, and a later caller of it is denied by default. The
`delegated_bearer` field of the endpoint profile holds the same list.

A switch of organization from a delegated session is refused `403 not_entitled`,
as it would mint the target a full session with a refresh credential.

## Permitted actions

The row stores the actions the session may use. The set is the delegable actions
of the target's organization role. When the actor holds `platform_owner` at
start, it is the role's full action set. The target role is an organization role,
so no platform action enters.

## Metric

`auth.session.delegations{outcome}` counts `started` and `refused`. It has no
`ended` outcome: a cascade ends a delegated session in
`internal/auth/delegationend`, which has no meter, and a partial count would
mislead. The `auth.session.delegation_ended` audit row is the record of an end.
