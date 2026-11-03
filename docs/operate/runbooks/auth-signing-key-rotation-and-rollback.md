---
page_id: op-rb-auth-signing-key-rotation
summary: Rotate the auth-service signing key set (planned), drop a compromised key (emergency), and roll the access-token issuer back, with the rehearsal that proves each step.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth signing-key rotation and rollback

This runbook covers three operations on the auth-service Ed25519 signing key set: a planned rotation, an emergency compromise, and an issuer rollback from the `eddsa` access-token class to `hs256_compat`. The verbs are `auth-migrate keys` (migration role; the runtime role is refused). Every verb is a dry run until `-apply` is added. This page gives no prod context, no Secret value and no values path.

## The states and the floors

A key is `pending` (published, not signing), `active` (the one signing key), `retiring` (published, not signing), `retired` (neither) or `compromised` (neither, and never again: no verb, rollback or key-set manifest takes a key out of it).

| Verb | Moves | Floor (real clock) |
|---|---|---|
| `adopt` | an empty table takes the custody states, once | none |
| `register -kid K` | a custodied key becomes `pending` | none |
| `activate -kid K` | `pending` to `active`; the active key becomes `retiring` | 300 s published as `pending` (the JWKS cache TTL) |
| `retire -kid K` | `retiring` to `retired` | 4200 s as `retiring` (60 min access token ceiling, 5 min clock skew, 5 min cache TTL) |
| `rollback -kid K` | `retiring` to `active` (the active key becomes `retiring`), or `retired` to `pending` | none; a retired key is published again before it may sign |
| `compromise -kid K [-successor S]` | any state to `compromised`; S (a `pending` key past its floor) becomes `active` when K is the active key | none |

A key-set manifest lists every key that must stay published: a registered key that the manifest does not name is treated as retired and is not published. The table, not the manifest, holds the states.

## Planned rotation

1. Stage the new key file and add it to the manifest (an atomic rename of the manifest; a key file is immutable per kid). `register -kid NEW -apply`. The JWKS now lists the old and the new key.
2. Wait 300 s. `activate -kid NEW` before that answers `refused: floor_not_reached (floor reached in <time>)` and changes nothing.
3. `activate -kid NEW -apply`. New tokens carry `kid` NEW. Tokens signed by the old key stay accepted while it is `retiring` (the overlap). The rehearsal probes the old token on go-api across the change: no non-2xx.
4. Back out inside the overlap with `rollback -kid OLD -apply`, and forward again with `rollback -kid NEW -apply`.
5. Wait 4200 s from the last switch. `retire -kid OLD -apply`. OLD leaves the JWKS. Every token OLD signed has expired by then, so the refusal of an OLD token is by expiry and by absence from the JWKS together.

## Emergency compromise

1. Have a successor `pending` for at least 300 s before you need it. Without one, `compromise -kid K` leaves no signing key.
2. `compromise -kid K -successor S`, dry run first, then `-apply`, in one transaction. K leaves the JWKS, S signs, and the sessions that hold tokens signed under K are revoked (the command prints `sessions revoked: N`). A session that never held a K token is not revoked.
3. Every verb on K afterwards answers `refused: compromised_terminal` (`register` answers `key_registered`). A manifest that names K `active` changes nothing.
4. Close the verifier window. go-api and query-api read the JWKS through a client with a 5 min cache and a 30 s refresh floor, and do not check a kid denylist. Right after the key drop, restart go-api and then query-api, one service at a time. The rehearsal measures both windows (see below).

## Issuer rollback

Set the access-token class from `eddsa` to `hs256_compat` and roll auth-service, go-api and query-api (go-api and query-api verify the EdDSA issuer only under `eddsa`). New tokens are HS256, accepted on the routes that allow the legacy token. A rollback does not touch the key table: a compromised kid stays out of the JWKS and denied, `rollback -kid K` on it is still refused, and a revoked session stays refused. The way forward is the same roll back to `eddsa`.

## The rehearsal

`ci/rehearse_auth_key_rotation.sh` runs these three operations on bigboy docker compose, in a private compose project, against the real `auth-service`, `auth-migrate`, `go-api` and `query-api` images pinned by digest from a main push run. It is not a CI job and never runs against prod. Stages: `smoke` (stack, login, JWKS over https, a go-api call, the class flip), `drill` (compromise and issuer rollback with the 300 s floor) and `rotation` (planned rotation with the 300 s and 4200 s floors, then the compromise and the issuer rollback). Each cell prints `PASS: <name>` or `FAIL: <name>`; the first `FAIL` stops the run, and one record file per scenario is written under the run directory. A verifier that still accepts the old kid after its restart is a `FINDING` line and a non-zero exit.

The stack needs three wirings that the plain `auth` profile of `compose.yml` does not have: a TLS front for the issuer (the verifiers fetch the JWKS from an https issuer only, trusted through `SSL_CERT_FILE`), `AUTH_SESSION_READER_ROLES` for the go-api and query-api logins on `auth-migrate` and `auth-grants`, and `AUTH_PROXY_SESSIONS=true` on query-api so its role posture expects the session read. The chart carries the same inputs as `authService.accessToken.issuerURL` and `authService.migrations.sessionReaders`.

The seeded user is marked verified with one `UPDATE` on the throwaway project database of the rehearsal. That statement is rehearsal-only: it never runs against another database, and the runbook steps above contain no SQL.

### Measured so far (rotation stage, compose, 2026-11-02)

One full `rotation` run passed: 102 cells, no `FAIL`, no `FINDING`.

- Planned rotation: the publish floor passed after 314 s (floor 300 s); an activate inside the floor was refused with 297 s remaining. A probe of the old kid's token every 0.5 s across the activate saw 172 answers and no non-2xx one. The retire floor passed after 4224 s (floor 4200 s); a retire inside it was refused with 4175 s remaining.
- The signer reads the durable key states again every 15 s (`KeyStateRefresh`), so the first login after an `activate` or `rollback` can still be signed by the old key. The rehearsal waits 20 s before it judges a new signer. Login is limited to 10 per 15 min per address and account, so a script that polls the login is refused (429).
- Window a, no restart (compromise of b): the JWKS dropped b 10 s after the compromise, and go-api and query-api accepted the b token for 10 s (cache bound 330 s). The compromise revoked 13 sessions.
- Window b (compromise of c, restart after the drop): go-api refused the c token 13 s after the drop (restart took 8 s), query-api 20 s after (restart took 7 s); both answered 401. The compromise revoked 4 sessions.
- Issuer rollback (`hs256_compat` and back to `eddsa`): HS256 tokens accepted, the compromised kid stayed refused and out of the JWKS, `rollback -kid` on it stayed refused, and the revoked session stayed refused.

The records of the run are the six files under the run directory `records/` (`00-environment` to `40-S3-issuer-rollback`).

Open item: the k3s rehearsal of the issuer rollback with the chart is not run yet.
