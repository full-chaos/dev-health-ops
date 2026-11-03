---
page_id: op-rb-auth-signing-key-emergency-rotation
summary: Drop a compromised auth-service signing key with a successor, close the verifier window, and roll the access-token issuer back from eddsa to hs256_compat, with the rehearsal measurements.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth signing-key emergency rotation and rollback

This runbook covers two operations on the auth-service Ed25519 signing key set: an emergency compromise of a key, and an issuer rollback from the `eddsa` access-token class to `hs256_compat`. For the key states, the floors and a planned rotation, use [Auth signing-key planned rotation](auth-signing-key-planned-rotation.md). The verbs are `auth-migrate keys` (migration role; the runtime role is refused). Every verb is a dry run until `-apply` is added. This page gives no prod context, no Secret value and no values path.

A `compromised` key is never `active`, `pending`, `retiring` or `retired` again: no verb, rollback or key-set manifest takes a key out of that state. The `compromise` verb moves any state to `compromised`, and a `pending` successor S that is past the 300 s publish floor becomes `active` when the compromised key was the active one.

## Emergency compromise

1. Have a successor `pending` for at least 300 s before you need it. Without one, `compromise -kid K` leaves no signing key.
2. `compromise -kid K -successor S`, dry run first, then `-apply`, in one transaction. K leaves the JWKS, S signs, and the sessions that hold tokens signed under K are revoked (the command prints `sessions revoked: N`). A session that never held a K token is not revoked.
3. Every verb on K afterwards answers `refused: compromised_terminal` (`register` answers `key_registered`). A manifest that names K `active` changes nothing.
4. Close the verifier window. go-api and query-api read the JWKS through a client with a 5 min cache and a 30 s refresh floor, and do not check a kid denylist. Right after the key drop, restart go-api and then query-api, one service at a time. The rehearsal measures both windows (see below).

## Issuer rollback

Set the access-token class from `eddsa` to `hs256_compat` and roll auth-service, go-api and query-api (go-api and query-api verify the EdDSA issuer only under `eddsa`). New tokens are HS256, accepted on the routes that allow the legacy token. A rollback does not touch the key table: a compromised kid stays out of the JWKS and denied, `rollback -kid K` on it is still refused, and a revoked session stays refused. The way forward is the same roll back to `eddsa`.

## The rehearsal measurements

The rehearsal `ci/rehearse_auth_key_rotation.sh` is described on the [planned rotation page](auth-signing-key-planned-rotation.md#the-rehearsal). One full `rotation` run (compose, 2026-11-02) passed: 102 cells, no `FAIL`, no `FINDING`. The compromise and issuer-rollback cells:

- Window a, no restart (compromise of b): the JWKS dropped b 10 s after the compromise, and go-api and query-api accepted the b token for 10 s (cache bound 330 s). The compromise revoked 13 sessions.
- Window b (compromise of c, restart after the drop): go-api refused the c token 13 s after the drop (restart took 8 s), query-api 20 s after (restart took 7 s); both answered 401. The compromise revoked 4 sessions.
- Issuer rollback (`hs256_compat` and back to `eddsa`): HS256 tokens accepted, the compromised kid stayed refused and out of the JWKS, `rollback -kid` on it stayed refused, and the revoked session stayed refused.

The records of the run are the six files under the run directory `records/` (`00-environment` to `40-S3-issuer-rollback`).

Open item: the k3s rehearsal of the issuer rollback with the chart is not run yet.
