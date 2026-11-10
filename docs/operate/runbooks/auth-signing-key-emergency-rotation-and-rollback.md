---
page_id: op-rb-auth-signing-key-emergency-rotation
summary: Drop a compromised auth-service signing key with a successor, close the verifier window, and roll the access-token issuer back to the previous image, with the rehearsal measurements.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth signing-key emergency rotation and rollback

This runbook covers two operations on the auth-service Ed25519 signing key set: an emergency compromise of a key, and an issuer rollback to the previous image (the `hs256_compat` class is retired). For the key states, the floors and a planned rotation, use [Auth signing-key planned rotation](auth-signing-key-planned-rotation.md). The verbs are `auth-migrate keys` (migration role; the runtime role is refused). Every verb is a dry run until `-apply` is added. This page gives no prod context, no Secret value and no values path.

A `compromised` key is never `active`, `pending`, `retiring` or `retired` again: no verb, rollback or key-set manifest takes a key out of that state. The `compromise` verb moves any state to `compromised`, and a `pending` successor S that is past the 300 s publish floor becomes `active` when the compromised key was the active one.

## Emergency compromise

1. Have a successor `pending` for at least 300 s before you need it. Without one, `compromise -kid K` on the `active` key leaves no signing key; on a key that is not `active`, it changes only K and the active key stays active.
2. `compromise -kid K -successor S`, dry run first, then `-apply`, in one transaction. K leaves the JWKS, S signs, and the sessions that hold tokens signed under K are revoked (the command prints `sessions revoked: N`). A session that never held a K token is not revoked.
3. Every verb on K afterwards answers `refused: compromised_terminal` (`register` answers `key_registered`). A manifest that names K `active` changes nothing.
4. Close the verifier window. Whoever holds K's private key can sign a token for any live session, and the session revoke of step 2 does not stop it. auth-service checks its own key set and refuses such a token within 20 s of the decision. go-api and query-api read the JWKS through a client with a 5 s cache, a 2 s fetch timeout and a 1 s refresh floor, and do not check a kid denylist: while their JWKS fetches succeed, they refuse K at most 7 s after auth-service's JWKS drops it, so within 30 s of the decision. If every fetch fails from before the drop, they keep the last good key set, K included, for up to 10 min past the cache; then restart go-api and then query-api, one service at a time; a restarted verifier refuses K at once. The rehearsal measures both windows (see below), and the objective is in [readiness and SLO](../../reference/auth/readiness-and-slo.md#service-level-objectives).

## Issuer rollback

The class cannot go back to `hs256_compat`: it is retired and does not start. Roll auth-service, go-api and query-api back to the previous image instead (see [Legacy compatibility and rollback](auth-legacy-compat-and-rollback.md#roll-back)). A rollback does not touch the key table: a compromised kid stays out of the JWKS and denied, `rollback -kid K` on it is still refused, and a revoked session stays refused. The way forward is the same roll back to `eddsa`.

## The rehearsal measurements

The rehearsal `ci/rehearse_auth_key_rotation.sh` is described on the [planned rotation page](auth-signing-key-planned-rotation.md#the-rehearsal). One full `rotation` run (compose, 2026-11-02) passed: 102 cells, no `FAIL`, no `FINDING`. The compromise and issuer-rollback cells:

- Window a, no restart (compromise of b): the JWKS dropped b 10 s after the compromise, and go-api and query-api refused the b token 10 s after it. That token belonged to a session the compromise revoked, so this is the session revoke, not the kid; the key-compromise drill below measures the kid. The compromise revoked 13 sessions.
- Window b (compromise of c, restart after the drop): go-api refused the c token 13 s after the drop (restart took 8 s), query-api 20 s after (restart took 7 s); both answered 401. The compromise revoked 4 sessions.
- Issuer rollback (2026-11-02, before the class was retired; `hs256_compat` and back to `eddsa`): HS256 tokens accepted, the compromised kid stayed refused and out of the JWKS, `rollback -kid` on it stayed refused, and the revoked session stayed refused.

The records of the run are the six files under the run directory `records/` (`00-environment` to `40-S3-issuer-rollback`).

### The key-compromise drill (compose, 2026-11-10)

The `drill` stage measures what a holder of the compromised key can still do. It activates b, logs in once (a live session that only b signed for), and rolls back to a. A token is then made with the claims of that session and signed with a's private key (header kid `a`), as an attacker with the key would make it. Before the compromise, go-api, query-api and auth-service accept it. The clock starts at the decision, before the dry run of `compromise -kid a -successor c`. After the apply each verifier is asked every 5 s or less with the forged token and with the session's own token; a `401` on the forged token while the session's own token is still accepted is a refusal of the kid, not of the session. Two runs passed, on images from one main push run. Both ran before the 5 s JWKS cache, so their go-api and query-api rows were judged against the 330 s of that build:

| Cell | Run 1 | Run 2 | Objective |
| --- | --- | --- | --- |
| Decision to apply (dry run and apply) | 3 s | 3 s | - |
| go-api, no restart: first `401` of the forged token after the decision | 277 s | 274 s | 30 s |
| query-api, no restart: first `401` after the decision | 277 s | 274 s | 30 s |
| auth-service (`POST /api/v1/auth/validate`): first refusal after the decision | 18 s | 3 s | 20 s |
| Second compromise (c, successor d), auth-service | 7 s | 5 s | 20 s |
| Second compromise, go-api then query-api restarted right after the drop: forged token refused after the decision | go-api 20 s, query-api 27 s | go-api 18 s, query-api 25 s | - |

In both runs the session's own token stayed accepted at every verifier, and the token of the session the compromise revoked was refused within 18 s, which is the session revoke. A restart of auth-service alone, on a manifest that names a `active`, did not bring a back: the JWKS did not list it, a new login was signed by c, a stayed `compromised`, and the forged token stayed refused. The issuer rollback cells passed as above.

Open item: the k3s rehearsal of the issuer rollback with the chart is not run yet.
