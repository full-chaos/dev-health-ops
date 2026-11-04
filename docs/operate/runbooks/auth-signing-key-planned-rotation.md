---
page_id: op-rb-auth-signing-key-planned-rotation
summary: Rotate the auth-service Ed25519 signing key set on a planned schedule (register, activate, retire), with the key states, the floors and the compose rehearsal that proves each step.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth signing-key planned rotation

This runbook covers the planned rotation of the auth-service Ed25519 signing key set. For a compromised key, or to roll the access-token issuer back from `eddsa` to `hs256_compat`, use [Auth signing-key emergency rotation and rollback](auth-signing-key-emergency-rotation-and-rollback.md). The verbs are `auth-migrate keys` (migration role; the runtime role is refused). Every verb is a dry run until `-apply` is added. This page gives no prod context, no Secret value and no values path.

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

## The rehearsal

`ci/rehearse_auth_key_rotation.sh` runs these three operations on bigboy docker compose, in a private compose project, against the real `auth-service`, `auth-migrate`, `go-api` and `query-api` images pinned by digest from a main push run. It is not a CI job and never runs against prod. Stages: `smoke` (stack, login, JWKS over https, a go-api call, the class flip), `drill` (compromise and issuer rollback with the 300 s floor) and `rotation` (planned rotation with the 300 s and 4200 s floors, then the compromise and the issuer rollback). Each cell prints `PASS: <name>` or `FAIL: <name>`; the first `FAIL` stops the run, and one record file per scenario is written under the run directory. A verifier that still accepts the old kid after its restart is a `FINDING` line and a non-zero exit.

The stack needs three wirings that the plain `auth` profile of `compose.yml` does not have: a TLS front for the issuer (the verifiers fetch the JWKS from an https issuer only, trusted through `SSL_CERT_FILE`), `AUTH_SESSION_READER_ROLES` for the go-api and query-api logins on `auth-migrate` and `auth-grants`, and `AUTH_PROXY_SESSIONS=true` on query-api so its role posture expects the session read. The chart carries the same inputs as `authService.accessToken.issuerURL` and `authService.migrations.sessionReaders`.

The seeded user is marked verified with one `UPDATE` on the throwaway project database of the rehearsal. That statement is rehearsal-only: it never runs against another database, and the runbook steps above contain no SQL.

### Measured so far (rotation stage, compose, 2026-11-02)

One full `rotation` run passed: 102 cells, no `FAIL`, no `FINDING`. This page quotes the planned-rotation cells; the compromise and issuer-rollback cells are on the [emergency page](auth-signing-key-emergency-rotation-and-rollback.md#the-rehearsal-measurements).

- Planned rotation: the publish floor passed after 314 s (floor 300 s); an activate inside the floor was refused with 297 s remaining. A probe of the old kid's token every 0.5 s across the activate saw 172 answers and no non-2xx one. The retire floor passed after 4224 s (floor 4200 s); a retire inside it was refused with 4175 s remaining.
- The signer reads the durable key states again every 15 s (`KeyStateRefresh`), so the first login after an `activate` or `rollback` can still be signed by the old key. The rehearsal waits 20 s before it judges a new signer. Login is limited to 10 per 15 min per address and account, so a script that polls the login is refused (429).

The records of the run are the six files under the run directory `records/` (`00-environment` to `40-S3-issuer-rollback`).

### The k3s rehearsal of the issuer rollback (chart form)

`ci/rehearse_auth_key_rotation_k3s.sh --stage rollback` runs the issuer rollback with the chart on bigboy k3s. It installs the chart with `authService.keySet` (a `active`, b `pending`) and class `eddsa`, waits the real 300 s publish floor, compromises a with successor b, then runs `helm upgrade` to `hs256_compat` and `helm rollback` to the `eddsa` revision. After each step a is absent from the JWKS and still `compromised`, and `rollback -kid a` is refused with `compromised_terminal`. It needs a namespace-scoped ServiceAccount kubeconfig (never the k3s admin file: the driver refuses the exact path `/etc/rancher/k3s/k3s.yaml`, resolved through symlinks, and any file whose cluster-scope permission list (`kubectl auth can-i --list`) holds more than get on that one Namespace object, the self-review creates and get on non-resource URLs, and a list whose header or a row is not the expected four-column form is refused too; a ServiceAccount file may have any name; the fixture `ci/auth_k8s/rotation_rehearsal_role.yaml` also gives it `get` on that one Namespace object through a run-named ClusterRole and ClusterRoleBinding) and a namespace named `authrot-<suffix>` (the one allowed prefix; `prod`, `production`, `default` and every other name are refused) whose label `rehearsal-run` equals the run token (it refuses before any write if the read is denied or the label differs). `REHEARSAL_SERVER` names the API server URL of the bigboy rehearsal cluster and the kubeconfig must name exactly that server. Before the first write the driver also refuses if any fixed-name object it creates (the postgres and valkey Deployments and Services, its Secrets including `auth-pg-superuser`) or the Helm release `rot` already exists in the namespace, and on exit it deletes only the objects it recorded creating. It never creates or deletes a namespace. It checks the key table, the JWKS and the live class; it does not log in (token cells stay in the compose rehearsal). The record is `records/50-S3-issuer-rollback-k3s.txt` under the run directory.

Open item: the k3s rehearsal has not been run yet.
