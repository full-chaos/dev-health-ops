---
page_id: op-rb-auth-drills
summary: The auth recovery drills, what each one proves, the runbook an operator follows for the real incident, how to rehearse it, its last result, and the drills not run yet.
content_type: runbook
owner: platform-operations
source_of_truth:
  - ci/rehearse_auth_key_rotation.sh
  - ci/rehearse_auth_key_rotation_k3s.sh
  - scripts/drill-auth-pitr.sh
applicability: current
lifecycle: active
---

# Auth drills and their runbooks

Each drill rehearses one auth failure or recovery on a test stack, never on production, with test keys and test
users. The table names the runbook to follow when the same thing happens for real. The last result is the most
recent recorded run.

## Drills that ran

| Drill | What it proves | Runbook for the real incident | Rehearsal | Last result |
| --- | --- | --- | --- | --- |
| Planned signing-key rotation | Every verifier (go-api, query-api, auth-service) accepts tokens of the old and the new key in the overlap, then only the new key after retirement. | [Auth signing-key planned rotation](auth-signing-key-planned-rotation.md) | `ci/rehearse_auth_key_rotation.sh --stage rotation` (docker compose); `ci/rehearse_auth_key_rotation_k3s.sh` (k3s) | 2026-11-10: 108 checks pass, 0 fail. |
| Signing-key compromise | A token signed with a dropped key is refused, measured from the decision to drop it. | [Auth signing-key emergency rotation and rollback](auth-signing-key-emergency-rotation-and-rollback.md) | `ci/rehearse_auth_key_rotation.sh --stage drill` | 2026-11-10, two runs: go-api and query-api refuse at 274-277 s, auth-service at 3-18 s. Objective: 330 s and 20 s ([SLO](../../reference/auth/readiness-and-slo.md#service-level-objectives)). |
| Mass revocation and deactivation | A user's revoke-all, a deactivation and a workload disable reach every service within their bounds. | This page, [mass revocation](#mass-revocation-and-deactivation) | Run by hand on k3s; the script is kept with the drill record. | 2026-11-02: revoke-all refused everywhere within 1.74 s (bound 5 s); deactivation refused before the call returned; held workload tokens ended at expiry (bound 300 s). One gap, below. |
| Refresh-family reuse | A reused refresh token ends the whole session family and signs the holder out; a replay inside the grace window does not. | This page, [refresh reuse](#refresh-family-reuse) | Run on k3s with the production images; the repository no longer holds that drill script, the drill record keeps it. | 2026-11-03: 20 checks pass, 0 fail. |
| Backup, restore and point-in-time recovery | A restore never brings back a revoked credential, a lost password change or a reissued revision. | [Backup or restore failure](backup-or-restore-failure.md), [Disaster recovery](disaster-recovery.md) | `scripts/drill-auth-pitr.sh` | Without the restore fence: gap (a restore to time T brings back revocations made after T). With the fence: pass. See [the PITR drill](../maintain/backup-and-restore.md#auth-schema-backup-restore-and-pitr-drill). |
| Outbox backlog, duplicate, delayed and out-of-order events | The backlog drains, a duplicate applies once, a late or out-of-order event never lowers a revision, a revoked credential is never revived, and the reader resumes after a restart. | [Push projection drift alerts](auth-push-projection-drift-alerts.md) | Run by hand on k3s with the push projection on in the drill namespace only; the script is kept with the drill record. | 2026-11-04: every check passes. It holds for production only while the push projection is on there. |
| Workload bootstrap credential rotation (go-api) | go-api moves from the old to the new bootstrap credential inside the overlap with no failed call; a revoked value is refused. | [Workload bootstrap credential](auth-workload-bootstrap-credential.md) | Run by hand on k3s with a drill-only values layer; the script is kept with the drill record. | 2026-11-02: every rule passes. Production go-api uses its projected service account token, so this proves the Compose and self-hosted path. |
| Web assertion key rotation | During the overlap an assertion signed by the old or the new key is accepted; after `retire_after` only the new key is. | [Web assertion key rotation](auth-workload-assertion-key-rotation.md) | Steps 1 to 5 of the runbook, live on ACR. | Old and new key accepted (200) in the overlap; old key refused (401 `invalid_token`) after `retire_after`. |

The web session recovery and forced sign-out drill, and the ACR planned key rotation and ACR bootstrap rotation
drills, are recorded with the web and ACR components.

## Drills not run yet

| Drill | Runbook ready | Status |
| --- | --- | --- |
| auth-service, policy decision point (PDP) and JWKS outage, and cache expiry | [Auth PDP outage and rollback](auth-pdp-outage-and-rollback.md) | Not run. This page gets its row when it runs. |
| ACR credential revoke and assertion key rotation | Recorded with the ACR component | Not run. |

## Mass revocation and deactivation

What to expect when a user revokes all sessions, an administrator deactivates a principal, or a workload is
disabled:

- A user's revoke-all (`POST /v1/sessions/revoke-all`, see [authentication and authorization](../../reference/auth/authn-authz.md)):
  go-api, query-api and auth-service refuse the revoked sessions within seconds. In the drill the slowest refusal came
  1.74 s after the call returned. A refresh with a revoked family answers 401.
- A deactivation: refusals start before the call returns.
- A workload disable: the token exchange refuses at once. A workload token that is already issued stays valid until
  its expiry, at most 300 s.
- Known gap: after a membership removal, the query-api REST routes kept accepting the user's token for the whole 300 s
  of the drill, while go-api refused before the call returned. Treat a membership removal as complete on query-api only
  after the user's access token expires.

If a refusal does not arrive inside these bounds, check that the outbox is not backed up
([push projection drift alerts](auth-push-projection-drift-alerts.md)) and that auth-service is ready
([readiness and SLO](../../reference/auth/readiness-and-slo.md)).

## Refresh-family reuse

When a refresh token is used a second time after its successor was used, auth-service treats it as theft:

- The reused token gets 401 with the reason "reuse detected".
- The whole family is revoked: the legitimate holder's next refresh gets 401, and its session token stops working.
- One audit row `auth.session.refresh_reuse_revoked` is written, with the request id.
- A replay inside the 30 s grace window, while the successor is still unused, gets 200 and revokes nothing.
- Other sessions of the same user keep working.

The user signs in again. No operator action is needed unless the audit rows show reuse on many users at once; then
treat it as a credential leak and follow [mass revocation](#mass-revocation-and-deactivation).

## Service level objectives and incident response

The objectives, their windows and their alerts are in
[auth service readiness and SLO](../../reference/auth/readiness-and-slo.md#service-level-objectives). For an incident:

1. Confirm readiness on auth-service (`/readyz` names the failing check).
2. Find the matching runbook in the tables above.
3. Keep the request ids, the time window and the alert that fired.
