---
page_id: op-rb-auth-users-owned-flip
summary: Roll phase 4 (users owned) in two stages, go-api first and auth-service second, gate each step with the barrier script, and roll back in the reverse order.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth users-owned flip and rollback

Phase 4 moves ownership of users from go-api to auth-service. While the roll runs, old and new go-api pods and old and new auth-service pods serve at the same time. An old go-api pod writes `public.users` itself. If one is still alive when auth-service owns users, its password changes and new users are lost to auth-service (CHAOS-11606). The fix is the roll order below, and a script that fails when the order is not true.

## Rules

- Read the cluster only with `kubectl get ... -o jsonpath`, through `deploy/scripts/auth-users-owned-barrier.sh`. Never run `kubectl exec`, `kubectl get secret` or an env dump.
- Each step below is one `helm upgrade` or one script run. The exit code of a script run gates the next step: any non-zero exit means STOP and report. Do not retry in a loop.
- The values files set the stage (`authService.usersOwnedStage`). No `--set` is needed or allowed for it.
- Both stages need `goApi.enabled=true`. The render refuses phase 4 without a stage, and refuses `narrowAfterFlipBack` at every phase from 4.
- Expect up to 60 seconds of `503` with `Retry-After: 1` on the credential routes during the roll. Login, refresh and `me` stay up.

## Up: phase 3 to owned

1. **U1.** Set `authService.phase: 4` and `authService.usersOwnedStage: goApi`. Upgrade. go-api forwards the credential routes and carries `AUTH_USERS_OWNED`. auth-service, its Jobs and the grants stay in the phase 3 form (credential stubs, nothing owned).
2. Wait for the go-api rollout to finish. Run `RELEASE=<release> NAMESPACE=<ns> deploy/scripts/auth-users-owned-barrier.sh up-go-api`. Keep the `barrier_ok ... time=` line. It passes only when the go-api Deployment is settled, no old ReplicaSet has a pod, every go-api pod (a terminating one counts) has `AUTH_USERS_OWNED=true` and `AUTH_PROXY_CREDENTIALS=true`, and no other pod runs an `admin users` verb.
3. **U2.** Same values, plus `authService.backfill.enabled: true`, `verb: users-backfill`, `dryRun: false` and a new `runId`. Upgrade. The Job copies the last users now that no local writer is left.
4. Read the users-owned gap (public users with no auth user, auth users with no public user, provider users with no identity). It must be `0/0/0`. If not, STOP.
5. Run the barrier again with `up-owned`. It repeats the checks of step 2 and also fails if an auth-service pod is already owned.
6. **U3.** Set `usersOwnedStage: owned` and `authService.backfill.enabled: false`. Upgrade. The hooks widen the grants, auth-service rolls owned, and its pods carry the label `dev-health.io/auth-users-owned=true`.
7. Detection net. Run `users-reconcile` as a Job: `authService.backfill.enabled: true`, `verb: users-reconcile`, a new `runId`. It reads `public.users` and the auth schema in one read-only snapshot and exits 1 on any gated difference (`password_hash_diff`, `missing_in_auth`, `email_diff`, `token_version_diff`, and the rest). Run it once before U1 as the baseline, once after U3, and again 15 minutes after U3. The baseline must be 0 and every later run must be 0. The stop rule is: any value other than 0 after a 0 baseline means STOP and write one report with the barrier time.

## Rollback: owned to phase 3

Roll back in the opposite order. auth-service stops owning users first, so no owned pod writes while a local writer exists.

1. **D1.** Set `usersOwnedStage: goApi` (phase stays 4). Upgrade with the grant-narrowing acknowledgement as the phase 3 rollback needs it. auth-service returns to the phase 3 form; go-api still forwards and answers `503` for the credential routes until the stubs are up.
2. Run `auth-users-owned-barrier.sh down-auth`. It passes only when no auth-service pod carries `AUTH_USERS_OWNED` (a terminating pod counts), the auth-service Deployment is settled and no old ReplicaSet has a pod.
3. **D2.** Set `authService.phase: 3` and remove `usersOwnedStage`. Upgrade. go-api serves the credential routes again, now that no owned auth-service exists.
4. Do the rollback with `helm upgrade`, not `helm rollback`: `helm rollback` runs no auth hooks.

## If a barrier fails

The script prints one line per failed check on stderr. Fix the cause (a stuck old pod, a stalled rollout) and run the same script again. If the cause is a go-api pod that cannot reach `AUTH_USERS_OWNED=true`, do not run the next step. A script that cannot reach the cluster exits non-zero too: it never reads a failed call as a pass.
