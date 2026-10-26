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

- Read the cluster only with `kubectl get ... -o jsonpath`, through `deploy/scripts/auth-users-owned-barrier.sh`, and with `kubectl logs` of the Jobs a step names. Every kubectl call has `--request-timeout=30s`. Never run `kubectl exec` (the one read-only psql readback of D3 is the only exception), `kubectl get secret` or an env dump.
- Each step below is one `helm upgrade`, one script run or one read. The exit code of a script run gates the next step: any non-zero exit means STOP and report. Do not retry in a loop.
- The values files set the stage (`authService.usersOwnedStage`). No `--set` is needed or allowed for it.
- Both stages need `goApi.enabled=true`. The render refuses phase 4 without a stage, and refuses `narrowAfterFlipBack` at every phase from 4.
- Expect up to 60 seconds of `503` with `Retry-After: 1` on the credential routes during the roll. Login, refresh and `me` stay up.

## Up: phase 3 to owned

1. **U1.** Set `authService.phase: 4` and `authService.usersOwnedStage: goApi`. Upgrade. go-api forwards the credential routes and carries `AUTH_USERS_OWNED`. auth-service, its Jobs and the grants stay in the phase 3 form (credential stubs, nothing owned).
2. Wait for the go-api rollout to finish. Run `RELEASE=<release> NAMESPACE=<ns> deploy/scripts/auth-users-owned-barrier.sh up-go-api`. Keep the `barrier_ok ... time=` line. It passes only when the go-api Deployment is settled, no old ReplicaSet has a pod, every go-api pod (a terminating one counts) has `AUTH_USERS_OWNED=true` and `AUTH_PROXY_CREDENTIALS=true`, and no other pod runs an `admin users` verb.
3. **U2.** Same values, plus `authService.backfill.enabled: true`, `verb: users-backfill`, `dryRun: false` and a new `runId`. Upgrade. The Job copies the last users now that no local writer is left.
4. Read the users-owned gap with the read-only reconcile Job. Upgrade once more at stage `goApi` with `authService.backfill.enabled: true`, `verb: users-reconcile` and a new `runId`. Then read its log (counts only, no values):

   ```bash
   kubectl logs -n <namespace> --request-timeout=30s -l app.kubernetes.io/instance=<release>,app.kubernetes.io/component=auth-backfill --tail=-1 \
     | grep -E '^(gated_diffs|diff\.(missing_in_auth|extra_in_auth|identity_missing))='
   ```

   Expected output, exactly these four lines:

   ```text
   gated_diffs=0
   diff.missing_in_auth=0
   diff.extra_in_auth=0
   diff.identity_missing=0
   ```

   Any other output, or no output (the Job has not finished, or the log is of an older run: check the Job's `runId` annotation), means STOP.
5. Run the barrier again with `up-owned`. It repeats the checks of step 2 and also fails if an auth-service pod is already owned.
6. **U3.** Set `usersOwnedStage: owned` and `authService.backfill.enabled: false`. Upgrade. The hooks widen the grants, auth-service rolls owned, and its pods carry the label `dev-health.io/auth-users-owned=true`.
7. Detection net. Run `users-reconcile` as a Job: `authService.backfill.enabled: true`, `verb: users-reconcile`, a new `runId`. It reads `public.users` and the auth schema in one read-only snapshot and exits 1 on any gated difference (`password_hash_diff`, `missing_in_auth`, `email_diff`, `token_version_diff`, and the rest). Run it once before U1 as the baseline, once after U3, and again 15 minutes after U3. The baseline must be 0 and every later run must be 0. The stop rule is: any value other than 0 after a 0 baseline means STOP and write one report with the barrier time.

## Rollback: owned to phase 3

Roll back in the opposite order. auth-service stops owning users first, so no owned pod writes while a local writer exists.

**The 4 to 3 mixed window.** A one-step rollback (phase 4 straight to 3) opens a window where a go-api pod with the switch off (go-api-next) serves the credential routes itself beside an old owned auth-service. A password change through go-api-next does not reach that owned auth-service: the old password still signs in there and the new one answers 401 (fcs 11508 kit run-bdd2574a, console lines 119 to 123). This runbook closes that window by order: D1 and the `down-auth` barrier come before D2, so no go-api pod serves credentials locally while an owned auth-service pod is alive. Make no credential writes (no password change, no registration) from the start of D1 until D2 has finished; during D1 the credential routes answer `503` with `Retry-After: 1`.

1. **D1.** Set `usersOwnedStage: goApi` (phase stays 4) and `authService.migrate.acknowledgeGrantNarrowing` to the digest. Upgrade. auth-service returns to the phase 3 form and go-api still forwards, so the credential routes answer `503` until the stubs are up. D1 defers the grant narrowing (CHAOS-11581): the grants stay wide, so old owned pods that still serve do not meet a revoked grant.
2. Run `auth-users-owned-barrier.sh down-auth`. It passes only when no auth-service pod carries `AUTH_USERS_OWNED` (a terminating pod counts), the auth-service Deployment is settled and no old ReplicaSet has a pod.
3. **D2.** Set `authService.phase: 3` and remove `usersOwnedStage`; keep the acknowledgement (the `acknowledgeGrantNarrowing` value from D1). Upgrade. go-api serves the credential routes again, now that no owned auth-service exists. The acknowledgement is what makes the chart render `-defer-narrowing` on the auth-migrate and auth-grants Jobs: without it D2 renders no defer, auth-migrate would narrow while old owned pods may still serve, and the release is refused.
4. **D3.** Set `authService.phase: 3` (no stage), `authService.migrate.narrowAfterFlipBack: true` and `authService.migrate.acknowledgeGrantNarrowing` to the digest of the deferred grants. Upgrade. This is the narrowing release. The grants are still wide after D2, which is a security leftover, so D3 is required. Before it, run the pod check of step 3 in [the flip-back grants runbook](auth-users-flip-back-grants.md) and go on only when it prints `OK: 0 old owned pods`. A wrong digest is refused and the refusal prints the right one. After D3, read the grants back with the database admin login, read only:

   ```bash
   kubectl -n <namespace> --request-timeout=30s exec -i <postgres pod> -- psql -X -q -v ON_ERROR_STOP=1 -At -U <admin user> -d <database> <<'SQL'
   BEGIN READ ONLY;
   SELECT concat_ws(',',
     has_column_privilege('<runtime role>','public.users','password_hash','UPDATE')::text,
     has_column_privilege('<runtime role>','public.users','email','INSERT')::text,
     has_column_privilege('<runtime role>','public.memberships','org_id','INSERT')::text);
   ROLLBACK;
   SQL
   ```

   The SQL goes in on stdin, so no DSN and no password is on a command line. This is the one `kubectl exec` of this runbook: it reads, in a read-only transaction that is rolled back, and it runs no other command.

   Expected output, one line:

   ```text
   false,false,false
   ```

   Any `true` means the narrowing did not run: STOP, keep the hook Job logs, and do not hand-run a REVOKE. Then clear `narrowAfterFlipBack` and the acknowledgement in the next change.
5. Do the rollback with `helm upgrade`, not `helm rollback`: `helm rollback` runs no auth hooks.

## If a barrier fails

The script prints one line per failed check on stderr. Fix the cause (a stuck old pod, a stalled rollout) and run the same script again. If the cause is a go-api pod that cannot reach `AUTH_USERS_OWNED=true`, do not run the next step. A script that cannot reach the cluster exits non-zero too: it never reads a failed call as a pass.
