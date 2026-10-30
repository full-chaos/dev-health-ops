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
- **The admin users scan fails closed, and what it cannot see.** In all three modes (up-go-api, up-owned and down-auth) the barrier reads the pods, Jobs and CronJobs of the whole namespace (not only this release), every container, init container and ephemeral container, command and args together. Outside the trusted server containers below, a container STOPs the barrier, naming the kind, the object and the container, when its command or args: mentions dho at all; run a shell with -c (sh, bash, ash, dash, zsh or ksh with a flag that holds c, such as `-c` or `-ec`); hold `$(`, a backtick, `${` or `eval`; or hold both the words `admin` and `users` (so any users subcommand is refused, `dho admin users list` too). A row that does not split cleanly (an argument with `|` or the scan's own separators) STOPs as well. A false STOP is fine: the operator clears it by hand (finish or delete the Job, or read the container and decide), then runs the barrier again. A server container is trusted only when its name, its image repository and its command and args equal one container of the go-api or auth-service ReplicaSet pod templates (the auth-service init container `stage-signing-key` runs `sh -ec`), or a container of the live pod template of the bundled ClickHouse StatefulSet `<fullname>-clickhouse` (`templates/clickhouse.yaml:10-12`; its init container runs `sh -c`, `:53-59`) and the pod is owned by exactly that StatefulSet. The fullname follows the chart's rule: the `FULLNAME` environment variable, else the release name when it contains `dev-health`, else `<release>-dev-health`. A container of another release in the same namespace is scanned the same way. A label never trusts anything, and a Job or a CronJob is never trusted. Limits, named: a `kubectl exec ... dho admin users create` into a running container, a server container included, is not in a pod spec, so the barrier cannot see it (do not run it from the start of U1 to the end of U3 and from the start of D1 to the end of D2), and a change to the ClickHouse StatefulSet's own template, or to a go-api or auth-service ReplicaSet template, is trusted.
- Each step below is one `helm upgrade`, one script run or one read. The exit code of a script run gates the next step: any non-zero exit means STOP and report. Do not retry in a loop.
- The values files set the stage (`authService.usersOwnedStage`). No `--set` is needed or allowed for it.
- Both stages need `goApi.enabled=true`. The render refuses phase 4 without a stage, and refuses `narrowAfterFlipBack` at every phase from 4.
- **The stage outage.** Once no old go-api pod is left (the `up-go-api` barrier passed) and until the U3 rollout starts, six credential routes are stubbed: a valid request answers `503` with `Retry-After: 1`, and go-api validates the body first and answers `422` to an invalid one (it does not forward it; `internal/apiservice/authproxy.go:147-149`, `internal/api/session/register.go:37-55`, `internal/api/session/changepassword.go:38-57`). The six routes are `/api/v1/auth/verify` (GET), `/api/v1/auth/resend-verification`, `/api/v1/auth/forgot-password`, `/api/v1/auth/reset-password`, `/api/v1/auth/change-password` and `/api/v1/auth/register` (POST). go-api forwards them (`internal/apiservice/authproxy.go:150-161`, `:264-279`), and auth-service is not owned yet, so it serves the stub of `NotOwnedCredentialRoutes` (`internal/auth/authapi/credentials_unowned.go:13-31`, mounted by `internal/auth/authruntime/credentials.go:130-131`). **The U1 overlap.** Before that point, while the U1 rollout runs, phase 3 go-api pods (`AUTH_PROXY_CREDENTIALS` off) still serve these routes themselves and write `public.users`, and phase 4 pods forward them and get the stub: `authProxy.inner` returns the local handler when the flag is off (`internal/apiservice/authproxy.go:264-267`). No clean `503` and no whole-stage outage is promised during that overlap; the `up-go-api` barrier (no old pod, every go-api pod forwarding) ends it. **The U3 overlap.** When the U3 rollout runs, new owned auth-service pods serve the credential routes for real while old unowned pods still answer the stub: `credentialRoutes` returns the stub only when `UsersOwned` is false (`internal/auth/authruntime/credentials.go:147`), so a forwarded request gets either, and no clean `503` is promised until the old auth-service pods are gone. This is intended: no second writer exists while the users are copied. It is bounded by the budget below.
- **Login and refresh are not affected.** go-api forwards `POST /api/v1/auth/login` and `POST /api/v1/auth/refresh` to auth-service (`internal/apiservice/authproxy.go:151-153`, sessions and local login on from phase 3), and auth-service serves them in every phase (`internal/auth/authapi/compat.go:126,139`). This runbook makes no claim about social sign-in, which is not one of the six.
- **The stage time budget of 15 minutes.** The target for the whole goApi stage, U1 start to U3 end, is at most 15 minutes. If the stage is over budget, roll back with D1 and D2 and do not wait for it to finish.
- **A low-traffic window and a status notice.** Run the stage in a low-traffic window. Before U1, publish a status notice that registration, e-mail verification and password operations fail with `503` and a retry hint until the stage ends.
- **The roll windows.** Inside each release, while its pods hand over (the go-api rollout of U1, the auth-service rollout of U3 and the same two in D1 and D2), expect up to 60 seconds of `503` with `Retry-After: 1` on the credential routes beyond the stage outage. Those are the roll windows; the 60 seconds is not a bound on the stage.
- **The org provisioning switch (CHAOS-10988).** With the stage `owned` and `goApi.authProxy.orgProvisioning` off, go-api still writes member removals, role changes, invites and joins to the public tables only, so `auth.memberships` drifts (CHAOS-11779). Set the switch in the same roll as the stage `owned`, and only when all of these hold: CHAOS-11885 and CHAOS-11827 are merged and in the rolled images, auth-service has `AUTH_MEMBERSHIP_ROUTES=true`, and go-api's `svc_authsess_` credential holds `memberships:write` (CHAOS-11884 is recommended first). The chart refuses the switch below phase 4. While the go-api pods hand over, old pods still write locally and new pods call auth-service; `users-reconcile` can print `info.membership_*` lines for that window (expected, not a STOP).

## Up: phase 3 to owned

1. **U1.** Start the stage clock, after the low-traffic window and the status notice of the Rules. Set `authService.phase: 4` and `authService.usersOwnedStage: goApi`. Upgrade. go-api forwards the credential routes and carries `AUTH_USERS_OWNED`. auth-service, its Jobs and the grants stay in the phase 3 form (credential stubs, nothing owned).
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

   Memberships: `diff.membership_inactive_in_public` is gated (an `auth.memberships` row that is not active while `public.memberships` still holds the member). The `info.membership_*` lines are printed and never gated. After U3, go-api still removes a member with a DELETE in `public.memberships` only and changes a role in `public.memberships` only; the `auth.memberships` row stays active with the old role. So `info.membership_public_missing` grows with each go-api member removal and `info.membership_role_diff` with each go-api role change. That is expected and is not a STOP. With `goApi.authProxy.orgProvisioning` on (the line above), go-api's member writes go to auth-service and these lines do not grow from them.

## Rollback: owned to phase 3

Roll back in the opposite order. auth-service stops owning users first, so no owned pod writes while a local writer exists.

**The 4 to 3 mixed window.** A one-step rollback (phase 4 straight to 3) opens a window where a go-api pod with the switch off (go-api-next) serves the credential routes itself beside an old owned auth-service. A password change through go-api-next does not reach that owned auth-service: the old password still signs in there and the new one answers 401 (fcs 11508 kit run-bdd2574a, console lines 119 to 123). This runbook closes that window by order: D1 and the `down-auth` barrier come before D2, so no go-api pod serves credentials locally while an owned auth-service pod is alive. Make no credential writes (no password change, no registration) from the start of D1 until D2 has finished; during D1 the new unowned auth-service pods answer the stub `503` with `Retry-After: 1` and the old owned ones still serve (see the D1 overlap).

1. **D1.** Set `usersOwnedStage: goApi` (phase stays 4) and `authService.migrate.acknowledgeGrantNarrowing` to the digest. Upgrade. auth-service returns to the phase 3 form and go-api still forwards. **The D1 overlap.** While the D1 rollout runs, old owned auth-service pods still serve and accept credential writes, and new unowned pods answer the stub: a forwarded request gets either, so no clean `503` is promised until the `down-auth` barrier passes. D1 defers the grant narrowing (CHAOS-11581): the grants stay wide, so old owned pods that still serve do not meet a revoked grant.
2. Run `auth-users-owned-barrier.sh down-auth`. It passes only when no auth-service pod carries `AUTH_USERS_OWNED` (a terminating pod counts), the auth-service Deployment is settled, no old ReplicaSet has a pod, and the admin users scan above finds nothing.
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
