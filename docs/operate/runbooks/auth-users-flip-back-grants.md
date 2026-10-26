---
page_id: op-rb-auth-users-flip-back-grants
summary: Keep the users-owned grants through the 4->3 flip-back release, check that no old owned auth-service pod is left, then narrow them in a later release.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth users flip-back grants

The users flip-back is the release from `authService.phase=4` (users owned by auth-service) to phase 3. Phase 3 does not
give the auth runtime role the users-owned grants: the column writes on `public.users`, the register inserts on
`public.organizations` and `public.memberships`, and the legacy link purge on the two link-token tables. While that
release rolls, the old owned auth-service pods still serve, and each of their sign-ins, registers and password changes
writes `public.users`. If the grants were revoked before those pods were gone, these requests would answer 500.

The removal of these grants is therefore split over two releases:

1. **The flip-back release** keeps the grants. At phase 3 or lower, every auth-migrate Job that gets
   `authService.migrate.acknowledgeGrantNarrowing` also gets `-defer-narrowing`. With the digest, the run keeps every
   lost grant that the users-owned switch gives and narrows only the rest.
2. **The narrowing release** runs later, after the check below. Set `authService.migrate.narrowAfterFlipBack=true`
   with the acknowledgement. The same Jobs then run without `-defer-narrowing` and revoke the grants.

## Accepted transitional state

From the flip-back release until the narrowing release, the runtime role holds the users-owned grants, but the phase-3
auth-service does not use them. This wide-grant window is the accepted state. It ends with the narrowing release. Do
not hand-run a REVOKE.

## Steps

1. **Before the flip-back release, record the owned ReplicaSet.** Find the ReplicaSet that has replicas:

   ```bash
   kubectl get rs -n <namespace> -l app.kubernetes.io/instance=<release>,app.kubernetes.io/component=auth-service \
     -o custom-columns=NAME:.metadata.name,HASH:.metadata.labels.pod-template-hash,REPLICAS:.status.replicas
   ```

   Its `HASH` is the owned pod-template hash.
2. **The flip-back release:** phase 3, with `acknowledgeGrantNarrowing` set to the digest that a refused run of this
   release prints. The hook logs show `runtime grants narrowing deferred with operator acknowledgement`, with the
   number of deferred grants.
3. **Check that no old owned pod is left (read only).** Count the pods of the owned ReplicaSet, Terminating pods
   included:

   ```bash
   (
     set -o pipefail
     count="$(kubectl get pods -n <namespace> \
       -l app.kubernetes.io/instance=<release>,app.kubernetes.io/component=auth-service,pod-template-hash=<owned hash> \
       -o go-template='{{len .items}}')" || { echo "STOP: kubectl failed (rc $?)"; exit 1; }
     case "${count}" in
       0) echo "OK: 0 old owned pods" ;;
       '' | *[!0-9]*) echo "STOP: no pod count read: '${count}'"; exit 1 ;;
       *) echo "STOP: ${count} old owned pods left"; exit 1 ;;
     esac
   )
   ```

   Go on only when it prints `OK: 0 old owned pods` and exits 0. A `STOP` line (kubectl failed, no count read, or pods
   left) means stop: do not start the narrowing release. If the hash was not recorded in step 1, the owned ReplicaSet is the one whose pod template sets
   `AUTH_USERS_OWNED=true`. Read that one variable by name:

   ```bash
   kubectl get rs -n <namespace> -l app.kubernetes.io/instance=<release>,app.kubernetes.io/component=auth-service \
     -o custom-columns='NAME:.metadata.name,HASH:.metadata.labels.pod-template-hash,OWNED:.spec.template.spec.containers[0].env[?(@.name=="AUTH_USERS_OWNED")].value'
   ```

   After CHAOS-11519 has merged, the phase-4 pod template carries the label `dev-health.io/auth-users-owned="true"`.
   The same check then becomes:

   ```bash
   (
     set -o pipefail
     count="$(kubectl get pods -n <namespace> \
       -l app.kubernetes.io/instance=<release>,app.kubernetes.io/component=auth-service,dev-health.io/auth-users-owned=true \
       -o go-template='{{len .items}}')" || { echo "STOP: kubectl failed (rc $?)"; exit 1; }
     case "${count}" in
       0) echo "OK: 0 old owned pods" ;;
       '' | *[!0-9]*) echo "STOP: no pod count read: '${count}'"; exit 1 ;;
       *) echo "STOP: ${count} old owned pods left"; exit 1 ;;
     esac
   )
   ```

4. **The narrowing release:** still phase 3, with `authService.migrate.narrowAfterFlipBack=true` and
   `acknowledgeGrantNarrowing` set to the digest of the deferred grants. A run with a wrong digest is refused and
   prints the right one. The chart refuses the flag without the acknowledgement, and at phase 4 or higher. The hook
   logs show `runtime grants narrowed with operator acknowledgement`.
5. **Read back** that the runtime role no longer has `UPDATE` on `public.users.password_hash`, from the narrowing
   release's hook Job logs and posture check. Then clear `narrowAfterFlipBack` and the acknowledgement in the next
   change.

## If a release fails

- **The flip-back release fails:** the grants are as they were before it, because a refused or failed run rolls back
  in one transaction. Fix the cause and run the release again.
- **The narrowing release fails:** the grants stay wide (the accepted state above). The failed hook Job and its pod
  stay for their logs. Fix the cause and run the release again with the same values.
- **`helm rollback`** to a revision rendered before this change runs no auth hooks, so the grants stay as they are. Do
  the 4->3 change with `helm upgrade` and phase-3 values, not with `helm rollback`.
