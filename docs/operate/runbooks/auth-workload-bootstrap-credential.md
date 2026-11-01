---
page_id: op-rb-auth-workload-bootstrap-credential
summary: Create, rotate, revoke and emergency-disable the Compose bootstrap credential of a workload with auth-migrate, and recover when a verb fails.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Workload bootstrap credential

A workload that runs without Kubernetes (local Compose, self-hosted) presents a bootstrap credential
(`workload_bootstrap_credential`, prefix `fcwlb_`) to the auth-service workload-token exchange. The operator
manages it with `auth-migrate workloads`, as the migration role (`AUTH_MIGRATION_DATABASE_URI`). The runtime
role is refused.

## Rules

- Every verb is a dry run without `-apply`. A dry run reads only: it writes no row and no file.
- The value is written once, to the new file that `-out` names (mode 0600). It is never printed, logged or
  stored. The registry keeps only its digest.
- Put `-out` in a directory that only the operator can write. A verb that fails removes its file by path, so
  another user who can write the directory could change what is removed.
- `-out` must not exist yet. Hand the file to the workload's secret mount, then remove your copy.
- Every value expires: `-expires-in` defaults to `720h` (30 days), at most `2160h` (90 days).
- `bootstrap-create` and `bootstrap-rotate` refuse a disabled workload. The class says which row is disabled:
  `the workload row is disabled` or `the workload's principal is disabled, its row is not`. Each class names
  the `enable` verb that recovers it (see Enable a disabled workload). `bootstrap-revoke` and
  `bootstrap-disable` work on a disabled workload.

## Create

```sh
auth-migrate workloads bootstrap-create -workload NAME -out /secure/dir/NAME.bootstrap
auth-migrate workloads bootstrap-create -workload NAME -out /secure/dir/NAME.bootstrap -apply
```

The dry run lists the live credentials of the workload. A workload with a live credential is refused: rotate it.

## Rotate

```sh
auth-migrate workloads bootstrap-rotate -workload NAME -out /secure/dir/NAME.bootstrap.next -overlap 10m -apply
```

The new value is live at once. The old value stays live for at most `-overlap` (0 to `1h`); its expiry is only
ever lowered. Deliver the new file before the overlap ends. A second rotation is refused until the overlap ends.

## Revoke

```sh
auth-migrate workloads bootstrap-revoke -credential ID -reason owner_request -apply
```

Use `-reason compromised` for a leaked value. Revocation is terminal: nothing makes the value live again. To
undo a rotation, revoke the new value; the old value keeps its lowered expiry. To recover, create a new value.

## Emergency disable

```sh
auth-migrate workloads bootstrap-disable -workload NAME -apply
```

This revokes every live bootstrap value of the workload (reason `compromised`) in one transaction. The exchange
refuses them at once. Workload tokens issued before the disable stay valid for at most 300 seconds. The workload
and its Kubernetes bindings are not changed.

## Enable a disabled workload

```sh
auth-migrate workloads enable -workload NAME -operator ID
auth-migrate workloads enable -workload NAME -operator ID -apply
auth-migrate workloads enable -workload NAME -operator ID -enable-principal -apply
```

- `ID` is the auth principal id of the operator who enables the workload. It must exist, be enabled, and not
  be the workload's own principal. The audit row `auth.workload.enabled` (reason `workload_enabled`) names it
  as the actor.
- Without `-enable-principal` the verb clears only the workload row's disabled mark. It refuses a workload
  whose principal is disabled (`the workload's principal is disabled; add -enable-principal to enable it
  too`) and writes nothing. `Store.Disable` and the restore fence disable both rows, so such a workload needs
  the flag.
- The verb refuses while the workload holds a live bootstrap value (`the workload holds a live bootstrap
  value; revoke it first`) and writes nothing, not even an audit row: the value would be accepted again after
  `enable`. Rotate and create are refused while the workload is disabled; revoke works.
- With `-enable-principal` the verb also clears the principal's disabled mark, in the same transaction and
  the same audit row (`principal_enabled` = `true`, `principal_id`). It refuses a principal that is not of
  kind `workload`.
- The dry run prints what it would enable. A workload with nothing disabled writes nothing.
- The verb mints no credential.

Recovery after the restore fence or `Store.Disable` (both rows disabled): revoke, then enable, then create.

```sh
auth-migrate workloads bootstrap-disable -workload NAME -apply
auth-migrate workloads enable -workload NAME -operator ID -enable-principal -apply
auth-migrate workloads bootstrap-create -workload NAME -out /secure/dir/NAME.bootstrap -apply
```

`bootstrap-disable` revokes every live value; `bootstrap-revoke -credential ID -reason compromised -apply`
revokes one. Deliver the new file to the workload's secret mount.

## A verb fails

- A store refusal or failure prints `workloads error: <verb> failed (<class>)` and exits 1. When the out-file
  or its directory could not be written, the verb prints `workloads error: <verb>: <reason> (nothing was
  registered)` and exits 1 before it reads the store.
- A failed create or rotate removes its out-file. Check that the path is gone.
- Client error after the commit: if the connection drops or the run times out while the commit is sent, the
  row can be live although the verb reported a failure and removed the file. Nobody then holds that value. Run
  the dry run (`bootstrap-create -workload NAME -out PATH` without `-apply`; `PATH` must not exist) to list the
  live credentials. If one credential is live that you did not deliver, run
  `bootstrap-rotate -workload NAME -out PATH -overlap 0s -apply` (the old value is refused at once) and deliver
  the new file. If two are live (a rotation landed), run `bootstrap-disable -workload NAME -apply`, then
  `bootstrap-create`.

## Disable a workload

```sh
auth-migrate workloads disable -workload NAME
auth-migrate workloads disable -workload NAME -apply
```

The verb sets the disabled mark on the workload row and on its principal in one transaction and writes one
audit row. The exchange then refuses every binding of the workload and
`bootstrap-create` and `bootstrap-rotate` refuse it. A repeat writes nothing. It does not revoke live bootstrap
values: run `bootstrap-disable` as well, because `enable` refuses while one is live. Undo it with `enable` (see
above).

## Rebind a recreated ServiceAccount

A binding can pin the UID of its Kubernetes ServiceAccount. A ServiceAccount that is deleted and created again
with the same name has a new UID, and the exchange refuses it (`uid_mismatch`). A recreated ServiceAccount is a
new identity: moving the pin is a trust change. Read the new UID from the cluster, never from the failing
caller or its token:

```sh
kubectl get sa NAME -n NAMESPACE -o jsonpath='{.metadata.uid}'
auth-migrate workloads rebind -trust-domain T -issuer I -namespace NAMESPACE -service-account NAME -uid UID
auth-migrate workloads rebind -trust-domain T -issuer I -namespace NAMESPACE -service-account NAME -uid UID -apply
```

- The tuple names the binding; it is unique across workloads. An unknown tuple is refused and nothing is written.
- The change and its audit row (`auth.workload.binding_rebound`, with `binding_id` and `binding_disabled`)
  commit in one transaction. The old UID is refused at once and the new one accepted.
- The same UID is a no-op. A binding without a pin gets one.
- Rebind never enables a disabled binding (`binding_disabled` = `true` in the audit row). Use `binding-enable`.
- The verb never prints a UID.

## Disable or enable one binding

```sh
auth-migrate workloads binding-disable -trust-domain T -issuer I -namespace NAMESPACE -service-account NAME -apply
auth-migrate workloads binding-enable -trust-domain T -issuer I -namespace NAMESPACE -service-account NAME -apply
```

`binding-disable` makes the exchange refuse that one binding at once and leaves the workload and its other
bindings as they are. `binding-enable` clears the mark. Each writes one audit row
(`auth.workload.binding_disabled`, `auth.workload.binding_enabled`); a repeat writes nothing. Without `-apply`
the verb is a dry run.
