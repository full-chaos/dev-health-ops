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
- With `-enable-principal` the verb also clears the principal's disabled mark, in the same transaction and
  the same audit row (`principal_enabled` = `true`, `principal_id`). It refuses a principal that is not of
  kind `workload`.
- The dry run prints what it would enable. A workload with nothing disabled writes nothing.
- The verb mints no credential and revokes none. A bootstrap value that was live before the disable (and has
  not expired) is accepted again after `enable`. So after `enable`, run `bootstrap-rotate -overlap 0s` (or
  `bootstrap-create` when no value is live) and deliver the new file.

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
