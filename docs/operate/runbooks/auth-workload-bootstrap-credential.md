---
page_id: op-rb-auth-workload-bootstrap-credential
summary: Seed, create, rotate, revoke and emergency-disable the bootstrap credential of a workload with auth-migrate, name the audit event of each step, find the credential id, and recover when a verb fails.
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
- Every step that writes names its audit event and reason below. A dry run writes no audit row.
- A credential id is not a secret. Only the value is. Every verb prints ids, and none prints a value.
- When the exchange refuses a value, the caller gets one answer. The reason is in the auth-service log line and
  the counter `auth.workload.exchanges`: see
  [Troubleshooting by reason](auth-workload-kubernetes-federation.md#troubleshooting-by-reason) (subject
  `bootstrap`).
- `bootstrap-create` and `bootstrap-rotate` refuse a disabled workload. The class says which row is disabled:
  `the workload row is disabled` or `the workload's principal is disabled, its row is not`. Each class names
  the `enable` verb that recovers it (see Enable a disabled workload). `bootstrap-revoke` and
  `bootstrap-disable` work on a disabled workload.

## Seed the workload row

`bootstrap-create` refuses a workload that is not in the registry (`workload not registered`). Seed the registry
first. The [workload registry](../../reference/auth/workload-registry.md) lists the rows.

```sh
auth-migrate workloads seed
auth-migrate workloads seed -apply
```

Audit events of `seed`: a new row writes `auth.workload.registered` (reason `principal_provisioned`). A changed
audience list writes `auth.workload.audiences_changed`. A changed action list writes
`auth.workload.allowed_actions_changed`. A row that is equal already writes no audit row. `seed` mints no
credential.

## Create

```sh
auth-migrate workloads bootstrap-create -workload NAME -out /secure/dir/NAME.bootstrap
auth-migrate workloads bootstrap-create -workload NAME -out /secure/dir/NAME.bootstrap -apply
```

Audit event: `auth.workload.bootstrap_created` (reason `credential_issued`).

The dry run lists the live credentials of the workload, one line each: `live credential=ID expires_at=TIME`. A
workload with a live credential is refused: rotate it. The apply prints `credential=ID` and `expires_at=TIME` of
the new credential. Keep the id: you need it to revoke.

## Compose one-shots

The Compose stack (`compose.yml`, profile `auth`) creates the first credential of the `api` workload with three
one-shot services. No running service depends on them, because a later `up` would run `bootstrap-create` again
and it refuses a workload with a live credential.

| Service | What it does | Audit event |
| --- | --- | --- |
| `auth-workload-volume` | Sets the owner and mode of the volume directory `/run/workload` (user 65532, mode 0700), only when they differ. | no audit row |
| `auth-workload-seed` | Runs `workloads seed -apply`. | `auth.workload.registered`, `auth.workload.audiences_changed`, `auth.workload.allowed_actions_changed` (see [Seed the workload row](#seed-the-workload-row)) |
| `auth-workload-bootstrap` | Runs `workloads bootstrap-create -workload api -out /run/workload/api.bootstrap -apply`. | `auth.workload.bootstrap_created` (reason `credential_issued`) |

```sh
docker compose --profile auth run --rm auth-workload-bootstrap
```

- The one command is enough. Compose starts `auth-workload-volume` and `auth-workload-seed` first, in that
  dependency order (`auth-workload-seed` also waits for `auth-grants`), and each of them is safe to run again.
- The value is written once to `/run/workload/api.bootstrap` (mode 0600) in the named volume `workload_bootstrap`.
  The registry keeps only its digest. The command prints the credential id (`credential=ID`), never the value.
- go-api mounts the volume read-only at `/run/workload` and reads the file only when
  `AUTH_SESSION_ISSUE_CREDENTIAL` or `AUTH_PROVISIONING_CREDENTIAL` selects `workload`. auth-service never mounts
  it. To accept the value, auth-service needs `AUTH_WORKLOAD_BOOTSTRAP=true` and `AUTH_TOKEN_ISSUER_URL` (go-api's
  `AUTH_WORKLOAD_ISSUER` follows it).
- A second `run --rm auth-workload-bootstrap` is refused (`the workload has a live bootstrap value; rotate it`)
  and removes its own file. If the volume was removed and the database was not, the registry still holds the live
  credential: find its id with the dry run below, revoke it, then run the one-shot again. `down -v` removes the
  volume and the database volume together.
- The compose file does not script a rotation. The verbs below work against the same database with the migration
  login. Put the new file in place of `/run/workload/api.bootstrap` yourself, and deliver it before the overlap
  ends.

## Rotate

```sh
auth-migrate workloads bootstrap-rotate -workload NAME -out /secure/dir/NAME.bootstrap.next -overlap 10m -apply
```

Audit event: `auth.workload.bootstrap_rotated` (reason `credential_rotated`).

The new value is live at once. The old value stays live for at most `-overlap` (0 to `1h`); its expiry is only
ever lowered. Deliver the new file before the overlap ends. A second rotation is refused until the overlap ends.
The apply prints the new `credential=ID` and the `previous credential=ID` with its lowered expiry.

Rotate before the value expires. The exchange refuses an expired value (reason `expired`). The default expiry is
`720h`: read `expires_at` from the dry run (`bootstrap-rotate -workload NAME -out PATH -overlap 10m`, `PATH` not
yet existing) and rotate with time to spare, so the new file reaches the workload's secret mount first.

## Revoke

```sh
auth-migrate workloads bootstrap-revoke -credential ID -reason owner_request
auth-migrate workloads bootstrap-revoke -credential ID -reason owner_request -apply
```

Audit event: `auth.workload.bootstrap_revoked` (reason `owner_request` or `compromised`, as you pass it).

Use `-reason compromised` for a leaked value. Revocation is terminal: nothing makes the value live again. To
undo a rotation, revoke the new value; the old value keeps its lowered expiry. To recover, create a new value.

Find `ID` without any secret. Run the dry run of `bootstrap-create` or `bootstrap-rotate` for the workload (the
`-out` path must not exist yet; the dry run writes no file). It prints `live credential=ID expires_at=TIME` for
each live credential. The dry run of `bootstrap-revoke` prints `credential=ID status=STATE` for the id you
give it, so check the id before the apply. The dry run needs only the migration login, never a credential value.

`bootstrap-create` is refused while the workload has a live credential. Revoke the live credential first, then
create. To replace a value without a gap, rotate instead.

## Emergency disable

```sh
auth-migrate workloads bootstrap-disable -workload NAME
auth-migrate workloads bootstrap-disable -workload NAME -apply
```

Audit event: `auth.workload.bootstrap_disabled` (reason `compromised`).

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

### Recovery after the restore fence

After the restore fence or `Store.Disable` both rows are disabled: revoke, then enable, then create. Each step
writes its audit event: `auth.workload.bootstrap_disabled` (reason `compromised`), then `auth.workload.enabled`
(reason `workload_enabled`), then `auth.workload.bootstrap_created` (reason `credential_issued`).

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
