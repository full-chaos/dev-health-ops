---
page_id: op-rb-external-push-lifecycle
summary: Turn on External Push lifecycle events in the right order, backfill old tokens, rotate on a leak, and diagnose missing or late events.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# External Push lifecycle events

Use this page to run the lifecycle events of External Push credentials. Authority and settings: [External Push credential authority](../../reference/auth/external-push-authority.md). Administrator steps: [Manage External Push credentials](../security/external-push-credentials.md). Use placeholders for every value. Never print a token, a DSN or a Secret.

## Turn the events on

The roll order is fixed. The full text is in [auth control plane deployment](../../contribute/architecture/auth-control-plane-deployment.md) (External Push lifecycle events).

1. Privileged step: create the `NOLOGIN` writer role and grant it to the migration login.
2. Run `auth-migrate` with `AUTH_PUSH_LIFECYCLE_WRITER_ROLE` and `AUTH_PUSH_LIFECYCLE_CALLER_ROLES` set.
3. Only then roll go-api with `AUTH_PUSH_LIFECYCLE_EVENTS=true`. With the flag on and the grants missing, token writes fail as 500 and roll back.
4. Roll auth-service with `AUTH_PUSH_PROJECTION=on`. Check readiness `push_projection_reader`.

## Backfill old tokens

A token made before the events have no aggregate and no `created` event.

1. Dry run: `dho push-lifecycle backfill-created -dsn-env <NAME>`. It counts and writes nothing.
2. Check the count. Run again with `-apply` to write the events. A rerun is safe.
3. Output is counts only. `-batch` is 1 to 10000.
4. Reading the DSN without a read grant on the auth schema works.

## A token leaked

1. Revoke the token. See [Manage External Push credentials](../security/external-push-credentials.md).
2. Create a replacement, update the consumer, and check one request.
3. If the events are on, check that the `revoked` event reached auth-service: readiness `push_projection_reader` is ready and the credential shows as ended.
4. Do not turn the flags off to fix a missing event.

## Troubleshooting matrix

| Symptom | Check | Cause | Action |
| --- | --- | --- | --- |
| Token create, rotate or revoke returns 500 after the flag is on | go-api login has `EXECUTE` on the append function | Grants not applied before the flag | Apply the roll order above; roll the flag back to off meanwhile |
| `auth-migrate` stops naming the writer role | Role exists, cannot log in, membership inherits | Step 1 missing or a `SET`-only membership | Redo step 1 with the default inherit membership |
| No `created` event for an old token | Dry run count of `backfill-created` | Token made before the events | Backfill |
| No `expired` event for an expired token | Flag on in the worker; token expired within 30 days and never revoked | Sweeper idle (flag off) or token out of the window | Turn the flag on; an older token needs the operator to revoke it |
| Readiness `push_projection_reader` not ready | Reader logs and the outbox rows | Reader degrades after repeated failures on a row | Fix the cause; the reader retries parked rows with a delay |
| Revoked token still shown active in auth-service | Reader ready; flag on | Projection lag. go-api already refuses the token | Wait for the reader; the record is not a gate |
| Drift alert fires on old tokens | After the drift alert and the DSN read path are live | Tokens with no created event | Backfill, then the alert clears |
