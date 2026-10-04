# Sync dispatch transport routes v1

`transport-routes.json` is the language-neutral, validation-only contract for
the four existing sync-dispatch outbox wakeups. Its exact Draft 2020-12 shape
is defined by `transport-routes.schema.json`. It does not activate a production
transport, claim work, or publish a message.

All four kinds are checked in at `route: river` with `rollback_route: none`:
no Celery producer serves any of these kinds in any deployment, so there is
no supported rollback target left below `river`. Editing this artifact alone
still cannot activate or roll back a transport; that remains the audited
durable route-row controller's job (`internal/syncroute`).

All four wakeups are `at_least_once`. `post_sync` uses the same live-claim,
publish-or-insert, and terminal-mark transaction boundary as the other kinds.
On a publish or insert failure the claim is released with bounded backoff. The
post-sync consumers are generation-safe: readers select the newest compute
generation per logical key, so a re-drive cannot inflate their result.

## No bridge calls into the Python api

No native Go coordinator kind calls the Python api. The synchronous HTTP bridge
into `src/dev_health_ops/api/internal/worker_sync.py` was removed with the
Python api. `dispatch_sync_run` runs credential resolution and the six
per-provider budget estimators in-process in Go (`internal/syncbudget`,
CHAOS-6243), and reference discovery runs natively in Go. The error names in
`internal/syncdispatchruntime/bridge.go` still carry the word "bridge" for the
call path they replaced.
