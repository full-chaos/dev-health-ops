# Sync dispatch transport routes v1

`transport-routes.json` is the language-neutral, validation-only contract for
the four existing sync-dispatch outbox wakeups. Its exact Draft 2020-12 shape
is defined by `transport-routes.schema.json`. It does not activate a production
transport, claim work, or publish a message.

All checked-in routes and rollback routes remain `celery`. The current Celery
reconciler is therefore the only mutation owner. A later, separately approved
migration may select `river` per kind, but its rollback route remains `celery`;
editing this artifact alone cannot activate that migration.

`dispatch_sync_run`, `finalize_sync_run`, and `reference_discovery` are
`at_least_once`. `post_sync` is intentionally
`at_most_once_mark_before`: the outbox row is marked dispatched before
publication and is not re-armed if that publication fails, preventing duplicate
post-sync fanout until its downstream consumers are replay-safe.
