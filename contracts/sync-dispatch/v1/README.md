# Sync dispatch transport routes v1

`transport-routes.json` is the language-neutral, validation-only contract for
the four existing sync-dispatch outbox wakeups. Its exact Draft 2020-12 shape
is defined by `transport-routes.schema.json`. It does not activate a production
transport, claim work, or publish a message.

All checked-in routes and rollback routes remain `celery`. The current Celery
reconciler is therefore the only mutation owner. A later, separately approved
migration may select `river` per kind, but its rollback route remains `celery`;
editing this artifact alone cannot activate that migration.

All four wakeups are `at_least_once`. `post_sync` uses the same live-claim,
publish-or-insert, and terminal-mark transaction boundary as the other kinds.
On a publish or insert failure the claim is released with bounded backoff. The
post-sync consumers are generation-safe: readers select the newest compute
generation per logical key, so a re-drive cannot inflate their result.
