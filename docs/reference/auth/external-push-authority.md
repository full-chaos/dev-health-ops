---
page_id: ref-auth-external-push-authority
summary: Who decides an External Push credential, what auth-service holds about it, the lifecycle events, and the settings that turn each part on.
content_type: reference
owner: platform-operations
applicability: current
lifecycle: active
---

# External Push credential authority

An External Push token (`fcpush_…`, credential class `external_push_ingest_token`) is a customer-push write credential. This page says which service decides it, what auth-service holds about it, and which settings control that. It lists only what is merged on the main branch.

For ACR ownership and lifecycle delivery, see [ACR credential authority](acr-credential-authority.md).

## Who decides

- go-api owns the token. It creates, rotates, revokes and validates it, through the `/api/v1/admin/customer-push` routes. Rotation takes effect at once: the old token stops working when the replacement is issued.
- auth-service holds a projection of the token. It reads lifecycle events from the auth outbox and writes an `external_source` principal (one per organization and source), a `credential_registry` row and the projected state. It does not validate a push token and has no write path to the domain store.
- The projection is a record, not a gate. A late or lost event does not make go-api accept or refuse a token.

## Lifecycle events

go-api appends an event to the auth outbox in the same transaction as the token change, through the auth-schema function `append_external_push_lifecycle_event`. The events are `created`, `rotated`, `revoked` and `expired`. Each event carries a `binding`: `source` (with `source_id`) or `organization` (no `source_id`).

Rules the projection applies:

- A duplicate, out-of-order or replayed event does not reactivate a revoked or rotated credential and does not widen its scopes.
- A terminal event (`revoked` or `expired`) ends the credential once. A second terminal event for the same credential changes nothing.
- A terminal event applies over a gap in the revision numbers. A refused revision is decided and is not retried forever.
- The reader takes the restore-fence floor as an input, so it does not apply rows from before a database restore.

## Settings

| Setting | Where | Default | Effect |
| --- | --- | --- | --- |
| `AUTH_PUSH_LIFECYCLE_EVENTS` | go-api and the worker | off | `true` makes each token create, rotate and revoke append its events. Anything but `true` or `false` stops start-up. Needs the writer and caller roles first. |
| `AUTH_PUSH_LIFECYCLE_WRITER_ROLE`, `AUTH_PUSH_LIFECYCLE_CALLER_ROLES` | `auth-migrate` | empty | Set the owner role of the append function and the logins that may call it. See the [auth service configuration](configuration.md). |
| `AUTH_PUSH_PROJECTION` | auth-service | `off` | `on` runs the projection reader. Readiness check `push_projection_reader`. |

The expired event comes from a sweeper job (`sync.external_push_expiry_sweep`). The job is registered in every worker; with `AUTH_PUSH_LIFECYCLE_EVENTS` off each run touches nothing. A run reads only tokens that expired in the last 30 days and never revoked.

## Source of each fact

- Flag, default and grant warning: `internal/auth/pushevent/events_flag.go`.
- Reader, stream name and readiness check: `internal/auth/pushprojection/reader.go`.
- Terminal event once, window and sweep: `internal/jobs/pushexpiry/handler.go`, `internal/workerservice/push_expiry.go`.
- Admin routes: `internal/apiservice/responsemodel_admin_customer_push.go`.
- Roll order of the writer role: [auth control plane deployment](../../contribute/architecture/auth-control-plane-deployment.md).

## Not yet in this page

- The projection reconcile and the drift alert: not built yet.
- A refusal of a half-set writer or caller role in `auth-migrate`: not built yet.
