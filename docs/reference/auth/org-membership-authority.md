---
page_id: ref-auth-org-membership-authority
summary: Which table is the authority for organizations, memberships and platform roles, the provisioning event that records each change, how to reconcile the two copies, how to roll back and restore, and what retention exists today.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/jsonschema/provisioning-event.v1.schema.json
  - internal/auth/usersbackfill/reconcile_orgs.go
  - internal/auth/usersbackfill/command.go
  - internal/auth/membership/store.go
  - internal/auth/restorefence/specs.go
  - internal/audit/outboxloop.go
  - internal/audit/reaper.go
applicability: current
lifecycle: active
---

# Organization and membership authority

This page is the one overview of who owns organization, membership and platform-role data, and how an operator
checks, rolls back and restores it. It states what the code does now. It links the pages that hold the detail.
{: .fc-page-lede }

## The authority model (D6651)

| Data | Authority | The `auth` schema holds |
| --- | --- | --- |
| Users | `public.users` | Credentials, password and lockout state, and a copy of each shared column ([deployment, phase 4](../../contribute/architecture/auth-control-plane-deployment.md)). |
| Organizations | `public.organizations` | A copy. It is never a decision input. |
| Memberships | `public.memberships` | A copy. It is never a decision input. |
| Platform roles | `auth.platform_role_assignments`, once `AUTH_PLATFORM_ROLES_OWNED` is on | The authority itself (the one exception, D6659). |

- Orgs owned (phase 5, CHAOS-10998, ticket 4.13) is **cancelled** (D6651). The chart refuses phase 5 and refuses
  `AUTH_ORGS_OWNED` in any `extraEnv` list ([deployment](../../contribute/architecture/auth-control-plane-deployment.md),
  [configuration](configuration.md)). CHAOS-12718 removes the name. It is still parsed in `internal/auth/authconfig/config.go:127` and
  read by `orgs-backfill` (`internal/auth/usersbackfill/command.go:272`) at the time of writing.
- auth-service writes through the public tables. One membership write changes `public.memberships` and
  `auth.memberships` in one transaction (`internal/auth/membership/store.go:501-525`). The organization create and onboard routes
  write both schemas in one commit ([provisioning routes](configuration.md#provisioning-routes)).
- go-api sends its organization and member writes to auth-service only when `AUTH_PROXY_ORG_PROVISIONING=true`. With the switch off,
  go-api writes the public tables only and `auth.memberships` drifts (CHAOS-11779,
  [users-owned runbook](../../operate/runbooks/auth-users-owned-flip-and-rollback.md)).
- Platform roles: with `authService.platformRolesOwned: true`, platform authority comes from `auth.platform_role_assignments`, not from
  `public.users.is_superuser` ([runbook](../../operate/runbooks/auth-platform-roles-flip-and-rollback.md)).
  Platform-admin roles and organization roles stay separate ([platform administrator](platform-admin.md),
  [organization roles](org-member-roles.md)).

## The provisioning event

Each security change writes one `provisioning-event.v1` row to `auth_outbox_events` in the same transaction as the state change and the
audit row ([audit and outbox operations](../../contribute/architecture/auth-control-plane-audit-outbox.md), "The write path").
The schema is `contracts/auth/v1/jsonschema/provisioning-event.v1.schema.json`, with examples in `contracts/auth/v1/examples/provisioning-event/`.

- Required members: `schema_version`, `aggregate_type`, `aggregate_id`, `event_type`, `idempotency_key`, `created_at`, `payload`.
- `aggregate_type` is one of `principal`, `user`, `external_identity`, `organization`, `membership`, `platform_role_assignment`,
  `service_account`, `workload`, `resource_grant`, `invite`, `session`, `signing_key`.
- `event_type` is `<aggregate_type>.<verb>` from a closed list per aggregate. The organization verbs are `created`, `updated`,
  `suspended`, `reactivated` and `sessions_revoked`. The membership verbs are `invited`, `activated`, `role_changed`, `suspended` and
  `removed`. The platform-role verbs are `granted` and `revoked`.
- `idempotency_key` is `aggregate_type:aggregate_id:revision`. The revision is per aggregate, never global.
- Consumer rule (schema description): apply an event when its revision is the last applied revision plus 1. A revision at or below the last
  applied one is a duplicate or stale delivery and is ignored. A larger one is a gap: do not apply it, and resynchronise.
  So an out-of-order or duplicate event cannot restore an older state.
- `change` is an allowlist per aggregate. No member can hold a secret, an address or a provider subject.

## Reconcile

`dho auth orgs-reconcile` reads one snapshot of `public.organizations` and `public.memberships` against the auth schema. It prints class
names, counts and organization ids, never a value (`internal/auth/usersbackfill/reconcile_orgs.go`).

- Default mode exits 1 on any of: `org_missing_in_auth`, `org_extra_in_auth`, `org_slug_diff`, `org_name_diff`, `org_active_diff`,
  `membership_public_missing`, and a membership auth holds inactive while public has the member (`reconcile_orgs.go:16-41`, `:119-129`).
- `-exact` also gates `membership_auth_missing`, `membership_role_diff` and `membership_duplicate_public_source`
  (`reconcile_orgs.go:16-41`, `command.go:380-390`).
- At most 50 mismatching organization ids are listed (`reconcile_orgs.go:66`).
- After a rollback of `AUTH_PROXY_ORG_PROVISIONING`, `org_active_diff` must be 0 before the switch is turned on again
  ([configuration](configuration.md#provisioning-routes)).
- `dho auth users-reconcile` does the same for users. With `-platform-roles-gate` it also gates `no_active_platform_admin`
  (`command.go:46-48`).
- Gap: the chart's `authService.backfill.verb` accepts `users-backfill`, `users-reconcile`, `orgs-backfill` and `legacy-link-purge`
  only (`deploy/helm/dev-health/templates/auth-backfill-job.yaml:66`). `orgs-reconcile` has no Job form. Run it with `dho`.

## Roll back and restore

There is no orgs-owned flip, so there is no orgs-owned rollback. Each flip that exists has its own runbook, and each rolls back one step.

| Change | Runbook |
| --- | --- |
| Users owned (phase 4) | [users-owned flip and rollback](../../operate/runbooks/auth-users-owned-flip-and-rollback.md), [flip-back grants](../../operate/runbooks/auth-users-flip-back-grants.md) |
| Platform roles owned | [platform roles flip and rollback](../../operate/runbooks/auth-platform-roles-flip-and-rollback.md) |
| Sessions | [sessions flip and rollback](../../operate/runbooks/auth-sessions-flip-and-rollback.md) |
| Legacy compatibility | [legacy compat and rollback](../../operate/runbooks/auth-legacy-compat-and-rollback.md) |

A point-in-time restore of the auth schema uses the restore fence ([procedure](../../operate/maintain/backup-and-restore.md#restoring-the-auth-schema-to-a-point-in-time-the-restore-fence)).
It keeps the newer revoking state of the source and does not bring back what was removed (`internal/auth/restorefence/specs.go`):

- A restored membership that the source holds `suspended` or `removed` ends in that state (`specs.go:183-185`).
- A restored organization that the source disabled ends disabled (`specs.go:174-175`).
- A restored pending invite that the source holds accepted, revoked or no longer holds ends revoked (`specs.go:177-181`).
- A `public.memberships` row the source deleted is deleted again (`specs.go:265`).
- A role lowered after the restore target is lowered again. A role raised after it is not carried (`specs.go:187-189`).
- A platform-role grant made after the restore target is never inserted. The fence names it in `regrant_platform_role`
  ([platform roles runbook](../../operate/runbooks/auth-platform-roles-flip-and-rollback.md)).

## Retention

Only one auth retention rule exists in code: the outbox reaper.

- The reaper deletes **published** outbox events older than 7 days, at most 1000 per sweep, once per second
  (`internal/audit/outboxloop.go:58`, `internal/audit/reaper.go`). An unpublished event is never deleted, at any age (`reaper.go:24-28`).
- Deactivation denies first. The ADR says physical deletion and PII cleanup come later under a retention policy
  ([ADR](../../contribute/architecture/auth-control-plane-adrs.md), line 792).

Gaps. At origin/main `2f0f26be`, a search of `internal/` and `cmd/` for `DELETE FROM` finds no statement that deletes from these tables.
The names below are gaps, not rules:

- `security_audit_events` (the audit rows): no retention.
- `auth_outbox_events` that stay unpublished: kept by design, with no limit or alert on age beyond the lag gauge.
- `aggregate_revisions`, and the `sessions` and `refresh_credentials` of the auth schema: no purge. (The only refresh-token purge found is
  `refresh_tokens` of the legacy schema, `internal/maintenancecli/maintenance.go:121`.)
- Removed or deactivated principals, memberships, service accounts, workloads and grants: the rows stay with a status. No job removes them
  after a delay.
- No setting exists to change the 7 days: `PublishedRetention` is set in `DefaultLoopConfig` only.
- The code comments of `internal/auth/restorefence/specs.go` still call the public membership table a "legacy mirror". The authority is
  the public table (D6651).
