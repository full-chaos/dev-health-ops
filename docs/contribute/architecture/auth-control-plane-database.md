---
page_id: con-auth-cp-database
summary: Who owns what in the Auth Control Plane database - the auth schema, the separate migration lineage (ACP-ADR-04), the migration and runtime logins, the runtime privilege manifest, how to check and apply migrations, and what a backup and restore must cover.
content_type: architecture
owner: engineering
source_of_truth:
  - internal/storage/postgres/authschema/authschema.go
  - internal/storage/postgres/authschema/apply.go
  - internal/storage/postgres/authschema/grants.go
  - internal/storage/postgres/authschema/state.go
  - internal/storage/postgres/authschema/migrations
  - cmd/auth-migrate/main.go
applicability: current
lifecycle: active
---

# Auth Control Plane database: roles, migrations and backup

The Auth Control Plane owns one PostgreSQL **schema** (default `auth`,
`AUTH_DATABASE_SCHEMA`) in the same PostgreSQL instance as the platform, not a
separate cluster. Sharing the instance is what lets a security mutation and its
outbox event commit in one transaction ([ACP-ADR-04](auth-control-plane-adrs.md)).
{: .fc-page-lede }

## Two logins, one rule

| Login | Used by | May do |
| --- | --- | --- |
| **Migration** (`AUTH_MIGRATION_DATABASE_URI`) | `auth-migrate` only | DDL. It owns every object it creates. |
| **Runtime** (`AUTH_DATABASE_URI`) | `auth-service` | The DML listed in the manifest below, and nothing else. It owns no DDL. |

`auth-migrate` refuses a connection that authenticates **as** the runtime role
(`authschema.Apply`), because that would make the runtime role the owner of the
schema and the permanent holder of DDL: the posture the ADR forbids, reached by
a run that otherwise looks successful. `auth-service` has no migration code
path; readiness only observes that the schema is visible.

## A second migration lineage

ACP-ADR-04 left open whether to extend the ops alembic lineage. It was ruled on
2026-09-02 in favour of a **second, independent lineage**: the alembic lineage
has head revisions pinned as literals across many test files, so sharing it
would turn every auth migration into a cross-cutting change. The auth lineage
lives in `internal/storage/postgres/authschema/migrations/` as embedded SQL
named `NNNN_description.sql`, applied in order with its own version table,
`<schema>.schema_migrations`. A malformed name, a duplicate ordinal or a gap
fails at load, before anything is applied.

| Migration | Adds |
| --- | --- |
| `0001` | Principals, users, external identities. |
| `0002` | Organizations, memberships, service accounts, workloads. |
| `0003` | Sessions, refresh credentials, signing-key metadata, credential registry (hashes and references only, never a bearer value). |
| `0004` | The authorization model: actions, roles, grants, policy revisions, entitlement snapshots. |
| `0005` | `security_audit_events` and `auth_outbox_events`. |
| `0006` | The commit guard: a trigger that refuses a write to a guarded table outside a transaction armed by `internal/audit`. See [audit and outbox](auth-control-plane-audit-outbox.md). |
| `0007` | The remaining credential registry columns. |

Wave 1 migrations are **additive only** (a test over the embedded SQL enforces
it): no existing row moves, no writer changes, no drops.

## Applying and checking

```text
auth-migrate                 # apply pending migrations, then grant the runtime role
auth-migrate --check         # report the lineage position; issues no DDL
auth-migrate --version
```

Required: `AUTH_MIGRATION_DATABASE_URI` (or `_FILE`) and, to apply,
`AUTH_RUNTIME_DATABASE_ROLE`; optional `AUTH_DATABASE_SCHEMA`. A run takes a
PostgreSQL advisory lock for its whole duration, so two migrators cannot race,
and is bounded by a five-minute budget. `--check` prints
`auth schema current at version N` and exits 0, or reports the current and head
versions and exits 1; it never creates the schema or the version table. Run the
check as a deployment gate. In Compose and Helm the migration runs as a
one-shot service and a pre-install/pre-upgrade hook; see
[deployment](auth-control-plane-deployment.md).

## The runtime privilege manifest

`authschema.RuntimePosture()` is data, not a sequence of GRANTs, so what
`auth-migrate` grants, what the posture test asserts the role holds, and what a
reader audits cannot drift apart. `ApplyRuntimeGrants` revokes everything and
grants back exactly the manifest. Highlights:

- `security_audit_events`, `policy_revisions` and `entitlement_snapshots` are
  **insert and select only**: an audit row the runtime can edit is not an audit
  row.
- `signing_keys`, `actions`, `roles` and `role_actions` are **select only**: the
  service cannot mint itself a key or widen its own authority.
- `platform_role_assignments` has no update: an elevation changes by revoke then
  grant, leaving two audit rows.
- There is no field in the manifest type for a DDL privilege.

Live tests (`posture_integration_test.go`) prove the role holds exactly this and
cannot escalate (for example by `SET ROLE` to the schema owner).

## Backup and restore ownership

- The auth schema is part of the platform PostgreSQL instance: a backup of the
  instance covers it, and a restore brings back the `schema_migrations` row with
  the data, so `auth-migrate --check` after a restore tells you whether the
  restored database matches the binary.
- The **signing key is not in the database.** `signing_keys` holds metadata
  only. Back up the key file with the secret store that holds it, not with the
  database, and do not restore a database without the key that its `kid` rows
  name.
- Restoring the database to an older lineage position then running a newer
  `auth-migrate` re-applies the later migrations in order; never edit
  `schema_migrations` by hand.
- Owner: the platform operators own the instance, the logins and the backups;
  the lineage is owned by this repository and changes only through a migration.

## Verified against

- `internal/storage/postgres/authschema/authschema.go` (package comment: second lineage, ownership, additive only), `apply.go` (`advisoryLockKey`, `versionTable`, role-equality refusal), `state.go` (`Check`).
- `internal/storage/postgres/authschema/grants.go` (`RuntimePosture`, `ApplyRuntimeGrants`).
- `cmd/auth-migrate/main.go`: flags `--check` and `--version`, `AUTH_MIGRATION_DATABASE_URI`, `AUTH_RUNTIME_DATABASE_ROLE`, `AUTH_DATABASE_SCHEMA`, `AUTH_SESSION_READER_ROLES`, `runBudget`.
- `internal/storage/postgres/authschema/session_readers.go`: the session reader roles' grant (`SessionReaderColumns`) and its posture check. go-api and query-api (`internal/api/policy` `PGStore.SessionState`) read a session to end an access token that carries `sid` once the session is revoked.
- `internal/storage/postgres/authschema/migrations/` (files `0001_principals_and_identities.sql` through `0007`).
