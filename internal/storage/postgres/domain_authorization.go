package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// rolePostureQuery proves the connected identity holds exactly ONE role's
// declared semantic-runtime posture: the required_table_privileges and
// column_scoped_privileges manifests are query parameters (unnest-bound
// arrays), never literal VALUES lists, so the same ~230 lines of hardened
// predicate logic below run once per role rather than being copy-pasted per
// role. The expected role, River schema, and both manifests are query
// parameters, never interpolated identifiers or SQL text. Catalog predicates
// use effective privileges so inherited administrator or DDL access closes
// readiness as well as direct grants.
//
// One call proves one direction only: THIS role holds exactly its declared
// set, because every predicate that walks "other relations"
// (other_public_relations, river_relations, and the rest) asserts zero
// privilege of any kind, by any route, on anything outside the CALLING
// role's own manifest. It says nothing about any other role. The full
// cross-role property a multi-role deployment depends on — role A does not
// hold role B's exclusive privileges, AND role B does not hold role A's —
// only emerges from calling this once per role, each against that role's own
// accurate manifest: a privilege wrongly granted to the wrong role is caught
// exclusively by that WRONG role's own check finding an undeclared,
// unshared table, never by the role it was meant for. See CheckRolePosture's
// doc comment for the runtime story this depends on. No separate "and the
// other role does not hold it" check needs to exist in this query itself —
// but no single invocation of it proves that property alone, either.
const rolePostureQuery = `
WITH required_table_privileges(table_name, allow_insert, allow_update, allow_delete) AS (
	SELECT * FROM unnest($3::text[], $4::boolean[], $5::boolean[], $9::boolean[])
), required_tables AS (
	SELECT
		class.oid,
		required.allow_insert,
		required.allow_update,
		required.allow_delete
	FROM required_table_privileges AS required
	JOIN pg_catalog.pg_class AS class ON class.relname = required.table_name
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind IN ('r', 'p')
-- worker_job_completion_fences is deliberately NOT in required_table_privileges.
-- completed_at is server-owned (DEFAULT statement_timestamp()) and no domain
-- statement ever reads or writes it (joboutbox.MarkCompletionTx only inserts
-- completion_key; deletion is queue-side, queue_authorization.go). A
-- table-wide privilege would let the domain role forge completed_at and mint
-- a fence retention never reaps, so the domain role's posture here is
-- column-scoped instead of table-scoped, and required_table_privileges has
-- no way to express "this column only" — hence the separate CTEs below.
), column_scoped_privileges(table_name, column_name, privilege) AS (
	SELECT * FROM unnest($6::text[], $7::text[], $8::text[])
), column_scoped_relations AS (
	SELECT DISTINCT class.oid, class.relname
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind IN ('r', 'p')
		AND class.relname IN (SELECT table_name FROM column_scoped_privileges)
), column_scoped_privilege_types(privilege) AS (
	VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('REFERENCES')
), column_scoped_columns AS (
	-- Every real column of a column-scoped relation, so an excess privilege
	-- can be detected against the actual catalog rather than only against
	-- whichever columns already appear in a privileges catalog view.
	SELECT scoped.oid, scoped.relname, attribute.attname
	FROM column_scoped_relations AS scoped
	JOIN pg_catalog.pg_attribute AS attribute ON attribute.attrelid = scoped.oid
	WHERE attribute.attnum > 0
		AND NOT attribute.attisdropped
), other_public_relations AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind IN ('r', 'p', 'v', 'm', 'f')
		AND NOT EXISTS (
			SELECT 1
			FROM required_table_privileges AS required
			WHERE required.table_name = class.relname
		)
		AND NOT EXISTS (
			SELECT 1
			FROM column_scoped_privileges AS scoped
			WHERE scoped.table_name = class.relname
		)
), public_sequences AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind = 'S'
), river_relations AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = $2
		AND class.relkind IN ('r', 'p', 'v', 'm', 'f')
), river_sequences AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = $2
		AND class.relkind = 'S'
), river_functions AS (
	SELECT procedure.oid
	FROM pg_catalog.pg_proc AS procedure
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
	WHERE namespace.nspname = $2
), public_functions AS (
	SELECT procedure.oid
	FROM pg_catalog.pg_proc AS procedure
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
	WHERE namespace.nspname = 'public'
), member_roles AS (
	SELECT role.oid
	FROM pg_catalog.pg_roles AS role
	WHERE role.rolname <> current_user
), current_role_identity(oid) AS (
	SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user
)
SELECT
	current_user = $1
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_roles
		WHERE rolname = current_user
			AND rolcanlogin
			AND NOT rolsuper
			AND NOT rolcreatedb
			AND NOT rolcreaterole
			AND NOT rolreplication
			AND NOT rolbypassrls
	)
	AND NOT EXISTS (
		SELECT 1 FROM member_roles
		WHERE pg_has_role(current_user, oid, 'MEMBER')
	)
	-- Ownership is the one route that breaks every "holding a privilege WITH
	-- GRANT OPTION implies holding the plain privilege" argument this query
	-- otherwise relies on to treat required-ABSENT privileges as covered:
	-- an owner can REVOKE ALL on its own object (base has_*_privilege checks
	-- then read false) while PostgreSQL still treats ownership as carrying
	-- every grant option, letting it re-grant to itself, another role, or
	-- PUBLIC regardless of what it just revoked from itself. Confirmed
	-- empirically: CREATE SCHEMA s AUTHORIZATION owner; REVOKE ALL ON SCHEMA
	-- s FROM owner leaves has_schema_privilege(owner, s, 'CREATE') = false
	-- but has_schema_privilege(owner, s, 'CREATE WITH GRANT OPTION') = true.
	-- One "owns nothing at all" predicate closes the class across every
	-- object and privilege type at once, rather than adding an ownership
	-- carve-out to each option-form check individually, and is the more
	-- honest invariant regardless: a least-privilege runtime identity should
	-- own nothing. Verified both directions against a live server, not just
	-- the failing case: confirmed to reject a role that owns a schema and
	-- has revoked its own privileges on it (TestDomainAuthorizationRejectsSelfOwnedSchema),
	-- AND confirmed to pass for the posture runtimeGrantStatements actually
	-- provisions — both the domain and queue runtime roles were checked
	-- against a live server and found to own zero databases, schemas,
	-- relations, or functions, so this predicate does not fail closed on a
	-- working deployment.
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_database AS database, current_role_identity
		WHERE database.datdba = current_role_identity.oid
	)
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_namespace AS namespace, current_role_identity
		WHERE namespace.nspowner = current_role_identity.oid
	)
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_class AS class, current_role_identity
		WHERE class.relowner = current_role_identity.oid
			AND class.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
	)
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_proc AS procedure, current_role_identity
		WHERE procedure.proowner = current_role_identity.oid
	)
	AND NOT has_database_privilege(current_user, current_database(), 'CREATE')
	AND NOT has_database_privilege(current_user, current_database(), 'TEMPORARY')
	-- CONNECT itself is never asserted true — the query is already running as
	-- this role, so it is implied — but the option to re-delegate CONNECT is
	-- an ambient, ungated privilege no other predicate here would ever see.
	AND NOT has_database_privilege(current_user, current_database(), 'CONNECT WITH GRANT OPTION')
	AND has_schema_privilege(current_user, 'public', 'USAGE')
	-- Same ambient-privilege shape as CONNECT above, for the one schema
	-- privilege this role is required to actually hold.
	AND NOT has_schema_privilege(current_user, 'public', 'USAGE WITH GRANT OPTION')
	AND NOT has_schema_privilege(current_user, 'public', 'CREATE')
	AND (
		SELECT count(*) FROM required_tables
	) = (
		SELECT count(*) FROM required_table_privileges
	)
	AND (
		SELECT count(*) FROM column_scoped_relations
	) = (
		SELECT count(DISTINCT table_name) FROM column_scoped_privileges
	)
	AND NOT EXISTS (
		-- Every declared column privilege must actually be held, and not
		-- carry the option to re-delegate it: an option-bearing privilege
		-- would otherwise pass unnoticed on a declared pair, letting the
		-- domain role hand fence access to another role or PUBLIC itself.
		SELECT 1
		FROM column_scoped_privileges AS required
		JOIN column_scoped_relations AS scoped ON scoped.relname = required.table_name
		WHERE NOT has_column_privilege(
				current_user, scoped.oid, required.column_name, required.privilege
			)
			OR has_column_privilege(
				current_user, scoped.oid, required.column_name,
				required.privilege || ' WITH GRANT OPTION'
			)
	)
	AND NOT EXISTS (
		-- No column privilege beyond the declared set, catalog angle: reads
		-- information_schema.column_privileges directly. This alone is NOT
		-- sufficient — it is filtered to grants whose grantee literally is
		-- current_user, so it misses a column privilege the domain role
		-- holds only via PUBLIC or role membership. Kept as a second
		-- angle alongside the has_column_privilege sweep below, which
		-- resolves effective privilege and does not have that gap. A row
		-- with is_grantable = YES is excess even on a declared pair, since
		-- the declared posture never lets the role re-delegate its privilege.
		SELECT 1
		FROM information_schema.column_privileges AS granted
		JOIN column_scoped_relations AS scoped ON scoped.relname = granted.table_name
		WHERE granted.table_schema = 'public'
			AND granted.grantee = current_user
			AND (
				granted.is_grantable = 'YES'
				OR NOT EXISTS (
					SELECT 1
					FROM column_scoped_privileges AS required
					WHERE required.table_name = granted.table_name
						AND required.column_name = granted.column_name
						AND required.privilege = granted.privilege_type
				)
			)
	)
	AND NOT EXISTS (
		-- No column privilege beyond the declared set, effective-privilege
		-- angle: has_column_privilege resolves PUBLIC grants and role
		-- membership the same way has_any_column_privilege already does
		-- elsewhere in this query, so a column privilege granted to PUBLIC
		-- (e.g. on completed_at) is caught here even though the domain role
		-- was never named as grantee.
		SELECT 1
		FROM column_scoped_columns AS actual_column
		CROSS JOIN column_scoped_privilege_types AS privilege_type
		WHERE has_column_privilege(
				current_user, actual_column.oid, actual_column.attname, privilege_type.privilege
			)
			AND NOT EXISTS (
				SELECT 1
				FROM column_scoped_privileges AS required
				WHERE required.table_name = actual_column.relname
					AND required.column_name = actual_column.attname
					AND required.privilege = privilege_type.privilege
			)
	)
	AND NOT EXISTS (
		-- No table-wide privilege leakage on a column-scoped table.
		SELECT 1
		FROM column_scoped_relations AS scoped
		WHERE has_table_privilege(current_user, scoped.oid, 'SELECT')
			OR has_table_privilege(current_user, scoped.oid, 'INSERT')
			OR has_table_privilege(current_user, scoped.oid, 'UPDATE')
			OR has_table_privilege(current_user, scoped.oid, 'DELETE')
			OR has_table_privilege(current_user, scoped.oid, 'TRUNCATE')
			OR has_table_privilege(current_user, scoped.oid, 'REFERENCES')
			OR has_table_privilege(current_user, scoped.oid, 'TRIGGER')
			OR CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, scoped.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND NOT EXISTS (
		-- The required-flag columns only capture whether the base privilege
		-- is held, so a role that holds a required privilege WITH GRANT OPTION
		-- passes the base check unchanged (having the option always implies
		-- having the plain privilege) and could re-delegate it to another
		-- role or to PUBLIC itself. Check the option explicitly wherever the
		-- base privilege is required; where it is not required, the
		-- plain-form mismatch above already fails closed, since holding the
		-- option implies holding the plain form too.
		--
		-- has_table_privilege('... WITH GRANT OPTION') only sees the option
		-- on a TABLE-level ACL entry, though: a column-level grant such as
		-- GRANT SELECT (id) ON integrations TO domain_runtime WITH GRANT
		-- OPTION is invisible to it (has_table_privilege never inspects
		-- column ACLs at all, with or without the option). The
		-- has_any_column_privilege calls below close that: they resolve
		-- effective privilege the same way has_any_column_privilege already
		-- does elsewhere in this query, so PUBLIC- or membership-held
		-- column-level options are caught too, not just ones granted
		-- directly to the role by name.
		SELECT 1
		FROM required_tables
		WHERE NOT has_table_privilege(current_user, oid, 'SELECT')
			OR has_table_privilege(current_user, oid, 'SELECT WITH GRANT OPTION')
			OR has_any_column_privilege(current_user, oid, 'SELECT WITH GRANT OPTION')
			OR has_table_privilege(current_user, oid, 'INSERT') <> allow_insert
			OR (
				NOT allow_insert
				AND has_any_column_privilege(current_user, oid, 'INSERT')
			)
			OR (
				allow_insert
				AND (
					has_table_privilege(current_user, oid, 'INSERT WITH GRANT OPTION')
					OR has_any_column_privilege(current_user, oid, 'INSERT WITH GRANT OPTION')
				)
			)
			OR has_table_privilege(current_user, oid, 'UPDATE') <> allow_update
			OR (
				NOT allow_update
				AND has_any_column_privilege(current_user, oid, 'UPDATE')
			)
			OR (
				allow_update
				AND (
					has_table_privilege(current_user, oid, 'UPDATE WITH GRANT OPTION')
					OR has_any_column_privilege(current_user, oid, 'UPDATE WITH GRANT OPTION')
				)
			)
			-- DELETE is not column-grantable in PostgreSQL (only SELECT,
			-- INSERT, UPDATE, REFERENCES can be), so unlike the two checks
			-- above there is no column-level or has_any_column_privilege
			-- route to guard here — a table-level mismatch or option check
			-- is the whole surface.
			OR has_table_privilege(current_user, oid, 'DELETE') <> allow_delete
			OR (
				allow_delete
				AND has_table_privilege(current_user, oid, 'DELETE WITH GRANT OPTION')
			)
			OR has_table_privilege(current_user, oid, 'TRUNCATE')
			OR has_table_privilege(current_user, oid, 'REFERENCES')
			OR has_any_column_privilege(current_user, oid, 'REFERENCES')
			OR has_table_privilege(current_user, oid, 'TRIGGER')
			OR CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND NOT EXISTS (
		SELECT 1
		FROM other_public_relations
		WHERE has_table_privilege(current_user, oid, 'SELECT')
			OR has_table_privilege(current_user, oid, 'INSERT')
			OR has_table_privilege(current_user, oid, 'UPDATE')
			OR has_table_privilege(current_user, oid, 'DELETE')
			OR has_any_column_privilege(
				current_user, oid, 'SELECT, INSERT, UPDATE, REFERENCES'
			)
			OR has_table_privilege(current_user, oid, 'TRUNCATE')
			OR has_table_privilege(current_user, oid, 'REFERENCES')
			OR has_table_privilege(current_user, oid, 'TRIGGER')
			OR CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND NOT EXISTS (
		SELECT 1
		FROM public_sequences
		WHERE has_sequence_privilege(current_user, oid, 'USAGE')
			OR has_sequence_privilege(current_user, oid, 'SELECT')
			OR has_sequence_privilege(current_user, oid, 'UPDATE')
	)
	AND NOT has_schema_privilege(current_user, $2, 'USAGE')
	-- Mirrors the public-schema CREATE check above: migrate.go revokes ALL
	-- privileges on the River schema from the domain role, so this should
	-- never be reachable in practice, but this assertion exists to verify
	-- posture independently of what a migration claims to have done, and it
	-- already refuses to take that on trust for the public schema.
	AND NOT has_schema_privilege(current_user, $2, 'CREATE')
	AND NOT EXISTS (
		SELECT 1
		FROM river_relations
		WHERE has_table_privilege(current_user, oid, 'SELECT')
			OR has_table_privilege(current_user, oid, 'INSERT')
			OR has_table_privilege(current_user, oid, 'UPDATE')
			OR has_table_privilege(current_user, oid, 'DELETE')
			OR has_any_column_privilege(
				current_user, oid, 'SELECT, INSERT, UPDATE, REFERENCES'
			)
			OR has_table_privilege(current_user, oid, 'TRUNCATE')
			OR has_table_privilege(current_user, oid, 'REFERENCES')
			OR has_table_privilege(current_user, oid, 'TRIGGER')
			OR CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND NOT EXISTS (
		SELECT 1
		FROM river_sequences
		WHERE has_sequence_privilege(current_user, oid, 'USAGE')
			OR has_sequence_privilege(current_user, oid, 'SELECT')
			OR has_sequence_privilege(current_user, oid, 'UPDATE')
	)
	AND NOT EXISTS (
		SELECT 1
		FROM river_functions
		WHERE has_function_privilege(current_user, oid, 'EXECUTE')
	)
	AND NOT EXISTS (
		SELECT 1
		FROM public_functions
		WHERE has_function_privilege(current_user, oid, 'EXECUTE')
	)`

// TablePrivilege declares the exact table-level privileges one role's
// posture requires for one table. SELECT is always implied and always
// required; AllowInsert/AllowUpdate/AllowDelete state whether INSERT/UPDATE/
// DELETE must additionally be held. Every other table-level privilege
// (TRUNCATE, REFERENCES, TRIGGER, MAINTAIN) must always be absent, and
// rolePostureQuery checks that unconditionally — it is never expressible as
// "allowed" through this type.
type TablePrivilege struct {
	TableName   string
	AllowInsert bool
	AllowUpdate bool
	AllowDelete bool
}

// ColumnPrivilege declares a single column-scoped privilege one role's
// posture requires, for a table that is deliberately NOT granted at the
// table level — see worker_job_completion_fences below for why: a
// server-owned column must never become reachable through a table-wide
// grant just because some other column on the same table legitimately
// needs one.
type ColumnPrivilege struct {
	TableName  string
	ColumnName string
	Privilege  string
}

// RolePosture is the complete, declared semantic-runtime privilege manifest
// one role is expected to hold: exactly this, at both table and column
// granularity, and — via rolePostureQuery's own catch-all predicates —
// nothing else in the public or River schema. A role's manifest must omit
// every table and column that belongs exclusively to another role in the
// same deployment; see the package doc on rolePostureQuery for why that is
// what makes cross-role attribution hold without a separate check.
type RolePosture struct {
	RequiredTables []TablePrivilege
	ColumnScoped   []ColumnPrivilege
}

// domainPosture is the domain runtime role's declared manifest under the
// Option B two-role split. Table set and flags come from
// docs/architecture/chaos-3033-role-partition-manifest.md @ eda2d6b91
// (grant-deriver tool derivation + hand-verified Go source), the sole
// authority for role/privilege attribution — not this comment, and not any
// earlier revision of it.
//
//   - worker_job_routes, scheduled_jobs, scheduled_sync_occurrences,
//     fixed_schedule_occurrences moved OUT of this posture entirely: the
//     manifest attributes all four exclusively to the coordinator role (see
//     coordinatorPosture below). None has a domain-side SQL site.
//   - sync_configurations tightens from {SELECT, UPDATE} to {SELECT} only:
//     the domain-side site (internal/syncdispatchruntime/native_post_sync.go:263)
//     is a plain SELECT with no lock clause. The genuine row-locking write
//     this table used to need UPDATE for belongs to the coordinator-side
//     scheduler code, not the domain worker.
//   - sync_runs tightens from {SELECT} to {SELECT, UPDATE}: it is one of the
//     six dual-grant ("both") tables — the domain side genuinely holds
//     UPDATE via Fanout's `FOR SHARE` and the providersync hot path, not just
//     an implied SELECT.
//   - organizations, sync_dispatch_transport_routes, sync_dispatch_outbox,
//     sync_run_units, worker_job_runs are the remaining five of the six
//     dual-grant tables: each is declared here with the domain role's own
//     flags, and again in coordinatorPosture with the coordinator's — see
//     CheckRolePosture's doc comment for why a table required by two roles is
//     not a leak as long as each role's own manifest states it accurately.
//   - remaining_metric_runs, remaining_metric_partitions,
//     work_graph_execution_requests, daily_metrics_partitions,
//     daily_metrics_runs: genuine read-modify-write callers reached from the
//     worker's post-sync fanout, not just row locking.
//   - work_graph_execution_ledger: the ledger half of the same Claim
//     transaction as work_graph_execution_requests (an upsert keyed on
//     request_id; see internal/jobs/workgraph/postgres.go Claim/transition).
//     The upsert's conflict-target arm re-reads the existing row, so SELECT
//     is a real requirement here, not just rolePostureQuery's
//     insert-without-select limitation.
//   - external_ingest_batch_payloads, external_ingest_batches,
//     provider_rate_limit_observations: the three tables needing AllowDelete
//     — all domain-role retention/cleanup callers
//     (internal/streamhandlers/external_postgres.go,
//     internal/jobs/system/retention_postgres.go).
//
// worker_job_completion_fences is deliberately NOT in RequiredTables:
// completed_at is server-owned (DEFAULT statement_timestamp()) and no domain
// statement ever reads or writes it (joboutbox.MarkCompletionTx only inserts
// completion_key; deletion is queue-side, queue_authorization.go). A
// table-wide privilege would let the domain role forge completed_at and mint
// a fence retention never reaps, so its posture is column-scoped instead —
// hence ColumnScoped below rather than a RequiredTables row. The manifest's
// "possible 2nd reacher" flag on this table (a coordinator-side call into
// the same helper, unresolved by the grant-deriver tool) was independently
// run down by hand: internal/scheduler/fixed/producers.go only calls
// joboutbox.CompletionKey(...), a pure function computing a string stored in
// a DIFFERENT table's column (worker_job_outbox.prerequisite_completion_key)
// — it never touches worker_job_completion_fences itself. Confirmed via
// grep for MarkCompletionTx/worker_job_completion_fences across
// internal/scheduler/fixed: zero hits. This is a false positive, not an
// unresolved gap — coordinatorPosture has no row for this table.
func domainPosture() RolePosture {
	return RolePosture{
		RequiredTables: []TablePrivilege{
			{"integrations", false, false, false},
			{"integration_sources", false, false, false},
			{"integration_datasets", false, false, false},
			{"integration_credentials", false, false, false},
			{"sync_runs", false, true, false},
			{"sync_dispatch_transport_routes", false, false, false},
			{"sync_run_units", false, true, false},
			{"sync_watermarks", true, true, false},
			{"sync_dispatch_outbox", true, true, false},
			{"worker_job_outbox", true, false, false},
			{"sync_configurations", false, false, false},
			{"organizations", false, false, false},
			{"remaining_metric_runs", true, true, false},
			{"remaining_metric_partitions", true, true, false},
			{"work_graph_execution_requests", true, true, false},
			{"work_graph_execution_ledger", true, true, false},
			{"billing_notifications", false, false, false},
			{"daily_metrics_partitions", true, true, false},
			{"daily_metrics_runs", true, true, false},
			{"external_ingest_batch_payloads", false, false, true},
			{"external_ingest_batches", false, true, true},
			{"external_ingest_recompute_jobs", true, false, false},
			{"external_ingest_rejections", true, false, false},
			{"external_ingest_sources", false, false, false},
			{"feature_flags", false, false, false},
			{"org_feature_overrides", false, false, false},
			{"org_licenses", false, false, false},
			{"provider_rate_limit_observations", false, true, true},
			{"report_runs", false, true, false},
			{"saved_reports", false, true, false},
			{"webhook_deliveries", false, false, false},
			{"worker_job_runs", true, true, false},
		},
		ColumnScoped: []ColumnPrivilege{
			{"worker_job_completion_fences", "completion_key", "SELECT"},
			{"worker_job_completion_fences", "completion_key", "INSERT"},
		},
	}
}

// coordinatorPosture is the coordinator runtime role's declared manifest
// under the Option B two-role split, per
// docs/architecture/chaos-3033-role-partition-manifest.md @ eda2d6b91 — the
// same sole authority domainPosture defers to.
//
//   - internal_service_credentials, worker_operator_audits,
//     sync_run_reference_discoveries, sync_run_post_dispatches,
//     worker_job_routes: coordinator-exclusive, verified confidence (workerctl
//     and/or reconciler call sites, no domain-hot-path site for any of them).
//   - scheduled_jobs, scheduled_sync_occurrences, fixed_schedule_occurrences:
//     coordinator-exclusive but manifest confidence is "unverified-shape" —
//     the grant-deriver tool cannot trace reachability through a
//     closure-typed struct field here. Flags below are independently derived
//     by hand, not carried over blind: confirmed via
//     internal/scheduler/fixed/organizations.go (SELECT-only shape reasoning
//     applied to the same package) and by reading
//     internal/scheduler/sync/transaction.go's `FOR UPDATE OF config, job
//     SKIP LOCKED` (which requires UPDATE on scheduled_jobs for the locking
//     clause, matching the {false, true} already used pre-split) and
//     internal/scheduler/fixed/*.go's genuine INSERT+UPDATE occurrence
//     writers for the other two.
//   - sync_dispatch_outbox, sync_run_units, sync_runs,
//     sync_dispatch_transport_routes, worker_job_runs, organizations: the
//     coordinator side of the six dual-grant tables also declared in
//     domainPosture — see that function's doc comment.
//
// NOT included, pending confirmation: sync_configurations. The manifest
// attributes it to the domain role only (SELECT). While independently
// deriving the scheduled_jobs shape above, internal/scheduler/sync/
// transaction.go's schedulerHandoffCandidatesSQL was found to lock this
// table too — `FOR UPDATE OF config, job SKIP LOCKED` where `config` is
// public.sync_configurations, joined with scheduled_jobs — via the same
// closure-typed-struct-field blind spot the manifest already documents for
// scheduled_jobs itself. That would make sync_configurations a SEVENTH
// dual-grant table (coordinator: SELECT, UPDATE), not the domain-only table
// the manifest states. This is flagged, not applied: the manifest is
// authoritative over this comment's own reasoning, and this specific
// contradiction (a "verified"-confidence row, not an "unverified-shape" one)
// needs sign-off before the row changes. See the implementation report for
// full evidence.
func coordinatorPosture() RolePosture {
	return RolePosture{
		RequiredTables: []TablePrivilege{
			{"internal_service_credentials", false, true, false},
			{"worker_operator_audits", true, true, false},
			{"sync_run_reference_discoveries", false, false, false},
			{"sync_run_post_dispatches", false, false, false},
			{"worker_job_routes", false, true, false},
			{"scheduled_jobs", false, true, false},
			{"scheduled_sync_occurrences", true, true, false},
			{"fixed_schedule_occurrences", true, true, false},
			{"sync_dispatch_outbox", false, true, false},
			{"sync_run_units", false, false, false},
			{"sync_runs", false, true, false},
			{"sync_dispatch_transport_routes", false, true, false},
			{"worker_job_runs", false, true, false},
			{"organizations", false, false, false},
		},
	}
}

// CheckRolePosture is a read-only readiness check proving the active login
// holds exactly the declared posture: no more, no less, by any route
// (direct grant, PUBLIC, role membership, column-level, table-level, with
// or without grant option, or ownership). It never exposes catalog or
// driver details that could contain connection material.
//
// In a deployment with more than one runtime role, the cross-role
// attribution property — this role does not hold the OTHER role's exclusive
// privileges — is NOT proven by any single call to this function. It is
// proven by the UNION of calling it once per role, each against that role's
// own posture: rolePostureQuery's "everything outside my own manifest is
// illegal for me to hold" catch-all only ever inspects the CALLING role's
// privileges, so checking role A only ever proves role A is clean, never
// that role B is. A privilege wrongly granted to role B instead of role A is
// caught exclusively by role B's own readiness check finding an
// undeclared, unshared table in its grant set — role A's check has no way
// to see it. The runtime story this depends on: every role's own workers
// gate their own readiness on their own posture at startup (domain workers
// call CheckDomainAuthorization, coordinator workers call the coordinator's
// equivalent), so the full property holds across the deployment as a whole,
// not within any one process. A table legitimately required by more than
// one role (a multi-table transaction that must co-reside — see
// internal/syncreconciler/materializer.go for the concrete case driving
// this) is not a leak: it is simply declared in each such role's own
// RequiredTables/ColumnScoped, and each role's own check passes because the
// table is, correctly, in its own manifest.
func CheckRolePosture(ctx context.Context, pool *pgxpool.Pool, expectedRole, riverSchema string, posture RolePosture) error {
	if pool == nil || !validRuntimeIdentifier(expectedRole) || !validRuntimeIdentifier(riverSchema) {
		return ErrUnavailable
	}
	tableNames := make([]string, len(posture.RequiredTables))
	allowInserts := make([]bool, len(posture.RequiredTables))
	allowUpdates := make([]bool, len(posture.RequiredTables))
	allowDeletes := make([]bool, len(posture.RequiredTables))
	for i, table := range posture.RequiredTables {
		tableNames[i] = table.TableName
		allowInserts[i] = table.AllowInsert
		allowUpdates[i] = table.AllowUpdate
		allowDeletes[i] = table.AllowDelete
	}
	columnTables := make([]string, len(posture.ColumnScoped))
	columnNames := make([]string, len(posture.ColumnScoped))
	columnPrivileges := make([]string, len(posture.ColumnScoped))
	for i, column := range posture.ColumnScoped {
		columnTables[i] = column.TableName
		columnNames[i] = column.ColumnName
		columnPrivileges[i] = column.Privilege
	}
	// unnest() does not error on mismatched array-parameter lengths — it
	// silently NULL-pads the shorter ones (confirmed empirically against a
	// live server: unnest(ARRAY['a','b','c'], ARRAY[true,false]) produces a
	// third row with a NULL flag, not an error). has_table_privilege(...) <>
	// NULL is NULL, not true or false, so a construction bug that produced
	// unequal-length parallel arrays here would not raise an error either —
	// it would either read as a mystery 42501-shaped readiness failure or,
	// worse, let a required row's flag silently read NULL. These lengths
	// cannot actually differ from the loop immediately above (each quadruple
	// is built from one shared source slice — allow_delete joined allow_insert
	// and allow_update in that same loop, not as a bolted-on parallel array),
	// so this can never fire today; it exists so a future refactor of that
	// construction fails loudly here instead of silently at the SQL layer if
	// it ever breaks that invariant. See
	// TestCheckRolePostureRejectsRaggedParallelArrays.
	if err := validateParallelArrayLengths(
		"required table privileges", len(tableNames), len(allowInserts), len(allowUpdates), len(allowDeletes),
	); err != nil {
		return err
	}
	if err := validateParallelArrayLengths(
		"column-scoped privileges", len(columnTables), len(columnNames), len(columnPrivileges),
	); err != nil {
		return err
	}
	var authorized bool
	if err := pool.QueryRow(
		ctx, rolePostureQuery,
		expectedRole, riverSchema,
		tableNames, allowInserts, allowUpdates,
		columnTables, columnNames, columnPrivileges,
		allowDeletes,
	).Scan(&authorized); err != nil || !authorized {
		return ErrUnavailable
	}
	return nil
}

// validateParallelArrayLengths reports a descriptive, non-ErrUnavailable
// error if the given lengths are not all equal. It is deliberately louder
// than ErrUnavailable's opaque "not ready": a length mismatch means the
// CALLER built an internally inconsistent RolePosture, which is a
// programming bug to surface and fix, not a legitimate readiness state to
// report and retry.
func validateParallelArrayLengths(context string, lengths ...int) error {
	for _, length := range lengths[1:] {
		if length != lengths[0] {
			return fmt.Errorf("postgres: %s: mismatched parallel array lengths %v", context, lengths)
		}
	}
	return nil
}

// CheckDomainAuthorization is a read-only readiness check for the semantic
// PostgreSQL pool. It binds the active login to the declared domain role and
// never exposes catalog or driver details that could contain connection
// material. It is domainPosture's manifest run through CheckRolePosture; the
// domain role is not special-cased anywhere in rolePostureQuery itself.
//
// This alone proves only that the domain role holds nothing beyond its own
// declared posture — see CheckRolePosture's doc comment for why the full
// cross-role attribution property requires every other role in the
// deployment to independently check its own posture too.
func CheckDomainAuthorization(ctx context.Context, pool *pgxpool.Pool, expectedRole, riverSchema string) error {
	return CheckRolePosture(ctx, pool, expectedRole, riverSchema, domainPosture())
}

// CheckCoordinatorAuthorization is the coordinator role's counterpart to
// CheckDomainAuthorization: a read-only readiness check binding the active
// login to the declared coordinator role and running it through
// coordinatorPosture. Coordinator-role workers (workerctl, reconciler,
// scheduler) gate their own readiness on this at startup — see
// CheckRolePosture's doc comment for why the cross-role attribution property
// depends on every role checking itself this way, not on either check alone.
func CheckCoordinatorAuthorization(ctx context.Context, pool *pgxpool.Pool, expectedRole, riverSchema string) error {
	return CheckRolePosture(ctx, pool, expectedRole, riverSchema, coordinatorPosture())
}
