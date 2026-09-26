package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// queryAPIPosture is the query-api Service's full, hold-exactly Postgres
// privilege manifest (CHAOS-6804; its additive write half was CHAOS-6803).
// It is every relation `dho query-api` reaches through its Postgres pool, no
// more and no less, so a login that holds it can serve the whole read plane
// and the saved-report mutations and can do nothing else: CheckRolePosture
// refuses a role that owns an object, holds an undeclared privilege by any
// route, or holds any privilege on the River schema.
//
// Each entry is what the code actually executes (SELECT is always implied by
// TablePrivilege). The reads are listed with the statement that needs them so
// a reviewer can check the manifest against the code, and the live test
// TestQueryAPIRoleServesEveryPostgresPathItReaches (internal/queryapi/server)
// is the completeness proof: it drives each of these paths as the role.
//
// Reads:
//   - go_api_routing_state: routeswitch.PostgresSwitch.Enabled (every request),
//     the proof switch, and the registry route's digest-drift log.
//   - sync_configurations, job_runs, sync_runs: the data-health connectors
//     section and the home freshness panel's latest-successful-sync read (which
//     also joins scheduled_jobs, below).
//   - organizations: the product-telemetry org names, and the BYO-LLM feature
//     gate's tier fallback.
//   - org_licenses, feature_flags, org_feature_overrides: the BYO-LLM feature
//     gate.
//   - settings: the org's BYO-LLM settings rows.
//
// Writes (the five saved-report GraphQL mutations, CHAOS-6098):
//   - saved_reports: create and clone insert, update updates, delete deletes.
//   - scheduled_jobs: a report schedule is created (insert) or rewritten
//     (update) by create and update. A deleted report leaves its job row, as
//     the Python resolver does, so there is no delete.
//   - report_runs: triggerReport inserts the pending run. The rows of a deleted
//     report go by the database's ON DELETE CASCADE, which the foreign key's
//     own trigger performs without a privilege on the child table.
//   - worker_job_outbox: triggerReport publishes the on-demand execution in the
//     same transaction as the run (insert; the publisher's conflict read is the
//     implied SELECT).
//
// scheduled_report_occurrences is absent on purpose: only the SCHEDULED
// execution path writes it, and that path is the scheduler's, not this
// role's. go_api_candidate_build and go_api_proof_run are absent because only
// the goapiproof CLI touches them. No sequence is needed: every id this
// process writes is supplied by the caller.
//
// Any future query-api Postgres access edits this function in the SAME PR
// that adds it, the discipline domainPosture and apiPosture already follow.
func queryAPIPosture() RolePosture {
	return RolePosture{
		RequiredTables: []TablePrivilege{
			{"saved_reports", true, true, true},
			{"scheduled_jobs", true, true, false},
			{"report_runs", true, false, false},
			{"worker_job_outbox", true, false, false},
			{"go_api_routing_state", false, false, false},
			{"sync_configurations", false, false, false},
			{"job_runs", false, false, false},
			{"sync_runs", false, false, false},
			{"organizations", false, false, false},
			{"org_licenses", false, false, false},
			{"feature_flags", false, false, false},
			{"org_feature_overrides", false, false, false},
			{"settings", false, false, false},
		},
	}
}

// QueryAPIPosture exposes queryAPIPosture for callers outside this package.
// internal/rivermigrate derives the GRANT statements from this same
// declaration and CheckQueryAPIAuthorization asserts it, so the grant side and
// the readiness side are one list.
func QueryAPIPosture() RolePosture {
	return queryAPIPosture()
}

// CheckQueryAPIAuthorization is query-api's readiness check when the
// deployment names a query-api role (QUERY_API_DATABASE_ROLE). It proves three
// things, in this order, and nothing passes unless all three do:
//
//  1. The pool authenticated AS the role: session_user (the login the DSN
//     carries) and current_user are both the named role. A login that only
//     ACTS as the role (a startup option `-c role=...`, a SET ROLE in the DSN)
//     still holds its own, wider credential and could RESET ROLE, so it is
//     refused (CHAOS-6804 r1). The shared posture query below reads only
//     current_user, so this is checked here, first.
//  2. The role holds exactly queryAPIPosture's manifest, no more and no less, by
//     any route, on the public and River schemas (CheckRolePosture).
//  3. The role holds NO privilege outside those schemas either: no privilege on
//     any relation or sequence, and no CREATE on any schema, in any other
//     non-system schema (CHAOS-6804 r1). The migrate leg only revokes on the
//     public and River schemas, so a grant elsewhere is REFUSED here, loudly
//     and naming the first one, never silently revoked.
//
// It is meant to run only when a role is NAMED; a deployment that names none has
// not opted in and the caller must not call it. It has the same cost as every
// whole-catalog posture query (1.4-1.9 s on the production catalog,
// CHAOS-6765), so query-api runs it through NewCachedQueryAPIPostureCheck and
// never per probe.
func CheckQueryAPIAuthorization(ctx context.Context, pool *pgxpool.Pool, expectedRole, riverSchema string) error {
	if pool == nil || !validRuntimeIdentifier(expectedRole) || !validRuntimeIdentifier(riverSchema) {
		return ErrUnavailable
	}
	var sessionUser, currentUser string
	if err := pool.QueryRow(ctx, "SELECT session_user::text, current_user::text").Scan(&sessionUser, &currentUser); err != nil {
		return fmt.Errorf("%w: reading the active login: %w", ErrUnavailable, err)
	}
	if sessionUser != expectedRole || currentUser != expectedRole {
		return fmt.Errorf("%w: %w: the pool authenticated as %q and acts as %q, not the named query-api role %q",
			ErrUnavailable, ErrPostureRefused, sessionUser, currentUser, expectedRole)
	}
	if err := CheckRolePosture(ctx, pool, expectedRole, riverSchema, queryAPIPosture()); err != nil {
		return err
	}
	var outside *string
	err := pool.QueryRow(ctx, queryAPIOutsideManagedSchemasQuery, riverSchema).Scan(&outside)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("%w: reading privileges outside the managed schemas: %w", ErrUnavailable, err)
	case outside != nil:
		return fmt.Errorf("%w: %w for role %q: holds a privilege outside the public and %s schemas: %s",
			ErrUnavailable, ErrPostureRefused, expectedRole, riverSchema, *outside)
	}
	return nil
}

// queryAPIOutsideManagedSchemasQuery finds the first privilege the calling role
// holds, by any route (direct, PUBLIC, membership), on a relation or sequence in
// a schema other than public, the River schema ($1) and the system schemas, or
// CREATE on such a schema. Read-only; catalog only.
const queryAPIOutsideManagedSchemasQuery = `
SELECT found FROM (
	SELECT n.nspname || '.' || c.relname AS found
	FROM pg_catalog.pg_class AS c
	JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
	WHERE n.nspname NOT IN ('public', $1::text, 'pg_catalog', 'information_schema')
		AND n.nspname NOT LIKE 'pg\_toast%'
		AND n.nspname NOT LIKE 'pg\_temp\_%'
		AND (
			(c.relkind IN ('r', 'p', 'v', 'm', 'f') AND (
				has_table_privilege(current_user, c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
				OR has_any_column_privilege(current_user, c.oid, 'SELECT, INSERT, UPDATE, REFERENCES')))
			OR (c.relkind = 'S' AND has_sequence_privilege(current_user, c.oid, 'USAGE, SELECT, UPDATE'))
		)
	UNION ALL
	SELECT n.nspname AS found
	FROM pg_catalog.pg_namespace AS n
	WHERE n.nspname NOT IN ('public', $1::text, 'pg_catalog', 'information_schema')
		AND n.nspname NOT LIKE 'pg\_toast%'
		AND n.nspname NOT LIKE 'pg\_temp\_%'
		AND has_schema_privilege(current_user, n.oid, 'CREATE')
) AS outside
ORDER BY found
LIMIT 1`

// NewCachedQueryAPIPostureCheck is the query-api's cached, single-flight,
// non-blocking posture check: CheckQueryAPIAuthorization (identity, manifest,
// nothing outside the managed schemas) behind the same cache every posture-checked
// service uses. Use it, not NewCachedPostureCheck, or the two extra proofs above
// never run.
func NewCachedQueryAPIPostureCheck(
	pool *pgxpool.Pool, expectedRole, riverSchema string, options PostureCheckOptions,
) *CachedPostureCheck {
	return newCachedPostureCheck(expectedRole, func(ctx context.Context) error {
		return CheckQueryAPIAuthorization(ctx, pool, expectedRole, riverSchema)
	}, options)
}
