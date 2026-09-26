package postgres

import (
	"context"

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
// deployment names a query-api role (QUERY_API_DATABASE_ROLE): it binds the
// active login to that role and proves it holds exactly queryAPIPosture's
// manifest, no more and no less, by any route (see CheckRolePosture). It is
// meant to run only when a role is NAMED; a deployment that names none has not
// opted in and the caller must not call it.
//
// It has the same cost as every whole-catalog posture query (1.4-1.9 s on the
// production catalog, CHAOS-6765), so query-api wraps it in
// NewCachedPostureCheck rather than calling it per probe.
func CheckQueryAPIAuthorization(ctx context.Context, pool *pgxpool.Pool, expectedRole, riverSchema string) error {
	return CheckRolePosture(ctx, pool, expectedRole, riverSchema, queryAPIPosture())
}
