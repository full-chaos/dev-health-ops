package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
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

// CheckQueryAPIAuthorization is query-api's readiness check when the deployment
// names a query-api role (QUERY_API_DATABASE_ROLE). "Holds exactly the manifest" is
// defined ONCE (CHAOS-6804, lead D2616), over the role's EFFECTIVE grants, and
// every part must hold:
//
//  1. Identity (roleacl.IdentityPredicateSQL): the pool AUTHENTICATED as the role,
//     which is an unprivileged, membership-free login.
//  2. The role owns nothing (roleacl.OwnsNothingSQL).
//  3. The set of effective grants the role holds, enumerated from EVERY
//     ACL-bearing catalog with PUBLIC counted as granted to the role and its
//     role-level settings included (roleacl.Enumerate), EQUALS the manifest plus
//     the baseline (CONNECT on this database, USAGE on the public schema): no
//     grant outside it, none of it missing. A privilege kind nobody thought to
//     check cannot slip past a set-equality over a complete enumeration.
//  4. Every manifest table resolves, UNQUALIFIED, to its public relation: the
//     application's queries are unqualified, so a search_path that hides the
//     manifest (a DSN parameter; a role setting is caught by 3) would leave a
//     Ready pod failing every request.
//
// It is meant to run only when a role is NAMED; a deployment that names none has
// not opted in and the caller must not call it. It costs a whole-catalog scan
// (1.4-1.9 s on the production catalog, CHAOS-6765), so query-api runs it through
// NewCachedQueryAPIPostureCheck and never per probe.
func CheckQueryAPIAuthorization(ctx context.Context, pool *pgxpool.Pool, expectedRole, riverSchema string) error {
	if pool == nil || !validRuntimeIdentifier(expectedRole) || !validRuntimeIdentifier(riverSchema) {
		return ErrUnavailable
	}
	if err := checkRoleIdentity(ctx, pool, expectedRole); err != nil {
		return err
	}
	var ownsNothing bool
	if err := pool.QueryRow(ctx, "SELECT "+roleacl.OwnsNothingSQL, expectedRole).Scan(&ownsNothing); err != nil {
		return fmt.Errorf("%w: reading ownership: %w", ErrUnavailable, err)
	}
	if !ownsNothing {
		return refuseQueryAPI(expectedRole, "the role owns an object (a database, schema, relation or function)")
	}
	var database string
	if err := pool.QueryRow(ctx, "SELECT current_database()::text").Scan(&database); err != nil {
		return fmt.Errorf("%w: reading the database name: %w", ErrUnavailable, err)
	}
	grants, err := roleacl.Enumerate(ctx, pool, expectedRole)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if mismatch := diffQueryAPIGrants(database, queryAPIPosture(), grants); mismatch != "" {
		return refuseQueryAPI(expectedRole, mismatch)
	}
	tableNames := make([]string, len(queryAPIPosture().RequiredTables))
	for i, table := range queryAPIPosture().RequiredTables {
		tableNames[i] = table.TableName
	}
	var unresolved *string
	err = pool.QueryRow(ctx, `SELECT t FROM unnest($1::text[]) AS t
		WHERE to_regclass(quote_ident(t)) IS DISTINCT FROM to_regclass('public.' || quote_ident(t))
		ORDER BY t LIMIT 1`, tableNames).Scan(&unresolved)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return fmt.Errorf("%w: checking name resolution: %w", ErrUnavailable, err)
	case unresolved != nil:
		return refuseQueryAPI(expectedRole, fmt.Sprintf("%q does not resolve, unqualified, to public.%s: the connection's search_path hides the manifest", *unresolved, *unresolved))
	}
	return nil
}

func refuseQueryAPI(role, mismatch string) error {
	return fmt.Errorf("%w: %w for role %q: %s", ErrUnavailable, ErrPostureRefused, role, mismatch)
}

// diffQueryAPIGrants compares the enumerated grants with the manifest plus the
// baseline and names the first difference ("" when they are equal). Extra grants
// are reported in enumeration order (sorted), then missing ones (sorted), so the
// answer is deterministic.
func diffQueryAPIGrants(database string, posture RolePosture, grants []roleacl.Grant) string {
	key := func(class, object, privilege string) string { return class + "\x00" + object + "\x00" + privilege }
	allowed := map[string]struct{}{
		key("database", database, "CONNECT"): {},
		key("schema", "public", "USAGE"):     {},
	}
	// Baseline members that must be present: the schema USAGE (unqualified
	// resolution needs it). CONNECT is implied by the login that is running this.
	required := map[string]string{key("schema", "public", "USAGE"): "USAGE on schema public"}
	for _, table := range posture.RequiredTables {
		object := "public." + table.TableName
		privileges := []string{"SELECT"}
		if table.AllowInsert {
			privileges = append(privileges, "INSERT")
		}
		if table.AllowUpdate {
			privileges = append(privileges, "UPDATE")
		}
		if table.AllowDelete {
			privileges = append(privileges, "DELETE")
		}
		for _, privilege := range privileges {
			allowed[key("relation", object, privilege)] = struct{}{}
			required[key("relation", object, privilege)] = privilege + " on relation " + object
		}
	}
	held := map[string]struct{}{}
	for _, grant := range grants {
		k := key(grant.Class, grant.Object, grant.Privilege)
		held[k] = struct{}{}
		if _, ok := allowed[k]; !ok {
			return "holds " + grant.String() + ", which is outside the manifest"
		}
	}
	var missing []string
	for k, description := range required {
		if _, ok := held[k]; !ok {
			missing = append(missing, description)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return "lacks " + missing[0]
	}
	return ""
}

// NewCachedQueryAPIPostureCheck is the query-api's cached, single-flight,
// non-blocking posture check over CheckQueryAPIAuthorization. Use it, not
// NewCachedPostureCheck, or the definition above never runs.
func NewCachedQueryAPIPostureCheck(
	pool *pgxpool.Pool, expectedRole, riverSchema string, options PostureCheckOptions,
) *CachedPostureCheck {
	return newCachedPostureCheck(expectedRole, func(ctx context.Context) error {
		return CheckQueryAPIAuthorization(ctx, pool, expectedRole, riverSchema)
	}, options)
}
