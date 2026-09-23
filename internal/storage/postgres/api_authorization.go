package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// apiPosture is the dho api Service's declared Postgres privilege manifest
// (CHAOS-6269, spec.md §4.4). It started EMPTY on purpose: S0 provisions the
// role and proves it holds nothing beyond the baseline every runtime role
// must hold (CONNECT, USAGE on the public schema, no CREATE, no ownership,
// no privilege on any other relation, sequence or function in the public or
// River schema -- rolePostureQuery's own catch-all predicates, unconditional
// for every caller of CheckRolePosture). CHAOS-6244 is the first route PR
// to need anything more; it adds exactly the four tables its own query
// reads, all SELECT-only (no insert/update/delete: the acr entitlement
// route is a pure read, and it writes no audit row -- see
// internal/apiservice/acr's package doc for why the Python credential-audit
// path is not ported under R340). Each route PR that follows adds exactly
// the grants its own tables need directly to this function's
// RequiredTables/ColumnScoped/RequiredSequences, in the SAME PR that ships
// the route, the same discipline domainPosture/coordinatorPosture already
// follow.
//
// Unlike the three River runtime roles, the api role never opens a River
// pool and is not part of the CHAOS-3033 Option B split -- it is declared
// here, in the same package, because CheckRolePosture/RolePosture is the
// one reusable readiness mechanism this repo has for "prove a login holds
// exactly its declared manifest," not because api is a River role. Passing
// this package's riverSchema parameter through CheckAPIAuthorization still
// matters: it is what makes rolePostureQuery assert the api role holds ZERO
// privilege on the River schema too, exactly as it does for every other
// role's own posture.
//
// NOTE (CHAOS-6244): unlike the three River roles, no automated step yet
// applies this manifest as GRANT statements -- go-river-migrate's
// runtimeGrantStatements has no api-role equivalent. Until one exists (or an
// operator GRANTs these four tables by hand, the same manual step
// provision_river_roles.sql's api_role block already documents for role
// creation itself), CheckAPIAuthorization correctly reports the role's
// posture as refused wherever APIDatabaseURI is configured -- the safe
// direction: the api Service simply does not become ready, rather than
// silently running with unproven privileges.
func apiPosture() RolePosture {
	return RolePosture{
		RequiredTables: []TablePrivilege{
			// Read root for GET /api/v1/internal/acr/entitlements/{org_id}:
			// existence (404 if absent) and the tier fallback when no
			// org_licenses row exists (internal/apiservice/acr/store.go).
			{"organizations", false, false, false},
			// The one feature row this route ever reads (key =
			// "agent_context_runtime"), never any other feature.
			{"feature_flags", false, false, false},
			// Per-org override for that same feature.
			{"org_feature_overrides", false, false, false},
			// License tier + features_override JSON.
			{"org_licenses", false, false, false},
		},
	}
}

// APIPosture exposes apiPosture for callers outside this package -- the
// same reason DomainPosture/CoordinatorPosture/QueuePosture exist: a future
// grant-application step (the api Service's own equivalent of
// internal/storage/river/migrate.go's runtimeGrantStatements, applied at its
// own provisioning/rollout step once dho api exists) derives its GRANT
// statements from this SAME declaration, never a second hand-maintained
// list.
func APIPosture() RolePosture {
	return apiPosture()
}

// CheckAPIAuthorization is the dho api Service's readiness check: it binds
// the active login to the declared api role and proves it holds exactly
// apiPosture's manifest, no more and no less, by any route (direct grant,
// PUBLIC, role membership, column-level, table-level, with or without grant
// option, or ownership) -- see CheckRolePosture's doc comment for the full
// property and why every role in a multi-role deployment must check its own
// posture for the deployment-wide cross-role attribution property to hold.
// dho api registers this behind its own health.Registry entry (internal/
// apiservice/service.go) once that package exists; this function has no
// dependency on it and can be called, and tested, standing alone.
func CheckAPIAuthorization(ctx context.Context, pool *pgxpool.Pool, expectedRole, riverSchema string) error {
	return CheckRolePosture(ctx, pool, expectedRole, riverSchema, apiPosture())
}
