package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// apiPosture is the dho api Service's declared Postgres privilege manifest
// (CHAOS-6269, spec.md §4.4). It starts EMPTY on purpose: S0 provisions the
// role and proves it holds nothing beyond the baseline every runtime role
// must hold (CONNECT, USAGE on the public schema, no CREATE, no ownership,
// no privilege on any other relation, sequence or function in the public or
// River schema -- rolePostureQuery's own catch-all predicates, unconditional
// for every caller of CheckRolePosture) -- because no route exists yet to
// need anything more. Each route PR that follows adds exactly the grants its
// own tables need directly to this function's RequiredTables/ColumnScoped/
// RequiredSequences, in the SAME PR that ships the route, the same
// discipline domainPosture/coordinatorPosture already follow.
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
func apiPosture() RolePosture {
	return RolePosture{}
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
