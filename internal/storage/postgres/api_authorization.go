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
// for every caller of CheckRolePosture). CHAOS-6244 was the first route PR
// to need anything more (four SELECT-only tables: the acr entitlement route
// is a pure read, and it writes no audit row -- see internal/apiservice/acr's
// package doc for why the Python credential-audit path is not ported for
// that internal route). CHAOS-6246 (external-ingest) is the first to need
// writes: accepting a customer push durably records a Postgres status row
// and the raw payload before it is acknowledged (the CC22 accept sequence),
// and bumping a used token's last_used_at/last_used_ip is part of the auth
// path itself. Each route PR adds exactly the grants its own tables need
// directly to this function's RequiredTables/ColumnScoped/RequiredSequences,
// in the SAME PR that ships the route, the same discipline
// domainPosture/coordinatorPosture already follow.
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
			// external-ingest (CHAOS-6246): bearer-token auth resolves the
			// token row and bumps last_used_at/last_used_ip on every
			// request that reaches a scope check (auth.go's bumpLastUsed).
			{"external_ingest_tokens", false, true, false},
			// The token's bound source, and source-ownership resolution
			// (ownership.go's resolveEffectiveMode): read-only.
			{"external_ingest_sources", false, false, false},
			// The CC22 accept sequence's status row: created on NEW,
			// updated on RETRY/mark-stream-unavailable, read by
			// GET /batches* and the idempotency NEW/REPLAY/CONFLICT/RETRY
			// resolution.
			{"external_ingest_batches", true, true, false},
			// The raw batch payload, written (or refreshed) in the SAME
			// transaction as the status row (payload.go's upsertPayloadTx),
			// read back only to prove the fail-closed durability
			// precondition before XADD.
			{"external_ingest_batch_payloads", true, true, false},
			// Read-only: GET /batches/{id}'s per-record rejection detail.
			// This route never writes a rejection row -- that is the
			// CHAOS-2697 worker's job, over the domain role, not this one.
			{"external_ingest_rejections", false, false, false},
			// Managed-sync ownership matching (ownership.go's
			// findActiveManagedOwner): read-only.
			{"integration_sources", false, false, false},
			{"integrations", false, false, false},
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
