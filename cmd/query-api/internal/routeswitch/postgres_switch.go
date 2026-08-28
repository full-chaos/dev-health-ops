package routeswitch

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// lookupTimeout bounds a single Enabled() registry read. Enabled has no
// context parameter (the Switch interface -- shared with StaticSwitch and
// DynamicSwitch, deliberately not redesigned here, see the package doc
// comment -- takes none), so an unbounded context.Background() lookup
// would let a blocked connection or exhausted pool hang a request handler
// indefinitely during a registry outage. This is an internal bound, not a
// caller-supplied one; a future wave that threads a real request context
// through Dispatch can lower this further per-request.
const lookupTimeout = 2 * time.Second

// reachableModes is the subset of plan §5's mode vocabulary
// (python|shadow|canary|primary|disabled) that makes an operation
// reachable to a REAL client request in this Switch's sense. "shadow"
// deliberately does NOT count: the client still receives Python's
// response in shadow mode (plan §5 stage 4) -- shadow execution happens,
// but it is not what Switch.Enabled asks. "python" and "disabled" are
// both "not reachable" (the safe default a missing row already gives).
var reachableModes = map[string]bool{
	"canary":  true,
	"primary": true,
}

// PostgresSwitch is the go_api_registry-backed Switch implementation the
// routeswitch package doc comment forward-declares: it reads the current
// mode for an operation from the `go_api_routing_state` table (alembic
// 0114, src/dev_health_ops/models/go_api_registry.py) instead of an
// in-memory map. It implements the same Switch interface StaticSwitch and
// DynamicSwitch do -- this is that follow-up, not a redesign.
//
// PostgresSwitch is pinned to one schema_digest (Wave 0 has exactly one:
// the canonical SDL contracts/graphql/v1/schema.graphql pins) and a
// caller-supplied operation-name -> document_digest map, because
// Switch.Enabled only carries an operation NAME, while the registry's key
// is the 3-tuple (schema_digest, document_digest, selected_operation).
// An operation absent from documentDigests cannot be looked up and is
// therefore disabled -- consistent with "an operation absent from the
// registry stays on Python" (plan §5).
//
// Known, deliberate gaps in what this Wave-0 implementation enforces --
// named here so a MATCH on `mode` is never mistaken for full registry
// enforcement (codex review, 2026-08-27; "an inaccurate coverage claim is
// worse than an admitted gap", root AGENTS.md):
//
//  1. Document identity is NOT verified against the live request. Enabled
//     trusts the CALLER's documentDigests map for "which document this
//     operation name means"; it never receives or checks the actual
//     document digest of the incoming request. The Switch interface
//     (shared with StaticSwitch/DynamicSwitch) has no such parameter --
//     Mux.Dispatch(operation, w, r) doesn't carry one either. Wiring the
//     exact registered-document-identity contract end to end is a later
//     wave's job, when Mux is actually mounted on a live route.
//  2. eligible_orgs / rollout_percentage are NOT enforced. Enabled has no
//     org/tenant argument (same interface constraint as #1), so a
//     `canary` row scoped to specific orgs or a partial rollout
//     percentage is currently treated as fully enabled for every caller.
//     Real per-tenant gradual rollout requires threading the request's
//     org through Enabled, which is exactly the interface change #1 also
//     needs -- both wait for the same later wave.
//  3. current_candidate_build is NOT bound to reachability. Enabled
//     answers "is this operation's mode canary/primary", not "is THIS
//     candidate build the one currently live" -- which build actually
//     handles a reachable request is decided by whatever Register()'d a
//     handler in the Mux, a separate wiring step outside Wave 0's scope.
//     A registry rollback that changes current_candidate_build without
//     also changing mode does not, by itself, revoke or redirect
//     reachability here.
type PostgresSwitch struct {
	pool            *pgxpool.Pool
	schemaDigest    string
	documentDigests map[string]string
}

// NewPostgresSwitch builds a PostgresSwitch. pool must not be nil.
// documentDigests maps operation name -> the document digest that
// operation was registered under; it is copied, not retained by
// reference.
func NewPostgresSwitch(pool *pgxpool.Pool, schemaDigest string, documentDigests map[string]string) *PostgresSwitch {
	if pool == nil {
		panic("routeswitch: NewPostgresSwitch requires a non-nil pool")
	}
	copied := make(map[string]string, len(documentDigests))
	for k, v := range documentDigests {
		copied[k] = v
	}
	return &PostgresSwitch{pool: pool, schemaDigest: schemaDigest, documentDigests: copied}
}

// Enabled implements Switch. It queries `go_api_routing_state` for the
// current mode of (schemaDigest, documentDigest, operation) and returns
// true only when a row exists AND its mode is "canary" or "primary". Any
// failure to resolve reachability -- no document digest registered for
// this operation name, no routing-state row, or a query error -- resolves
// to false (unreachable), the same safe default StaticSwitch and
// DynamicSwitch already use for an unregistered operation. A query error
// is logged rather than silently swallowed: an unreachable registry and
// "not canaried yet" must not read as the same signal to an operator (the
// same reasoning as go_api_registry_telemetry's lookup-outcome counters
// on the Python side).
func (s *PostgresSwitch) Enabled(operation string) bool {
	documentDigest, ok := s.documentDigests[operation]
	if !ok {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	var mode string
	err := s.pool.QueryRow(ctx,
		`SELECT mode FROM go_api_routing_state
		 WHERE schema_digest = $1 AND document_digest = $2 AND selected_operation = $3`,
		s.schemaDigest, documentDigest, operation,
	).Scan(&mode)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Printf("routeswitch: PostgresSwitch lookup failed for operation %q: %v", operation, err)
		}
		return false
	}
	return reachableModes[mode]
}
