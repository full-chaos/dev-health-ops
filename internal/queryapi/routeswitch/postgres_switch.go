package routeswitch

import (
	"context"
	"log"
	"sync"
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
//  2. eligible_orgs / rollout_percentage are inert BY DESIGN (CHAOS-6807).
//     Enabled has no org/tenant argument, the Python edge dispatcher that
//     decides delegation does not enforce them either, and the delegated
//     operations have no Python resolver for an org outside a cohort to
//     fall back to (that org would get an error, not Python's answer). So
//     canary and primary are both "on for every authenticated org,
//     revocable only by mode"; a row recording a cohort or a partial
//     rollout is served to everyone. `dho goapi routing enable` refuses
//     such a row and `status` flags one that exists (`not_enforced`).
//  3. current_candidate_build is NOT bound to reachability. Enabled
//     answers "is this operation's mode canary/primary", not "is THIS
//     candidate build the one currently live" -- which build actually
//     handles a reachable request is decided by whatever Register()'d a
//     handler in the Mux, a separate wiring step outside Wave 0's scope.
//     A registry rollback that changes current_candidate_build without
//     also changing mode does not, by itself, revoke or redirect
//     reachability here.
type PostgresSwitch struct {
	// pool is *pgxpool.Pool in every production instance (the constructors take nothing else). It is
	// held as the one method Enabled calls so the unit tier can drive the whole decision, row state by
	// row state, without a database.
	pool            registryReader
	schemaDigest    string
	documentDigests map[string]string
	// legacyDigests maps an operation to the document digests of its LEGACY registered texts
	// (CHAOS-8000 dual accept): a text the operation accepted before its current one. A routing row at
	// any of them counts for the operation, so a row written for the old text keeps serving while the
	// new text's row is not yet enabled. Nil or an operation absent from it = no legacy text.
	legacyDigests map[string][]string
	// reachable is the mode set this instance treats as reachable.
	// NewPostgresSwitch sets reachableModes (production: canary|primary);
	// NewProofSwitch widens it by "shadow" for the measurement-only
	// route. Kept as a field rather than a second Enabled implementation
	// so both share ONE registry lookup -- a copied query is exactly the
	// thing that would get the (schema_digest, document_digest,
	// selected_operation) key wrong.
	reachable map[string]bool
	// serveUnrouted is the catalog rule (CHAOS-8517): an operation with NO routing row at all -- at any
	// schema digest, under any document digest -- is served. False for every switch but the one
	// NewCatalogSwitchWithLegacy builds; see catalog_switch.go for the rule and for why the class-row
	// and proof switches keep "no row = not reachable".
	serveUnrouted bool
	// announced holds the operations whose "served with no row" decision has been logged once.
	announced sync.Map
}

// registryReader is the read PostgresSwitch makes of go_api_routing_state.
type registryReader interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
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
	return &PostgresSwitch{pool: pool, schemaDigest: schemaDigest, documentDigests: copied, reachable: reachableModes}
}

// NewPostgresSwitchWithLegacy is NewPostgresSwitch for operations that accept more than one registered text
// (CHAOS-8000 dual accept). legacyDigests maps an operation to the digests of its older texts; it is
// copied. An operation absent from it behaves exactly as under NewPostgresSwitch.
func NewPostgresSwitchWithLegacy(pool *pgxpool.Pool, schemaDigest string, documentDigests map[string]string, legacyDigests map[string][]string) *PostgresSwitch {
	sw := NewPostgresSwitch(pool, schemaDigest, documentDigests)
	sw.legacyDigests = copyLegacy(legacyDigests)
	return sw
}

func copyLegacy(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for operation, digests := range in {
		out[operation] = append([]string(nil), digests...)
	}
	return out
}

// liveModesSQL is the one read every switch built on PostgresSwitch makes for an operation: the mode
// of each row at its live key. Named so the unit tier can tell it from anyRowSQL.
const liveModesSQL = `SELECT mode FROM go_api_routing_state
		 WHERE schema_digest = $1 AND document_digest = ANY($2::text[]) AND selected_operation = $3`

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
// on the Python side). A digest MISS -- the key resolves but no row
// exists for it -- is its own distinct, observable case (CHAOS-5415):
// it means delegation is silently reverting to Python for this
// operation, which looks identical to "not canaried yet" unless it is
// counted and logged separately; see recordDigestMiss.
//
// The one exception to "no routing-state row resolves to false" is the
// catalog switch (serveUnrouted, CHAOS-8517): see unroutedIsServed in
// catalog_switch.go. Every other switch built on this type keeps the rule
// above unchanged.
func (s *PostgresSwitch) Enabled(operation string) bool {
	documentDigest, ok := s.documentDigests[operation]
	if !ok {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, liveModesSQL,
		s.schemaDigest, acceptedDigests(documentDigest, s.legacyDigests[operation]), operation,
	)
	if err != nil {
		log.Printf("routeswitch: PostgresSwitch lookup failed for operation %q: %v", operation, err)
		return false
	}
	defer rows.Close()
	var modes []string
	for rows.Next() {
		var mode string
		if err := rows.Scan(&mode); err != nil {
			log.Printf("routeswitch: PostgresSwitch lookup failed for operation %q: %v", operation, err)
			return false
		}
		modes = append(modes, mode)
	}
	if err := rows.Err(); err != nil {
		log.Printf("routeswitch: PostgresSwitch lookup failed for operation %q: %v", operation, err)
		return false
	}
	if len(modes) == 0 {
		if s.serveUnrouted {
			return s.unroutedIsServed(ctx, operation, documentDigest)
		}
		recordDigestMiss(ctx, operation, s.schemaDigest, documentDigest)
		return false
	}
	return anyReachable(modes, s.reachable)
}

// acceptedDigests is the operation's current digest followed by its legacy ones, without a repeat.
func acceptedDigests(current string, legacy []string) []string {
	out := []string{current}
	for _, digest := range legacy {
		if digest != current && !contains(out, digest) {
			out = append(out, digest)
		}
	}
	return out
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// anyReachable is true when at least one row's mode is reachable: a row at any accepted digest that is
// on keeps the operation on, and a legacy row left at "disabled" does not switch off a live current row.
func anyReachable(modes []string, reachable map[string]bool) bool {
	for _, mode := range modes {
		if reachable[mode] {
			return true
		}
	}
	return false
}
