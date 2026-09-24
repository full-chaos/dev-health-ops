package routeswitch

import "github.com/jackc/pgx/v5/pgxpool"

// proofReachableModes widens reachableModes by exactly one entry:
// "shadow".
//
// Why a second mode set exists at all. PostgresSwitch's reachableModes is
// the PRODUCTION reachability answer, and "shadow" is deliberately absent
// from it: in shadow mode the client still receives Python's response
// (plan §5 stage 4), so a shadowed operation must not be reachable to a
// real request. That is correct and stays untouched.
//
// It also means the deployed Go build cannot execute a shadowed operation
// AT ALL -- /query answers 404, measured live on 2026-09-07 (enablement
// artifact 51-harness-control.json: flowMatrix go_status=[404,404]). So
// the four shadow operations could never be proven, and the parity
// defects behind their shadow status (CHAOS-5447-5451) could never be
// re-measured after a fix, because the only way to see Go's answer was to
// canary them -- exposing real traffic to the very divergence under
// investigation.
//
// This switch resolves that without touching production reachability: it
// backs a SEPARATE handler, on a separate path, registered only under an
// explicit environment flag and never in a production posture, which the
// Python edge never forwards to. Every receipt produced through it
// records measurement_route='proof', so a proof-route observation can
// never be mistaken for served traffic (team-lead ruling R50,
// 2026-09-09).
//
// "disabled" and "python" stay unreachable here too. They are not
// "awaiting measurement" -- they are decisions, and a measurement route
// that quietly executed an operation somebody turned OFF would be the
// same class of surprise this whole file guards against.
var proofReachableModes = map[string]bool{
	"canary":  true,
	"primary": true,
	"shadow":  true,
}

// NewProofSwitch builds a measurement-only Switch over the SAME
// go_api_routing_state read PostgresSwitch performs, differing only in
// which modes it treats as reachable.
//
// Sharing the query rather than copying it is deliberate: the lookup key
// (schema_digest, document_digest, selected_operation) is the exact thing
// a stale copy would get wrong, and getting it wrong is what CHAOS-5416
// records six days of silent fallback for.
func NewProofSwitch(pool *pgxpool.Pool, schemaDigest string, documentDigests map[string]string) *PostgresSwitch {
	sw := NewPostgresSwitch(pool, schemaDigest, documentDigests)
	sw.reachable = proofReachableModes
	return sw
}
