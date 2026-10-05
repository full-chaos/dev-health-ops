package routeswitch

// The class-row switch (CHAOS-8704, owner ruling D4789 of 2026-10-04, refined by D4796): an MCP class root is
// decided by its NEWEST class row across all schema digests. Class rows used to be keyed to the live schema
// digest, so every schema change left them dark until `carry` moved them; with carry gone the digest is no
// longer a condition, and no re-proof is needed after a schema change. The newest row decides (ordered by
// updated_at; on a tie the row at the live digest wins), so a decision stays durable: a `disable` written at
// one digest is not undone by an older canary row at another.
//
// A root with no class row stays dark. The root is served only when its newest row is in a served mode
// (canary or primary). The one-time proof (`dho goapi routing enable` with a receipt) is still what writes a
// class row.

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ClassSwitch is the switch of the MCP class-row gate and the MCP listener.
type ClassSwitch struct {
	pool            registryReader
	schemaDigest    string
	documentDigests map[string]string
}

// NewClassSwitch builds a ClassSwitch. pool must not be nil. documentDigests is the class-root inventory
// (server/class_row_gate.go mcpRoutingDigests): a root absent from it is never served.
func NewClassSwitch(pool *pgxpool.Pool, schemaDigest string, documentDigests map[string]string) *ClassSwitch {
	if pool == nil {
		panic("routeswitch: NewClassSwitch requires a non-nil pool")
	}
	copied := make(map[string]string, len(documentDigests))
	for k, v := range documentDigests {
		copied[k] = v
	}
	return &ClassSwitch{pool: pool, schemaDigest: schemaDigest, documentDigests: copied}
}

const classRowsSQL = `SELECT mode FROM go_api_routing_state
		 WHERE document_digest = $1 AND selected_operation = $2
		 ORDER BY updated_at DESC, (schema_digest = $3) DESC
		 LIMIT 1`

// Enabled reports whether the class root's rows serve it. A read that fails refuses: a table this process
// cannot read cannot show that the root is lit.
func (s *ClassSwitch) Enabled(operation string) bool {
	documentDigest, ok := s.documentDigests[operation]
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, classRowsSQL, documentDigest, operation, s.schemaDigest)
	if err != nil {
		log.Printf("routeswitch: ClassSwitch lookup failed for operation %q: %v", operation, err)
		return false
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			log.Printf("routeswitch: ClassSwitch lookup failed for operation %q: %v", operation, err)
		}
		return false
	}
	var mode string
	if err := rows.Scan(&mode); err != nil {
		log.Printf("routeswitch: ClassSwitch lookup failed for operation %q: %v", operation, err)
		return false
	}
	return reachableModes[mode]
}
