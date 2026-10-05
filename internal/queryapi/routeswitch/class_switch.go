package routeswitch

// The class-row switch (CHAOS-8704, owner ruling D4789 of 2026-10-04): an MCP class root is served when
// its class row is in a served mode at ANY schema digest. Class rows used to be keyed to the live schema
// digest, so every schema change left them stale and dark until `carry` copied them forward; with carry
// gone the digest is no longer a condition. The one-time proof (`dho goapi routing enable` with a
// receipt) is still what writes a class row.
//
// A root with no class row stays dark. A row in a non-served mode stays dark. When rows exist at the live
// schema digest they decide alone, so `disable` at the live digest still darks a root whose older row
// is canary; with none, every row of the root decides, and any served one serves it.

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

const classRowsSQL = `SELECT mode, schema_digest = $3 FROM go_api_routing_state
		 WHERE document_digest = $1 AND selected_operation = $2`

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
	var liveModes, otherModes []string
	for rows.Next() {
		var mode string
		var live bool
		if err := rows.Scan(&mode, &live); err != nil {
			log.Printf("routeswitch: ClassSwitch lookup failed for operation %q: %v", operation, err)
			return false
		}
		if live {
			liveModes = append(liveModes, mode)
		} else {
			otherModes = append(otherModes, mode)
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("routeswitch: ClassSwitch lookup failed for operation %q: %v", operation, err)
		return false
	}
	if len(liveModes) > 0 {
		return anyReachable(liveModes, reachableModes)
	}
	return anyReachable(otherModes, reachableModes)
}
