package routeswitch

// The MCP class switches (CHAOS-8735, owner ruling D4797). A class root's decision is one row per operation in
// go_api_class_decision, so the answer cannot depend on a schema digest: a schema change moves nothing, and
// there is no ordering of rows to get wrong. The serving switch answers true for canary and primary; the proof
// switch (the route that measures a root before it is enabled) also for shadow, exactly as the proof switch of
// the document rows does. A root absent from the inventory, or with no decision, is dark.

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ClassDecisionSwitch is the switch of the MCP class-row gate and the MCP listener.
type ClassDecisionSwitch struct {
	pool      registryReader
	operation map[string]struct{}
	reachable map[string]bool
}

func newClassDecisionSwitch(pool *pgxpool.Pool, operations map[string]string, reachable map[string]bool) *ClassDecisionSwitch {
	if pool == nil {
		panic("routeswitch: a class decision switch requires a non-nil pool")
	}
	inventory := make(map[string]struct{}, len(operations))
	for operation := range operations {
		inventory[operation] = struct{}{}
	}
	return &ClassDecisionSwitch{pool: pool, operation: inventory, reachable: reachable}
}

// NewClassDecisionSwitch serves canary and primary decisions.
func NewClassDecisionSwitch(pool *pgxpool.Pool, operations map[string]string) *ClassDecisionSwitch {
	return newClassDecisionSwitch(pool, operations, reachableModes)
}

// NewClassDecisionProofSwitch also serves shadow decisions: the measurement-only proof route.
func NewClassDecisionProofSwitch(pool *pgxpool.Pool, operations map[string]string) *ClassDecisionSwitch {
	return newClassDecisionSwitch(pool, operations, proofReachableModes)
}

const classDecisionModeSQL = `SELECT mode FROM go_api_class_decision WHERE operation = $1`

// Enabled reports whether the class root's decision serves it. A read that fails refuses: a table this process
// cannot read cannot show that the root is lit.
func (s *ClassDecisionSwitch) Enabled(operation string) bool {
	if _, ok := s.operation[operation]; !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, classDecisionModeSQL, operation)
	if err != nil {
		log.Printf("routeswitch: ClassDecisionSwitch lookup failed for operation %q: %v", operation, err)
		return false
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			log.Printf("routeswitch: ClassDecisionSwitch lookup failed for operation %q: %v", operation, err)
		}
		return false
	}
	var mode string
	if err := rows.Scan(&mode); err != nil {
		log.Printf("routeswitch: ClassDecisionSwitch lookup failed for operation %q: %v", operation, err)
		return false
	}
	return s.reachable[mode]
}
