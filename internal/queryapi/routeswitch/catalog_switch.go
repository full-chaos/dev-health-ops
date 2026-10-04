package routeswitch

// The catalog rule (CHAOS-8517, owner's decision of 2026-10-03: "Serve the catalog").
//
// go_api_routing_state was the per-operation switch between the Python api and Go. The Python api is
// gone, so on a stack whose table is empty -- every fresh self-hosted stack -- "no row" stopped meaning
// "Python serves this" and came to mean "nobody does": every registered operation answered
// OPERATION_NOT_ENABLED, and `enable` could not help, because it admits an operation only on a proof
// run or a compiled ledger limit.
//
// The rule: a registered (catalog) operation that has NO routing row -- at any schema digest, under any
// document digest -- is served. An operation that has a row keeps every rule it had:
//
//	rows the switch can see for one catalog operation        before     after
//	no row at any schema digest                              refused    SERVED (reason catalog_no_row)
//	live row, mode canary or primary                         served     served (the same read, nothing added)
//	live row, mode shadow, python or disabled                refused    refused: the row holds the operation dark
//	no live row, a row at another schema digest              refused    refused (digest miss, as before)
//	no live row, a row under another document digest         refused    refused (digest miss, as before)
//	the table cannot be read                                 refused    refused (fails closed)
//
// "Live row" is a row at this process's schema digest under a document digest the operation accepts
// (its current text or a legacy one). current_candidate_build is not read, before or after.
//
// Why "no row at ANY digest" and not "no row at the live digest". `carry` copies served and shadow rows
// to a new schema digest and skips python and disabled rows, because a missing row and a python row
// used to answer alike (goapiproof/routing_carry.go, DecideCarry). If a missing LIVE row were enough,
// every `disable` would be undone by the next schema-digest change: the skipped row would stay at the
// old digest and the operation would be served again with nobody having decided it. With this rule a
// row left behind still holds the operation dark, so `carry`, `disable` and the stale-rows alarm
// (server/registry_route.go) mean what they meant.
//
// The lever to hold a catalog operation dark is therefore a routing row in a non-served mode: `disable`
// where a row exists, `seed` (it writes a shadow row) where none does. It is stored, it is visible in
// `dho goapi routing status`, and only `enable` lifts it -- with no proof rule for a catalog operation,
// since a check on the row that lifts a hold guards nothing the rule does not already serve (CHAOS-8586).
//
// What this does NOT touch: the class-row switch (server/class_row_gate.go, newClassRowSwitch) and the
// proof switches (proof_switch.go) are built by the other constructors and never set serveUnrouted, so
// an MCP class root with no row is dark and a measurement route still needs a row.

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewCatalogSwitchWithLegacy is NewPostgresSwitchWithLegacy with the catalog rule: the switch /query
// and /graphql serve registered documents through. documentDigests must be the registered-document
// inventory of the process (server/query_route.go digestByOperation): an operation absent from it is
// never served, row or no row.
func NewCatalogSwitchWithLegacy(pool *pgxpool.Pool, schemaDigest string, documentDigests map[string]string, legacyDigests map[string][]string) *PostgresSwitch {
	sw := NewPostgresSwitchWithLegacy(pool, schemaDigest, documentDigests, legacyDigests)
	sw.serveUnrouted = true
	return sw
}

// anyRowSQL asks whether an operation has a routing row at all: any schema digest, any document digest,
// any mode. Only the catalog switch runs it, and only for an operation with no live row -- so an
// operation served by its row is answered by liveModesSQL alone, exactly as before.
const anyRowSQL = `SELECT 1 FROM go_api_routing_state WHERE selected_operation = $1 LIMIT 1`

// unroutedIsServed decides an operation that has no live row. It is served only when it has no routing
// row at all; a row anywhere else keeps today's answer and today's digest-miss signal. A read that
// fails is a refusal: a table this process cannot read cannot show that nothing holds the operation
// dark.
func (s *PostgresSwitch) unroutedIsServed(ctx context.Context, operation, documentDigest string) bool {
	rows, err := s.pool.Query(ctx, anyRowSQL, operation)
	if err != nil {
		log.Printf("routeswitch: PostgresSwitch lookup failed for operation %q: %v", operation, err)
		return false
	}
	defer rows.Close()
	routed := rows.Next()
	if err := rows.Err(); err != nil {
		log.Printf("routeswitch: PostgresSwitch lookup failed for operation %q: %v", operation, err)
		return false
	}
	if routed {
		recordDigestMiss(ctx, operation, s.schemaDigest, documentDigest)
		return false
	}
	_, announced := s.announced.LoadOrStore(operation, true)
	recordServedWithoutRow(ctx, operation, !announced)
	return true
}
