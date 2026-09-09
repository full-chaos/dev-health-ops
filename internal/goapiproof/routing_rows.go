package goapiproof

// The one reader of go_api_routing_state that every verb shares.
//
// It exists so the verbs cannot disagree about WHICH rows they read, in
// WHAT ORDER, or what a row's columns mean. Two multi-row writers that
// visit rows in different orders contend on this table in different
// orders too, so "the ordered predicate" is a shared artifact rather than
// something each verb spells out for itself.

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// routingRowColumns is the column list every read below selects, in the
// order routingRow's scan expects. Shared so a column added to one read
// cannot silently shift another's scan.
const routingRowColumns = `selected_operation, document_digest, mode, current_candidate_build`

// selectRepointCandidatesSQL reads every row at a schema digest, in a
// TOTAL order, LOCKING each one.
//
// The order is not cosmetic, and it must be total (r2 R2-09):
// `selected_operation` alone TIES whenever an operation has several rows
// under different document digests, and a tie means two concurrent
// writers can take the same rows in different orders -- which is a
// deadlock cycle on this table by itself, with the candidate-build table
// never involved. `(selected_operation, document_digest)` is the row's
// full identity within a schema digest, so it cannot tie.
//
// Unfiltered by operation on purpose: a verb that locked only the rows it
// intended to write would leave two concurrent writers holding
// overlapping-but-different sets.
const selectRepointCandidatesSQL = `
SELECT ` + routingRowColumns + `
  FROM public.go_api_routing_state
 WHERE schema_digest = $1
 ORDER BY selected_operation, document_digest
   FOR UPDATE`

// selectDisableCandidatesSQL is the SAME read, and it LOCKS.
//
// The Python verb's `plan_disable` does not lock, and this matched it
// until r2 R2-08: an unlocked read means a concurrent `enable` can commit
// between the plan and the write, so the off-ramp reports success while
// the operation is reachable again. For a verb whose entire job is "make
// this not served", reporting success over a still-reachable row is the
// worst failure available to it. Python's behaviour here is a
// baseline_defect, not a contract worth matching.
//
// This introduces no lock-order hazard. `disable` touches ONLY
// go_api_routing_state -- it never registers a candidate build -- so it
// can never hold a candidate-build lock while waiting for a routing row,
// which is the cycle CHAOS-5507 is about. Aliased rather than retyped so
// the two verbs cannot drift into different predicates or orders.
const selectDisableCandidatesSQL = selectRepointCandidatesSQL

// routingRow is one row as any read returns it.
type routingRow struct {
	operation, documentDigest, mode, build string
}

func readRoutingRows(ctx context.Context, tx pgx.Tx, sql, schemaDigest string) ([]routingRow, error) {
	rows, err := tx.Query(ctx, sql, schemaDigest)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read routing rows: %w", err)
	}
	defer rows.Close()
	var found []routingRow
	for rows.Next() {
		var row routingRow
		if err := rows.Scan(&row.operation, &row.documentDigest, &row.mode, &row.build); err != nil {
			return nil, fmt.Errorf("goapiproof: scan routing row: %w", err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goapiproof: read routing rows: %w", err)
	}
	return found, nil
}
