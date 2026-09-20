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

// routingRowSource is the FROM/WHERE/ORDER half every read below shares,
// spelled once so a second column list cannot come with a second row set
// or a second order. The order must stay TOTAL for the reason
// selectRepointCandidatesSQL's own comment gives.
const routingRowSource = `
  FROM public.go_api_routing_state
 WHERE schema_digest = $1
 ORDER BY selected_operation, document_digest`

// carryRowColumns is the WIDER list `carry` needs, in carryRow's scan
// order. Every column it selects is one `carry` COPIES verbatim to the
// target schema digest, so reading a narrower set would mean inventing a
// value for whatever was left out -- and `eligible_orgs` and
// `rollout_percentage` are exactly the columns a "preserving" verb must
// not normalise.
//
// `eligible_orgs::text` rather than the json value itself: the column is
// `json`, not `jsonb`, so Postgres stores the operator's bytes verbatim
// and `::text` hands them back unchanged -- NULL included, which is a
// DIFFERENT fact from an empty object and must survive the copy as one.
const carryRowColumns = routingRowColumns + `, owner, rollout_percentage, eligible_orgs::text, COALESCE(review_evidence, '')`

// surveyCarryRowsSQL is the UNLOCKED wide read, at whichever schema
// digest is passed -- the live one (the rows to carry) or the target one
// (the rows already there). Unlocked for the same reason
// surveyRoutingRowsSQL is: the candidate build must be registered before
// any routing-row lock is taken, and this read is what tells the verb
// which (document_digest, operation) keys to register.
const surveyCarryRowsSQL = `
SELECT ` + carryRowColumns + routingRowSource

// lockCarrySourceRowsSQL is that same wide read at the LIVE digest, now
// taking a SHARE lock, and it is what makes `carry`'s preservation claim
// true AT COMMIT rather than merely at the moment of an earlier read.
//
// THE DEFECT IT CLOSES, executed: under READ COMMITTED (pgx's default),
// `surveyCarryRowsSQL` sees the rows as they were when IT ran. An
// operator who changes a live row's mode from `canary` to `python` after
// that read and before this transaction commits changes the decision
// `carry` is in the middle of copying -- and the old version committed
// the SUPERSEDED decision at the target digest, so the first new pod
// would have served Go for an operation the operator had just turned
// off. "Preserve the operator's decision" has to mean the decision that
// is standing when the copy becomes real.
//
// FOR SHARE, not FOR UPDATE: this verb never writes at the live digest,
// and must not. A share lock blocks a concurrent WRITER of these rows
// (enable/disable/repoint) for the rest of this transaction while
// leaving every plain reader -- the Python edge's dispatcher and
// query-api's route switch, which take no locks at all -- completely
// unaffected. Production traffic never waits on a carry.
//
// WHERE IT RUNS is part of the fix, not an implementation detail: AFTER
// every candidate build has been registered and every target row
// written, immediately before the audit row and the commit. Taking it
// earlier would mean holding a routing-row lock while still registering
// candidate builds, which is the exact lock-order inversion CHAOS-5507
// left this package's shared order to prevent. Taken here, nothing
// remains to wait on, and the rows cannot move between this read and the
// commit.
const lockCarrySourceRowsSQL = `
SELECT ` + carryRowColumns + routingRowSource + `
   FOR SHARE`

// surveyRoutingRowsSQL is the UNLOCKED read.
//
// Its only job is to learn each row's document_digest so the candidate
// build can be registered BEFORE any routing-row lock is taken
// (CHAOS-5507). It takes no lock precisely because taking one here is the
// defect: document_digest is one of the four columns in the
// candidate-build key, so a verb cannot register without first reading
// it, and reading it under a lock is what put the two writers in opposite
// orders.
const surveyRoutingRowsSQL = `
SELECT ` + routingRowColumns + routingRowSource

// selectRepointCandidatesSQL is that same read, now LOCKING.
//
// Defined AS the survey plus FOR UPDATE rather than retyped, so the two
// passes cannot drift into scanning different rows in a different order.
//
// The order must be TOTAL: `selected_operation` alone TIES whenever an
// operation has several rows under different document digests, and a tie
// means two concurrent writers can take the same rows in different orders
// -- a deadlock cycle on this table by itself, with the candidate-build
// table never involved. `(selected_operation, document_digest)` is the
// row's full identity within a schema digest, so it cannot tie.
//
// Unfiltered by operation on purpose: a verb that locked only the rows it
// intended to write would leave two concurrent writers holding
// overlapping-but-different sets.
const selectRepointCandidatesSQL = surveyRoutingRowsSQL + `
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

// readCarryRows is readRoutingRows over the wider column list. Two
// readers rather than one widened reader because `repoint` and `disable`
// must keep scanning exactly what they write from: a reader that handed
// them four more columns would invite a future write to set one.
func readCarryRows(ctx context.Context, tx pgx.Tx, sql, schemaDigest string) ([]CarryRow, error) {
	rows, err := tx.Query(ctx, sql, schemaDigest)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read routing rows: %w", err)
	}
	defer rows.Close()
	var found []CarryRow
	for rows.Next() {
		row, err := scanCarryRow(rows)
		if err != nil {
			return nil, err
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goapiproof: read routing rows: %w", err)
	}
	return found, nil
}

// rowScanner is what both a multi-row pgx.Rows and a single pgx.Row
// satisfy, so the wide row is scanned by ONE function wherever it is
// read -- a second hand-written scan is how a column list and its scan
// order drift apart.
type rowScanner interface {
	Scan(destination ...any) error
}

func scanCarryRow(scanner rowScanner) (CarryRow, error) {
	var row CarryRow
	if err := scanner.Scan(&row.Operation, &row.DocumentDigest, &row.Mode, &row.Build,
		&row.Owner, &row.RolloutPercentage, &row.EligibleOrgs, &row.ReviewEvidence); err != nil {
		return CarryRow{}, fmt.Errorf("goapiproof: scan routing row: %w", err)
	}
	return row, nil
}
