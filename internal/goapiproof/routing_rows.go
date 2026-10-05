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
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"

	"github.com/jackc/pgx/v5"
)

// ErrRoutingTableEmpty reports that go_api_routing_state holds NO row at any
// schema digest (CHAOS-8543).
//
// It is not a refusal. Since the catalog rule (queryapi/routeswitch/
// catalog_switch.go) an empty table is a valid state: query-api serves every
// registered operation that has no routing row, and no MCP class root is
// enabled. `carry` and `repoint` have nothing to do on it and nothing is
// wrong, so the commands answer it as a no-op, exit 0, and say so in one
// line -- a chart install that serves from an empty table must not fail its
// pre-upgrade or post-upgrade hook.
//
// It is a DIFFERENT value from ErrRepointNoRows on purpose. That one names "rows exist, none at the live digest, and one of them is
// in a served mode": by the same catalog rule a row left at another digest
// holds its operation dark, so an operation somebody turned on is dark, and
// that state stays a refusal an upgrade cannot hide. "The table is empty" and
// "every row died" must not read alike (CHAOS-5416), and until this value
// existed both verbs answered the two with the same error. Rows elsewhere that
// are ALL in a dark mode are ErrRoutingRowsOnlyDark, the third value.
var ErrRoutingTableEmpty = errors.New("goapiproof: go_api_routing_state has no row at any schema digest")

// ErrRoutingRowsOnlyDark reports that go_api_routing_state holds no row at the
// schema digest a verb works on, and every row it holds elsewhere is in a mode
// no plane serves -- python, disabled or shadow (CHAOS-8586).
//
// Not a refusal, for the reason ErrRoutingTableEmpty is not one. By the catalog
// rule each of those rows holds its operation dark at ANY digest, so a roll
// changes nothing that anyone is served: the operation is dark before and dark
// after, as its row says. It is the state a stack reaches one roll after it
// holds an operation dark -- `carry` skips a python or disabled row, the row
// stays at the old digest -- so refusing it failed the post-upgrade `repoint`
// of that same roll, and the pre-upgrade `carry` of the next.
//
// A row in a served mode (canary or primary) at another digest is different:
// an operation an operator turned on is dark, and that stays the refusal
// (ErrRepointNoRows) an upgrade must not hide.
var ErrRoutingRowsOnlyDark = errors.New("goapiproof: no routing row at this schema digest, and every row at another digest holds its operation dark by its own mode")

// DarkRoutingRow is one row behind an ErrRoutingRowsOnlyDark answer: where it
// sits and the dark mode that holds its operation.
type DarkRoutingRow struct {
	SchemaDigest   string
	DocumentDigest string
	Operation      string
	Mode           string
}

// RoutingRowsOnlyDarkError is the ErrRoutingRowsOnlyDark answer with the rows
// it rests on, read under the same lock as the decision, so the verb can name
// every operation it left dark rather than only say that it did nothing.
type RoutingRowsOnlyDarkError struct {
	SchemaDigest string
	Rows         []DarkRoutingRow
}

func (e *RoutingRowsOnlyDarkError) Error() string {
	return fmt.Sprintf("%v (%d row(s), none at %s)", ErrRoutingRowsOnlyDark, len(e.Rows), e.SchemaDigest)
}

func (e *RoutingRowsOnlyDarkError) Unwrap() error { return ErrRoutingRowsOnlyDark }

// servedMode reports whether a row in this mode is served to a client by
// either plane (routeswitch/postgres_switch.go reachableModes).
func servedMode(mode string) bool {
	return mode == TargetModeCanary || mode == TargetModePrimary
}

// lockRoutingTableSQL takes a SHARE lock on the whole routing table.
//
// SHARE conflicts with ROW EXCLUSIVE, the lock every INSERT, UPDATE and DELETE
// takes, and with nothing a plain reader takes, so the planes' route switches
// never wait on it. Two holders of SHARE do not conflict.
const lockRoutingTableSQL = `LOCK TABLE public.go_api_routing_state IN SHARE MODE`

// noLiveRowCensusSQL counts, in ONE statement, what the answer to "no row at
// this schema digest" rests on: rows at the digest (a writer committed since
// the survey), rows in a served mode at any other digest, and rows at all.
const noLiveRowCensusSQL = `
SELECT count(*) FILTER (WHERE schema_digest = $1),
       count(*) FILTER (WHERE schema_digest <> $1 AND mode = ANY($2)),
       count(*)
  FROM public.go_api_routing_state
 WHERE left(selected_operation, ` + mcpClassPrefixLength + `) <> '` + mcpclass.OperationPrefix + `'`

// darkRowsElsewhereSQL lists every row once the census has found them all dark
// and none at $1, in the table's one total order.
const darkRowsElsewhereSQL = `
SELECT schema_digest, document_digest, selected_operation, mode
  FROM public.go_api_routing_state
 WHERE left(selected_operation, ` + mcpClassPrefixLength + `) <> '` + mcpclass.OperationPrefix + `'
 ORDER BY selected_operation, schema_digest, document_digest`

// noLiveRowAnswer is what a verb whose survey found no row at its schema digest
// learns once it holds the table lock.
type noLiveRowAnswer int

const (
	// noLiveRowAppeared: a row now exists at the digest. The survey is stale.
	noLiveRowAppeared noLiveRowAnswer = iota + 1
	// noLiveRowTableEmpty: no row at any digest (ErrRoutingTableEmpty).
	noLiveRowTableEmpty
	// noLiveRowOnlyDark: rows exist, none at the digest, none in a served
	// mode (ErrRoutingRowsOnlyDark).
	noLiveRowOnlyDark
	// noLiveRowServedElsewhere: a canary or primary row sits at another
	// digest, so an operation somebody turned on is dark.
	noLiveRowServedElsewhere
)

// noLiveRowError is the error a verb returns for answers that need no verb-specific
// sentinel: ErrRoutingTableEmpty, or the RoutingRowsOnlyDarkError naming the rows.
func noLiveRowError(ctx context.Context, tx pgx.Tx, schemaDigest string, answer noLiveRowAnswer) error {
	if answer == noLiveRowTableEmpty {
		return ErrRoutingTableEmpty
	}
	rows, err := tx.Query(ctx, darkRowsElsewhereSQL)
	if err != nil {
		return fmt.Errorf("goapiproof: read the dark routing rows: %w", err)
	}
	defer rows.Close()
	dark := &RoutingRowsOnlyDarkError{SchemaDigest: schemaDigest}
	for rows.Next() {
		var row DarkRoutingRow
		if err := rows.Scan(&row.SchemaDigest, &row.DocumentDigest, &row.Operation, &row.Mode); err != nil {
			return fmt.Errorf("goapiproof: scan a dark routing row: %w", err)
		}
		dark.Rows = append(dark.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("goapiproof: read the dark routing rows: %w", err)
	}
	return dark
}

// answerNoLiveRow is the one answer `carry` and `repoint` give when their
// unlocked survey found no row at the schema digest they work on.
//
// It LOCKS THE TABLE FIRST and decides from a read taken under that lock
// (CHAOS-8586 defect 3). The check used to be a plain read, so a writer whose
// row was written but not committed when the check ran committed a moment
// later, and the verb had already answered "nothing here": its row was never
// carried, and the roll left its operation dark. Under the SHARE lock the read
// waits for every writer already in flight and no new writer can commit before
// this transaction ends, so the answer is still true when the verb returns it.
//
// Taken here, the lock adds no lock-order hazard: the verb has registered no
// candidate build and holds no routing-row lock -- only the ACCESS SHARE of its
// survey, which no row writer (enable, disable, seed, carry, repoint) ever waits
// on -- so it can wait in a cycle but never hold one. Once granted it reads and
// ends its transaction on every answer: noLiveRowAppeared is answered by
// refusing (carry) or by starting over (repoint), never by registering a build
// while it holds the lock.
func answerNoLiveRow(ctx context.Context, tx pgx.Tx, schemaDigest string) (noLiveRowAnswer, error) {
	if _, err := tx.Exec(ctx, lockRoutingTableSQL); err != nil {
		return 0, fmt.Errorf("goapiproof: lock go_api_routing_state: %w", err)
	}
	var atDigest, servedElsewhere, total int
	if err := tx.QueryRow(ctx, noLiveRowCensusSQL, schemaDigest, []string{TargetModeCanary, TargetModePrimary}).Scan(&atDigest, &servedElsewhere, &total); err != nil {
		return 0, fmt.Errorf("goapiproof: read which routing rows exist: %w", err)
	}
	switch {
	case atDigest > 0:
		return noLiveRowAppeared, nil
	case total == 0:
		return noLiveRowTableEmpty, nil
	case servedElsewhere == 0:
		return noLiveRowOnlyDark, nil
	default:
		return noLiveRowServedElsewhere, nil
	}
}

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
SELECT ` + routingRowColumns + documentRoutingRowSource

// documentRoutingRowSource is routingRowSource without the MCP class rows: a class root is decided by
// go_api_class_decision (CHAOS-8735), so the document verbs (repoint, disable) neither survey nor lock a
// legacy class row, and a stale one at another digest cannot make them refuse.
const documentRoutingRowSource = `
  FROM public.go_api_routing_state
 WHERE schema_digest = $1
   AND left(selected_operation, ` + mcpClassPrefixLength + `) <> '` + mcpclass.OperationPrefix + `'
 ORDER BY selected_operation, document_digest`

const mcpClassPrefixLength = "4"

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
