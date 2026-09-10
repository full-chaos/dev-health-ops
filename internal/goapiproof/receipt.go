package goapiproof

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// EnablementProofStage and EnablementProofTerminalState are two of the
// values `dev-hops go-api routing enable`'s preflight selects on.
//
// They are NOT the whole rule, and this comment used to say they were
// ("the exact pair ... a receipt written with any other pair authorizes
// nothing"). CHAOS-5484 made a FULLY-CITED mismatch enablement proof and
// split admission by target mode, so the rule is now
// EnablementProofClause -- stage, terminal state, the citation counters,
// AND the route. Read that function; do not infer the rule from these two
// constants.
const (
	EnablementProofStage         = "deployed_executed"
	EnablementProofTerminalState = "match"
)

// Receipt is one immutable proof-run row.
//
// The four-column key (SchemaDigest, DocumentDigest, SelectedOperation,
// CandidateBuild) is the whole point: plan §8.3's rule is that a proof is
// evidence for exactly one such tuple and is "never carried forward
// across any of the four changing". CandidateBuild here is always the
// build identity READ FROM THE RUNNING PROCESS, never an operator-typed
// name -- see BuildIdentity.
type Receipt struct {
	SchemaDigest         string
	DocumentDigest       string
	SelectedOperation    string
	CandidateBuild       string
	RequestIdentity      string
	Stage                string
	TerminalState        string
	BaselineResponseRef  string
	CandidateResponseRef string
	DataWatermark        string
	OrgID                string
	ReviewEvidence       string
	RecordedBy           string
	ObservedAt           time.Time

	// MeasurementRoute is "edge" or "proof" -- which route executed the
	// candidate leg. A proof-route receipt must never read as served
	// traffic (team-lead ruling R50, 2026-09-09), and a receipt that did
	// not SAY which route produced it would leave that distinction in a
	// chat message instead of in the row.
	MeasurementRoute string

	// BaselineDefects names the tickets whose declared field paths cover
	// this comparison's differences. It NEVER softens TerminalState --
	// see BaselineDefect for why that separation is the whole point.
	BaselineDefects []string

	// DifferencesOutsideBaselineDefect is written even when zero, so
	// "every difference here is a known Python defect" is a claim a
	// reader can check rather than take on trust.
	DifferencesOutsideBaselineDefect int

	// BuildBinding is EdgeBuildPresent or EdgeBuildAbsent -- how strongly
	// the measurement tied the serving build to the request it compared
	// (alembic 0129, CHAOS-5484). Required for the same reason
	// MeasurementRoute is: a receipt that did not say how well it knew
	// which build served it cannot be told apart later from one that knew
	// exactly.
	BuildBinding string
}

// Querier is the subset of pgx this package needs, so a test can pass a
// pool, a connection, or a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// TxBeginner is implemented by a pgx pool. A caller that can begin a
// transaction gets one; a test fake that cannot is written directly.
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// WriteAtomic writes one receipt inside its own transaction when db can
// begin one, and falls back to a direct Write when it cannot.
//
// Write is two statements -- a candidate-build upsert and a proof-run
// insert -- and the runner previously handed it a bare pool, so a failure
// between them left a registered build with no receipt (codex r1 F7). That
// orphan is harmless on its own (the table is append-only and the upsert is
// idempotent), but "harmless" was an argument, not a guarantee, and the
// guarantee costs one BEGIN. The fallback exists so the in-memory tests can
// still drive Write directly.
func WriteAtomic(ctx context.Context, db Querier, receipt Receipt) (uuid.UUID, error) {
	beginner, ok := db.(TxBeginner)
	if !ok {
		return Write(ctx, db, receipt)
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("goapiproof: begin receipt transaction: %w", err)
	}
	id, err := Write(ctx, tx, receipt)
	if err != nil {
		// Rollback's own error is deliberately not returned: it would
		// replace the error that actually explains the failure.
		_ = tx.Rollback(ctx)
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("goapiproof: commit receipt: %w", err)
	}
	return id, nil
}

// Write records one receipt, registering its candidate build first.
//
// Two statements, in this order and inside the caller's transaction:
//
//  1. go_api_candidate_build INSERT ... ON CONFLICT DO NOTHING. That
//     table is immutable and append-only (never an UPDATE), and
//     go_api_proof_run carries a 4-column composite FK to it -- so a
//     receipt for a build that was never registered would be rejected by
//     the database, not silently dropped.
//  2. go_api_proof_run INSERT. Append-only by contract: a proof is a
//     historical fact, so a re-run adds a row, never overwrites one. Two
//     receipts for the same tuple with different verdicts is a legible
//     history ("it matched on Monday and mismatched on Tuesday"), which
//     an upsert would destroy.
//
// The stage/terminal_state vocabularies are enforced by CHECK
// constraints; this validates them first so a programming error at the
// call site fails with a specific message rather than a constraint name.
func Write(ctx context.Context, db Querier, receipt Receipt) (uuid.UUID, error) {
	if err := validateVocabulary(receipt); err != nil {
		return uuid.Nil, err
	}
	if strings.Trim(receipt.CandidateBuild, blankCitationCutset) == "" {
		// Trimmed with blankCitationCutset, not `== ""`: `proven` is
		// keyed on this column, and a
		// whitespace-only build is as unmatchable as an absent one while
		// LOOKING present in every listing. Found by enumerating the
		// guard's input domain rather than by a failure.
		return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a receipt with an empty candidate build")
	}
	if !buildBindings[receipt.BuildBinding] {
		return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a receipt with build binding %q -- expected %q or %q", receipt.BuildBinding, EdgeBuildPresent, EdgeBuildAbsent)
	}
	// A citation that names nothing is not a citation. The enablement
	// predicate admits a mismatch when `cardinality(baseline_defect) > 0`
	// and every difference is covered -- and `cardinality(ARRAY[''])` is
	// 1, so a receipt citing a single empty ticket was admitted as fully
	// cited. Executed against real PostgreSQL before this guard existed:
	// `EMPTY-STRING CITATION admitted as enablement proof: true`.
	//
	// Refused HERE rather than widened in SQL, because both predicates
	// read the column and a rule stated in two places drifts -- which is
	// the defect r2 found in this very PR. The writer is the one place
	// that can refuse it once.
	for _, ticket := range receipt.BaselineDefects {
		if strings.Trim(ticket, blankCitationCutset) == "" {
			return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a receipt whose baseline_defect array contains an empty citation (%d entries) -- cardinality() counts it, so the enablement predicate would read this as a fully-cited mismatch while it cites nothing", len(receipt.BaselineDefects))
		}
	}
	if !measurementRoutes[receipt.MeasurementRoute] {
		// A receipt with no route cannot be told apart from served
		// traffic later, which is the entire reason the column exists.
		return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a receipt with measurement route %q -- expected %q or %q", receipt.MeasurementRoute, RouteEdge, RouteProof)
	}

	if _, err := db.Exec(ctx,
		`INSERT INTO go_api_candidate_build
		   (schema_digest, document_digest, selected_operation, candidate_build, registered_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (schema_digest, document_digest, selected_operation, candidate_build) DO NOTHING`,
		receipt.SchemaDigest, receipt.DocumentDigest, receipt.SelectedOperation,
		receipt.CandidateBuild, receipt.ObservedAt,
	); err != nil {
		return uuid.Nil, fmt.Errorf("goapiproof: register candidate build for %s: %w", receipt.SelectedOperation, err)
	}

	id := uuid.New()
	if _, err := db.Exec(ctx,
		`INSERT INTO go_api_proof_run
		   (id, schema_digest, document_digest, selected_operation, candidate_build,
		    request_identity, stage, terminal_state,
		    baseline_response_ref, candidate_response_ref,
		    data_watermark, org_id, review_evidence, recorded_by, observed_at,
		    measurement_route, baseline_defect, differences_outside_baseline_defect,
		    build_binding)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
		id, receipt.SchemaDigest, receipt.DocumentDigest, receipt.SelectedOperation, receipt.CandidateBuild,
		receipt.RequestIdentity, receipt.Stage, receipt.TerminalState,
		nullIfEmpty(receipt.BaselineResponseRef), nullIfEmpty(receipt.CandidateResponseRef),
		nullIfEmpty(receipt.DataWatermark), nullIfEmpty(receipt.OrgID),
		nullIfEmpty(receipt.ReviewEvidence), nullIfEmpty(receipt.RecordedBy), receipt.ObservedAt,
		nullIfEmpty(receipt.MeasurementRoute), receipt.BaselineDefects,
		receipt.DifferencesOutsideBaselineDefect,
		nullIfEmpty(receipt.BuildBinding),
	); err != nil {
		return uuid.Nil, fmt.Errorf("goapiproof: record proof run for %s: %w", receipt.SelectedOperation, err)
	}
	return id, nil
}

// stages and terminalStates mirror
// src/dev_health_ops/models/go_api_registry.py's STAGES and
// TERMINAL_STATES exactly. The DB CHECK constraints are the backstop;
// this is the fast, specific failure for a programming error.
var stages = map[string]bool{
	"dual_run": true, "deployed_executed": true, "shadow": true, "canary": true,
}

var measurementRoutes = map[string]bool{RouteEdge: true, RouteProof: true}

var buildBindings = map[string]bool{EdgeBuildPresent: true, EdgeBuildAbsent: true}

var terminalStates = map[string]bool{
	"match": true, "mismatch": true, "auth_rejected": true, "validation_rejected": true,
	"dependency_failed": true, "timeout": true, "cancelled": true, "resource_exhausted": true,
	"fallback": true, "unsupported": true, "proof_failed": true,
}

func validateVocabulary(receipt Receipt) error {
	if !stages[receipt.Stage] {
		return fmt.Errorf("goapiproof: invalid proof-run stage %q", receipt.Stage)
	}
	if !terminalStates[receipt.TerminalState] {
		return fmt.Errorf("goapiproof: invalid proof-run terminal_state %q", receipt.TerminalState)
	}
	// The DB carries the same rule as a CHECK
	// (ck_go_api_proof_run_shadow_requires_watermark); stating it here
	// makes the reason legible at the call site.
	if receipt.Stage == "shadow" && receipt.DataWatermark == "" {
		return fmt.Errorf("goapiproof: a shadow-stage receipt requires a data watermark (same-watermark comparison is what the stage claims)")
	}
	return nil
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// Target modes `dev-hops go-api routing enable` can be asked for, and the
// route rule each carries (team-lead ruling, 2026-09-09; CHAOS-5484).
//
// TargetModeCanary admits a receipt measured on ANY recorded route,
// because a shadow operation can only ever be measured on /query/proof:
// PostgresSwitch.Enabled admits canary|primary only, so the deployed
// build will not execute a shadow operation on /query at all. Requiring
// edge evidence there would mean an operation could only be proven after
// it had already been enabled.
//
// TargetModePrimary admits ONLY RouteEdge. Promotion to primary is
// promotion to served traffic, and /query/proof is a measurement-only
// handler unreachable from the product edge -- a proof-route receipt says
// the build CAN serve the operation, not that the edge DOES.
const (
	TargetModeCanary  = "canary"
	TargetModePrimary = "primary"
)

// EnablementCitedMismatchState is the one terminal state other than
// `match` that can authorize an enablement, and only under the citation
// conditions below. The receipt is never rewritten: a mismatch stays a
// mismatch in the row and is merely READ as sufficient when every
// difference it found was a named defect in the PYTHON baseline.
const EnablementCitedMismatchState = "mismatch"

// enablementProofPredicate is the admission rule, as SQL over one
// go_api_proof_run row.
//
// The three conditions on the mismatch arm are each load-bearing:
//
//   - differences_outside_baseline_defect = 0 -- nothing diverged that no
//     declared defect covers.
//   - baseline_defect IS NOT NULL -- because that counter is NOT NULL
//     DEFAULT 0, so a bare `= 0` cannot tell "computed, and every
//     difference was cited" apart from "this row predates the column and
//     nothing ever computed it". A citation is what disambiguates.
//   - cardinality(...) > 0 -- an empty array is a writer that had the
//     column and cited nothing; without this, IS NOT NULL could be
//     satisfied by writing '{}'.
//
// The Python preflight (go_api_routing_admin.build_enablement_proof_select)
// implements the same rule in SQLAlchemy expressions with a tuple-OR join
// shape, so the two statements can never be compared as text. They are
// pinned by BEHAVIOUR instead: both sides' tests drive their own
// production predicate over tests/fixtures/enablement_proof_admission_cases.json.
//
// `baseline_defect IS NOT NULL` is deliberately ABSENT. It reads as a
// guard and is not one: `cardinality(NULL) > 0` evaluates to NULL, which
// is not TRUE, so a NULL citation list is already excluded by the
// cardinality clause alone. r1 proved it by removing the IS NOT NULL from
// both implementations and watching every case still pass. Both the NULL
// and the empty-array cases are in the shared table
// (`mismatch_cited_but_defect_null_refused`,
// `mismatch_cited_but_defect_empty_refused`), so this is covered rather
// than merely argued -- and a clause that can never change an outcome is
// one more thing a reader has to reason about for nothing.
//
// blankCitationCutset is THE definition of "names nothing", and it is
// deliberately an explicit character set rather than Unicode's space class.
//
// opus r5 (P1) found the change shipping TWO definitions: the writer used
// strings.TrimSpace, the predicates used one-argument btrim(). Measured,
// they disagree on every whitespace character except the space itself --
// a tab, newline, CR, vertical tab, form feed or NBSP citation was
// refused by the writer and ADMITTED by both predicates, and promoted to
// primary through the shipped CLI.
//
// They cannot share Unicode's definition. Measured against this Postgres
// (UTF8):
//
//	btrim(E'\u00a0', <this set>) = ''   -> true    unicode.IsSpace -> true
//	btrim(E'\u2028', <this set>) = ''   -> false   unicode.IsSpace -> true
//	E'\u00a0' ~ '^[[:space:]]+$'        -> false
//	E'\u2028' ~ '^[[:space:]]+$'        -> true
//
// So the explicit set and POSIX [[:space:]] have OPPOSITE gaps, and
// neither equals unicode.IsSpace, which is open-ended by design. A shared
// definition therefore has to be enumerated, not named.
//
// DIRECTION, stated because it matters: the SQL definition is the
// authority and Go implements exactly it. SQL is the side that reads rows
// from producers Go has never seen -- rows already in the table, a future
// writer, a manual repair -- so the engine that must be right about a
// stored row owns the rule, and the writer conforms. Go therefore uses
// strings.Trim with this cutset, NEVER strings.TrimSpace.
//
// Characters outside the set (U+2028, U+3000, ...) are treated as REAL
// citation text by BOTH sides. That is the trade: a ticket identifier made
// only of exotic Unicode spacing is not a case worth a definition the two
// engines cannot share, and both agreeing matters more than either being
// maximal. TestTheBlankDefinitionIsIdenticalInBothEngines pins the
// agreement character by character, in-set and out.
const blankCitationCutset = " \t\n\v\f\r\u00a0"

// blankCitationSQL renders the cutset as a Postgres escape-string literal,
// so the SQL below and the Go check above cannot drift: there is one
// constant and the statement is generated from it.
func blankCitationSQL() string {
	return `E' \t\n\v\f\r\u00a0'`
}

// THE BINDING IS PART OF THE RULE, on both arms, uniformly.
//
// opus r5 (P2) found the new admission rule being applied retroactively
// to rows the OLD writer produced under different counting semantics. At
// the base build, proveOne did NOT count an $.http.* difference and did
// NOT count an unbound edge mismatch into
// differences_outside_baseline_defect -- both increments are this
// change's. Before it, a mismatch authorized nothing, so that did not
// matter; it does now. Every row the base build wrote with
// mismatch/edge/outside=0/cited became PRIMARY-admissible, including rows
// whose real difference set held an uncited HTTP status difference and
// rows tied to no replica. Those are exactly the pre-0129 rows:
// build_binding IS NULL.
//
// Requiring per_request excludes precisely them and NOTHING the current
// writer produces, which is why it is stated once for both arms rather
// than only on the mismatch arm:
//
//   - proof route          -> per_request always
//   - edge WITH the header -> per_request
//   - edge WITHOUT it, match    -> downgraded to `unsupported`, never proof
//   - edge WITHOUT it, mismatch -> outside >= 1, so already excluded
//
// So "match or cited mismatch, from this writer" already implies
// per_request. Saying it out loud costs nothing and closes the retroactive
// window; a NULL binding now means what it is, a row written before the
// column existed.
//
// A citation that names NOTHING is not a citation. `cardinality(ARRAY[”])`
// is 1, so before the NOT EXISTS below a mismatch citing a single empty
// ticket read as fully cited and authorized a promotion -- executed
// against real PostgreSQL and confirmed `admitted as enablement proof:
// true`. The writer refuses to create such a row, but the writer is not
// the only producer the predicate will ever read: rows already in the
// table, a future writer and a manual repair all reach it. A guard on the
// producer alone is a guard on today's producers.
//
// btrim, not `<> ”`: a whitespace-only citation names nothing either, and
// so does a whitespace-only candidate_build -- and `proven` is keyed on
// that column, so such a row is unmatchable while looking present.
//
// Every value here is a package CONSTANT, never caller input, so they are
// inlined rather than bound: that lets EnablementProofClause compose into
// a query of any shape, which is what makes ONE predicate serve every
// reader (r2 P1 found a third reader that had drifted).
func enablementProofPredicate(alias string) string {
	return alias + `.stage = '` + EnablementProofStage + `'
		    AND (
		          ` + alias + `.terminal_state = '` + EnablementProofTerminalState + `'
		       OR (` + alias + `.terminal_state = '` + EnablementCitedMismatchState + `'
		           AND ` + alias + `.differences_outside_baseline_defect = 0
		           AND cardinality(` + alias + `.baseline_defect) > 0
		           AND NOT EXISTS (
		                 SELECT 1 FROM unnest(` + alias + `.baseline_defect) AS citation
		                  WHERE citation IS NULL
		                     OR btrim(citation, ` + blankCitationSQL() + `) = ''))
		    )
		    AND btrim(` + alias + `.candidate_build, ` + blankCitationSQL() + `) <> ''
		    AND ` + alias + `.build_binding = '` + EdgeBuildPresent + `'`
}

// EnablementProofClause is THE rule deciding whether a go_api_proof_run
// row is enablement proof for targetMode, as a SQL fragment over `alias`.
//
// It exists because r2 (P1) found a THIRD reader of this rule --
// internal/migrationmatrix's routingStateQuery -- carrying its own copy
// that predated CHAOS-5484's route split. That copy still hardcoded
// `terminal_state = 'match'` with no route clause at all, so the
// migration-status page rendered a primary proof-route receipt PROVEN
// while enablement correctly refused it, and rendered a canary cited
// mismatch UNPROVEN while enablement accepted it. Its own comment claimed
// it was "deliberately the same predicate the enablement command uses" --
// true when written, false the moment the rule moved, and nothing failed.
//
// A shared truth table pins the implementations it knows about. It cannot
// pin one nobody enumerated, so the rule now has ONE source and callers
// compose it rather than restate it.
func EnablementProofClause(alias, targetMode string) (string, error) {
	var routeClause string
	switch targetMode {
	case TargetModePrimary:
		routeClause = alias + ".measurement_route = '" + RouteEdge + "'"
	case TargetModeCanary:
		// Recorded, not merely anything. A NULL is a pre-0128 row that
		// says nothing about how it was measured, and "any route" is not
		// "no route" -- admitting unknown provenance is the same failure
		// shape as reading a DEFAULT 0 as an assertion.
		routeClause = alias + ".measurement_route IS NOT NULL"
	default:
		return "", fmt.Errorf("goapiproof: unknown enablement target mode %q -- expected %q or %q; refusing to build a predicate whose route rule is undefined", targetMode, TargetModeCanary, TargetModePrimary)
	}
	return enablementProofPredicate(alias) + "\n\t\t    AND " + routeClause, nil
}

// OperationsWithEnablementProof is the Go reader for the exact predicate
// `enable`'s preflight uses (go_api_routing_admin.build_enablement_proof_select).
//
// It exists so an integration test can assert the receipts this package
// WRITES are the ones that command READS -- proving the two halves meet,
// rather than each agreeing with itself. Scoped to the exact
// (schema_digest, candidate_build) tuple, and matching operation and
// document digest as a tuple-OR rather than two independent IN lists: the
// cross-product form would accept a proof recorded against a DIFFERENT
// registered document.
//
// targetMode selects the route rule -- see TargetModeCanary/TargetModePrimary.
// An unrecognised mode is an ERROR, never a fallthrough to the more
// permissive branch: that is exactly how a promotion to served traffic
// would quietly come to rest on measurement-only evidence.
func OperationsWithEnablementProof(
	ctx context.Context,
	db Querier,
	schemaDigest string,
	candidateBuild string,
	targetMode string,
	documentDigestByOperation map[string]string,
) (map[string]bool, error) {
	clause, err := EnablementProofClause("p", targetMode)
	if err != nil {
		return nil, err
	}

	found := map[string]bool{}
	if len(documentDigestByOperation) == 0 {
		return found, nil
	}

	operations := make([]string, 0, len(documentDigestByOperation))
	digests := make([]string, 0, len(documentDigestByOperation))
	for operation, digest := range documentDigestByOperation {
		operations = append(operations, operation)
		digests = append(digests, digest)
	}

	rows, err := db.Query(ctx,
		`SELECT DISTINCT p.selected_operation
		   FROM go_api_proof_run AS p
		   JOIN unnest($3::text[], $4::text[]) AS want(operation, document_digest)
		     ON want.operation = p.selected_operation
		    AND want.document_digest = p.document_digest
		  WHERE p.schema_digest = $1
		    AND p.candidate_build = $2
		    AND `+clause,
		schemaDigest, candidateBuild, operations, digests,
	)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read enablement proof: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var operation string
		if err := rows.Scan(&operation); err != nil {
			return nil, fmt.Errorf("goapiproof: scan enablement proof row: %w", err)
		}
		found[operation] = true
	}
	return found, rows.Err()
}
