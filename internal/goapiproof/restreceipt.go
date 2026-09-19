package goapiproof

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// RESTReceipt is one immutable go_api_rest_proof_run row -- the REST
// sibling of Receipt (receipt.go). REST has no GraphQL schema digest,
// document digest or selected-operation name to key a receipt by, and no
// go_api_routing_state row to read a candidate build from the way a
// GraphQL operation does, so this receipt's identity is what the ported
// route actually is: (Method, Path, CandidateBuild) -- see alembic
// 0134_add_go_api_rest_proof_run.py's own module doc comment for why
// that replaces Receipt's four-column key rather than reusing it, and why
// there is no companion candidate-build registry table to foreign-key
// against.
//
// Every other field is named identically to Receipt's own field of the
// same purpose, and that is load-bearing, not cosmetic:
// EnablementProofClause (receipt.go) is one predicate parameterised only
// by a SQL alias, and it is reused VERBATIM against go_api_rest_proof_run
// by ReadRESTProof (internal/migrationmatrix/restproven.go) precisely
// because both tables name their stage/terminal_state/candidate_build/
// build_binding/baseline_defect/differences_outside_baseline_defect
// columns the same way.
type RESTReceipt struct {
	Method               string
	Path                 string
	CandidateBuild       string
	RequestIdentity      string
	Stage                string
	TerminalState        string
	BaselineResponseRef  string
	CandidateResponseRef string
	OrgID                string
	ReviewEvidence       string
	RecordedBy           string
	ObservedAt           time.Time

	// MeasurementRoute is RouteEdge or RouteProof -- see Receipt's own
	// field of the same name. go-api-rest-prove always writes RouteProof
	// today (restadmit.go's own doc comment: no REST route in this
	// service is reached through a shared edge that could drop the build
	// header), but the column carries the same two-value vocabulary
	// Receipt's does rather than a REST-only constant, so a future edge
	// path does not need a schema change to be recorded.
	MeasurementRoute string

	// BaselineDefects mirrors Receipt.BaselineDefects: it NEVER softens
	// TerminalState, it only names which declared Python-plane defects
	// covered this comparison's differences.
	BaselineDefects []string

	// DeclaredDefects names every ticket this request's own corpus entry
	// DECLARED at the moment this receipt was written -- alongside, never
	// instead of, BaselineDefects' existing matched-only record (alembic
	// 0135's own module doc comment: a silent citation and a
	// not-yet-declared one used to read identically from this table; this
	// is what tells them apart). nil writes SQL NULL, meaning "unknown"
	// -- the caller did not populate this field, the same convention a
	// pre-0135 row carries structurally. A non-nil slice, even empty,
	// writes a real array meaning "known": this request declared exactly
	// these tickets (possibly none) that run. cmd/go-api-rest-prove is
	// the only writer today and always passes a non-nil slice, so every
	// row it writes is "known" from the moment this column exists.
	DeclaredDefects []string

	// DifferencesOutsideBaselineDefect mirrors Receipt's own field of the
	// same name -- written even when zero, for the same reason.
	DifferencesOutsideBaselineDefect int

	// BuildBinding is EdgeBuildPresent or EdgeBuildAbsent, same vocabulary
	// as Receipt's own field. RESTAdmit refuses admission outright when
	// the candidate leg's build header is absent or mismatched (see
	// RESTRefusalBuildUnbound), so a REST receipt is only ever built from
	// an observation that already satisfies EdgeBuildPresent -- there is
	// no REST equivalent of Receipt's RouteEdge/EdgeBuildAbsent fallback.
	BuildBinding string

	// BoundIDs names every id (restidbind.go's RESTIDBinding.Producer ->
	// the resolved value) this request's Query/Path were bound to before
	// either leg was sent, e.g. {"repo_id": "3fae..."}. NOT a database
	// column -- go_api_rest_proof_run carries no bound_ids column, and
	// alembic 0134 is fixed schema this ticket does not migrate: the same
	// "no column, exists purely for operator visibility" precedent
	// Receipt.Variant's own doc comment already establishes for a
	// per-request value RequestIdentity already digests (the bound id is
	// part of the resolved Query WriteREST's caller folds into
	// RequestIdentity). Ids are not secrets -- unlike a bearer token, this
	// is safe to carry on the struct and in the JSON report
	// (cmd/go-api-rest-prove's own outcome.BoundIDs) even though it is
	// never written to the row.
	BoundIDs map[string]string
}

// WriteREST records one REST receipt. A single INSERT, not two statements
// like Write's candidate-build upsert + proof-run insert: there is no
// go_api_rest_candidate_build registry for this row to reference, so
// there is nothing to upsert first.
//
// The vocabulary and blank-citation guards are byte-for-byte the same
// checks Write applies, reused rather than re-derived, because the rule
// ("a citation that names nothing is not a citation", NamesNothing's own
// doc comment) is not a GraphQL-specific rule -- it is a rule about what a
// citation array means, which this table's baseline_defect column carries
// unchanged.
func WriteREST(ctx context.Context, db Querier, receipt RESTReceipt) (uuid.UUID, error) {
	if err := validateRESTVocabulary(receipt); err != nil {
		return uuid.Nil, err
	}
	if NamesNothing(receipt.Method) || NamesNothing(receipt.Path) {
		return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a REST receipt with an empty method or path")
	}
	if NamesNothing(receipt.CandidateBuild) {
		return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a REST receipt with an empty candidate build")
	}
	if !buildBindings[receipt.BuildBinding] {
		return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a REST receipt with build binding %q -- expected %q or %q", receipt.BuildBinding, EdgeBuildPresent, EdgeBuildAbsent)
	}
	if !measurementRoutes[receipt.MeasurementRoute] {
		return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a REST receipt with measurement route %q -- expected %q or %q", receipt.MeasurementRoute, RouteEdge, RouteProof)
	}
	for _, ticket := range receipt.BaselineDefects {
		if NamesNothing(ticket) {
			return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a REST receipt whose baseline_defect array contains an empty citation (%d entries) -- cardinality() counts it, so the enablement predicate would read this as a fully-cited mismatch while it cites nothing", len(receipt.BaselineDefects))
		}
	}
	// REST has no go-only class: the prefix is refused on either array.
	for _, ticket := range append(append([]string(nil), receipt.BaselineDefects...), receipt.DeclaredDefects...) {
		if HasGoOnlyPrefix(ticket) {
			return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a REST receipt whose citation starts with %q: only a GraphQL go-only receipt may carry it", GoOnlyCitationPrefix)
		}
	}
	for _, ticket := range receipt.DeclaredDefects {
		if NamesNothing(ticket) {
			return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a REST receipt whose baseline_defect_declared array contains an empty citation (%d entries)", len(receipt.DeclaredDefects))
		}
	}

	id := uuid.New()
	if _, err := db.Exec(ctx,
		`INSERT INTO go_api_rest_proof_run
		   (id, method, path, candidate_build,
		    request_identity, stage, terminal_state,
		    baseline_response_ref, candidate_response_ref,
		    org_id, review_evidence, recorded_by, observed_at,
		    measurement_route, baseline_defect, baseline_defect_declared,
		    differences_outside_baseline_defect,
		    build_binding)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		id, receipt.Method, receipt.Path, receipt.CandidateBuild,
		receipt.RequestIdentity, receipt.Stage, receipt.TerminalState,
		nullIfEmpty(receipt.BaselineResponseRef), nullIfEmpty(receipt.CandidateResponseRef),
		nullIfEmpty(receipt.OrgID), nullIfEmpty(receipt.ReviewEvidence), nullIfEmpty(receipt.RecordedBy),
		receipt.ObservedAt, nullIfEmpty(receipt.MeasurementRoute), receipt.BaselineDefects, receipt.DeclaredDefects,
		receipt.DifferencesOutsideBaselineDefect, nullIfEmpty(receipt.BuildBinding),
	); err != nil {
		return uuid.Nil, fmt.Errorf("goapiproof: record REST proof run for %s %s: %w", receipt.Method, receipt.Path, err)
	}
	return id, nil
}

// WriteRESTAtomic writes one REST receipt inside its own transaction when
// db can begin one, matching WriteAtomic's own fallback contract. One
// statement is already atomic on any single connection, but a caller that
// can begin a transaction gets one anyway -- consistent with Write's own
// caller-facing contract, so a future second statement (a REST candidate-
// build registry, should one ever be added) does not silently change this
// function's atomicity guarantee out from under an existing caller.
func WriteRESTAtomic(ctx context.Context, db Querier, receipt RESTReceipt) (uuid.UUID, error) {
	beginner, ok := db.(TxBeginner)
	if !ok {
		return WriteREST(ctx, db, receipt)
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("goapiproof: begin REST receipt transaction: %w", err)
	}
	id, err := WriteREST(ctx, tx, receipt)
	if err != nil {
		_ = tx.Rollback(ctx)
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("goapiproof: commit REST receipt: %w", err)
	}
	return id, nil
}

// validateRESTVocabulary mirrors validateVocabulary's stage/terminal_state
// checks, reusing the SAME package-level stages/terminalStates maps
// receipt.go defines -- go_api_rest_proof_run's CHECK constraints enforce
// the identical vocabulary (alembic 0134), so the fast, specific Go-side
// failure is identical too. No shadow-requires-watermark check: REST
// receipts carry no data_watermark column at all (0134's own doc comment
// on why) and no REST route is ever proven in the shadow stage.
func validateRESTVocabulary(receipt RESTReceipt) error {
	if !stages[receipt.Stage] {
		return fmt.Errorf("goapiproof: invalid REST proof-run stage %q", receipt.Stage)
	}
	if !terminalStates[receipt.TerminalState] {
		return fmt.Errorf("goapiproof: invalid REST proof-run terminal_state %q", receipt.TerminalState)
	}
	return nil
}
