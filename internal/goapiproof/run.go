package goapiproof

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Stage is always deployed_executed for this command: every request it
// makes reaches a DEPLOYED build over HTTP through the real edge. The
// shadow-mode operations get a deployed_executed receipt too -- they were
// executed on the deployed build -- but their verdict is the comparator's
// true one, so a divergence lands as `mismatch` and authorizes nothing.
// Promotion is a separate decision that reads terminal_state = 'match'.
const Stage = EnablementProofStage

// Refusal reasons. Every one of these is NAMED and printed: "prove
// measured nothing" and "prove measured everything and found nothing
// wrong" must never look alike (D15/R4).
const (
	RefusalNoPayload           = "no_committed_request_payload"
	RefusalDocumentDigestDrift = "document_digest_drift"
	RefusalNotRouted           = "operation_not_routed_to_go"
	RefusalShadowUnmeasurable  = "shadow_mode_has_no_measurement_route"
	RefusalTransport           = "transport_error"
	RefusalUndecodableResponse = "undecodable_response"
	RefusalWrongPlane          = "served_by_the_wrong_plane"
	RefusalStaleExclusion      = "declared_exclusion_matched_nothing"
	RefusalStaleTierB          = "declared_tier_b_field_matched_nothing"
	RefusalStaleBaselineDefect = "declared_baseline_defect_matched_nothing"
	RefusalNonFinite           = "response_carried_a_non_finite_json_number"
	RefusalPlaneUnidentified   = "response_carried_no_plane_evidence"
	RefusalBuildMismatch       = "serving_build_is_not_the_named_build"
)

// Measurement routes. Recorded on every receipt so a proof-route
// observation can never be read as served traffic: /query/proof exists
// only to measure, is registered only under an explicit env flag, and is
// unreachable from the Python edge -- but a receipt that did not SAY which
// route produced it would leave that distinction in a chat message
// instead of in the row (team-lead ruling R50, 2026-09-09).
const (
	RouteEdge  = "edge"
	RouteProof = "proof"
)

// planeHeader is the response header the Python edge stamps with the
// plane that actually served a request (go_api_dispatcher's
// _with_plane_header, gated by GO_API_PLANE_HEADER_ENABLED). Without it,
// a fallback to Python is indistinguishable from a Go-served response,
// and every receipt this command writes would be worthless.
const planeHeader = "x-dev-health-plane"

// buildHeader names the build of the process that served THIS request.
//
// Only /query/proof stamps it (cmd/query-api/buildinfo_route.go). The
// Python edge cannot: go_api_dispatcher's _forward_to_go builds a NEW
// Response carrying only content, status and media_type, so any header
// query-api sets on a /query response is dropped before the client sees
// it. So per-request build binding is available on the proof route and
// NOT on the edge route -- see proveOne, and the run-level stability
// check the command performs to bound the residual.
const buildHeader = "x-dev-health-build"

// contentTypeHeader is compared between the planes. A body that compares
// equal under a DIFFERENT content type is not parity: the client is being
// told to interpret those identical bytes differently.
const contentTypeHeader = "content-type"

// baselineComment is appended to a registered document to obtain the
// PYTHON side of the comparison through the real edge.
//
// Why this works, and why it is not a bypass: the Python edge dispatches
// on the document DIGEST, so a document carrying one extra GraphQL
// comment line is not a registered document, and the edge takes its
// documented safe default -- "unregistered documents stay on Python". The
// request still traverses ingress, authentication, org context, parse and
// validation, execution and serialization; only the plane differs. A
// GraphQL comment is ignored by the parser, so the two documents are
// semantically identical. This is the same control lane-goapi-enable used
// on 2026-09-07 (enablement artifact 51-harness-control.json, which
// recorded python_plane_header=python for every arm).
//
// The alternative -- flipping the routing row to python, measuring, and
// flipping it back -- mutates live routing state twice per operation and
// is exactly the exposure this measurement is supposed to avoid.
const baselineComment = "\n# dev-health prove: python-plane control (CHAOS-5425). Semantically inert.\n"

// Observation is one plane's outer HTTP observation -- status, headers
// and body, not just a decoded payload. The Python comparator compares
// bodies; HTTP status, headers and effects are the outer runner's job,
// and a 200-with-errors is a different fact from a 500.
type Observation struct {
	StatusCode int    `json:"status_code"`
	Plane      string `json:"plane"`
	// Build is the serving process's build identity, when the route stamps
	// one. Empty on the edge route by construction -- see buildHeader.
	Build string `json:"build,omitempty"`
	// Headers is the BOUNDED set of response headers this comparison
	// treats as part of the observable response: content-type, the plane
	// header, the build header. Deliberately not every header -- Date and
	// Content-Length differ on every pair of requests and would drown a
	// real divergence in noise. An earlier version's doc comment claimed
	// headers were observed while the struct recorded none, so a
	// content-type divergence was invisible (codex r1 F4, reproduced:
	// application/problem+json vs application/json compared as match).
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"-"`
	BodyRef string            `json:"body_ref,omitempty"`
	Elapsed time.Duration     `json:"elapsed_ns"`
}

// Outcome is one operation's complete result: what was attempted, what
// actually executed, and -- when nothing executed -- why, by name.
type Outcome struct {
	Operation      string `json:"operation"`
	DocumentDigest string `json:"document_digest"`
	Mode           string `json:"mode"`
	Executed       bool   `json:"executed"`
	// Admitted records that this pair passed Admit. Executed is never true
	// without it; they are separate fields so a regression that sets
	// Executed some other way shows up in the report rather than reading
	// as a legitimate measurement.
	Admitted bool `json:"admitted"`
	// admitted is the SEALED form of the field above, and the one every
	// receipt constructor actually reads.
	//
	// r3 found the exported bool is not a boundary: a caller in any
	// package can build an Outcome with Admitted=true, hand it to
	// ReceiptsFor, and get a deployed_executed/match receipt for a pair of
	// responses that never passed Admit. R57 made Admit the only door on
	// the PRODUCTION path; it did not make it the only door.
	//
	// This field is unexported, so nothing outside this package can set
	// it, and it is written in exactly one place -- proveOne, from Admit's
	// own verdict. The exported bool stays for the report, where it is
	// what a human reads; this is what the code trusts.
	admitted bool
	// terminalState is the SEALED verdict, and the one every receipt
	// carries. r4 showed that sealing `admitted` alone was half the job:
	// after a real run, assigning the exported TerminalState turned an
	// unbound `unsupported` result into a `match` receipt whose own
	// provenance still said edge_build_binding=absent. The seal covered
	// "this passed the gate" and left "what the gate concluded" writable.
	//
	// Written in exactly one place -- proveOne -- alongside `admitted`.
	// The exported field stays for the report; this is what a receipt
	// derives from.
	terminalState string
	RefusalReason string `json:"refusal_reason,omitempty"`
	RefusalDetail string `json:"refusal_detail,omitempty"`
	Route         string `json:"route"`
	// EdgeBuildBinding is EdgeBuildPresent or EdgeBuildAbsent -- whether
	// the measured response carried the serving build on itself. Taken
	// from the admission rather than re-derived from the route, so the
	// receipt cannot claim a binding the gate did not check.
	EdgeBuildBinding string `json:"edge_build_binding,omitempty"`
	// RoutingRowBuild is the build the operation's ROUTING ROW named when
	// this measurement was taken, recorded only when it differs from the
	// build that actually served the request. It is an observation about
	// the enablement record, never about the receipt: the receipt names
	// the running build by construction. See StaleRoutingRows for why
	// this is recorded rather than refused.
	RoutingRowBuild string    `json:"routing_row_build,omitempty"`
	TerminalState   string    `json:"terminal_state"`
	Findings        []Finding `json:"findings,omitempty"`
	BaselineDefects []string  `json:"baseline_defect,omitempty"`
	// DifferencesOutsideBaselineDefect is serialised even when zero: "every
	// difference is a known Python defect" and "there were no differences"
	// are different facts.
	DifferencesOutsideBaselineDefect int          `json:"differences_outside_baseline_defect"`
	Candidate                        *Observation `json:"candidate,omitempty"`
	Baseline                         *Observation `json:"baseline,omitempty"`
	ReceiptWritten                   bool         `json:"receipt_written"`
}

// sealedOutcome is what a measurement ACTUALLY established, captured by
// proveOne at the moment it was established, and the only thing a receipt
// is ever built from.
//
// This type exists because sealing one field at a time did not work, three
// rounds running. r3 sealed `admitted` after a hand-built Outcome forged a
// receipt; r4 sealed the verdict after a real outcome's TerminalState was
// reassigned; r5 then relabelled the build, schema digest, org, document,
// operation and route on a genuine admitted match and got
// `receipt build=never-measured-build terminal=match`. Each fix closed the
// field that had just been used and left the rest open, which is a
// blacklist whose default is TRUST -- the exact shape R57 was written to
// end on the admission side and which I rebuilt here.
//
// So there is no field to reassign rather than a growing list of fields
// that may not be. Every field is unexported; the struct never leaves this
// package; the exported Outcome is a report VIEW derived from it, and no
// constructor, writer or receipt reads that view. TestSealedOutcomeHasNoExportedFields
// fails if anyone adds one.
type sealedOutcome struct {
	operation       string
	documentDigest  string
	schemaDigest    string
	candidateBuild  string
	orgID           string
	mode            string
	route           string
	edgeBinding     string
	routingRowBuild string
	terminalState   string
	executed        bool
	admitted        bool

	baselineRef  string
	candidateRef string

	baselineDefects                  []string
	differencesOutsideBaselineDefect int
}

// Summary is the explicit-zero telemetry block. Every field is printed on
// every run, including the zeros: a run that measured nothing must be
// impossible to mistake for a run that found nothing wrong.
type Summary struct {
	Attempted int `json:"attempted"`
	// Admitted is serialised even when zero. "nothing passed admission"
	// and "everything passed and nothing differed" are different facts,
	// and a run whose admitted count is 0 measured nothing at all.
	Admitted int `json:"admitted"`
	Executed int `json:"executed"`
	Refused  int `json:"refused"`
	// StaleRoutingRows is serialised even when zero. A run in which every
	// routing row still named an older build is a run worth looking at,
	// and "none were stale" is a different fact from "nobody checked".
	StaleRoutingRows int            `json:"stale_routing_rows"`
	ReceiptsWritten  int            `json:"receipts_written"`
	ByTerminalState  map[string]int `json:"by_terminal_state"`
	ByRefusalReason  map[string]int `json:"by_refusal_reason"`
}

// RegistryView is what the RUNNING query-api reports about itself.
type RegistryView struct {
	SchemaDigest   string
	BuildIdentity  string
	DocumentDigest map[string]string // operation -> digest, as the process registers it
}

// RoutingRow is one go_api_routing_state row as the operator's status
// read reports it.
type RoutingRow struct {
	Mode           string
	CandidateBuild string
}

// Config is one prove invocation.
type Config struct {
	OrgID  string
	Auth   AuthContext
	Window Window

	// EdgeCredential authenticates the PYTHON EDGE (/graphql) -- both
	// every baseline leg and the candidate leg of a canary/primary
	// operation. On the deployed stack this is an access token.
	//
	// EdgeCredential and ProofCredential are separate fields, not one
	// header map, because the two planes accept DIFFERENT credential
	// kinds: measured on the stack, an access token gets 200 on the edge
	// and 401 on /buildinfo, and an envelope gets the reverse. One map
	// applied to both meant every run failed on one leg or the other
	// (JOB 4, 2026-09-09). Two fields make that a compile-time
	// distinction rather than a runtime discovery.
	EdgeCredential *Credential

	// ProofCredential authenticates the Go plane's own routes --
	// /buildinfo and /query/proof. On the deployed stack this is an
	// effective-principal envelope, which expires in 60 seconds, so it is
	// normally a MintedCredential rather than a fixed string.
	ProofCredential *Credential

	// PythonEdgeURL is the real product edge (/graphql). Both the
	// candidate leg (for canary/primary operations) and every baseline
	// leg go through it.
	PythonEdgeURL string

	// GoProofURL is the measurement-only route that can execute a
	// SHADOW-mode operation on the deployed Go build. Empty means the
	// route does not exist in this deployment, and shadow operations are
	// refused by name rather than silently skipped.
	GoProofURL string

	RecordedBy     string
	ReviewEvidence string
	Timeout        time.Duration
}

// Runner executes the proof run.
type Runner struct {
	Client    *http.Client
	Documents map[string]string // operation -> registered document TEXT
	Registry  RegistryView
	Routing   map[string]RoutingRow
	Artifacts *ArtifactStore
	Config    Config
	Now       func() time.Time

	// sealed is what the last Run measured. Receipts are built from THIS
	// and never from anything a caller holds -- see sealedOutcome. It is
	// populated only by Run, so ReceiptsFor takes no outcomes at all:
	// there is nothing to hand it and therefore nothing to forge.
	sealed []sealedOutcome
}

// ErrNothingMeasured is returned when a run produced no executed
// measurement at all. D15/R4: "a measurement that did not happen must
// FAIL the run, not merely print" -- returning quietly is exactly what
// drops the failure signal.
var ErrNothingMeasured = errors.New("goapiproof: no operation was executed -- this run measured NOTHING")

// Run executes every registered operation and returns one Outcome each plus
// the summary. It WRITES NOTHING.
//
// Receipt writing is a separate, later phase on purpose (round 2's F3).
// Receipts used to commit as each operation finished, before the run-level
// build-stability check had a chance to run -- so a build that moved
// mid-run left already-committed match receipts behind, eligible for
// enablement, describing a build that was not serving for all of the run.
// Nothing rolled them back and the run error did not reach them. That is
// the same false-proof class as every finding before it: something
// certified while a condition it depends on was never established.
//
// So the whole run is buffered, everything it depends on is verified, and
// only then is anything written -- see WriteReceipts and RefusalReceipts.
//
// The returned error is non-nil when the RUN itself is not trustworthy
// (nothing measured, or a refusal with no named reason). A recorded
// MISMATCH is not an error: it is the measurement succeeding.
func (r *Runner) Run(ctx context.Context) ([]Outcome, Summary, error) {
	now := r.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}

	operations := make([]string, 0, len(r.Registry.DocumentDigest))
	for operation := range r.Registry.DocumentDigest {
		operations = append(operations, operation)
	}
	sort.Strings(operations)

	summary := Summary{
		ByTerminalState: map[string]int{},
		ByRefusalReason: map[string]int{},
	}
	outcomes := make([]Outcome, 0, len(operations))
	// Reset, so a second Run cannot write receipts for the first one's
	// measurements.
	r.sealed = make([]sealedOutcome, 0, len(operations))

	for _, operation := range operations {
		summary.Attempted++
		outcome := r.proveOne(ctx, operation)
		if outcome.Admitted {
			summary.Admitted++
		}
		if outcome.RoutingRowBuild != "" {
			summary.StaleRoutingRows++
		}
		if outcome.Executed {
			summary.Executed++
			summary.ByTerminalState[outcome.TerminalState]++
		} else {
			summary.Refused++
			if outcome.RefusalReason == "" {
				return outcomes, summary, fmt.Errorf("goapiproof: %s was refused with NO named reason -- an unnamed refusal is the failure this command exists to prevent", operation)
			}
			summary.ByRefusalReason[outcome.RefusalReason]++
		}

		outcomes = append(outcomes, outcome)
		r.sealed = append(r.sealed, r.seal(outcome))
	}

	if summary.Executed == 0 {
		return outcomes, summary, ErrNothingMeasured
	}
	return outcomes, summary, nil
}

// ReceiptsFor turns the buffered outcomes into the receipts they justify.
//
// Only ADMITTED outcomes produce a receipt: a refusal is a measurement that
// did not happen, and the run report is where those are visible.
//
// Both flags are checked, not just Executed. `Run` sets Executed only after
// Admit passes, so today they cannot disagree -- but this is an EXPORTED
// function that will take whatever outcomes a caller hands it, and the
// entire history of this file is defaults that admitted things nobody had
// thought about. A confirmation pass drove exactly that probe: a
// hand-built outcome with Executed set and Admitted false produced a
// receipt. Unreachable from the command, one line to make unreachable
// everywhere.
func (r *Runner) ReceiptsFor(observedAt time.Time) ([]Receipt, error) {
	receipts := make([]Receipt, 0, len(r.sealed))
	for _, sealed := range r.sealed {
		if !sealed.executed || !sealed.admitted {
			continue
		}
		identity, err := RequestIdentity(sealed.orgID, r.Config.Auth, r.variablesFor(sealed.operation))
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, Receipt{
			SchemaDigest:                     sealed.schemaDigest,
			DocumentDigest:                   sealed.documentDigest,
			SelectedOperation:                sealed.operation,
			CandidateBuild:                   sealed.candidateBuild,
			RequestIdentity:                  identity,
			Stage:                            Stage,
			TerminalState:                    sealed.terminalState,
			OrgID:                            sealed.orgID,
			ReviewEvidence:                   r.reviewEvidence(sealed, "", 0, 0),
			RecordedBy:                       r.Config.RecordedBy,
			ObservedAt:                       observedAt,
			BaselineResponseRef:              sealed.baselineRef,
			CandidateResponseRef:             sealed.candidateRef,
			MeasurementRoute:                 sealed.route,
			BaselineDefects:                  sealed.baselineDefects,
			DifferencesOutsideBaselineDefect: sealed.differencesOutsideBaselineDefect,
		})
	}
	return receipts, nil
}

// RefusalReceipts is what a run writes when the build moved underneath it.
//
// NO match receipt is written -- every outcome the run produced described a
// build that was not serving for all of it. But writing nothing at all
// would leave the run invisible, indistinguishable from one that never
// happened, so each operation the run reached gets a `proof_failed` receipt
// naming the cause and the counts.
//
// `proof_failed` rather than a new `refused` state: the terminal-state
// vocabulary is fixed by the signed plan and enforced by a CHECK
// constraint, and `proof_failed` already means exactly this -- the proof,
// not the operation, is what failed. A per-operation row rather than one
// run-level row for a structural reason: go_api_proof_run carries a
// 4-column composite FK to go_api_candidate_build, so a row that named no
// operation could not be written without inventing one.
func (r *Runner) RefusalReceipts(observedAt time.Time, cause string) ([]Receipt, error) {
	admitted := 0
	for _, sealed := range r.sealed {
		if sealed.executed && sealed.admitted {
			admitted++
		}
	}
	receipts := make([]Receipt, 0, len(r.sealed))
	for _, sealed := range r.sealed {
		if !sealed.executed || !sealed.admitted {
			continue
		}
		identity, err := RequestIdentity(sealed.orgID, r.Config.Auth, r.variablesFor(sealed.operation))
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, Receipt{
			SchemaDigest:      sealed.schemaDigest,
			DocumentDigest:    sealed.documentDigest,
			SelectedOperation: sealed.operation,
			CandidateBuild:    sealed.candidateBuild,
			RequestIdentity:   identity,
			Stage:             Stage,
			TerminalState:     "proof_failed",
			OrgID:             sealed.orgID,
			ReviewEvidence:    r.reviewEvidence(sealed, cause, admitted, len(r.sealed)),
			RecordedBy:        r.Config.RecordedBy,
			ObservedAt:        observedAt,
			MeasurementRoute:  sealed.route,
		})
	}
	return receipts, nil
}

// WriteReceipts writes a whole run's receipts, each atomically, and returns
// the operations it actually wrote.
//
// It returns the SET rather than a count so the per-outcome
// `receipt_written` flag can be set from what really happened. Reporting a
// run-level `receipts_written=N` while every outcome still said
// `receipt_written: false` was a small lie of exactly the kind this
// instrument exists not to tell (confirmation pass, P3).
//
// A partial write is reported partially rather than rolled back: each
// receipt is individually atomic, and losing a whole run's evidence to one
// bad row would be worse than an honest partial record. The caller emits
// the report either way.
func WriteReceipts(ctx context.Context, db Querier, receipts []Receipt) (map[string]bool, error) {
	written := map[string]bool{}
	for _, receipt := range receipts {
		if _, err := WriteAtomic(ctx, db, receipt); err != nil {
			return written, err
		}
		written[receipt.SelectedOperation] = true
	}
	return written, nil
}

func observationRef(observation *Observation) string {
	if observation == nil {
		return ""
	}
	return observation.BodyRef
}

func (r *Runner) variablesFor(operation string) map[string]any {
	spec, err := SpecFor(operation)
	if err != nil {
		return nil
	}
	return spec.Variables(r.Config.OrgID, r.Config.Window)
}

func (r *Runner) proveOne(ctx context.Context, operation string) Outcome {
	registryDigest := r.Registry.DocumentDigest[operation]
	row := r.Routing[operation]
	outcome := Outcome{Operation: operation, DocumentDigest: registryDigest, Mode: row.Mode}
	// Recorded on EVERY outcome, refused or not: a refusal taken while the
	// enablement record was stale is exactly as worth knowing as a match
	// taken then. Empty when the row agrees with what is running.
	if row.CandidateBuild != "" && row.CandidateBuild != r.Registry.BuildIdentity {
		outcome.RoutingRowBuild = row.CandidateBuild
	}

	refuse := func(reason, detail string) Outcome {
		outcome.RefusalReason = reason
		outcome.RefusalDetail = detail
		outcome.TerminalState = terminalStateForRefusal(reason)
		outcome.terminalState = outcome.TerminalState
		return outcome
	}

	spec, err := SpecFor(operation)
	if err != nil {
		return refuse(RefusalNoPayload, err.Error())
	}

	document, ok := r.Documents[operation]
	if !ok {
		return refuse(RefusalDocumentDigestDrift, "this checkout enumerates no document for the operation the running process registers")
	}

	// The candidate leg's URL is decided by the operation's MODE, because
	// the production route switch admits canary/primary only
	// (routeswitch.PostgresSwitch.reachableModes). A shadow operation sent
	// to /graphql is served by Python and would produce a receipt claiming
	// Go executed when it did not.
	var candidateURL string
	// The credential follows the URL, not the operation: /graphql checks
	// an access token and /query/proof checks an envelope, so the leg that
	// decides the URL decides the credential too. Keeping them in one
	// switch is what stops the two drifting apart again.
	var candidateCredential *Credential
	switch row.Mode {
	case "canary", "primary":
		candidateURL = r.Config.PythonEdgeURL
		candidateCredential = r.Config.EdgeCredential
		outcome.Route = RouteEdge
	case "shadow":
		outcome.Route = RouteProof
		candidateCredential = r.Config.ProofCredential
		if r.Config.GoProofURL == "" {
			return refuse(RefusalShadowUnmeasurable,
				"mode=shadow: PostgresSwitch.Enabled admits canary|primary only, and this deployment exposes no measurement-only route, so the deployed Go build cannot execute this operation at all")
		}
		candidateURL = r.Config.GoProofURL
	default:
		return refuse(RefusalNotRouted, fmt.Sprintf("mode=%q is not a Go-serving mode", row.Mode))
	}

	variables := spec.Variables(r.Config.OrgID, r.Config.Window)

	candidate, err := r.post(ctx, candidateURL, document, candidateCredential, variables)
	if err != nil {
		return refuse(RefusalTransport, "candidate leg: "+err.Error())
	}
	outcome.Candidate = &candidate

	// The baseline ALWAYS goes through the Python edge, whatever route
	// the candidate took, so it always uses the edge credential.
	baseline, err := r.post(ctx, r.Config.PythonEdgeURL, document+baselineComment, r.Config.EdgeCredential, variables)
	if err != nil {
		return refuse(RefusalTransport, "baseline leg: "+err.Error())
	}
	outcome.Baseline = &baseline

	candidateSnapshot, refusal := decodeLeg("candidate", candidate)
	if refusal.Reason != "" {
		return refuse(refusal.Reason, refusal.Detail)
	}
	baselineSnapshot, refusal := decodeLeg("baseline", baseline)
	if refusal.Reason != "" {
		return refuse(refusal.Reason, refusal.Detail)
	}

	// THE ONLY DOOR TO executed=true.
	//
	// Every precondition a receipt depends on is checked here, by name, and
	// anything not satisfying all of them is refused. Nothing below this
	// point may set Executed on a pair Admit did not admit -- that is the
	// property TestMatchIsReachableOnlyThroughAdmission pins, and the
	// reason this is one call instead of a sequence of guards scattered
	// through the function (see admission.go's opening comment for the ten
	// instances that shape cost).
	admission := Admit(AdmissionInput{
		Route:         outcome.Route,
		NamedBuild:    r.Registry.BuildIdentity,
		ResponseRoot:  spec.ResponseRoot,
		RootNullable:  spec.RootNullable,
		Candidate:     candidate,
		Baseline:      baseline,
		CandidateSnap: candidateSnapshot,
		BaselineSnap:  baselineSnapshot,
	})
	if !admission.Admitted {
		return refuse(admission.Reason, admission.Detail)
	}
	outcome.Admitted = true
	outcome.admitted = true
	// Recorded from the admission, never re-derived from the route: the
	// receipt must state the binding that was actually checked.
	outcome.EdgeBuildBinding = admission.EdgeBuildBinding

	result := Compare(baselineSnapshot, candidateSnapshot, spec.Parity)
	if len(result.UnusedTierB) > 0 {
		// Same rule as a stale exclusion, one tier over: a Tier-B
		// declaration that relaxed nothing means the comparison that ran
		// is not the comparison anybody declared.
		return refuse(RefusalStaleTierB, fmt.Sprintf("declared Tier-B float fields matched nothing: %v", result.UnusedTierB))
	}
	if len(result.StaleBaselineDefects) > 0 {
		// The Python defect was fixed, or the cited paths are wrong.
		// Either way the entry must go before this run can stand.
		return refuse(RefusalStaleBaselineDefect, fmt.Sprintf("declared baseline defects covered no difference: %v", result.StaleBaselineDefects))
	}
	if len(result.UnusedExclusions) > 0 {
		// A declared exclusion that matched nothing is either stale or
		// misspelled; either way the comparison it produced is not the
		// comparison anybody declared, so it cannot stand as a verdict.
		return refuse(RefusalStaleExclusion, fmt.Sprintf("declared volatile fields matched nothing: %v", result.UnusedExclusions))
	}

	outcome.Executed = true
	outcome.TerminalState = result.TerminalState
	outcome.Findings = result.Findings
	outcome.BaselineDefects = result.BaselineDefectsMatched
	outcome.DifferencesOutsideBaselineDefect = result.DifferencesOutsideBaselineDefect

	// Admission guarantees both legs are 2xx; it does NOT make them equal.
	// 200 against 202 passes admission and is still a real divergence, so
	// the status comparison stays a parity finding. Same for content type:
	// identical bodies served under different types are not parity, because
	// the client is told to interpret the same bytes differently.
	if candidate.StatusCode != baseline.StatusCode {
		outcome.TerminalState = TerminalStateMismatch
		outcome.Findings = append(outcome.Findings, Finding{
			Kind:   FindingMismatch,
			Path:   "$.http.status",
			Detail: fmt.Sprintf("baseline %d != candidate %d", baseline.StatusCode, candidate.StatusCode),
		})
	}
	for _, header := range comparedHeaders {
		baselineValue, candidateValue := baseline.Headers[header], candidate.Headers[header]
		if baselineValue != candidateValue {
			outcome.TerminalState = TerminalStateMismatch
			outcome.Findings = append(outcome.Findings, Finding{
				Kind:   FindingMismatch,
				Path:   "$.http.header." + header,
				Detail: fmt.Sprintf("baseline %q != candidate %q", baselineValue, candidateValue),
			})
		}
	}

	// LAST, so nothing below can turn it back into a match: an edge
	// measurement with no per-request build binding may never be
	// enablement-eligible.
	//
	// r2 proved why with an executed test. A deployment that BEGINS during
	// the run defeats every other defence at once: the routing row can
	// legitimately name the running build, /buildinfo answers from the old
	// replica before and after, and the measured request is served by the
	// new one in between. Without a per-request header there is nothing
	// left that can tell them apart, and the run produced a
	// deployed_executed/match receipt naming the wrong build.
	//
	// So the verdict is downgraded rather than the run refused. The
	// measurement HAPPENED and is worth recording -- terminal_state
	// `unsupported`, which already means "the runner could not establish
	// the claim" and is what this package uses for an errored response, an
	// empty root and an unmeasurable shadow op. The enablement predicate
	// admits only `match`, so the receipt is inert by construction rather
	// than by anybody remembering to exclude it. The provenance object
	// carries edge_build_binding=absent, which is the reason a reader
	// needs.
	//
	// `binding_absent` would be more legible, and is deliberately not used:
	// terminal_state's vocabulary is fixed by the signed plan and enforced
	// by a CHECK constraint, and a hotfix does not widen it (team-lead
	// ruling, 2026-09-09 -- the same trade #2395 made in choosing
	// proof_failed over inventing `refused`).
	//
	// Scoped to `match` deliberately. A mismatch already authorizes
	// nothing, and rewriting it as `unsupported` would DESTROY the
	// divergence the run found -- turning "these planes disagree" into
	// "we could not tell", which is a worse record and a false one. Only
	// the enablement-eligible verdict is downgraded.
	if outcome.TerminalState == TerminalStateMatch &&
		outcome.Route == RouteEdge && outcome.EdgeBuildBinding == EdgeBuildAbsent {
		outcome.TerminalState = TerminalStateUnsupported
		outcome.Findings = append(outcome.Findings, Finding{
			Kind:   FindingMismatch,
			Path:   "$.http.header." + buildHeader,
			Detail: "the measured response carried no serving-build header, so this measurement is not bound to a query-api process. A deployment beginning mid-run would be indistinguishable from a stable one, so this cannot authorize an enablement (CHAOS-5479 delivers the header; until every replica serves it, edge measurements stay unsupported)",
		})
	}
	// Sealed LAST, from whatever the run concluded after every adjustment
	// above. Assigning any exported field afterwards changes the report
	// and cannot change a receipt.
	outcome.terminalState = outcome.TerminalState
	return outcome
}

// seal captures what a measurement established, at the moment it was
// established. Every value comes from the run -- the registry the process
// reported, the config this run was given, the outcome proveOne produced --
// and none of it can be reached again from outside this package.
func (r *Runner) seal(outcome Outcome) sealedOutcome {
	return sealedOutcome{
		operation:                        outcome.Operation,
		documentDigest:                   outcome.DocumentDigest,
		schemaDigest:                     r.Registry.SchemaDigest,
		candidateBuild:                   r.Registry.BuildIdentity,
		orgID:                            r.Config.OrgID,
		mode:                             outcome.Mode,
		route:                            outcome.Route,
		edgeBinding:                      outcome.EdgeBuildBinding,
		routingRowBuild:                  outcome.RoutingRowBuild,
		terminalState:                    outcome.terminalState,
		executed:                         outcome.Executed,
		admitted:                         outcome.admitted,
		baselineRef:                      observationRef(outcome.Baseline),
		candidateRef:                     observationRef(outcome.Candidate),
		baselineDefects:                  append([]string(nil), outcome.BaselineDefects...),
		differencesOutsideBaselineDefect: outcome.DifferencesOutsideBaselineDefect,
	}
}

// decodeLeg turns one leg's body into a Snapshot, or names why it cannot.
//
// Non-finite numbers are handled at TWO levels, and round 2's F9 was right
// that the earlier comment blurred them:
//
//   - In the COMPARATOR, parity rule 3 stands unchanged: a decoded value
//     that is NaN or Infinity is always a mismatch, never tolerance-compared
//     (compare.go's compareNumber, pinned by TestCompareNonFiniteAlwaysMismatches).
//   - At the WIRE, a body containing a bare `NaN`/`Infinity` LITERAL is not
//     valid JSON. Go's decoder rejects it outright, so there is no decoded
//     value to compare and no Snapshot to build. Under the admission
//     invariant an unreadable body is refused rather than guessed at.
//
// Those are not in conflict: the first is about a value, the second about
// bytes that never became one. The refusal names the cause specifically so
// an operator is not left reading it as a generic decode failure.
func decodeLeg(leg string, observation Observation) (Snapshot, Admission) {
	snapshot, err := DecodeSnapshot(observation.Body)
	if err == nil {
		return snapshot, Admission{Admitted: true}
	}
	if errors.Is(err, ErrNonFiniteNumber) {
		return Snapshot{}, refused(RefusalNonFinite,
			fmt.Sprintf("%s response carries a non-finite JSON number (parity rule 3)", leg))
	}
	return Snapshot{}, refused(RefusalUndecodableResponse, leg+" leg: "+err.Error())
}

// terminalStateForRefusal keeps the recorded terminal state meaningful on a
// refusal instead of flattening every one to proof_failed.
//
// The vocabulary already distinguishes these outcomes and an operator
// reading a row should not have to open the detail text to learn that the
// edge fell back to Python rather than that the instrument broke.
func terminalStateForRefusal(reason string) string {
	switch reason {
	case RefusalWrongPlane:
		return "fallback"
	case RefusalNonSuccessStatus:
		return "dependency_failed"
	case RefusalErroredResponse, RefusalEmptyResponseRoot, RefusalShadowUnmeasurable:
		return TerminalStateUnsupported
	case RefusalTransport:
		return "timeout"
	default:
		return "proof_failed"
	}
}

// comparedHeaders is the bounded set of response headers treated as part
// of the observable response. See Observation.Headers for why it is a set
// and not "every header".
var comparedHeaders = []string{contentTypeHeader}

// erroredResponse reports why these two responses cannot back a proof,
// or "" when both carry a clean, data-bearing result.
func erroredResponse(baseline, candidate Snapshot) string {
	switch {
	case len(candidate.Errors) > 0 && len(baseline.Errors) > 0:
		return fmt.Sprintf("both planes returned GraphQL errors (%d candidate, %d baseline)", len(candidate.Errors), len(baseline.Errors))
	case len(candidate.Errors) > 0:
		return fmt.Sprintf("the candidate returned %d GraphQL error(s)", len(candidate.Errors))
	case len(baseline.Errors) > 0:
		return fmt.Sprintf("the baseline returned %d GraphQL error(s)", len(baseline.Errors))
	case !candidate.DataPresent || candidate.Data == nil:
		return "the candidate returned no data"
	}
	return ""
}

func (r *Runner) post(ctx context.Context, url, document string, credential *Credential, variables map[string]any) (Observation, error) {
	body, err := json.Marshal(map[string]any{"query": document, "variables": variables})
	if err != nil {
		return Observation{}, fmt.Errorf("encode request: %w", err)
	}

	if r.Config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.Config.Timeout)
		defer cancel()
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return Observation{}, fmt.Errorf("build request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if err := credential.Apply(ctx, request); err != nil {
		return Observation{}, err
	}

	// Wrapped so redirects are REFUSED. The measured routes are direct
	// endpoints; following a redirect would fetch a URL the operator never
	// supplied and this package never validated, and every redirect
	// finding in this seam's history arrived through a Location header.
	client := NoRedirectClient(r.Client)
	started := time.Now()
	response, err := client.Do(request)
	if err != nil {
		// The transport error is DROPPED, not quoted and not scrubbed.
		// What comes back is the endpoint label this package rebuilt and a
		// failure class -- see TransportFailure for the four rounds of
		// sanitising that preceded that decision.
		return Observation{}, transportError(url, err)
	}
	defer func() { _ = response.Body.Close() }()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return Observation{}, fmt.Errorf("read response body: %w", err)
	}

	observation := Observation{
		StatusCode: response.StatusCode,
		Plane:      strings.ToLower(response.Header.Get(planeHeader)),
		Build:      strings.TrimSpace(response.Header.Get(buildHeader)),
		Headers: map[string]string{
			contentTypeHeader: strings.ToLower(strings.TrimSpace(response.Header.Get(contentTypeHeader))),
		},
		Body:    responseBody,
		Elapsed: time.Since(started),
	}
	if r.Artifacts != nil {
		ref, err := r.Artifacts.Put(responseBody)
		if err != nil {
			return observation, fmt.Errorf("store response artifact: %w", err)
		}
		observation.BodyRef = ref
	}
	return observation, nil
}

// MaxOperatorEvidenceBytes bounds the operator's --review-evidence text.
//
// Same class as the 8 KiB bound on the minting helper's output, one column
// over: an unbounded operator string flowing into a JSON document in a
// Text column is an unbounded write nobody chose. r2 confirmed a 2 MiB note
// stays valid JSON, which is the point -- it would be accepted, stored, and
// read back by every future query against this table.
//
// Refused rather than truncated, for the reason truncation was wrong for
// the helper: a silently shortened value looks like a whole one. 4 KiB is
// far more than a ticket reference and a sentence, which is what this field
// is for.
const MaxOperatorEvidenceBytes = 4 << 10

// ErrOperatorEvidenceTooLong is returned before anything is measured, so an
// over-long note fails as a configuration error rather than after a run.
var ErrOperatorEvidenceTooLong = errors.New("goapiproof: --review-evidence is too long")

// ValidateOperatorEvidence refuses an over-long operator note.
func ValidateOperatorEvidence(evidence string) error {
	if len(evidence) > MaxOperatorEvidenceBytes {
		return fmt.Errorf("%w: %d bytes, limit is %d. It is stored inside a JSON provenance object on every receipt this run writes; put the detail in the ticket and reference it here",
			ErrOperatorEvidenceTooLong, len(evidence), MaxOperatorEvidenceBytes)
	}
	return nil
}

// ReceiptProvenance is what goes into go_api_proof_run.review_evidence.
//
// A JSON OBJECT rather than prose. r1 found the previous design appending
// generated text to operator-authored text with a " | " separator, which
// is ambiguous in both directions -- a reader cannot tell which half a
// machine wrote, and an operator whose note contains the separator forges
// the other half. The operator's words keep their own key and are never
// modified.
//
// A JSON string in a Text column rather than new columns because alembic
// 0128 has no field for any of this and a migration does not belong in a
// hotfix. Every key here is a candidate for promotion to a real column
// later; until then the object keeps them named, typed and parseable
// rather than embedded in a sentence.
type ReceiptProvenance struct {
	// Operator is the operator's own --review-evidence text, verbatim.
	Operator string `json:"operator,omitempty"`
	// MeasurementRoute is RouteEdge or RouteProof.
	MeasurementRoute string `json:"measurement_route,omitempty"`
	// EdgeBuildBinding is EdgeBuildPresent or EdgeBuildAbsent: whether the
	// measured response carried the serving build on itself. Absence is
	// CHAOS-5479's known gap, recorded so a later reader does not have to
	// assume which it was.
	EdgeBuildBinding string `json:"edge_build_binding,omitempty"`
	// RoutingRowBuild is the build the routing row named, when it differed
	// from the running build. A run refuses on this today
	// (VerifyCandidateBuild), so a receipt carrying it means the refusal
	// was bypassed -- which is worth being able to see in the table.
	RoutingRowBuild string `json:"routing_row_build,omitempty"`
	// Refusal is the run-level cause, on a proof_failed receipt only.
	Refusal string `json:"refusal,omitempty"`
	// Measured and Attempted count the run this receipt came from, so a
	// proof_failed row says how much work it is reporting on.
	Measured  int `json:"measured_operations,omitempty"`
	Attempted int `json:"attempted_operations,omitempty"`
}

// reviewEvidence renders the provenance for one receipt. ONE constructor
// for both the success and the refusal path (r1 P2): the refusal path used
// to build its own prose and dropped the routing-row fact entirely, so the
// receipts that most needed provenance had the least.
func (r *Runner) reviewEvidence(sealed sealedOutcome, refusal string, measured, attempted int) string {
	provenance := ReceiptProvenance{
		Operator:         r.Config.ReviewEvidence,
		MeasurementRoute: sealed.route,
		EdgeBuildBinding: sealed.edgeBinding,
		RoutingRowBuild:  sealed.routingRowBuild,
		Refusal:          refusal,
	}
	if refusal != "" {
		provenance.Measured, provenance.Attempted = measured, attempted
	}
	encoded, err := json.Marshal(provenance)
	if err != nil {
		// Every field is a string or an int, so this cannot fail. If it
		// somehow does, the operator's own words are worth more than a
		// dropped column: return them rather than writing nothing.
		return r.Config.ReviewEvidence
	}
	return string(encoded)
}
