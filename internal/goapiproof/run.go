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
	Admitted        bool      `json:"admitted"`
	RefusalReason   string    `json:"refusal_reason,omitempty"`
	RefusalDetail   string    `json:"refusal_detail,omitempty"`
	Route           string    `json:"route"`
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

// Summary is the explicit-zero telemetry block. Every field is printed on
// every run, including the zeros: a run that measured nothing must be
// impossible to mistake for a run that found nothing wrong.
type Summary struct {
	Attempted int `json:"attempted"`
	// Admitted is serialised even when zero. "nothing passed admission"
	// and "everything passed and nothing differed" are different facts,
	// and a run whose admitted count is 0 measured nothing at all.
	Admitted        int            `json:"admitted"`
	Executed        int            `json:"executed"`
	Refused         int            `json:"refused"`
	ReceiptsWritten int            `json:"receipts_written"`
	ByTerminalState map[string]int `json:"by_terminal_state"`
	ByRefusalReason map[string]int `json:"by_refusal_reason"`
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
	OrgID   string
	Auth    AuthContext
	Window  Window
	Headers map[string]string // e.g. Authorization; values are NEVER logged

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

	for _, operation := range operations {
		summary.Attempted++
		outcome := r.proveOne(ctx, operation)
		if outcome.Admitted {
			summary.Admitted++
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
func (r *Runner) ReceiptsFor(outcomes []Outcome, observedAt time.Time) ([]Receipt, error) {
	receipts := make([]Receipt, 0, len(outcomes))
	for _, outcome := range outcomes {
		if !outcome.Executed {
			continue
		}
		identity, err := RequestIdentity(r.Config.OrgID, r.Config.Auth, r.variablesFor(outcome.Operation))
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, Receipt{
			SchemaDigest:                     r.Registry.SchemaDigest,
			DocumentDigest:                   outcome.DocumentDigest,
			SelectedOperation:                outcome.Operation,
			CandidateBuild:                   r.Registry.BuildIdentity,
			RequestIdentity:                  identity,
			Stage:                            Stage,
			TerminalState:                    outcome.TerminalState,
			OrgID:                            r.Config.OrgID,
			ReviewEvidence:                   r.Config.ReviewEvidence,
			RecordedBy:                       r.Config.RecordedBy,
			ObservedAt:                       observedAt,
			BaselineResponseRef:              observationRef(outcome.Baseline),
			CandidateResponseRef:             observationRef(outcome.Candidate),
			MeasurementRoute:                 outcome.Route,
			BaselineDefects:                  outcome.BaselineDefects,
			DifferencesOutsideBaselineDefect: outcome.DifferencesOutsideBaselineDefect,
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
func (r *Runner) RefusalReceipts(outcomes []Outcome, observedAt time.Time, cause string) ([]Receipt, error) {
	admitted := 0
	for _, outcome := range outcomes {
		if outcome.Executed {
			admitted++
		}
	}
	evidence := fmt.Sprintf(
		"run refused: %s. %d of %d operation(s) had been measured and none of their results were written; this row records that the run happened and certified nothing. Operator note: %s",
		cause, admitted, len(outcomes), r.Config.ReviewEvidence)

	receipts := make([]Receipt, 0, len(outcomes))
	for _, outcome := range outcomes {
		if !outcome.Executed {
			continue
		}
		identity, err := RequestIdentity(r.Config.OrgID, r.Config.Auth, r.variablesFor(outcome.Operation))
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, Receipt{
			SchemaDigest:      r.Registry.SchemaDigest,
			DocumentDigest:    outcome.DocumentDigest,
			SelectedOperation: outcome.Operation,
			CandidateBuild:    r.Registry.BuildIdentity,
			RequestIdentity:   identity,
			Stage:             Stage,
			TerminalState:     "proof_failed",
			OrgID:             r.Config.OrgID,
			ReviewEvidence:    evidence,
			RecordedBy:        r.Config.RecordedBy,
			ObservedAt:        observedAt,
			MeasurementRoute:  outcome.Route,
		})
	}
	return receipts, nil
}

// WriteReceipts writes a whole run's receipts, each atomically.
func WriteReceipts(ctx context.Context, db Querier, receipts []Receipt) (int, error) {
	written := 0
	for _, receipt := range receipts {
		if _, err := WriteAtomic(ctx, db, receipt); err != nil {
			return written, err
		}
		written++
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

	refuse := func(reason, detail string) Outcome {
		outcome.RefusalReason = reason
		outcome.RefusalDetail = detail
		outcome.TerminalState = terminalStateForRefusal(reason)
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
	switch row.Mode {
	case "canary", "primary":
		candidateURL = r.Config.PythonEdgeURL
		outcome.Route = RouteEdge
	case "shadow":
		outcome.Route = RouteProof
		if r.Config.GoProofURL == "" {
			return refuse(RefusalShadowUnmeasurable,
				"mode=shadow: PostgresSwitch.Enabled admits canary|primary only, and this deployment exposes no measurement-only route, so the deployed Go build cannot execute this operation at all")
		}
		candidateURL = r.Config.GoProofURL
	default:
		return refuse(RefusalNotRouted, fmt.Sprintf("mode=%q is not a Go-serving mode", row.Mode))
	}

	variables := spec.Variables(r.Config.OrgID, r.Config.Window)

	candidate, err := r.post(ctx, candidateURL, document, variables)
	if err != nil {
		return refuse(RefusalTransport, "candidate leg: "+err.Error())
	}
	outcome.Candidate = &candidate

	baseline, err := r.post(ctx, r.Config.PythonEdgeURL, document+baselineComment, variables)
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
	return outcome
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

func (r *Runner) post(ctx context.Context, url, document string, variables map[string]any) (Observation, error) {
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
	for name, value := range r.Config.Headers {
		request.Header.Set(name, value)
	}

	client := r.Client
	if client == nil {
		client = http.DefaultClient
	}
	started := time.Now()
	response, err := client.Do(request)
	if err != nil {
		// The error is returned verbatim from net/http, which includes the
		// URL but never a header value -- no credential can reach a log
		// through this path.
		return Observation{}, fmt.Errorf("request failed: %w", err)
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
