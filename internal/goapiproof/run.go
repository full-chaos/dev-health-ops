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
)

// planeHeader is the response header the Python edge stamps with the
// plane that actually served a request (go_api_dispatcher's
// _with_plane_header, gated by GO_API_PLANE_HEADER_ENABLED). Without it,
// a fallback to Python is indistinguishable from a Go-served response,
// and every receipt this command writes would be worthless.
const planeHeader = "x-dev-health-plane"

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
	StatusCode int           `json:"status_code"`
	Plane      string        `json:"plane"`
	Body       []byte        `json:"-"`
	BodyRef    string        `json:"body_ref,omitempty"`
	Elapsed    time.Duration `json:"elapsed_ns"`
}

// Outcome is one operation's complete result: what was attempted, what
// actually executed, and -- when nothing executed -- why, by name.
type Outcome struct {
	Operation      string       `json:"operation"`
	DocumentDigest string       `json:"document_digest"`
	Mode           string       `json:"mode"`
	Executed       bool         `json:"executed"`
	RefusalReason  string       `json:"refusal_reason,omitempty"`
	RefusalDetail  string       `json:"refusal_detail,omitempty"`
	TerminalState  string       `json:"terminal_state"`
	Findings       []Finding    `json:"findings,omitempty"`
	Candidate      *Observation `json:"candidate,omitempty"`
	Baseline       *Observation `json:"baseline,omitempty"`
	ReceiptWritten bool         `json:"receipt_written"`
}

// Summary is the explicit-zero telemetry block. Every field is printed on
// every run, including the zeros: a run that measured nothing must be
// impossible to mistake for a run that found nothing wrong.
type Summary struct {
	Attempted       int            `json:"attempted"`
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

// Run executes every registered operation and returns one Outcome each
// plus the summary. It writes receipts through db when db is non-nil.
//
// The returned error is non-nil when the RUN itself is not trustworthy
// (nothing measured, or a refusal with no named reason). A recorded
// MISMATCH is not an error: it is the measurement succeeding.
func (r *Runner) Run(ctx context.Context, db Querier) ([]Outcome, Summary, error) {
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

		if outcome.Executed && db != nil {
			receipt := Receipt{
				SchemaDigest:         r.Registry.SchemaDigest,
				DocumentDigest:       outcome.DocumentDigest,
				SelectedOperation:    operation,
				CandidateBuild:       r.Registry.BuildIdentity,
				Stage:                Stage,
				TerminalState:        outcome.TerminalState,
				OrgID:                r.Config.OrgID,
				ReviewEvidence:       r.Config.ReviewEvidence,
				RecordedBy:           r.Config.RecordedBy,
				ObservedAt:           now(),
				BaselineResponseRef:  observationRef(outcome.Baseline),
				CandidateResponseRef: observationRef(outcome.Candidate),
			}
			identity, err := RequestIdentity(r.Config.OrgID, r.Config.Auth, r.variablesFor(operation))
			if err != nil {
				return outcomes, summary, err
			}
			receipt.RequestIdentity = identity
			if _, err := Write(ctx, db, receipt); err != nil {
				return outcomes, summary, err
			}
			outcome.ReceiptWritten = true
			summary.ReceiptsWritten++
		}
		outcomes = append(outcomes, outcome)
	}

	if summary.Executed == 0 {
		return outcomes, summary, ErrNothingMeasured
	}
	return outcomes, summary, nil
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
		outcome.TerminalState = "proof_failed"
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
	case "shadow":
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

	// The candidate MUST have been served by Go. A fallback is a real,
	// recordable outcome -- but it is a fallback, not a proof, and it must
	// never be written as one.
	if row.Mode != "shadow" && candidate.Plane != "go" {
		outcome.RefusalReason = RefusalWrongPlane
		outcome.RefusalDetail = fmt.Sprintf("candidate leg was served by plane %q (status %d): the edge fell back to Python", candidate.Plane, candidate.StatusCode)
		outcome.TerminalState = "fallback"
		return outcome
	}

	baseline, err := r.post(ctx, r.Config.PythonEdgeURL, document+baselineComment, variables)
	if err != nil {
		return refuse(RefusalTransport, "baseline leg: "+err.Error())
	}
	outcome.Baseline = &baseline
	if baseline.Plane != "" && baseline.Plane != "python" {
		return refuse(RefusalWrongPlane, fmt.Sprintf("baseline leg was served by plane %q -- the control must be Python", baseline.Plane))
	}

	candidateSnapshot, err := DecodeSnapshot(candidate.Body)
	if err != nil {
		if errors.Is(err, ErrNonFiniteNumber) {
			outcome.Executed = true
			outcome.TerminalState = TerminalStateMismatch
			outcome.Findings = []Finding{{Kind: FindingMismatch, Path: "$", Detail: "candidate response carries a non-finite JSON number (parity rule 3)"}}
			return outcome
		}
		return refuse(RefusalUndecodableResponse, "candidate leg: "+err.Error())
	}
	baselineSnapshot, err := DecodeSnapshot(baseline.Body)
	if err != nil {
		if errors.Is(err, ErrNonFiniteNumber) {
			outcome.Executed = true
			outcome.TerminalState = TerminalStateMismatch
			outcome.Findings = []Finding{{Kind: FindingMismatch, Path: "$", Detail: "baseline response carries a non-finite JSON number (parity rule 3)"}}
			return outcome
		}
		return refuse(RefusalUndecodableResponse, "baseline leg: "+err.Error())
	}

	result := Compare(baselineSnapshot, candidateSnapshot, spec.Parity)
	if len(result.UnusedExclusions) > 0 {
		// A declared exclusion that matched nothing is either stale or
		// misspelled; either way the comparison it produced is not the
		// comparison anybody declared, so it cannot stand as a verdict.
		return refuse(RefusalStaleExclusion, fmt.Sprintf("declared volatile fields matched nothing: %v", result.UnusedExclusions))
	}

	outcome.Executed = true
	outcome.TerminalState = result.TerminalState
	outcome.Findings = result.Findings

	// The outer HTTP observation is part of the verdict, not decoration: a
	// body that compares equal under a different status code is not parity.
	if candidate.StatusCode != baseline.StatusCode && outcome.TerminalState == TerminalStateMatch {
		outcome.TerminalState = TerminalStateMismatch
		outcome.Findings = append(outcome.Findings, Finding{
			Kind:   FindingMismatch,
			Path:   "$.http.status",
			Detail: fmt.Sprintf("baseline %d != candidate %d", baseline.StatusCode, candidate.StatusCode),
		})
	}
	return outcome
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
		Body:       responseBody,
		Elapsed:    time.Since(started),
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
