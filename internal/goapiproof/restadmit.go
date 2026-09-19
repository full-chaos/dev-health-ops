package goapiproof

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// This file is the REST sibling of admission.go and snapshot.go.
//
// Why a sibling rather than a reuse of Admit/DecodeSnapshot as-is: both
// are shaped around the GraphQL edge's own evidence. DecodeSnapshot reads
// a `{"data": ..., "errors": [...]}` envelope, which no REST response in
// this service carries -- a REST handler writes its result (or a plain
// http.Error text body) directly as the whole body. admitPlanes reads
// x-dev-health-plane on BOTH legs, which the Python api service has never
// stamped on a REST response and, under this repo's no-Python-changes
// policy, never will -- so a REST admission gate cannot require it. What
// DOES carry over unchanged: the build-header binding rule (buildHeader,
// EdgeBuildPresent/EdgeBuildAbsent, RouteProof/RouteEdge), because
// withProofProvenance stamps the SAME header on every REST response this
// tool measures (cmd/query-api's route files wrap each REST handler with
// it, mirroring /query's own wiring) -- see RESTAdmit.

// DecodeRESTSnapshot turns one plane's raw REST response body into a
// Snapshot whose Data is the WHOLE decoded body -- there is no `data`
// envelope key to unwrap, so DataPresent is true whenever the body
// decodes to any JSON value at all (including a bare `null`), and Errors
// is always empty (a REST response reports failure via its status code,
// never a GraphQL errors array).
//
// Every other guard DecodeSnapshot enforces is enforced here too, and for
// the same reason: a non-finite literal, invalid UTF-8, an unpaired
// surrogate escape or trailing bytes after the first JSON value would
// each let two materially different bodies decode to the same comparison
// value. Trailing bytes are judged against RFC 8259's insignificant
// whitespace set (space, tab, CR, LF) ONLY: a Go response's trailing
// encoder newline and a Python response's absent one both decode to the
// same comparison value and are admissible on either leg, but any other
// trailing byte -- including a second JSON value -- is not, because the
// comparison is over decoded VALUES and RFC 8259 whitespace is the only
// trailing content the spec itself calls insignificant.
func DecodeRESTSnapshot(body []byte) (Snapshot, error) {
	if nonFiniteLiteral.Match(body) {
		return Snapshot{}, ErrNonFiniteNumber
	}
	if !utf8.Valid(body) {
		return Snapshot{}, errors.New("goapiproof: response body is not valid UTF-8 -- a byte this decoder cannot represent would otherwise be silently replaced before comparison")
	}
	if err := rejectUnpairedSurrogateEscapes(body); err != nil {
		return Snapshot{}, fmt.Errorf("goapiproof: %w", err)
	}

	snapshot := Snapshot{BodyBytes: len(body)}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return Snapshot{}, fmt.Errorf("goapiproof: decode REST response body: %w", err)
	}
	if hasSignificantTrailingBytes(body[decoder.InputOffset():]) {
		snapshot.TrailingBytes = true
	}
	snapshot.Data = value
	snapshot.DataPresent = true
	return snapshot, nil
}

// hasSignificantTrailingBytes reports whether trailing (everything after
// the first decoded JSON value) carries any byte other than RFC 8259
// insignificant whitespace (space, tab, CR, LF). A second JSON value is
// significant regardless of what precedes it: its own leading bytes are
// never themselves whitespace.
func hasSignificantTrailingBytes(trailing []byte) bool {
	for _, b := range trailing {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			return true
		}
	}
	return false
}

// RESTLeg is one plane's raw HTTP observation for one REST request.
type RESTLeg struct {
	// StatusCode is the HTTP status the leg answered.
	StatusCode int
	// Body is the raw response bytes, exactly as received.
	Body []byte
	// Build is the x-dev-health-build header value, when the leg carried
	// one. Only ever set on the candidate leg in practice -- the Python
	// api service does not stamp it -- but read generically so a future
	// deployment that adds it to the baseline is not silently ignored.
	Build string
}

// RESTAdmissionInput is everything RESTAdmit may consider.
type RESTAdmissionInput struct {
	// NamedBuild is the build the receipt will name (from /buildinfo).
	NamedBuild string
	// WantCandidateStatus/WantBaselineStatus are the request's declared
	// admissible status codes (RESTRequest.WantCandidateStatus/
	// WantBaselineStatus). A leg answering any other status is a refusal:
	// the request did not reach the state this entry measures.
	WantCandidateStatus int
	WantBaselineStatus  int

	Candidate, Baseline RESTLeg
}

// REST admission refusal reasons. Distinct from admission.go's constants
// (never the same string) so a report line naming one cannot be misread
// as the GraphQL gate's own vocabulary.
const (
	RESTRefusalUnexpectedStatus = "rest_response_status_did_not_match_the_declared_want"
	RESTRefusalBuildUnbound     = "rest_candidate_build_header_absent_or_mismatched"
	RESTRefusalTrailingBytes    = "rest_response_body_carried_bytes_after_the_json_value"
	RESTRefusalBodyNotJSON      = "rest_response_body_did_not_decode_as_json"
)

// REST leg transport refusal reasons: a leg that never produced a
// response at all -- it timed out, or failed at the transport level some
// other way -- is refused by one of these four, never by one of the
// admission reasons above (those all judge a response that DID arrive).
// Each name states both the FAILING LEG and the FAILURE CLASS, so a
// report line naming one is never folded into, or mistaken for, a
// different cause. See TransportFailure/TransportTimeout for the class
// vocabulary a caller classifies its own error against before choosing
// one of these four.
const (
	RESTRefusalCandidateLegTimedOut       = "rest_candidate_leg_timed_out"
	RESTRefusalCandidateLegTransportError = "rest_candidate_leg_transport_error"
	RESTRefusalBaselineLegTimedOut        = "rest_baseline_leg_timed_out"
	RESTRefusalBaselineLegTransportError  = "rest_baseline_leg_transport_error"
)

// REST leg refusals for a leg the run itself cut short: the run's own
// deadline, or a signal, ended the leg's request, so nothing is known
// about how the plane would have answered. Never folded into the
// timed_out/transport_error names above, which state that the plane did
// not answer.
const (
	RESTRefusalCandidateLegCutByRunDeadline = "rest_candidate_leg_cut_by_run_deadline"
	RESTRefusalBaselineLegCutByRunDeadline  = "rest_baseline_leg_cut_by_run_deadline"
	RESTRefusalCandidateLegCutBySignal      = "rest_candidate_leg_cut_by_signal"
	RESTRefusalBaselineLegCutBySignal       = "rest_baseline_leg_cut_by_signal"
)

// RESTAdmission is RESTAdmit's verdict.
type RESTAdmission struct {
	Admitted bool
	Reason   string
	Detail   string
	// CandidateSnap/BaselineSnap are populated only when BodyMode calls
	// for a JSON comparison AND admission held -- callers decide whether
	// to decode at all so a StatusOnly request never pays for (or can
	// fail on) a body this table does not promise is JSON.
	CandidateSnap, BaselineSnap Snapshot
}

func restRefused(reason, detail string) RESTAdmission {
	return RESTAdmission{Reason: reason, Detail: detail}
}

// RESTAdmit reports whether this pair of REST observations may back a
// proof receipt, and decodes both bodies as JSON when decodeBody is true.
//
// Ordered like Admit: the most fundamental failure first.
func RESTAdmit(in RESTAdmissionInput, decodeBody bool) RESTAdmission {
	// 1. Each leg answered the status this request declared admissible.
	if in.Candidate.StatusCode != in.WantCandidateStatus {
		return restRefused(RESTRefusalUnexpectedStatus,
			fmt.Sprintf("candidate answered HTTP %d, this request declared %d admissible", in.Candidate.StatusCode, in.WantCandidateStatus))
	}
	if in.Baseline.StatusCode != in.WantBaselineStatus {
		return restRefused(RESTRefusalUnexpectedStatus,
			fmt.Sprintf("baseline answered HTTP %d, this request declared %d admissible", in.Baseline.StatusCode, in.WantBaselineStatus))
	}

	// 2. The candidate build that SERVED the request. Required
	// unconditionally: every REST route this tool measures is wrapped in
	// withProofProvenance (cmd/query-api), so an absent or mismatched
	// header means either the deployment predates that wrapper or the
	// candidate URL did not actually reach query-api.
	switch {
	case in.Candidate.Build == "":
		return restRefused(RESTRefusalBuildUnbound,
			"the candidate response carried no x-dev-health-build header: without it the receipt would name a build nothing showed served the request")
	case in.Candidate.Build != in.NamedBuild:
		return restRefused(RESTRefusalBuildUnbound,
			fmt.Sprintf("the process that served this request reports build %q, but the receipt would name %q", in.Candidate.Build, in.NamedBuild))
	}

	if !decodeBody {
		return RESTAdmission{Admitted: true}
	}

	candidateSnap, err := DecodeRESTSnapshot(in.Candidate.Body)
	if err != nil {
		return restRefused(RESTRefusalBodyNotJSON, "candidate: "+err.Error())
	}
	baselineSnap, err := DecodeRESTSnapshot(in.Baseline.Body)
	if err != nil {
		return restRefused(RESTRefusalBodyNotJSON, "baseline: "+err.Error())
	}
	if candidateSnap.TrailingBytes || baselineSnap.TrailingBytes {
		return restRefused(RESTRefusalTrailingBytes,
			"a response body carried bytes after its first JSON value: the decoder ignores them, so the comparison would not be over the bytes actually served")
	}
	return RESTAdmission{Admitted: true, CandidateSnap: candidateSnap, BaselineSnap: baselineSnap}
}
