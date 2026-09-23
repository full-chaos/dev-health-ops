package restprove

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// These tests drive RESTBodyModeCandidateShape directly through
// proveOneRESTRequest: a synthetic corpus request shaped exactly like a
// deleted-Python-body entry (restdeletedbody.go) -- baseline declares the
// fixed sentinel, candidate declares 200 -- against a fake baseline that
// answers that sentinel and a fake candidate whose body this file
// controls per cell.

func sentinelBaseline(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"detail":"this route is served by query-api and has no Python implementation"}`))
	})))
	t.Cleanup(srv.Close)
	return srv
}

func TestProveOneRESTRequest_CandidateShapeLivenessPasses(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		_, _ = w.Write([]byte(`{"version":"1.2.3","db_kind":"clickhouse"}`))
	}))
	defer candidate.Close()
	baseline := sentinelBaseline(t)
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/meta"}
	request := goapiproof.RESTRequest{
		Name: "meta", WantCandidateStatus: 200, WantBaselineStatus: 500,
		StatusDivergenceReason: goapiproof.PythonBodyDeletedReason,
		BodyMode:               goapiproof.RESTBodyModeCandidateShape,
	}
	writer := &fakeReceiptWriter{}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/meta", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if !out.Admitted || out.Refusal != "" {
		t.Fatalf("out = %+v, want admitted with no refusal", out)
	}
	// TerminalStateUnsupported, never TerminalStateMatch: a liveness+kind
	// check is not a body comparison, and "match" would let this receipt
	// satisfy EnablementProofClause -- the same predicate `enable` and
	// migrationmatrix's REST "proven" column read -- and silently promote
	// a route on weaker evidence than a real comparison provides.
	if out.TerminalState != goapiproof.TerminalStateUnsupported {
		t.Fatalf("TerminalState = %q, want unsupported -- candidate_shape never compares bodies and must not satisfy EnablementProofClause", out.TerminalState)
	}
	if len(writer.receipts) != 1 || writer.receipts[0].TerminalState != goapiproof.TerminalStateUnsupported {
		t.Fatalf("receipts = %+v, want exactly 1 with TerminalState unsupported", writer.receipts)
	}
	// The written receipt's own terminal_state cannot satisfy
	// EnablementProofClause (receipt.go): that predicate admits ONLY
	// EnablementProofTerminalState ("match") or a fully-cited
	// EnablementCitedMismatchState ("mismatch"), neither of which this
	// receipt carries -- so ReadRESTProof (migrationmatrix) will not
	// promote this route to RESTProven off this receipt alone.
	if got := writer.receipts[0].TerminalState; got == goapiproof.EnablementProofTerminalState || got == goapiproof.EnablementCitedMismatchState {
		t.Fatalf("receipt TerminalState = %q, want neither of the two states EnablementProofClause admits", got)
	}
}

func TestProveOneRESTRequest_CandidateShapeRefusesNullBody(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		_, _ = w.Write([]byte(`null`))
	}))
	defer candidate.Close()
	baseline := sentinelBaseline(t)
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/meta"}
	request := goapiproof.RESTRequest{
		Name: "meta", WantCandidateStatus: 200, WantBaselineStatus: 500,
		StatusDivergenceReason: goapiproof.PythonBodyDeletedReason,
		BodyMode:               goapiproof.RESTBodyModeCandidateShape,
	}
	writer := &fakeReceiptWriter{}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/meta", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if out.Admitted || out.Refusal != goapiproof.RESTRefusalCandidateShapeInvalid {
		t.Fatalf("out = %+v, want a named RESTRefusalCandidateShapeInvalid refusal", out)
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote %d receipts, want 0 for a refused request", len(writer.receipts))
	}
}

func TestProveOneRESTRequest_CandidateShapeRefusesEmptyObject(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer candidate.Close()
	baseline := sentinelBaseline(t)
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/meta"}
	request := goapiproof.RESTRequest{
		Name: "meta", WantCandidateStatus: 200, WantBaselineStatus: 500,
		StatusDivergenceReason: goapiproof.PythonBodyDeletedReason,
		BodyMode:               goapiproof.RESTBodyModeCandidateShape,
	}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/meta", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if out.Admitted || out.Refusal != goapiproof.RESTRefusalCandidateShapeInvalid {
		t.Fatalf("out = %+v, want a named RESTRefusalCandidateShapeInvalid refusal -- an empty object is not live", out)
	}
}

// TestProveOneRESTRequest_CandidateShapeArrayAllowsEmpty proves the wiring
// (not just AssertRESTCandidateShape's own unit tests) respects
// CandidateShapeArray: a real array-shaped route (GET /api/v1/people,
// list[PersonSearchResult]) answering an empty array is admitted -- a
// scope with genuinely zero rows is still a live answer, never refused as
// not-live the way an empty OBJECT would be.
func TestProveOneRESTRequest_CandidateShapeArrayAllowsEmpty(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer candidate.Close()
	baseline := sentinelBaseline(t)
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/people"}
	request := goapiproof.RESTRequest{
		Name: "people", WantCandidateStatus: 200, WantBaselineStatus: 500,
		StatusDivergenceReason: goapiproof.PythonBodyDeletedReason,
		BodyMode:               goapiproof.RESTBodyModeCandidateShape,
		CandidateShapeArray:    true,
	}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/people", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if !out.Admitted || out.Refusal != "" {
		t.Fatalf("out = %+v, want admitted -- an empty array is a live answer for an array-shaped route", out)
	}
}

// TestProveOneRESTRequest_CandidateShapeProducesFromCandidate proves
// candidate_shape's own decode feeds Produces the SAME way status_only's
// candidate-producer path already does (restidbind.go's own doc comment):
// an id declared Produced is extracted from the candidate body once
// admission and the shape assertion both hold.
func TestProveOneRESTRequest_CandidateShapeProducesFromCandidate(t *testing.T) {
	const build = "abc123def456"
	const wantID = "p-1"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		_, _ = w.Write([]byte(`[{"person_id":"` + wantID + `"}]`))
	}))
	defer candidate.Close()
	baseline := sentinelBaseline(t)
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/people"}
	request := goapiproof.RESTRequest{
		Name: "people", WantCandidateStatus: 200, WantBaselineStatus: 500,
		StatusDivergenceReason: goapiproof.PythonBodyDeletedReason,
		BodyMode:               goapiproof.RESTBodyModeCandidateShape,
		CandidateShapeArray:    true,
		Produces:               []goapiproof.RESTIDProducer{{Name: "person_id", IDField: "person_id"}},
	}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/people", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if !out.Admitted {
		t.Fatalf("request refused: %s -- %s", out.Refusal, out.Detail)
	}
	if got := out.producedIDs["person_id"]; got != wantID {
		t.Fatalf("producedIDs[person_id] = %q, want %q (from the candidate leg)", got, wantID)
	}
}

// TestProveOneRESTRequest_CandidateShapeStillRefusesUnexpectedBaselineStatus
// proves the shape assertion runs only AFTER ordinary admission: a
// baseline that answers 200 instead of the declared sentinel refuses by
// name (RESTRefusalUnexpectedStatus) before the candidate body is ever
// decoded for its shape.
func TestProveOneRESTRequest_CandidateShapeStillRefusesUnexpectedBaselineStatus(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		_, _ = w.Write([]byte(`{"version":"1.2.3"}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"version":"1.2.3"}`))
	})))
	defer baseline.Close()
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/meta"}
	request := goapiproof.RESTRequest{
		Name: "meta", WantCandidateStatus: 200, WantBaselineStatus: 500,
		StatusDivergenceReason: goapiproof.PythonBodyDeletedReason,
		BodyMode:               goapiproof.RESTBodyModeCandidateShape,
	}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/meta", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if out.Admitted || out.Refusal != goapiproof.RESTRefusalUnexpectedStatus {
		t.Fatalf("out = %+v, want a named RESTRefusalUnexpectedStatus refusal (baseline recovered to 200)", out)
	}
}

// TestProveOneRESTRequest_DeletedBodyStatusOnlyAlsoTerminatesUnsupported
// proves the OTHER half of the deleted-body override never writes
// TerminalStateMatch either: a deleted-body request whose CANDIDATE itself
// does not reach 200 (a Go-side 404/422/503 refusal) gets
// RESTBodyModeStatusOnly, not RESTBodyModeCandidateShape, so it never runs
// AssertRESTCandidateShape at all -- but it still compares no body, and an
// admitted status_only request otherwise defaults its terminal state to
// "match" (proveOneRESTRequest's own zero value). Uses the real committed
// corpus entry GET /api/v1/flame's own unknown_entity_type (candidate 404,
// handler-computed, not framework-level -- restdeletedbody.go converts it
// to status_only/500), the exact shape a round-2 review reproduced as a
// false "match" receipt before this fix.
func TestProveOneRESTRequest_DeletedBodyStatusOnlyAlsoTerminatesUnsupported(t *testing.T) {
	const build = "abc123def456"
	spec, err := goapiproof.SpecForREST("REST:GET:/api/v1/flame")
	if err != nil {
		t.Fatalf("SpecForREST: %v", err)
	}
	var request goapiproof.RESTRequest
	for _, r := range spec.Requests {
		if r.Name == "unknown_entity_type" {
			request = r
		}
	}
	if request.Name == "" {
		t.Fatal("GET /api/v1/flame has no unknown_entity_type entry")
	}
	if request.BodyMode != goapiproof.RESTBodyModeStatusOnly || request.WantCandidateStatus != 404 || request.WantBaselineStatus != 500 {
		t.Fatalf("unknown_entity_type = %+v, want (404, 500)/status_only -- corpus shape changed, update this test's fixture", request)
	}
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"detail":"not found"}`))
	}))
	defer candidate.Close()
	baseline := sentinelBaseline(t)
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	writer := &fakeReceiptWriter{}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/flame", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if !out.Admitted {
		t.Fatalf("request refused: %s -- %s", out.Refusal, out.Detail)
	}
	if out.TerminalState != goapiproof.TerminalStateUnsupported {
		t.Fatalf("TerminalState = %q, want unsupported -- a status_only deleted-body receipt must not satisfy EnablementProofClause either", out.TerminalState)
	}
	if len(writer.receipts) != 1 || writer.receipts[0].TerminalState != goapiproof.TerminalStateUnsupported {
		t.Fatalf("receipts = %+v, want exactly 1 with TerminalState unsupported", writer.receipts)
	}
}

// TestProveOneRESTRequest_UnrelatedStatusOnlyStillTerminatesMatch is the
// negative control for the fix above: an admitted status_only request
// whose StatusDivergenceReason is NOT goapiproof.PythonBodyDeletedReason
// (a pre-existing, unrelated declared-baseline-failure entry, the
// CHAOS-5868 shape -- every real corpus operation is a deleted-body one
// today, so this is constructed rather than looked up) keeps writing
// TerminalStateMatch exactly as it always has. The fix is scoped by
// StatusDivergenceReason, not by BodyMode alone, and this proves that
// scope actually holds rather than silently widening to every status_only
// entry.
func TestProveOneRESTRequest_UnrelatedStatusOnlyStillTerminatesMatch(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"detail":"Data unavailable"}`))
	})))
	defer baseline.Close()
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/quadrant"}
	request := goapiproof.RESTRequest{
		Name: "wip_throughput_org", WantCandidateStatus: 200, WantBaselineStatus: 503,
		StatusDivergenceReason: "constructed for this test, unrelated to the deleted-body override",
		BodyMode:               goapiproof.RESTBodyModeStatusOnly,
	}
	writer := &fakeReceiptWriter{}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/quadrant", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if !out.Admitted {
		t.Fatalf("request refused: %s -- %s", out.Refusal, out.Detail)
	}
	if out.TerminalState != goapiproof.TerminalStateMatch {
		t.Fatalf("TerminalState = %q, want match -- an unrelated status_only entry's terminal state must not change", out.TerminalState)
	}
}
