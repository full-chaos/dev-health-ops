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
	if out.TerminalState != goapiproof.TerminalStateMatch {
		t.Fatalf("TerminalState = %q, want match -- candidate_shape never compares bodies", out.TerminalState)
	}
	if len(writer.receipts) != 1 {
		t.Fatalf("wrote %d receipts, want 1", len(writer.receipts))
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
