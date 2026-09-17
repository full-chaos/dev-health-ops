package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/migrationmatrix"
)

// resetFlagsForTest gives parseFlags a fresh, silent flag.CommandLine --
// parseFlags registers its flags on the package-level flag.CommandLine,
// so a second call in the same process (a second test) would otherwise
// panic with "flag redefined". ContinueOnError (not the default
// ExitOnError) so a bad argument returns an error a test can assert on
// instead of calling os.Exit inside the test binary.
func resetFlagsForTest(t *testing.T) {
	t.Helper()
	flag.CommandLine = flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
}

// setOSArgs points os.Args at args for the duration of the test, since
// parseFlags calls flag.Parse(), which reads os.Args[1:].
func setOSArgs(t *testing.T, args []string) func() {
	t.Helper()
	old := os.Args
	os.Args = args
	return func() { os.Args = old }
}

func TestParseFlags_RefusesMissingRequiredFlags(t *testing.T) {
	resetFlagsForTest(t)
	restoreArgs := setOSArgs(t, []string{"go-api-rest-prove"})
	defer restoreArgs()
	if _, err := parseFlags(); err == nil {
		t.Fatal("want an error when every required flag is missing")
	}
}

func TestParseFlags_DefaultsBuildInfoURLFromQueryAPIURL(t *testing.T) {
	resetFlagsForTest(t)
	restoreArgs := setOSArgs(t, []string{
		"go-api-rest-prove",
		"-query-api-url", "http://query-api:8090",
		"-python-api-url", "http://api:8000",
		"-candidate-bearer-exec", `["/bin/true"]`,
		"-baseline-bearer-exec", `["/bin/true"]`,
		"-org", "org-1",
		"-recorded-by", "chris",
		"-review-evidence", "test",
		"-artifact-dir", t.TempDir(),
		"-dry-run",
	})
	defer restoreArgs()
	f, err := parseFlags()
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.buildInfoURL != "http://query-api:8090/buildinfo" {
		t.Fatalf("buildInfoURL = %q, want derived from -query-api-url", f.buildInfoURL)
	}
}

func TestParseFlags_DryRunDoesNotRequirePostgresURI(t *testing.T) {
	resetFlagsForTest(t)
	restoreArgs := setOSArgs(t, []string{
		"go-api-rest-prove",
		"-python-api-url", "http://api:8000",
		"-candidate-bearer-exec", `["/bin/true"]`,
		"-baseline-bearer-exec", `["/bin/true"]`,
		"-org", "org-1",
		"-recorded-by", "chris",
		"-review-evidence", "test",
		"-artifact-dir", t.TempDir(),
		"-dry-run",
	})
	defer restoreArgs()
	if _, err := parseFlags(); err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
}

func TestParseHelperArgv_RefusesNonJSON(t *testing.T) {
	if _, err := parseHelperArgv("-x", "not json"); err == nil {
		t.Fatal("want an error for non-JSON argv")
	}
}

func TestParseHelperArgv_RefusesEmptyArray(t *testing.T) {
	if _, err := parseHelperArgv("-x", "[]"); err == nil {
		t.Fatal("want an error for an empty argv")
	}
}

// mintBearer/limitedWriter moved to goapiproof.MintViaAllowlistedHelper --
// see internal/goapiproof/mintexec_test.go for their tests. This package
// no longer has an exec.Command call site of its own.

func TestResultVacuityErrors_EmptyForACleanResult(t *testing.T) {
	if errs := resultVacuityErrors(goapiproof.Result{}); len(errs) != 0 {
		t.Fatalf("got %v, want none", errs)
	}
}

func TestResultVacuityErrors_NamesEveryKind(t *testing.T) {
	result := goapiproof.Result{
		UnusedExclusions:             []string{"a"},
		UnusedTierB:                  []string{"b"},
		StaleBaselineDefects:         []string{"CHAOS-1"},
		UnusedOrderInsensitiveLists:  []string{"c"},
		OrderInsensitiveListRefusals: []string{"d"},
	}
	errs := resultVacuityErrors(result)
	if len(errs) != 5 {
		t.Fatalf("got %d errors, want 5: %v", len(errs), errs)
	}
}

// fakeReceiptWriter records what it was asked to write, so
// proveOneRESTRequest can be driven end to end with no Postgres.
// firingHistory/firingHistoryErr are canned ReadFiringHistory answers a
// test can set; firingHistoryCalls records every call it received, so a
// test can assert whether it was called at all, and with what.
type fakeReceiptWriter struct {
	receipts []goapiproof.RESTReceipt

	firingHistory      []goapiproof.FiringHistory
	firingHistoryErr   error
	firingHistoryCalls []fakeFiringHistoryCall
}

type fakeFiringHistoryCall struct {
	method, path, requestIdentity string
	tickets                       []string
}

func (w *fakeReceiptWriter) WriteReceipt(_ context.Context, receipt goapiproof.RESTReceipt) (uuid.UUID, error) {
	w.receipts = append(w.receipts, receipt)
	return uuid.New(), nil
}

func (w *fakeReceiptWriter) ReadFiringHistory(_ context.Context, method, path, requestIdentity string, tickets []string) ([]goapiproof.FiringHistory, error) {
	w.firingHistoryCalls = append(w.firingHistoryCalls, fakeFiringHistoryCall{method, path, requestIdentity, tickets})
	if w.firingHistoryErr != nil {
		return nil, w.firingHistoryErr
	}
	return w.firingHistory, nil
}

func TestProveOneRESTRequest_MatchWritesAReceipt(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"teams":["a","b"]}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"teams":["a","b"]}`))
	}))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/filters/options", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if !out.Admitted || out.TerminalState != goapiproof.TerminalStateMatch {
		t.Fatalf("out = %+v, want an admitted match", out)
	}
	if len(writer.receipts) != 1 {
		t.Fatalf("wrote %d receipts, want 1", len(writer.receipts))
	}
	if writer.receipts[0].Method != spec.Method || writer.receipts[0].Path != spec.Path {
		t.Fatalf("receipt Method/Path = %q/%q, want %q/%q", writer.receipts[0].Method, writer.receipts[0].Path, spec.Method, spec.Path)
	}
	if writer.receipts[0].CandidateBuild != build {
		t.Fatalf("receipt CandidateBuild = %q, want %q", writer.receipts[0].CandidateBuild, build)
	}
	if writer.receipts[0].BuildBinding != goapiproof.EdgeBuildPresent {
		t.Fatalf("receipt BuildBinding = %q, want %q", writer.receipts[0].BuildBinding, goapiproof.EdgeBuildPresent)
	}
	if writer.receipts[0].DeclaredDefects == nil || len(writer.receipts[0].DeclaredDefects) != 0 {
		t.Fatalf("receipt DeclaredDefects = %#v, want a non-nil EMPTY slice -- this request declares no BaselineDefects, and that must write a KNOWN empty array, not NULL", writer.receipts[0].DeclaredDefects)
	}
	if len(writer.firingHistoryCalls) != 0 {
		t.Fatalf("firingHistoryCalls = %v, want none -- this request declares no BaselineDefects", writer.firingHistoryCalls)
	}
	if out.DeclarationFiring != nil {
		t.Fatalf("out.DeclarationFiring = %v, want nil", out.DeclarationFiring)
	}
}

// TestProveOneRESTRequest_DeclaredDefectAttachesFiringHistoryFromTheReceiptTable
// proves a request that DOES declare a BaselineDefect reads that
// ticket's own firing history back, through the receiptWriter interface,
// AFTER its own receipt is written -- with the exact (method, path,
// request identity, tickets) proveOneRESTRequest computed for the
// receipt itself, so a history read can never drift from the receipt it
// is reporting on.
func TestProveOneRESTRequest_DeclaredDefectAttachesFiringHistoryFromTheReceiptTable(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"teams":["a","b"]}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"teams":["a","b"]}`))
	}))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{
		Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: goapiproof.RESTBodyModeJSON,
		Parity: goapiproof.Options{
			BaselineDefects: []goapiproof.BaselineDefect{
				{Ticket: "ABC-123", Reason: "constructed for this test", Paths: []string{"data.teams"}},
			},
		},
	}
	wantHistory := []goapiproof.FiringHistory{
		{Ticket: "ABC-123", RunsLive: 5, RunsFired: 0, LastFiredBuild: "", NeverFired: true},
	}
	writer := &fakeReceiptWriter{firingHistory: wantHistory}

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/filters/options", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if len(writer.firingHistoryCalls) != 1 {
		t.Fatalf("firingHistoryCalls = %v, want exactly 1", writer.firingHistoryCalls)
	}
	call := writer.firingHistoryCalls[0]
	if call.method != spec.Method || call.path != spec.Path {
		t.Fatalf("firing history call method/path = %q/%q, want %q/%q", call.method, call.path, spec.Method, spec.Path)
	}
	if len(call.tickets) != 1 || call.tickets[0] != "ABC-123" {
		t.Fatalf("firing history call tickets = %v, want [ABC-123]", call.tickets)
	}
	wantIdentity, err := goapiproof.RequestIdentity(f.org, goapiproof.AuthContext{}, restRequestVariables(spec.Method, request.Query, request.Body))
	if err != nil {
		t.Fatalf("RequestIdentity: %v", err)
	}
	if call.requestIdentity != wantIdentity {
		t.Fatalf("firing history call requestIdentity = %q, want %q (the SAME identity written onto the receipt)", call.requestIdentity, wantIdentity)
	}
	if len(out.DeclarationFiring) != 1 || out.DeclarationFiring[0] != wantHistory[0] {
		t.Fatalf("out.DeclarationFiring = %+v, want %+v", out.DeclarationFiring, wantHistory)
	}
	if len(writer.receipts) != 1 || len(writer.receipts[0].DeclaredDefects) != 1 || writer.receipts[0].DeclaredDefects[0] != "ABC-123" {
		t.Fatalf("receipt DeclaredDefects = %#v, want [ABC-123] -- the written receipt must carry what this request actually declares", writer.receipts[0].DeclaredDefects)
	}
}

// TestProveOneRESTRequest_StatusOnlyRequestNeverReadsFiringHistory proves
// a StatusOnly request -- whose body is never decoded or compared, so
// Compare never runs and no declared defect could possibly have fired --
// never reads firing history even if its Parity happens to carry a
// BaselineDefect: no corpus entry does this today, but a request that
// was never actually checked must never be read back as though it was.
func TestProveOneRESTRequest_StatusOnlyRequestNeverReadsFiringHistory(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{
		Name: "status_only", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: goapiproof.RESTBodyModeStatusOnly,
		Parity: goapiproof.Options{
			BaselineDefects: []goapiproof.BaselineDefect{
				{Ticket: "ABC-123", Reason: "constructed for this test", Paths: []string{"data.teams"}},
			},
		},
	}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/filters/options", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if len(writer.firingHistoryCalls) != 0 {
		t.Fatalf("firingHistoryCalls = %v, want none -- a StatusOnly request never runs Compare", writer.firingHistoryCalls)
	}
	if out.DeclarationFiring != nil {
		t.Fatalf("out.DeclarationFiring = %v, want nil", out.DeclarationFiring)
	}
}

// TestProveOneRESTRequest_FiringHistoryReadErrorFailsTheRequest proves a
// ReadFiringHistory failure surfaces as an error from proveOneRESTRequest
// exactly like a WriteReceipt failure already does above it -- a broken
// read is a tool failure, never silently dropped evidence.
func TestProveOneRESTRequest_FiringHistoryReadErrorFailsTheRequest(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"teams":["a","b"]}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"teams":["a","b"]}`))
	}))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{
		Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: goapiproof.RESTBodyModeJSON,
		Parity: goapiproof.Options{
			BaselineDefects: []goapiproof.BaselineDefect{
				{Ticket: "ABC-123", Reason: "constructed for this test", Paths: []string{"data.teams"}},
			},
		},
	}
	writer := &fakeReceiptWriter{firingHistoryErr: fmt.Errorf("constructed read failure")}

	if _, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/filters/options", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil); err == nil {
		t.Fatal("want an error when ReadFiringHistory fails")
	}
}

// readArtifactFile reads back a goapiproof.ArtifactStore ref exactly the
// way a human reviewer would: strip the "file://" scheme Put returns and
// read what is at the path, so a test that only holds the ref string can
// still assert on the bytes actually written -- the same evidence a
// reader needs to recover after this process exits.
func readArtifactFile(t *testing.T, ref string) []byte {
	t.Helper()
	if ref == "" {
		t.Fatal("artifact ref is empty")
	}
	const scheme = "file://"
	if !strings.HasPrefix(ref, scheme) {
		t.Fatalf("artifact ref %q does not carry the file:// scheme", ref)
	}
	body, err := os.ReadFile(strings.TrimPrefix(ref, scheme))
	if err != nil {
		t.Fatalf("read artifact %q: %v", ref, err)
	}
	return body
}

// TestProveOneRESTRequest_PersistsLegBodiesAndFindingsToTheArtifactDir
// drives a real mismatch through proveOneRESTRequest with a real
// (t.TempDir()-backed) ArtifactStore and reads every ref back from disk:
// both legs' raw bodies byte-for-byte, and the finding list as the same
// shape Compare returned -- so a receipt's own refs are provably not just
// present but READABLE evidence, not a name pointing at nothing.
func TestProveOneRESTRequest_PersistsLegBodiesAndFindingsToTheArtifactDir(t *testing.T) {
	const build = "abc123def456"
	const candidateBody = `{"teams":["a","b"]}`
	const baselineBody = `{"teams":["a","c"]}`
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(candidateBody))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(baselineBody))
	}))
	defer baseline.Close()

	artifacts, err := goapiproof.NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "why this ran"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/filters/options", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, artifacts, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if out.TerminalState != goapiproof.TerminalStateMismatch {
		t.Fatalf("out.TerminalState = %q, want mismatch (teams[1] differs)", out.TerminalState)
	}

	// The outcome's own refs (the JSON report a caller reads even for a
	// refused or dry-run request, per FindingsRef's own doc comment).
	if got := string(readArtifactFile(t, out.CandidateResponseRef)); got != candidateBody {
		t.Fatalf("candidate artifact body = %q, want %q", got, candidateBody)
	}
	if got := string(readArtifactFile(t, out.BaselineResponseRef)); got != baselineBody {
		t.Fatalf("baseline artifact body = %q, want %q", got, baselineBody)
	}
	var findings []goapiproof.Finding
	if err := json.Unmarshal(readArtifactFile(t, out.FindingsRef), &findings); err != nil {
		t.Fatalf("decode findings artifact: %v", err)
	}
	if len(findings) == 0 {
		t.Fatal("findings artifact decoded to zero findings, want at least the teams[1] mismatch")
	}

	// The written RECEIPT: same two body refs on their own dedicated
	// columns, and the findings ref folded into review_evidence's own
	// JSON envelope (go_api_rest_proof_run carries no findings_ref
	// column -- see encodeRESTReviewEvidence's own doc comment).
	if len(writer.receipts) != 1 {
		t.Fatalf("wrote %d receipts, want 1", len(writer.receipts))
	}
	receipt := writer.receipts[0]
	if receipt.CandidateResponseRef != out.CandidateResponseRef {
		t.Fatalf("receipt.CandidateResponseRef = %q, want %q", receipt.CandidateResponseRef, out.CandidateResponseRef)
	}
	if receipt.BaselineResponseRef != out.BaselineResponseRef {
		t.Fatalf("receipt.BaselineResponseRef = %q, want %q", receipt.BaselineResponseRef, out.BaselineResponseRef)
	}
	var evidence restReviewEvidence
	if err := json.Unmarshal([]byte(receipt.ReviewEvidence), &evidence); err != nil {
		t.Fatalf("decode receipt.ReviewEvidence as JSON: %v (%q)", err, receipt.ReviewEvidence)
	}
	if evidence.Operator != f.reviewEvidence {
		t.Fatalf("evidence.Operator = %q, want %q (the operator's own -review-evidence text, unmodified)", evidence.Operator, f.reviewEvidence)
	}
	if evidence.FindingsRef != out.FindingsRef {
		t.Fatalf("evidence.FindingsRef = %q, want %q", evidence.FindingsRef, out.FindingsRef)
	}
}

// TestProveOneRESTRequest_RefusedRequestStillPersistsLegBodies pins that
// artifact storage happens BEFORE admission runs: a refused request is
// exactly the case that most needs its bodies on disk, since a reader
// cannot otherwise see what either plane actually answered with.
func TestProveOneRESTRequest_RefusedRequestStillPersistsLegBodies(t *testing.T) {
	const candidateBody = "unavailable"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(candidateBody))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer baseline.Close()

	artifacts, err := goapiproof.NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "op", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), "abc123", goapiproof.AuthContext{}, time.Now().UTC(), nil, artifacts, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if out.Admitted {
		t.Fatalf("out = %+v, want a refusal", out)
	}
	if got := string(readArtifactFile(t, out.CandidateResponseRef)); got != candidateBody {
		t.Fatalf("candidate artifact body = %q, want %q -- a refused request's own bodies must still be on disk", got, candidateBody)
	}
}

func TestProveOneRESTRequest_DryRunWritesNoReceipt(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "op", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, true, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if out.ReceiptID != "" {
		t.Fatalf("ReceiptID = %q, want empty on a dry run", out.ReceiptID)
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote %d receipts, want 0 on a dry run", len(writer.receipts))
	}
}

func TestProveOneRESTRequest_RefusesOnUnexpectedStatus(t *testing.T) {
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "op", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), "abc123", goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if out.Admitted {
		t.Fatalf("out = %+v, want a refusal", out)
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote a receipt for a refused request: %v", writer.receipts)
	}
}

// TestProveOneRESTRequest_VacuousComparisonIsRefusedNotCrashed pins the
// fix for a real gap: a request whose BOTH legs decode to zero non-null
// leaves under a Parity that declares a BaselineDefect (e.g. a live
// window with genuinely zero PRs returned by either plane) makes
// goapiproof.Compare return a StructuralRefusal with NO TerminalState set
// (goapiproof/run.go's own GraphQL runner treats this identically --
// see refuse() there). Before this fix, proveOneRESTRequest read
// result.TerminalState unconditionally and tried to write a receipt with
// an empty (invalid) terminal_state -- either a real Postgres CHECK
// violation in production, or a silently wrong value if the guard were
// ever loosened. This proves the run is REFUSED, cleanly, with no error
// and no receipt, exactly like an admission-level refusal.
func TestProveOneRESTRequest_VacuousComparisonIsRefusedNotCrashed(t *testing.T) {
	const build = "abc123def456"
	empty := []byte(`{"items":[]}`)
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(empty)
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(empty)
	}))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/drilldown/prs"}
	request := goapiproof.RESTRequest{
		Name: "default_window", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: goapiproof.RESTBodyModeJSON,
		Parity: goapiproof.Options{BaselineDefects: []goapiproof.BaselineDefect{{
			Ticket: "CHAOS-0001", Reason: "r", Paths: []string{"data.items.created_at"},
			Intermittent: true, IntermittentReason: "present only while items are returned",
		}}},
	}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/drilldown/prs", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v -- a vacuous comparison must be a clean refusal, never a tool error", err)
	}
	if out.Admitted {
		t.Fatalf("out = %+v, want a refusal (a vacuous comparison is not a match)", out)
	}
	if out.Refusal != goapiproof.RefusalVacuousEmptyLegs {
		t.Fatalf("out.Refusal = %q, want %q", out.Refusal, goapiproof.RefusalVacuousEmptyLegs)
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote a receipt for a structurally refused comparison: %v", writer.receipts)
	}
}

func TestDoREST_SendsQueryAndBodyAndReadsBuildHeader(t *testing.T) {
	var gotQuery, gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		var decoded map[string]any
		_ = json.NewDecoder(r.Body).Decode(&decoded)
		if v, ok := decoded["x"]; ok {
			gotBody = v.(string)
		}
		w.Header().Set("x-dev-health-build", "abc123")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	leg, err := doREST(context.Background(), http.DefaultClient, server.URL, http.MethodPost, "/p",
		url.Values{"q": {"1"}}, map[string]any{"x": "y"}, staticCredentialForTest(), 0)
	if err != nil {
		t.Fatalf("doREST: %v", err)
	}
	if leg.StatusCode != 200 || leg.Build != "abc123" {
		t.Fatalf("leg = %+v", leg)
	}
	if gotQuery != "q=1" {
		t.Fatalf("query = %q, want q=1", gotQuery)
	}
	if gotBody != "y" {
		t.Fatalf("body.x = %q, want y", gotBody)
	}
}

// TestProveEdgeCredentialOnCandidate_Admitted pins the whole reason this
// leg exists: sending the edge credential DIRECTLY to query-api (never to the
// baseline) is admitted when the candidate answers the declared status
// with a matching build header -- exactly the same admission bar the
// ordinary (envelope) candidate leg already clears.
func TestProveEdgeCredentialOnCandidate_Admitted(t *testing.T) {
	const build = "abc123def456"
	var gotAuth string
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer candidate.Close()

	f := flags{queryAPIURL: candidate.URL}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200}

	out, err := proveEdgeCredentialOnCandidate(context.Background(), http.DefaultClient, f,
		"REST:GET:/api/v1/filters/options", spec, request, staticCredentialForTest(), build, nil)
	if err != nil {
		t.Fatalf("proveEdgeCredentialOnCandidate: %v", err)
	}
	if !out.Admitted {
		t.Fatalf("out = %+v, want admitted", out)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("candidate saw Authorization = %q, want the edge credential's own value", gotAuth)
	}
}

// TestProveEdgeCredentialOnCandidate_PersistsBodyToArtifactDir proves this
// leg's response body lands in the SAME artifact store proveOneRESTRequest's
// own legs use, under CandidateResponseRef -- so a 401 here leaves the same
// kind of evidence behind a refusal on the ordinary candidate leg does.
func TestProveEdgeCredentialOnCandidate_PersistsBodyToArtifactDir(t *testing.T) {
	const build = "abc123def456"
	const body = `{"teams":["a","b"]}`
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer candidate.Close()

	artifacts, err := goapiproof.NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}

	f := flags{queryAPIURL: candidate.URL}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200}

	out, err := proveEdgeCredentialOnCandidate(context.Background(), http.DefaultClient, f,
		"op", spec, request, staticCredentialForTest(), build, artifacts)
	if err != nil {
		t.Fatalf("proveEdgeCredentialOnCandidate: %v", err)
	}
	if !out.Admitted {
		t.Fatalf("out = %+v, want admitted", out)
	}
	if got := string(readArtifactFile(t, out.CandidateResponseRef)); got != body {
		t.Fatalf("candidate (edge credential) artifact body = %q, want %q", got, body)
	}
}

// TestProveEdgeCredentialOnCandidate_RefusesOnUnexpectedStatus proves
// this is a REAL admission check, not a rubber stamp: a candidate that
// answers 401 for the edge credential is refused, with the SAME refusal
// vocabulary RESTAdmit's own status check uses.
func TestProveEdgeCredentialOnCandidate_RefusesOnUnexpectedStatus(t *testing.T) {
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", "abc123")
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer candidate.Close()

	f := flags{queryAPIURL: candidate.URL}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200}

	out, err := proveEdgeCredentialOnCandidate(context.Background(), http.DefaultClient, f,
		"op", spec, request, staticCredentialForTest(), "abc123", nil)
	if err != nil {
		t.Fatalf("proveEdgeCredentialOnCandidate: %v", err)
	}
	if out.Admitted {
		t.Fatalf("out = %+v, want a refusal", out)
	}
	if out.Refusal != goapiproof.RESTRefusalUnexpectedStatus {
		t.Fatalf("Refusal = %q, want %q", out.Refusal, goapiproof.RESTRefusalUnexpectedStatus)
	}
}

// TestProveEdgeCredentialOnCandidate_RefusesOnBuildMismatch proves the
// build-header binding is enforced on this leg too: an answer with no
// x-dev-health-build header (or the wrong one) is refused rather than
// silently admitted, matching RESTAdmit's own candidate build check.
func TestProveEdgeCredentialOnCandidate_RefusesOnBuildMismatch(t *testing.T) {
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer candidate.Close()

	f := flags{queryAPIURL: candidate.URL}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200}

	out, err := proveEdgeCredentialOnCandidate(context.Background(), http.DefaultClient, f,
		"op", spec, request, staticCredentialForTest(), "abc123", nil)
	if err != nil {
		t.Fatalf("proveEdgeCredentialOnCandidate: %v", err)
	}
	if out.Admitted {
		t.Fatalf("out = %+v, want a refusal for a missing build header", out)
	}
	if out.Refusal != goapiproof.RESTRefusalBuildUnbound {
		t.Fatalf("Refusal = %q, want %q", out.Refusal, goapiproof.RESTRefusalBuildUnbound)
	}
}

func staticCredentialForTest() *goapiproof.Credential {
	return goapiproof.StaticCredential("Authorization", "test", "Bearer test-token")
}

// TestCorpusCoversExactlyWhatQueryAPIMounts drives AssertRESTPathCoverage
// against the REAL cmd/query-api source tree (not a fixture) -- the same
// discipline restendpoints_test.go's own tests apply to
// LoadQueryAPIMuxRoutes. A corpus entry for a route this binary no longer
// mounts, or a mounted route with no corpus entry, fails here before it
// can fail silently in a live run.
func TestCorpusCoversExactlyWhatQueryAPIMounts(t *testing.T) {
	mounted, err := migrationmatrix.LoadQueryAPIMuxRoutes("../query-api")
	if err != nil {
		t.Fatalf("LoadQueryAPIMuxRoutes: %v", err)
	}
	paths := make([]string, 0, len(mounted))
	for _, route := range mounted {
		paths = append(paths, route.Path)
	}
	if err := goapiproof.AssertRESTPathCoverage(paths); err != nil {
		t.Fatal(err)
	}
}

// TestMountedRESTPathsMatchesTheRealQueryAPIMux is what pins
// goapiproof.MountedRESTPaths -- the checked-in snapshot run() uses by
// default (no -query-api-src) -- against reality: it runs the SAME live
// parse TestCorpusCoversExactlyWhatQueryAPIMounts uses, against the REAL
// cmd/query-api source tree, and fails with the exact diff the moment a
// route is added, removed or renamed without that checked-in list being
// updated to match. This is the test MountedRESTPaths' own doc comment
// tells a developer to run and read before hand-editing the list.
func TestMountedRESTPathsMatchesTheRealQueryAPIMux(t *testing.T) {
	mounted, err := migrationmatrix.LoadQueryAPIMuxRoutes("../query-api")
	if err != nil {
		t.Fatalf("LoadQueryAPIMuxRoutes: %v", err)
	}
	live := make([]string, 0, len(mounted))
	for _, route := range mounted {
		live = append(live, route.Path)
	}
	sort.Strings(live)
	checkedIn := goapiproof.MountedRESTPaths()
	if !slices.Equal(live, checkedIn) {
		t.Fatalf("goapiproof.MountedRESTPaths (internal/goapiproof/restmounted.go) has drifted from the real cmd/query-api mux:\n  live:       %v\n  checked-in: %v\nUpdate the mountedRESTPaths literal to match live.", live, checkedIn)
	}
}

func TestProveOneRESTRequest_PublicNoAuthSendsNoAuthorizationHeader(t *testing.T) {
	const build = "abc123def456"
	var gotCandidateAuth, gotBaselineAuth string
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCandidateAuth = r.Header.Get("Authorization")
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBaselineAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/meta", PublicNoAuth: true}
	request := goapiproof.RESTRequest{Name: "meta", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}

	// A credential that would FAIL the test if ever applied -- Apply on a
	// StaticCredential built from an empty value returns an error, so
	// reaching it at all (rather than being skipped via PublicNoAuth)
	// would surface as a returned error from proveOneRESTRequest, not a
	// silently-sent header.
	poisonedCredential := goapiproof.StaticCredential("Authorization", "poisoned", "")

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/meta", spec, request,
		poisonedCredential, poisonedCredential, build, goapiproof.AuthContext{}, time.Now().UTC(), nil, nil, true, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if !out.Admitted {
		t.Fatalf("out = %+v, want admitted", out)
	}
	if gotCandidateAuth != "" || gotBaselineAuth != "" {
		t.Fatalf("Authorization header sent on a PublicNoAuth route: candidate=%q baseline=%q", gotCandidateAuth, gotBaselineAuth)
	}
}

// TestBuildInfoRead_RejectsUnauthenticatedAcceptsTheMintedBearer proves
// run()'s own /buildinfo call site (goapiproof.FetchBuildIdentity, called
// with candidateCredential -- see run()'s own doc comment on that call)
// against a fake buildinfo route that requires a bearer: a request
// carrying no credential, or the wrong one, is rejected exactly like a
// prod query-api's own bearer-envelope verifier would reject an
// unauthenticated or mismatched request, and the SAME credential every
// corpus request's own candidate leg uses (staticCredentialForTest, the
// same helper TestProveOneRESTRequest_MatchWritesAReceipt and its
// siblings above already use in place of a real minting helper) is
// accepted.
func TestBuildInfoRead_RejectsUnauthenticatedAcceptsTheMintedBearer(t *testing.T) {
	const wantCommit = "abc123def456abc123def456abc123def456ab"
	const wantAuth = "Bearer test-token" // staticCredentialForTest's own value

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != wantAuth {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"commit":"` + wantCommit + `","modified":false}`))
	}))
	defer server.Close()

	// No Authorization header at all -- an expired or never-configured
	// credential.
	if _, err := goapiproof.FetchBuildIdentity(context.Background(), http.DefaultClient, server.URL, nil); err == nil {
		t.Fatal("want an error when no credential is applied")
	}

	// The wrong bearer -- rejected the same way a real bearer-envelope
	// verifier would reject a mismatched or stale one.
	wrongCredential := goapiproof.StaticCredential("Authorization", "candidate bearer", "Bearer not-the-right-token")
	_, err := goapiproof.FetchBuildIdentity(context.Background(), http.DefaultClient, server.URL, wrongCredential)
	if err == nil {
		t.Fatal("want an error when the credential does not match")
	}
	// No token reaches any output: the error names the credential's KIND
	// (a safe label), never its value.
	if strings.Contains(err.Error(), "not-the-right-token") {
		t.Fatalf("error leaked the credential value: %v", err)
	}

	// The SAME shape of credential run() actually applies to /buildinfo
	// -- candidateCredential, the same one every corpus request's own
	// candidate leg uses.
	got, err := goapiproof.FetchBuildIdentity(context.Background(), http.DefaultClient, server.URL, staticCredentialForTest())
	if err != nil {
		t.Fatalf("FetchBuildIdentity: %v", err)
	}
	if got != wantCommit {
		t.Fatalf("got %q, want %q", got, wantCommit)
	}
}

// TestIDBinding_EndToEnd drives the full producer -> consumer pipeline
// run()'s own loop implements: a fake Python ("baseline") /api/v1/people
// returns a person list whose FIRST element's person_id is empty (an
// unusable id, per this ticket's own "the first person_id" ruling --
// ExtractRESTID must skip it), so the id actually bound is the SECOND
// element's. That extracted id is then resolved into a consumer
// request's PathParam binding (mirroring GET /api/v1/people/{person_id}/
// summary's own corpus entry), and BOTH legs must receive the identical
// resolved path -- "the same id is used on both legs", this ticket's own
// ruling.
func TestIDBinding_EndToEnd(t *testing.T) {
	const build = "abc123def456"
	const wantID = "p-777"

	personListBody := `[{"person_id":"","display_name":"unusable"},{"person_id":"` + wantID + `","display_name":"usable"}]`

	var candidatePaths, baselinePaths []string
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		candidatePaths = append(candidatePaths, r.URL.Path)
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(personListBody))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		baselinePaths = append(baselinePaths, r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(personListBody))
	}))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	writer := &fakeReceiptWriter{}
	produced := map[string]string{}

	// 1. The PRODUCER request: GET /api/v1/people, Produces person_id
	// from the BASELINE leg's own first NON-EMPTY element.
	producerSpec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/people"}
	producerRequest := goapiproof.RESTRequest{
		Name: "query_string_search", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: goapiproof.RESTBodyModeJSON,
		Produces: []goapiproof.RESTIDProducer{{Name: "person_id", IDField: "person_id"}},
	}
	producerOut, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/people", producerSpec, producerRequest,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("producer proveOneRESTRequest: %v", err)
	}
	if !producerOut.Admitted {
		t.Fatalf("producer request refused: %s -- %s", producerOut.Refusal, producerOut.Detail)
	}

	// run()'s own loop merges out.producedIDs into `produced` -- the same
	// step, done by hand here since this test drives proveOneRESTRequest
	// directly rather than the whole corpus.
	for name, id := range producerOut.producedIDs {
		produced[name] = id
	}
	if produced["person_id"] != wantID {
		t.Fatalf("produced[person_id] = %q, want %q (the first NON-EMPTY person_id, not the first element)", produced["person_id"], wantID)
	}

	// 2. The CONSUMER request: GET /api/v1/people/{person_id}/summary,
	// PathParam-bound to the id the producer just yielded.
	consumerSpec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/people/{person_id}/summary"}
	consumerRequest := goapiproof.RESTRequest{
		Name: "summary_default", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode:   goapiproof.RESTBodyModeJSON,
		IDBindings: []goapiproof.RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
	}

	resolvedPath, resolvedQuery, unresolved := goapiproof.ResolveRESTIDBindings(consumerSpec.Path, consumerRequest, produced)
	if len(unresolved) != 0 {
		t.Fatalf("unresolved = %v, want none", unresolved)
	}
	resolvedSpec := consumerSpec
	resolvedSpec.Path = resolvedPath
	resolvedRequest := consumerRequest
	resolvedRequest.Query = resolvedQuery

	consumerOut, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/people/{person_id}/summary", resolvedSpec, resolvedRequest,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false,
		map[string]string{"person_id": produced["person_id"]})
	if err != nil {
		t.Fatalf("consumer proveOneRESTRequest: %v", err)
	}
	if !consumerOut.Admitted {
		t.Fatalf("consumer request refused: %s -- %s", consumerOut.Refusal, consumerOut.Detail)
	}
	if consumerOut.BoundIDs["person_id"] != wantID {
		t.Fatalf("consumer outcome BoundIDs[person_id] = %q, want %q -- must be recorded on the outcome (JSON report)", consumerOut.BoundIDs["person_id"], wantID)
	}
	if len(writer.receipts) == 0 || writer.receipts[len(writer.receipts)-1].BoundIDs["person_id"] != wantID {
		t.Fatalf("consumer receipt's own BoundIDs[person_id] was not %q", wantID)
	}

	wantSuffix := "/api/v1/people/" + wantID + "/summary"
	gotCandidatePath := candidatePaths[len(candidatePaths)-1]
	gotBaselinePath := baselinePaths[len(baselinePaths)-1]
	if gotCandidatePath != wantSuffix {
		t.Fatalf("candidate leg path = %q, want %q", gotCandidatePath, wantSuffix)
	}
	if gotBaselinePath != wantSuffix {
		t.Fatalf("baseline leg path = %q, want %q", gotBaselinePath, wantSuffix)
	}
	if gotCandidatePath != gotBaselinePath {
		t.Fatalf("candidate and baseline legs used different resolved paths: %q vs %q -- the same id must be used on both legs", gotCandidatePath, gotBaselinePath)
	}
}

// hijackAndCloseServer returns an httptest server whose handler accepts
// the connection and closes it immediately, without writing any HTTP
// response at all -- the "drops the connection" transport failure, as
// opposed to a slow response the client's own timeout catches. The
// client's own Do() call surfaces this as an io/EOF-shaped error, never
// context.DeadlineExceeded, so it classifies to a DIFFERENT
// TransportFailure class than a stall does -- see
// TestDoREST_ConnectionDropClassifiesAsTransportError.
func hijackAndCloseServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("response writer does not support hijacking")
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Fatalf("hijack: %v", err)
		}
		_ = conn.Close()
	}))
	return server
}

// stallingServer returns an httptest server whose handler blocks until
// the request's own context is done, then writes nothing more (the
// client has already given up by then) -- simulating a leg that is
// genuinely still working, just past this run's own budget for it,
// exactly the shape the production abort line (a baseline GET against
// the Python api) reproduced: a slow response, not a dead one.
func stallingServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
}

// TestDoREST_TimeoutClassifiesAsTransportTimeout pins doREST's own
// contract for a leg that never answers within its budget: the error is
// a goapiproof.TransportFailure (not a bare wrapped string a caller would
// have to pattern-match), classed TransportTimeout specifically -- the
// class legTransportOutcome reads to choose *TimedOut over
// *TransportError.
func TestDoREST_TimeoutClassifiesAsTransportTimeout(t *testing.T) {
	server := stallingServer(t)
	defer server.Close()

	_, err := doREST(context.Background(), http.DefaultClient, server.URL, http.MethodGet, "/p", nil, nil, nil, 30*time.Millisecond)
	if err == nil {
		t.Fatal("doREST: want an error from a leg that never answers, got nil")
	}
	var failure goapiproof.TransportFailure
	if !errors.As(err, &failure) {
		t.Fatalf("doREST error = %v (%T), want a goapiproof.TransportFailure", err, err)
	}
	if failure.Class != goapiproof.TransportTimeout {
		t.Fatalf("failure.Class = %q, want %q", failure.Class, goapiproof.TransportTimeout)
	}
}

// TestDoREST_ConnectionDropClassifiesAsTransportError pins the sibling
// case: a connection that closes with NO response is a transport
// failure too, but never classified TransportTimeout (this call's own
// timeout is generous -- 5s -- so a false "timeout" classification here
// would mean the class came from the wrong signal).
func TestDoREST_ConnectionDropClassifiesAsTransportError(t *testing.T) {
	server := hijackAndCloseServer(t)
	defer server.Close()

	_, err := doREST(context.Background(), http.DefaultClient, server.URL, http.MethodGet, "/p", nil, nil, nil, 5*time.Second)
	if err == nil {
		t.Fatal("doREST: want an error from a dropped connection, got nil")
	}
	var failure goapiproof.TransportFailure
	if !errors.As(err, &failure) {
		t.Fatalf("doREST error = %v (%T), want a goapiproof.TransportFailure", err, err)
	}
	if failure.Class == goapiproof.TransportTimeout {
		t.Fatalf("failure.Class = %q, a dropped connection under a 5s budget must never classify as a timeout", failure.Class)
	}
}

// TestProveOneRESTRequest_CandidateLegTimeoutIsRefusedPerCase is this
// ticket's central proof: a candidate leg that stalls past its budget
// must become a named, per-case refusal -- Admitted=false,
// Refusal=RESTRefusalCandidateLegTimedOut -- and NOT a Go error. Before
// the fix, doREST's own error propagated straight out of
// proveOneRESTRequest, which is exactly what killed the whole run and
// left the report empty; asserting err == nil here is the fix, not a
// convenience -- run()'s own loop only stops when this returns a non-nil
// error, so a nil error here is what "later cases still run" reduces to
// at this call's own boundary.
func TestProveOneRESTRequest_CandidateLegTimeoutIsRefusedPerCase(t *testing.T) {
	candidate := stallingServer(t)
	defer candidate.Close()
	baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 30 * time.Millisecond}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/home"}
	request := goapiproof.RESTRequest{Name: "home", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/home", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), "build123", goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v -- a leg timeout must be a refusal, never a tool error", err)
	}
	if out.Admitted {
		t.Fatalf("out = %+v, want a refusal (a leg that never answered is not a match)", out)
	}
	if out.Refusal != goapiproof.RESTRefusalCandidateLegTimedOut {
		t.Fatalf("out.Refusal = %q, want %q", out.Refusal, goapiproof.RESTRefusalCandidateLegTimedOut)
	}
	if out.Detail == "" {
		t.Fatal("out.Detail is empty, want the transport failure's own detail")
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote a receipt for a leg that never answered: %v", writer.receipts)
	}
}

// TestProveOneRESTRequest_BaselineLegConnectionDropIsRefusedPerCase is
// the sibling of the timeout test, for the OTHER leg and the OTHER
// failure class: a baseline (Python) connection that drops mid-request
// is refused by name too, distinctly from the candidate-leg reasons.
func TestProveOneRESTRequest_BaselineLegConnectionDropIsRefusedPerCase(t *testing.T) {
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", "build123")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer candidate.Close()
	baseline := hijackAndCloseServer(t)
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/home"}
	request := goapiproof.RESTRequest{Name: "home", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), http.DefaultClient, f, "REST:GET:/api/v1/home", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), "build123", goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v -- a dropped baseline leg must be a refusal, never a tool error", err)
	}
	if out.Admitted {
		t.Fatalf("out = %+v, want a refusal", out)
	}
	if out.Refusal != goapiproof.RESTRefusalBaselineLegTransportError {
		t.Fatalf("out.Refusal = %q, want %q", out.Refusal, goapiproof.RESTRefusalBaselineLegTransportError)
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote a receipt for a leg that never answered: %v", writer.receipts)
	}
}

// TestProveOneRESTRequest_LaterRequestStillRunsAfterALegFailure proves
// the loop-level claim directly: a leg failure on one request does not
// poison the next one. run()'s own loop calls proveOneRESTRequest once
// per planned request with independent state; this reproduces that at
// the unit level -- one call that hits a leg timeout, then a second,
// wholly independent call that matches cleanly -- and requires BOTH to
// come back exactly as they would standing alone.
func TestProveOneRESTRequest_LaterRequestStillRunsAfterALegFailure(t *testing.T) {
	stalledCandidate := stallingServer(t)
	defer stalledCandidate.Close()
	firstBaseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer firstBaseline.Close()

	okCandidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", "build123")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"a":1}`))
	}))
	defer okCandidate.Close()
	okBaseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"a":1}`))
	}))
	defer okBaseline.Close()

	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/home"}
	request := goapiproof.RESTRequest{Name: "home", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	firstFlags := flags{queryAPIURL: stalledCandidate.URL, pythonAPIURL: firstBaseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 30 * time.Millisecond}
	first, err := proveOneRESTRequest(context.Background(), http.DefaultClient, firstFlags, "REST:GET:/api/v1/home", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), "build123", goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("first proveOneRESTRequest: %v, want a refusal not an error", err)
	}
	if first.Admitted || first.Refusal != goapiproof.RESTRefusalCandidateLegTimedOut {
		t.Fatalf("first = %+v, want an unadmitted candidate-leg-timeout refusal", first)
	}

	secondFlags := flags{queryAPIURL: okCandidate.URL, pythonAPIURL: okBaseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	second, err := proveOneRESTRequest(context.Background(), http.DefaultClient, secondFlags, "REST:GET:/api/v1/home", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), "build123", goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("second proveOneRESTRequest: %v, want a clean match", err)
	}
	if !second.Admitted || second.TerminalState != goapiproof.TerminalStateMatch {
		t.Fatalf("second = %+v, want an admitted match -- a leg timeout on the FIRST request must not affect the SECOND, independent one", second)
	}
	if len(writer.receipts) != 1 {
		t.Fatalf("wrote %d receipts, want exactly 1 (only the second, admitted request)", len(writer.receipts))
	}
}

// TestProveEdgeCredentialOnCandidate_LegTimeoutIsRefusedPerCase proves
// item 1's "either credential" half: the edge-credential-on-candidate
// leg is still a CANDIDATE leg (same query-api URL, different
// credential), so a stall there gets the identical candidate-leg-timeout
// reason, never a fatal error.
func TestProveEdgeCredentialOnCandidate_LegTimeoutIsRefusedPerCase(t *testing.T) {
	candidate := stallingServer(t)
	defer candidate.Close()

	f := flags{queryAPIURL: candidate.URL, timeout: 30 * time.Millisecond}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/home"}
	request := goapiproof.RESTRequest{Name: "home", WantCandidateStatus: 200}

	out, err := proveEdgeCredentialOnCandidate(context.Background(), http.DefaultClient, f,
		"REST:GET:/api/v1/home", spec, request, staticCredentialForTest(), "build123", nil)
	if err != nil {
		t.Fatalf("proveEdgeCredentialOnCandidate: %v, want a refusal not an error", err)
	}
	if out.Admitted || out.Refusal != goapiproof.RESTRefusalCandidateLegTimedOut {
		t.Fatalf("out = %+v, want an unadmitted candidate-leg-timeout refusal", out)
	}
}

// TestResolveRESTTimeout pins the override rule: a corpus entry's own
// Timeout wins whenever it is set, the run's -timeout default otherwise
// -- never the other way, and never a mix of the two.
func TestResolveRESTTimeout(t *testing.T) {
	cases := []struct {
		name       string
		perRequest time.Duration
		runDefault time.Duration
		want       time.Duration
	}{
		{"unset entry uses the run default", 0, 5 * time.Second, 5 * time.Second},
		{"a declared entry overrides the run default", 90 * time.Second, 5 * time.Second, 90 * time.Second},
		{"both zero stays zero (no deadline)", 0, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := resolveRESTTimeout(c.perRequest, c.runDefault)
			if got != c.want {
				t.Fatalf("resolveRESTTimeout(%v, %v) = %v, want %v", c.perRequest, c.runDefault, got, c.want)
			}
		})
	}
}

// TestLegFailuresIn_NamesOnlyTheFourTransportReasons proves the
// exit-code check's own boundary: a leg-transport refusal is named, an
// ORDINARY refusal (a business-as-usual outcome, e.g. an unresolved id
// binding) and an admitted match are both left out -- so a normal run
// full of ordinary refusals never trips the non-zero exit this ticket
// reserves for an instrument that did not complete cleanly.
func TestLegFailuresIn_NamesOnlyTheFourTransportReasons(t *testing.T) {
	outcomes := []outcome{
		{Operation: "REST:GET:/a", Request: "r1", Refusal: goapiproof.RESTRefusalCandidateLegTimedOut, Detail: "d1"},
		{Operation: "REST:GET:/b", Request: "r2", Refusal: goapiproof.RESTRefusalBaselineLegTransportError, Detail: "d2"},
		{Operation: "REST:GET:/c", Request: "r3", Refusal: goapiproof.RESTRefusalIDBindingUnresolved, Detail: "d3"},
		{Operation: "REST:GET:/d", Request: "r4", Admitted: true, TerminalState: goapiproof.TerminalStateMatch},
	}
	got := legFailuresIn(outcomes)
	if len(got) != 2 {
		t.Fatalf("legFailuresIn = %v, want exactly 2 entries", got)
	}
	for _, want := range []string{"REST:GET:/a/r1", "REST:GET:/b/r2"} {
		found := false
		for _, line := range got {
			if strings.HasPrefix(line, want+": ") {
				found = true
			}
		}
		if !found {
			t.Fatalf("legFailuresIn = %v, missing a line for %q", got, want)
		}
	}
}

// TestNotRunKeys_ListsUnattemptedPlannedRequests pins the partial-run
// bookkeeping: a request nobody attempted is named by its own key, and a
// request that WAS attempted but whose edge-credential-on-candidate
// sibling was not (the run stopped between the two) is named by that
// sibling's own key -- never silently dropped because the base request
// itself has an entry.
func TestNotRunKeys_ListsUnattemptedPlannedRequests(t *testing.T) {
	planned := []plannedRequest{
		{operation: "REST:GET:/a", request: goapiproof.RESTRequest{Name: "r1"}, spec: goapiproof.RESTEndpointSpec{PublicNoAuth: false}},
		{operation: "REST:GET:/a", request: goapiproof.RESTRequest{Name: "r2"}, spec: goapiproof.RESTEndpointSpec{PublicNoAuth: true}},
	}
	attempted := map[string]bool{
		"REST:GET:/a/r1": true, // base request ran; its edge-credential sibling did not
	}
	got := notRunKeys(planned, attempted)
	want := []string{"REST:GET:/a/r1 (edge-credential-on-candidate)", "REST:GET:/a/r2"}
	if !slices.Equal(got, want) {
		t.Fatalf("notRunKeys = %v, want %v", got, want)
	}
}

// TestWriteJSONReport_PartialRunMarksPartialAndListsNotRun proves the
// report file itself -- not just stdout -- says a run is partial and
// names what it never reached, in the shape a reader (or a later
// process) can parse back.
func TestWriteJSONReport_PartialRunMarksPartialAndListsNotRun(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/report.json"
	outcomes := []outcome{
		{Operation: "REST:GET:/a", Request: "r1", Admitted: true, TerminalState: goapiproof.TerminalStateMatch},
		{Operation: "REST:GET:/b", Request: "r2", Refusal: goapiproof.RESTRefusalCandidateLegTimedOut, Detail: "timed out"},
	}
	notRun := []string{"REST:GET:/c/r3"}

	if err := writeJSONReport(path, outcomes, notRun); err != nil {
		t.Fatalf("writeJSONReport: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var got jsonReport
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}
	if !got.Partial {
		t.Fatalf("report.Partial = false, want true (NotRun is non-empty)")
	}
	if !slices.Equal(got.NotRun, notRun) {
		t.Fatalf("report.NotRun = %v, want %v", got.NotRun, notRun)
	}
	if len(got.Outcomes) != 2 {
		t.Fatalf("report has %d outcomes, want 2 -- a partial report must still carry every outcome it DID measure", len(got.Outcomes))
	}
}

// TestWriteJSONReport_CompleteRunIsNotPartial is the sibling case: a run
// that reached the end of its plan writes Partial=false and no NotRun,
// so "partial" is never true by omission of the check, only by an
// actual gap.
func TestWriteJSONReport_CompleteRunIsNotPartial(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/report.json"
	outcomes := []outcome{{Operation: "REST:GET:/a", Request: "r1", Admitted: true, TerminalState: goapiproof.TerminalStateMatch}}

	if err := writeJSONReport(path, outcomes, nil); err != nil {
		t.Fatalf("writeJSONReport: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var got jsonReport
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}
	if got.Partial {
		t.Fatal("report.Partial = true, want false -- nothing was left unattempted")
	}
	if len(got.NotRun) != 0 {
		t.Fatalf("report.NotRun = %v, want empty", got.NotRun)
	}
}

// captureStdout redirects os.Stdout for the duration of fn and returns
// everything written to it -- runMeasurement prints its per-request
// lines and its own summary/partial lines with fmt.Println/fmt.Printf,
// straight to os.Stdout, so this is the only way a test can see them.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()

	fn()

	_ = w.Close()
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	return string(raw)
}

// genericRESTStubHandler answers every request with 200, the given
// build header (candidate only -- pass "" for the baseline stub, which
// never stamps one), and an empty JSON object body -- good enough
// admission for a StatusOnly request and a clean "vacuous_empty_legs"
// structural refusal for a JSON-body one, neither of which is a tool
// error. stall, when non-nil, names one exact (method, path, raw query)
// this handler blocks on until the request's own context is done,
// instead of answering at all -- the "leg that stalls past the timeout"
// this test drives through the REAL request loop, not a hand-built call.
type stalledRequest struct {
	method, path, rawQuery string
}

func genericRESTStubHandler(t *testing.T, build string, stall *stalledRequest) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if stall != nil && r.Method == stall.method && r.URL.Path == stall.path && r.URL.RawQuery == stall.rawQuery {
			<-r.Context().Done()
			return
		}
		if build != "" {
			w.Header().Set("x-dev-health-build", build)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}
}

// TestRunMeasurement_BaselineLegTimeoutIsNonFatalReportedAndLaterCaseRuns
// drives runMeasurement -- the exact loop/report/exit-code logic run()
// itself calls -- over the REAL, full REST corpus (goapiproof.
// RESTRunOrder), with one single baseline leg (GET /api/v1/home with no
// query -- the "home_default_org" entry) stalling past its budget,
// reproducing the shape of the production abort line this ticket closes
// (a baseline GET home request hitting the client timeout). It proves,
// through the real loop rather than a single hand-built call: the
// process exits non-zero, the summary line reaches stdout, the report
// file parses and is NOT partial (the loop reached the end of its plan),
// the stalled case carries the new named refusal, and a case that runs
// LATER in RESTRunOrder (work-units, the corpus's own last two entries)
// is present with its own real outcome.
func TestRunMeasurement_BaselineLegTimeoutIsNonFatalReportedAndLaterCaseRuns(t *testing.T) {
	const build = "build123"
	candidate := httptest.NewServer(genericRESTStubHandler(t, build, nil))
	defer candidate.Close()
	baseline := httptest.NewServer(genericRESTStubHandler(t, "", &stalledRequest{method: http.MethodGet, path: "/api/v1/home", rawQuery: ""}))
	defer baseline.Close()

	dir := t.TempDir()
	reportPath := dir + "/report.json"
	artifacts, err := goapiproof.NewArtifactStore(dir + "/artifacts")
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}
	f := flags{
		queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL,
		org: "org-1", recordedBy: "chris", reviewEvidence: "test",
		timeout: 50 * time.Millisecond, dryRun: true, reportPath: reportPath,
	}

	var runErr error
	stdout := captureStdout(t, func() {
		runErr = runMeasurement(context.Background(), http.DefaultClient, f,
			staticCredentialForTest(), staticCredentialForTest(), build, nil, artifacts)
	})

	if runErr == nil {
		t.Fatal("runMeasurement: want a non-nil error (non-zero exit) -- this run measured a leg that never answered")
	}
	if !strings.Contains(runErr.Error(), goapiproof.RESTRefusalBaselineLegTimedOut) {
		t.Fatalf("runMeasurement error = %v, want it to name %q", runErr, goapiproof.RESTRefusalBaselineLegTimedOut)
	}
	if !strings.Contains(stdout, "attempted=") || !strings.Contains(stdout, "admitted=") {
		t.Fatalf("stdout = %q, want the summary line (attempted=.../admitted=...)", stdout)
	}

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}
	if report.Partial {
		t.Fatalf("report.Partial = true, want false -- the loop reached the end of its plan despite the leg failure (not_run = %v)", report.NotRun)
	}

	var stalledOutcome, laterOutcome *outcome
	for i := range report.Outcomes {
		o := &report.Outcomes[i]
		if o.Operation == "REST:GET:/api/v1/home" && o.Request == "home_default_org" {
			stalledOutcome = o
		}
		if o.Operation == "REST:GET:/api/v1/work-units" {
			laterOutcome = o
		}
	}
	if stalledOutcome == nil {
		t.Fatalf("report has no outcome for REST:GET:/api/v1/home/home_default_org among %d outcomes", len(report.Outcomes))
	}
	if stalledOutcome.Admitted || stalledOutcome.Refusal != goapiproof.RESTRefusalBaselineLegTimedOut {
		t.Fatalf("stalled outcome = %+v, want an unadmitted %q refusal", stalledOutcome, goapiproof.RESTRefusalBaselineLegTimedOut)
	}
	if laterOutcome == nil {
		t.Fatalf("report has no outcome for REST:GET:/api/v1/work-units (runs AFTER home in RESTRunOrder) among %d outcomes -- the loop must not have continued past the stalled case", len(report.Outcomes))
	}
}

// TestRunMeasurement_NonTransportErrorMidRunStillWritesAPartialReport is
// this fix's OTHER claim: item 2 says the report must be written on ANY
// later error, not only a leg timeout. This makes the artifact directory
// unwritable AFTER it is created (proveOneRESTRequest's very first call
// stores a leg body to it, unconditionally, before admission even runs),
// so the FIRST request in RESTRunOrder fails with a real filesystem
// error -- the non-transport class this loop still treats as fatal (it
// stops the loop, same as before this ticket), but the report/summary
// write this ticket moved to run unconditionally must still happen, and
// must say the run is partial and name what never ran.
func TestRunMeasurement_NonTransportErrorMidRunStillWritesAPartialReport(t *testing.T) {
	const build = "build123"
	candidate := httptest.NewServer(genericRESTStubHandler(t, build, nil))
	defer candidate.Close()
	baseline := httptest.NewServer(genericRESTStubHandler(t, "", nil))
	defer baseline.Close()

	dir := t.TempDir()
	reportPath := dir + "/report.json"
	artifactDir := dir + "/artifacts"
	artifacts, err := goapiproof.NewArtifactStore(artifactDir)
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}
	// Revoke write access AFTER the directory exists -- Put's own
	// os.CreateTemp call then fails on the very first leg body this run
	// tries to store, a real (not simulated) filesystem error.
	if err := os.Chmod(artifactDir, 0o555); err != nil {
		t.Fatalf("chmod artifact dir read-only: %v", err)
	}
	defer func() { _ = os.Chmod(artifactDir, 0o750) }()

	f := flags{
		queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL,
		org: "org-1", recordedBy: "chris", reviewEvidence: "test",
		timeout: 5 * time.Second, dryRun: true, reportPath: reportPath,
	}

	var runErr error
	captureStdout(t, func() {
		runErr = runMeasurement(context.Background(), http.DefaultClient, f,
			staticCredentialForTest(), staticCredentialForTest(), build, nil, artifacts)
	})
	if runErr == nil {
		t.Fatal("runMeasurement: want a non-nil error from the unwritable artifact directory")
	}

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v -- the report must still be written when a NON-transport error stops the loop mid-run", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}
	if !report.Partial {
		t.Fatal("report.Partial = false, want true -- the run stopped before its plan finished")
	}
	if len(report.NotRun) == 0 {
		t.Fatal("report.NotRun is empty, want the requests this run never reached named by key")
	}
	found := false
	for _, key := range report.NotRun {
		if strings.HasPrefix(key, "REST:GET:/api/v1/work-units") {
			found = true
		}
	}
	if !found {
		t.Fatalf("report.NotRun = %v, want it to name a request from LATER in RESTRunOrder (e.g. work-units) that never ran", report.NotRun)
	}
}
