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

// TestParseFlags_AcceptsAWellFormedBind is -bind's positive counterpart to
// the three malformed cases below: NAME=VALUE lands in flags.binds under
// NAME, unaltered.
func TestParseFlags_AcceptsAWellFormedBind(t *testing.T) {
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
		"-bind", "deployment_entity_id=00000000-0000-0000-0000-000000000000:d-1",
	})
	defer restoreArgs()
	f, err := parseFlags()
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.binds["deployment_entity_id"] != "00000000-0000-0000-0000-000000000000:d-1" {
		t.Fatalf("f.binds[deployment_entity_id] = %q, want the value after \"=\"", f.binds["deployment_entity_id"])
	}
}

// TestParseFlags_RejectsMalformedBind pins -bind's own eager validation:
// a malformed value is rejected AT PARSE TIME (parseFlags returns an
// error before ever reaching the post-parse required-flag check, let
// alone runMeasurement's own request loop), for every one of the three
// ways NAME=VALUE can be malformed.
func TestParseFlags_RejectsMalformedBind(t *testing.T) {
	for _, tc := range []struct {
		name string
		bind string
	}{
		{"no equals sign", "deployment_entity_id"},
		{"empty name", "=some-value"},
		{"empty value", "deployment_entity_id="},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				"-bind", tc.bind,
			})
			defer restoreArgs()
			if _, err := parseFlags(); err == nil {
				t.Fatalf("parseFlags: want an error for -bind %q", tc.bind)
			}
		})
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

// TestResultVacuityErrors_NamesEveryKind covers resultVacuityErrors'
// SOFT fields (goapiproof.AcceptanceRefusal.Hard == false): a declaration
// that matched nothing ELSE in an otherwise honest, fully-executed
// comparison. OrderInsensitiveListRefusals is set here too, but is a
// HARD field -- its own comparison could have skipped a whole list (see
// AcceptanceRefusal.Hard's own doc comment) -- so it must NOT contribute
// to this count; the real call path never even reaches
// resultVacuityErrors with it set (proveOneRESTRequest refuses that
// request immediately, before this function ever runs), so leaving it in
// this Result also exercises resultVacuityErrors' own defensive filter
// standing alone.
func TestResultVacuityErrors_NamesEveryKind(t *testing.T) {
	result := goapiproof.Result{
		UnusedExclusions:             []string{"a"},
		UnusedTierB:                  []string{"b"},
		StaleBaselineDefects:         []string{"CHAOS-1"},
		UnusedOrderInsensitiveLists:  []string{"c"},
		OrderInsensitiveListRefusals: []string{"d"},
	}
	errs := resultVacuityErrors(result)
	if len(errs) != 4 {
		t.Fatalf("got %d errors, want 4: %v", len(errs), errs)
	}
}

// TestResultVacuityErrors_HardRefusalsAloneProduceNoVacuityError pins the
// same defensive filter with no soft field set at all: every field here
// is HARD (goapiproof.AcceptanceRefusal.Hard), so the result must be
// empty, not just short one entry.
func TestResultVacuityErrors_HardRefusalsAloneProduceNoVacuityError(t *testing.T) {
	result := goapiproof.Result{
		UndeclaredNumericLeaves:      []string{"a"},
		OrderInsensitiveListRefusals: []string{"b"},
		StochasticLeafRefusals:       []string{"c"},
	}
	if errs := resultVacuityErrors(result); len(errs) != 0 {
		t.Fatalf("got %v, want none -- Hard refusals are never reported as vacuity errors", errs)
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"teams":["a","b"]}`))
	})))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/filters/options", spec, request,
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

// TestProveOneRESTRequest_UndeclaredNumericLeafRefusesWithNoReceipt pins
// that a request whose Parity sets NumericLeavesDeclared but leaves a
// reached numeric leaf ("count") unnamed in FloatTierB, FloatExactLeaves
// or IntegerLeaves must refuse -- the SAME rule the GraphQL prover
// already enforces (run.go, RefusalUndeclaredNumericLeaf) -- with NO
// receipt written, exactly like a StructuralRefusal: an undeclared
// numeric leaf compares under the unverified Tier-A default, so a
// receipt built from it could read as a fully-checked match having
// verified nothing about that leaf's type.
func TestProveOneRESTRequest_UndeclaredNumericLeafRefusesWithNoReceipt(t *testing.T) {
	const build = "abc123def456"
	body := `{"teams":["a","b"],"count":5}`
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{
		Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: goapiproof.RESTBodyModeJSON,
		Parity:   goapiproof.Options{NumericLeavesDeclared: true},
	}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/filters/options", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if out.Admitted {
		t.Fatalf("out = %+v, want unadmitted (an undeclared numeric leaf must refuse)", out)
	}
	if out.Refusal != goapiproof.RefusalUndeclaredNumericLeaf {
		t.Fatalf("out.Refusal = %q, want %q", out.Refusal, goapiproof.RefusalUndeclaredNumericLeaf)
	}
	if !strings.Contains(out.Detail, "count") {
		t.Fatalf("out.Detail = %q, want it to name the undeclared leaf", out.Detail)
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote %d receipts, want 0 -- an undeclared numeric leaf must never reach a receipt", len(writer.receipts))
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"teams":["a","b"]}`))
	})))
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

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/filters/options", spec, request,
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))
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

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/filters/options", spec, request,
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"teams":["a","b"]}`))
	})))
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

	if _, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/filters/options", spec, request,
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(baselineBody))
	})))
	defer baseline.Close()

	artifacts, err := goapiproof.NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "why this ran"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/filters/options", spec, request,
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})))
	defer baseline.Close()

	artifacts, err := goapiproof.NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "op", spec, request,
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "op", spec, request,
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/filters/options"}
	request := goapiproof.RESTRequest{Name: "options", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "op", spec, request,
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(empty)
	})))
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

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/drilldown/prs", spec, request,
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

	leg, err := doREST(context.Background(), goapiproof.NewLegClient(0), server.URL, http.MethodPost, "/p",
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

	out, err := proveEdgeCredentialOnCandidate(context.Background(), goapiproof.NewLegClient(0), f,
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

	out, err := proveEdgeCredentialOnCandidate(context.Background(), goapiproof.NewLegClient(0), f,
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

	out, err := proveEdgeCredentialOnCandidate(context.Background(), goapiproof.NewLegClient(0), f,
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

	out, err := proveEdgeCredentialOnCandidate(context.Background(), goapiproof.NewLegClient(0), f,
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBaselineAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})))
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

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/meta", spec, request,
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
	if _, err := goapiproof.FetchBuildIdentity(context.Background(), goapiproof.NewLegClient(0), server.URL, nil); err == nil {
		t.Fatal("want an error when no credential is applied")
	}

	// The wrong bearer -- rejected the same way a real bearer-envelope
	// verifier would reject a mismatched or stale one.
	wrongCredential := goapiproof.StaticCredential("Authorization", "candidate bearer", "Bearer not-the-right-token")
	_, err := goapiproof.FetchBuildIdentity(context.Background(), goapiproof.NewLegClient(0), server.URL, wrongCredential)
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
	got, err := goapiproof.FetchBuildIdentity(context.Background(), goapiproof.NewLegClient(0), server.URL, staticCredentialForTest())
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		baselinePaths = append(baselinePaths, r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(personListBody))
	})))
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
	producerOut, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/people", producerSpec, producerRequest,
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

	resolvedPath, resolvedQuery, resolvedBody, unresolved := goapiproof.ResolveRESTIDBindings(consumerSpec.Path, consumerRequest, produced)
	if len(unresolved) != 0 {
		t.Fatalf("unresolved = %v, want none", unresolved)
	}
	resolvedSpec := consumerSpec
	resolvedSpec.Path = resolvedPath
	resolvedRequest := consumerRequest
	resolvedRequest.Query = resolvedQuery
	resolvedRequest.Body = resolvedBody

	consumerOut, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/people/{person_id}/summary", resolvedSpec, resolvedRequest,
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

// iteratingFixtureServers builds the candidate/baseline pair
// resolveIteratingRequest's own tests below drive: GET /people Produces
// person_id from EVERY search result (not just the first); GET
// /people/{person_id}/issues is StatusOnly (baseline declared failing,
// 503 on every path), and its own candidate leg's response depends on
// WHICH person_id is in the path -- candidateIssues maps a person_id to
// the "items" the candidate answers for it, so a test can control exactly
// which candidates win and which lose.
func iteratingFixtureServers(t *testing.T, build string, personIDs []string, candidateIssues map[string]string) (candidateURL, baselineURL string, candidatePaths *[]string) {
	t.Helper()
	var paths []string

	people := `[`
	for i, id := range personIDs {
		if i > 0 {
			people += ","
		}
		people += `{"person_id":"` + id + `"}`
	}
	people += `]`

	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("x-dev-health-build", build)
		if r.URL.Path == "/people" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(people))
			return
		}
		items, ok := candidateIssues[r.URL.Path]
		if !ok {
			items = "[]"
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":` + items + `}`))
	}))
	t.Cleanup(candidate.Close)

	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/people" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(people))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})))
	t.Cleanup(baseline.Close)

	return candidate.URL, baseline.URL, &paths
}

// runIteratingProducer drives the real producer request (GET /people)
// through proveOneRESTRequest exactly the way run()'s own loop does, and
// returns the produced/producedCandidates maps a consumer's own
// resolveIteratingRequest call reads from -- the same hand-merge
// TestIDBinding_EndToEnd above already performs for the single-shot case.
func runIteratingProducer(t *testing.T, f flags, build string, writer receiptWriter) (map[string]string, map[string][]string) {
	t.Helper()
	producerSpec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/people"}
	producerRequest := goapiproof.RESTRequest{
		Name: "search", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: goapiproof.RESTBodyModeJSON,
		Produces: []goapiproof.RESTIDProducer{{Name: "person_id", IDField: "person_id"}},
	}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/people", producerSpec, producerRequest,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("producer proveOneRESTRequest: %v", err)
	}
	if !out.Admitted {
		t.Fatalf("producer request refused: %s -- %s", out.Refusal, out.Detail)
	}
	produced := map[string]string{}
	for name, id := range out.producedIDs {
		produced[name] = id
	}
	producedCandidates := map[string][]string{}
	for name, ids := range out.producedCandidateIDs {
		producedCandidates[name] = ids
	}
	return produced, producedCandidates
}

func iteratingConsumerRequest() (goapiproof.RESTEndpointSpec, goapiproof.RESTRequest, goapiproof.RESTIDBinding) {
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/people/{person_id}/issues"}
	binding := goapiproof.RESTIDBinding{Producer: "person_id", PathParam: "person_id", Candidates: 10, ExposeAs: "issues_person_id"}
	request := goapiproof.RESTRequest{
		Name: "issues_default", WantCandidateStatus: 200, WantBaselineStatus: 503,
		StatusDivergenceReason: "test fixture",
		BodyMode:               goapiproof.RESTBodyModeStatusOnly,
		IDBindings:             []goapiproof.RESTIDBinding{binding},
		Produces:               []goapiproof.RESTIDProducer{{Name: "issue_status", ListPath: "items", IDField: "status"}},
	}
	return spec, request, binding
}

// TestResolveIteratingRequest_SelectsTheFirstCandidateWhoseConsumerYieldsItsProducers
// is this ticket's central claim: the FIRST search result (p-1) has no
// issues (the candidate leg answers empty items, so this request's own
// declared Produces -- issue_status -- cannot extract), so it LOSES; the
// SECOND (p-2) has one, and wins. The winning candidate is exposed as
// issues_person_id, and exactly one receipt is written -- for the winner,
// never for the losing attempt.
func TestResolveIteratingRequest_SelectsTheFirstCandidateWhoseConsumerYieldsItsProducers(t *testing.T) {
	const build = "abc123def456"
	candidateURL, baselineURL, paths := iteratingFixtureServers(t, build,
		[]string{"p-1", "p-2"},
		map[string]string{"/people/p-1/issues": "[]", "/people/p-2/issues": `[{"status":"open"}]`},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	produced, producedCandidates := runIteratingProducer(t, f, build, &fakeReceiptWriter{})
	if len(producedCandidates["person_id"]) != 2 {
		t.Fatalf("producedCandidates[person_id] = %v, want both p-1 and p-2", producedCandidates["person_id"])
	}

	writer := &fakeReceiptWriter{}
	spec, request, binding := iteratingConsumerRequest()
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/people/{person_id}/issues",
		spec, request, binding, produced, producedCandidates,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if !attempt.out.Admitted || !attempt.legsSent {
		t.Fatalf("attempt = %+v, want an admitted win", attempt.out)
	}
	if attempt.out.producedIDs["issues_person_id"] != "p-2" {
		t.Fatalf("producedIDs[issues_person_id] = %q, want p-2 (the winning candidate, not p-1)", attempt.out.producedIDs["issues_person_id"])
	}
	if len(attempt.out.Attempts) != 1 || attempt.out.Attempts[0].CandidateID != "p-1" {
		t.Fatalf("Attempts = %+v, want exactly one losing attempt naming p-1", attempt.out.Attempts)
	}
	if attempt.out.Attempts[0].Refusal != goapiproof.RESTRefusalCandidateProducerUnresolved {
		t.Fatalf("Attempts[0].Refusal = %q, want %q", attempt.out.Attempts[0].Refusal, goapiproof.RESTRefusalCandidateProducerUnresolved)
	}
	if len(writer.receipts) != 1 {
		t.Fatalf("wrote %d receipts, want exactly 1 -- one per corpus request, for the winner only", len(writer.receipts))
	}
	if !slices.Contains(*paths, "/people/p-1/issues") || !slices.Contains(*paths, "/people/p-2/issues") {
		t.Fatalf("candidate paths = %v, want both p-1 and p-2 tried", *paths)
	}
}

// TestResolveIteratingRequest_SiblingBoundToExposeAsReceivesTheWinner
// proves a SIBLING request's own IDBindings, bound to issues_person_id
// (never the raw person_id producer), resolves to the WINNING candidate
// -- not candidate 0 -- exactly the way run()'s own loop would apply it
// after merging this outcome's producedIDs into `produced`.
func TestResolveIteratingRequest_SiblingBoundToExposeAsReceivesTheWinner(t *testing.T) {
	const build = "abc123def456"
	candidateURL, baselineURL, _ := iteratingFixtureServers(t, build,
		[]string{"p-1", "p-2"},
		map[string]string{"/people/p-1/issues": "[]", "/people/p-2/issues": `[{"status":"open"}]`},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	produced, producedCandidates := runIteratingProducer(t, f, build, &fakeReceiptWriter{})

	spec, request, binding := iteratingConsumerRequest()
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/people/{person_id}/issues",
		spec, request, binding, produced, producedCandidates,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}

	// The run loop's own produced-merge step (runMeasurement), performed
	// by hand here exactly like TestIDBinding_EndToEnd already does for
	// the single-shot case.
	for name, id := range attempt.out.producedIDs {
		produced[name] = id
	}

	siblingSpec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/people/{person_id}/issues"}
	siblingRequest := goapiproof.RESTRequest{
		Name: "valid_cursor", WantCandidateStatus: 200, WantBaselineStatus: 503,
		IDBindings: []goapiproof.RESTIDBinding{{Producer: "issues_person_id", PathParam: "person_id"}},
	}
	resolvedPath, _, _, unresolved := goapiproof.ResolveRESTIDBindings(siblingSpec.Path, siblingRequest, produced)
	if len(unresolved) != 0 {
		t.Fatalf("unresolved = %v, want none", unresolved)
	}
	if resolvedPath != "/people/p-2/issues" {
		t.Fatalf("sibling resolved path = %q, want /people/p-2/issues (the winner, not candidate 0)", resolvedPath)
	}
}

// TestResolveIteratingRequest_RefusesByNameAfterExhaustingTheBound
// proves that when NO candidate's own declared Produces resolve, the
// request refuses by name (RESTRefusalCandidateIterationExhausted),
// naming every candidate tried, and writes NO receipt at all.
func TestResolveIteratingRequest_RefusesByNameAfterExhaustingTheBound(t *testing.T) {
	const build = "abc123def456"
	candidateURL, baselineURL, _ := iteratingFixtureServers(t, build,
		[]string{"p-1", "p-2"},
		map[string]string{"/people/p-1/issues": "[]", "/people/p-2/issues": "[]"},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	produced, producedCandidates := runIteratingProducer(t, f, build, &fakeReceiptWriter{})

	writer := &fakeReceiptWriter{}
	spec, request, binding := iteratingConsumerRequest()
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/people/{person_id}/issues",
		spec, request, binding, produced, producedCandidates,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if attempt.out.Admitted || attempt.legsSent {
		t.Fatalf("attempt = %+v, want a refusal with legsSent=false", attempt.out)
	}
	if attempt.out.Refusal != goapiproof.RESTRefusalCandidateIterationExhausted {
		t.Fatalf("Refusal = %q, want %q", attempt.out.Refusal, goapiproof.RESTRefusalCandidateIterationExhausted)
	}
	if !strings.Contains(attempt.out.Detail, "p-1") || !strings.Contains(attempt.out.Detail, "p-2") {
		t.Fatalf("Detail = %q, want it to name both tried candidates", attempt.out.Detail)
	}
	if len(attempt.out.Attempts) != 2 {
		t.Fatalf("Attempts = %+v, want exactly 2 (both candidates tried)", attempt.out.Attempts)
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote %d receipts, want 0 -- exhaustion writes no receipt at all", len(writer.receipts))
	}

	// The exhausted-binding cascade: a sibling bound to issues_person_id
	// refuses through the SAME EXISTING RESTRefusalIDBindingUnresolved
	// path an ordinary unresolved binding already uses -- no new
	// mechanism, since exhaustion never sets produced[ExposeAs].
	siblingSpec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/people/{person_id}/issues"}
	siblingRequest := goapiproof.RESTRequest{
		Name:       "valid_cursor",
		IDBindings: []goapiproof.RESTIDBinding{{Producer: "issues_person_id", PathParam: "person_id"}},
	}
	_, _, _, unresolved := goapiproof.ResolveRESTIDBindings(siblingSpec.Path, siblingRequest, produced)
	if len(unresolved) != 1 || unresolved[0] != "issues_person_id" {
		t.Fatalf("unresolved = %v, want [issues_person_id] -- the existing unresolved-binding path, not a new mechanism", unresolved)
	}
}

// TestResolveIteratingRequest_StopsAtTheDeclaredBoundNeverTriesBeyondIt
// isolates the bound itself: Candidates is set to 3 against a pool of 5
// candidates. The FIRST 3 (p-1..p-3) lose; a winner DOES exist, but only
// at position 4 (p-4) -- one past the declared bound. This proves the
// bound is actually honoured, not merely present in the struct: a
// mutant that iterated the whole pool regardless of Candidates would
// find that winner and pass every OTHER test in this file (none of them
// puts a winner beyond N), but must fail HERE -- the request must
// refuse by exhaustion after EXACTLY 3 attempts, never reach p-4 or p-5
// at all, and write no receipt.
func TestResolveIteratingRequest_StopsAtTheDeclaredBoundNeverTriesBeyondIt(t *testing.T) {
	const build = "abc123def456"
	candidateURL, baselineURL, paths := iteratingFixtureServers(t, build,
		[]string{"p-1", "p-2", "p-3", "p-4", "p-5"},
		map[string]string{
			"/people/p-1/issues": "[]",
			"/people/p-2/issues": "[]",
			"/people/p-3/issues": "[]",
			"/people/p-4/issues": `[{"status":"open"}]`,
			"/people/p-5/issues": `[{"status":"open"}]`,
		},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	produced, producedCandidates := runIteratingProducer(t, f, build, &fakeReceiptWriter{})
	if len(producedCandidates["person_id"]) != 5 {
		t.Fatalf("producedCandidates[person_id] = %v, want all 5", producedCandidates["person_id"])
	}

	writer := &fakeReceiptWriter{}
	spec, request, binding := iteratingConsumerRequest()
	binding.Candidates = 3
	request.IDBindings = []goapiproof.RESTIDBinding{binding}
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/people/{person_id}/issues",
		spec, request, binding, produced, producedCandidates,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if attempt.out.Admitted || attempt.legsSent {
		t.Fatalf("attempt = %+v, want exhaustion at the bound -- a winner exists at p-4, one PAST the declared bound of 3, and must never be reached", attempt.out)
	}
	if attempt.out.Refusal != goapiproof.RESTRefusalCandidateIterationExhausted {
		t.Fatalf("Refusal = %q, want %q", attempt.out.Refusal, goapiproof.RESTRefusalCandidateIterationExhausted)
	}
	wantTried := []string{"p-1", "p-2", "p-3"}
	if len(attempt.out.Attempts) != len(wantTried) {
		t.Fatalf("Attempts = %+v, want exactly %d (the declared bound), not the whole 5-candidate pool", attempt.out.Attempts, len(wantTried))
	}
	for i, want := range wantTried {
		if attempt.out.Attempts[i].CandidateID != want {
			t.Fatalf("Attempts[%d].CandidateID = %q, want %q", i, attempt.out.Attempts[i].CandidateID, want)
		}
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote %d receipts, want 0", len(writer.receipts))
	}
	if slices.Contains(*paths, "/people/p-4/issues") || slices.Contains(*paths, "/people/p-5/issues") {
		t.Fatalf("candidate paths = %v, want p-4/p-5 NEVER tried -- both sit past the declared bound of 3", *paths)
	}
}

// jsonBodyModeIteratingFixtureServers is iteratingFixtureServers' own
// sibling for a JSON-body-mode consumer: unlike that helper (whose
// baseline always answers 503 off the /people path, the StatusOnly shape),
// BOTH planes answer real 200 JSON bodies for every consumer path, from
// candidateBodies/baselineBodies respectively -- so a genuine value
// divergence between the two legs, not just an empty-vs-non-empty split,
// can be exercised.
func jsonBodyModeIteratingFixtureServers(t *testing.T, build string, personIDs []string, candidateBodies, baselineBodies map[string]string) (candidateURL, baselineURL string, candidatePaths *[]string) {
	t.Helper()
	var paths []string

	people := `[`
	for i, id := range personIDs {
		if i > 0 {
			people += ","
		}
		people += `{"person_id":"` + id + `"}`
	}
	people += `]`

	respond := func(bodies map[string]string, path string) string {
		if body, ok := bodies[path]; ok {
			return body
		}
		return `{"items":[]}`
	}

	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("x-dev-health-build", build)
		if r.URL.Path == "/people" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(people))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(respond(candidateBodies, r.URL.Path)))
	}))
	t.Cleanup(candidate.Close)

	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/people" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(people))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(respond(baselineBodies, r.URL.Path)))
	})))
	t.Cleanup(baseline.Close)

	return candidate.URL, baseline.URL, &paths
}

// TestResolveIteratingRequest_JSONBodyModeMismatchWinsOverALaterMatch
// proves the class ruling for a JSON-body-mode (real-compare, not
// StatusOnly) consumer: a candidate whose legs are both empty is refused
// as vacuous (structural.go's own vacuousEmptyLegs, armed here by a
// declared BaselineDefect) and the loop tries the next candidate -- but a
// candidate whose legs are non-empty and disagree on a field OUTSIDE
// every declared defect is an ADMITTED mismatch, not a refusal: iteration
// stops there and reports that mismatch, never trying a THIRD candidate
// whose legs would have matched cleanly. Only a named refusal
// (vacuous_empty_legs here) ever advances the loop; a real finding,
// inside or outside a declaration, wins.
func TestResolveIteratingRequest_JSONBodyModeMismatchWinsOverALaterMatch(t *testing.T) {
	const build = "abc123def456"
	candidateURL, baselineURL, paths := jsonBodyModeIteratingFixtureServers(t, build,
		[]string{"p-1", "p-2", "p-3"},
		map[string]string{
			"/people/p-1/prs": `{"items":[]}`,
			"/people/p-2/prs": `{"items":[{"title":"candidate-title"}]}`,
			"/people/p-3/prs": `{"items":[{"title":"same-title"}]}`,
		},
		map[string]string{
			"/people/p-1/prs": `{"items":[]}`,
			"/people/p-2/prs": `{"items":[{"title":"baseline-title"}]}`,
			"/people/p-3/prs": `{"items":[{"title":"same-title"}]}`,
		},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	produced, producedCandidates := runIteratingProducer(t, f, build, &fakeReceiptWriter{})
	if len(producedCandidates["person_id"]) != 3 {
		t.Fatalf("producedCandidates[person_id] = %v, want all three of p-1/p-2/p-3", producedCandidates["person_id"])
	}

	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/people/{person_id}/prs"}
	binding := goapiproof.RESTIDBinding{Producer: "person_id", PathParam: "person_id", Candidates: 10, ExposeAs: "prs_person_id"}
	request := goapiproof.RESTRequest{
		Name: "prs_default", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode:   goapiproof.RESTBodyModeJSON,
		IDBindings: []goapiproof.RESTIDBinding{binding},
		Parity: goapiproof.Options{
			BaselineDefects: []goapiproof.BaselineDefect{
				{Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.items.unused_field"}},
			},
		},
	}

	writer := &fakeReceiptWriter{}
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/people/{person_id}/prs",
		spec, request, binding, produced, producedCandidates,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if !attempt.out.Admitted || !attempt.legsSent {
		t.Fatalf("attempt = %+v, want an admitted win", attempt.out)
	}
	if attempt.out.TerminalState != goapiproof.TerminalStateMismatch {
		t.Fatalf("TerminalState = %q, want mismatch -- a real divergence must win, not be treated as a refusal", attempt.out.TerminalState)
	}
	if attempt.out.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("DifferencesOutsideBaselineDefect = 0, want at least 1 for the title divergence")
	}
	if attempt.out.producedIDs["prs_person_id"] != "p-2" {
		t.Fatalf("producedIDs[prs_person_id] = %q, want p-2 (the first candidate with real, non-vacuous data) -- not p-3's later, fully-matching candidate", attempt.out.producedIDs["prs_person_id"])
	}
	if len(attempt.out.Attempts) != 1 || attempt.out.Attempts[0].CandidateID != "p-1" {
		t.Fatalf("Attempts = %+v, want exactly one losing attempt naming p-1", attempt.out.Attempts)
	}
	if attempt.out.Attempts[0].Refusal != goapiproof.RefusalVacuousEmptyLegs {
		t.Fatalf("Attempts[0].Refusal = %q, want %q", attempt.out.Attempts[0].Refusal, goapiproof.RefusalVacuousEmptyLegs)
	}
	if len(writer.receipts) != 1 {
		t.Fatalf("wrote %d receipts, want exactly 1 -- one per corpus request, for the winner only", len(writer.receipts))
	}
	if slices.Contains(*paths, "/people/p-3/prs") {
		t.Fatalf("candidate paths = %v, want p-3 never tried -- p-2 already won", *paths)
	}
}

// TestResolveSingleShotRequest_UnaffectedByTheIterationMechanism is the
// regression pin ruling 1 requires: a binding with no Candidates opt-in
// (the zero value every existing corpus entry carries) still resolves to
// the single first-extracted candidate, byte for byte, whether or not
// producedCandidates happens to hold more than one -- proving the two
// mechanisms are genuinely independent, not "iteration always tried,
// short-circuited when Candidates==0".
func TestResolveSingleShotRequest_UnaffectedByTheIterationMechanism(t *testing.T) {
	const build = "abc123def456"
	candidateURL, baselineURL, _ := iteratingFixtureServers(t, build,
		[]string{"p-1", "p-2"},
		map[string]string{"/people/p-1/issues": "[]", "/people/p-2/issues": `[{"status":"open"}]`},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	writer := &fakeReceiptWriter{}
	produced, _ := runIteratingProducer(t, f, build, writer)
	if produced["person_id"] != "p-1" {
		t.Fatalf("produced[person_id] = %q, want p-1 (the first search result, single-shot rule)", produced["person_id"])
	}

	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/people/{person_id}/issues"}
	request := goapiproof.RESTRequest{
		Name: "issues_default", WantCandidateStatus: 200, WantBaselineStatus: 503,
		StatusDivergenceReason: "test fixture",
		BodyMode:               goapiproof.RESTBodyModeStatusOnly,
		IDBindings:             []goapiproof.RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
	}
	attempt, err := resolveSingleShotRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/people/{person_id}/issues",
		spec, request, produced,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveSingleShotRequest: %v", err)
	}
	if !attempt.legsSent {
		t.Fatal("legsSent = false, want true -- person_id resolved from produced")
	}
	if attempt.spec.Path != "/people/p-1/issues" {
		t.Fatalf("resolved path = %q, want /people/p-1/issues -- the single-shot binding must still take the FIRST candidate, never iterate", attempt.spec.Path)
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

	_, err := doREST(context.Background(), goapiproof.NewLegClient(0), server.URL, http.MethodGet, "/p", nil, nil, nil, 30*time.Millisecond)
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

	_, err := doREST(context.Background(), goapiproof.NewLegClient(0), server.URL, http.MethodGet, "/p", nil, nil, nil, 5*time.Second)
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
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 30 * time.Millisecond}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/home"}
	request := goapiproof.RESTRequest{Name: "home", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/home", spec, request,
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

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/home", spec, request,
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
	firstBaseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})))
	defer firstBaseline.Close()

	okCandidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", "build123")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"a":1}`))
	}))
	defer okCandidate.Close()
	okBaseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"a":1}`))
	})))
	defer okBaseline.Close()

	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/home"}
	request := goapiproof.RESTRequest{Name: "home", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}

	firstFlags := flags{queryAPIURL: stalledCandidate.URL, pythonAPIURL: firstBaseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 30 * time.Millisecond}
	first, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), firstFlags, "REST:GET:/api/v1/home", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), "build123", goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("first proveOneRESTRequest: %v, want a refusal not an error", err)
	}
	if first.Admitted || first.Refusal != goapiproof.RESTRefusalCandidateLegTimedOut {
		t.Fatalf("first = %+v, want an unadmitted candidate-leg-timeout refusal", first)
	}

	secondFlags := flags{queryAPIURL: okCandidate.URL, pythonAPIURL: okBaseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	second, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), secondFlags, "REST:GET:/api/v1/home", spec, request,
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

	out, err := proveEdgeCredentialOnCandidate(context.Background(), goapiproof.NewLegClient(0), f,
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

	if err := writeJSONReport(path, jsonReport{Outcomes: outcomes, NotRun: notRun}); err != nil {
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

	if err := writeJSONReport(path, jsonReport{Outcomes: outcomes}); err != nil {
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
// The reader drains the pipe on its OWN goroutine, started before fn
// runs: an os.Pipe carries a fixed OS buffer (64KiB on Linux), and fn
// can write past it -- a reader started only after fn returns would
// block fn's own write forever the moment total output exceeds that
// buffer, since nothing would ever be there to drain it.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()

	type readResult struct {
		data []byte
		err  error
	}
	done := make(chan readResult, 1)
	go func() {
		data, readErr := io.ReadAll(r)
		done <- readResult{data: data, err: readErr}
	}()

	fn()

	_ = w.Close()
	result := <-done
	if result.err != nil {
		t.Fatalf("read captured stdout: %v", result.err)
	}
	return string(result.data)
}

// TestCaptureStdoutDrainsMoreThanOnePipeBufferOfOutput writes past the OS
// pipe's own buffer (64KiB on Linux) from inside fn, in one line long
// enough that a single fmt.Println already exceeds it -- captureStdout's
// own doc comment states why a reader started only after fn returns
// would block that write forever. This test completing at all, within
// its normal timeout, is the assertion; it also checks the exact byte
// count survives the round trip.
func TestCaptureStdoutDrainsMoreThanOnePipeBufferOfOutput(t *testing.T) {
	const lineLen = 70000 // > 65536, the default Linux pipe buffer.
	line := strings.Repeat("x", lineLen)

	got := captureStdout(t, func() {
		fmt.Println(line)
	})

	want := line + "\n"
	if got != want {
		t.Fatalf("captureStdout: got %d bytes, want %d bytes", len(got), len(want))
	}
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
	baseline := httptest.NewServer(referencePlane(genericRESTStubHandler(t, "", &stalledRequest{method: http.MethodGet, path: "/api/v1/home", rawQuery: ""})))
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
		runErr = runMeasurement(context.Background(), goapiproof.NewLegClient(0), f,
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

// TestRunMeasurement_OperatorSuppliedBindingUnsuppliedRefusesByName drives
// runMeasurement over the REAL, full REST corpus with NO -bind flags at
// all: GET /api/v1/flame's own deployment_entity_id_bound_200 entry binds
// entity_id to deployment_entity_id, an operator-supplied producer
// (goapiproof.IsOperatorSuppliedIDProducer) no request in this corpus
// Produces -- an unsupplied run must refuse that one request by the SAME
// RESTRefusalIDBindingUnresolved reason any other unproduced id gets, with
// Detail naming the flag to supply, not the generic "no earlier request"
// wording that would mislead an operator into looking for a missing
// producer request that can never exist.
func TestRunMeasurement_OperatorSuppliedBindingUnsuppliedRefusesByName(t *testing.T) {
	const build = "build123"
	candidate := httptest.NewServer(genericRESTStubHandler(t, build, nil))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(genericRESTStubHandler(t, "", nil)))
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
		timeout: 2 * time.Second, dryRun: true, reportPath: reportPath,
	}

	// runMeasurement's own returned error is NOT asserted here: driving
	// the REAL, full corpus against a generic {} stub body leaves plenty
	// of OTHER operations' own FloatTierB/VolatileFields declarations
	// genuinely unused (their real fields never appear in a bare {}
	// body), which independently fails the run via its own vacuity check
	// -- orthogonal to this test's own claim. The JSON report is written
	// unconditionally before that check runs (runMeasurement's own
	// "report first, error second" discipline), so it is still complete
	// and worth reading regardless.
	_ = captureStdout(t, func() {
		_ = runMeasurement(context.Background(), goapiproof.NewLegClient(0), f,
			staticCredentialForTest(), staticCredentialForTest(), build, nil, artifacts)
	})

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}

	var deploymentOutcome *outcome
	for i := range report.Outcomes {
		if report.Outcomes[i].Operation == "REST:GET:/api/v1/flame" && report.Outcomes[i].Request == "deployment_entity_id_bound_200" {
			deploymentOutcome = &report.Outcomes[i]
		}
	}
	if deploymentOutcome == nil {
		t.Fatalf("report has no outcome for REST:GET:/api/v1/flame/deployment_entity_id_bound_200 among %d outcomes", len(report.Outcomes))
	}
	if deploymentOutcome.Admitted {
		t.Fatalf("deployment outcome = %+v, want unadmitted (no -bind supplied)", deploymentOutcome)
	}
	if deploymentOutcome.Refusal != goapiproof.RESTRefusalIDBindingUnresolved {
		t.Fatalf("deployment outcome Refusal = %q, want %q", deploymentOutcome.Refusal, goapiproof.RESTRefusalIDBindingUnresolved)
	}
	if !strings.Contains(deploymentOutcome.Detail, "supply -bind deployment_entity_id=") {
		t.Fatalf("deployment outcome Detail = %q, want it to name the flag to supply", deploymentOutcome.Detail)
	}
}

// TestRunMeasurement_OperatorSuppliedBindingSuppliedResolves is the
// positive counterpart: f.binds carries deployment_entity_id, so
// deployment_entity_id_bound_200 resolves and runs like any other bound
// request, and its own outcome records the name and value it bound --
// the run record must show what was bound without reading the driver
// that produced it.
func TestRunMeasurement_OperatorSuppliedBindingSuppliedResolves(t *testing.T) {
	const build = "build123"
	const boundValue = "00000000-0000-0000-0000-000000000000:d-1"
	candidate := httptest.NewServer(genericRESTStubHandler(t, build, nil))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(genericRESTStubHandler(t, "", nil)))
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
		timeout: 2 * time.Second, dryRun: true, reportPath: reportPath,
		binds: map[string]string{"deployment_entity_id": boundValue},
	}

	// See the sibling test above for why runMeasurement's own returned
	// error is not asserted here.
	_ = captureStdout(t, func() {
		_ = runMeasurement(context.Background(), goapiproof.NewLegClient(0), f,
			staticCredentialForTest(), staticCredentialForTest(), build, nil, artifacts)
	})

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}

	var deploymentOutcome *outcome
	for i := range report.Outcomes {
		if report.Outcomes[i].Operation == "REST:GET:/api/v1/flame" && report.Outcomes[i].Request == "deployment_entity_id_bound_200" {
			deploymentOutcome = &report.Outcomes[i]
		}
	}
	if deploymentOutcome == nil {
		t.Fatalf("report has no outcome for REST:GET:/api/v1/flame/deployment_entity_id_bound_200 among %d outcomes", len(report.Outcomes))
	}
	if deploymentOutcome.Refusal == goapiproof.RESTRefusalIDBindingUnresolved {
		t.Fatalf("deployment outcome = %+v, want the id to resolve (a -bind was supplied)", deploymentOutcome)
	}
	if deploymentOutcome.BoundIDs["deployment_entity_id"] != boundValue {
		t.Fatalf("deployment outcome BoundIDs[deployment_entity_id] = %q, want %q -- the run record must carry the supplied value", deploymentOutcome.BoundIDs["deployment_entity_id"], boundValue)
	}
}

// TestRunMeasurement_DeploymentGapBindingUnsuppliedRefusesByName is
// TestRunMeasurement_OperatorSuppliedBindingUnsuppliedRefusesByName's
// sibling for GET /api/v1/flame's OTHER operator-supplied deployment
// producer: deployment_gap_entity_id_bound_422's own IDBindings names
// deployment_gap_entity_id (restcorpus.go's own restOperatorSuppliedProducers),
// a distinct id from deployment_entity_id_bound_200's deployment_entity_id
// -- an unsupplied run must refuse THIS request too, by name, independent
// of whether -bind deployment_entity_id was supplied.
func TestRunMeasurement_DeploymentGapBindingUnsuppliedRefusesByName(t *testing.T) {
	const build = "build123"
	candidate := httptest.NewServer(genericRESTStubHandler(t, build, nil))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(genericRESTStubHandler(t, "", nil)))
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
		timeout: 2 * time.Second, dryRun: true, reportPath: reportPath,
	}

	// See TestRunMeasurement_OperatorSuppliedBindingUnsuppliedRefusesByName
	// for why runMeasurement's own returned error is not asserted here.
	_ = captureStdout(t, func() {
		_ = runMeasurement(context.Background(), goapiproof.NewLegClient(0), f,
			staticCredentialForTest(), staticCredentialForTest(), build, nil, artifacts)
	})

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}

	var gapOutcome *outcome
	for i := range report.Outcomes {
		if report.Outcomes[i].Operation == "REST:GET:/api/v1/flame" && report.Outcomes[i].Request == "deployment_gap_entity_id_bound_422" {
			gapOutcome = &report.Outcomes[i]
		}
	}
	if gapOutcome == nil {
		t.Fatalf("report has no outcome for REST:GET:/api/v1/flame/deployment_gap_entity_id_bound_422 among %d outcomes", len(report.Outcomes))
	}
	if gapOutcome.Admitted {
		t.Fatalf("deployment gap outcome = %+v, want unadmitted (no -bind supplied)", gapOutcome)
	}
	if gapOutcome.Refusal != goapiproof.RESTRefusalIDBindingUnresolved {
		t.Fatalf("deployment gap outcome Refusal = %q, want %q", gapOutcome.Refusal, goapiproof.RESTRefusalIDBindingUnresolved)
	}
	if !strings.Contains(gapOutcome.Detail, "supply -bind deployment_gap_entity_id=") {
		t.Fatalf("deployment gap outcome Detail = %q, want it to name the flag to supply", gapOutcome.Detail)
	}
}

// TestRunMeasurement_PRGapBindingUnsuppliedRefusesByName is
// TestRunMeasurement_DeploymentGapBindingUnsuppliedRefusesByName's own
// sibling for GET /api/v1/flame's "pr" operator-supplied gap producer:
// pr_gap_entity_id_bound_status_divergence's own IDBindings names
// pr_gap_id (restcorpus.go's own restOperatorSuppliedProducers) -- an
// unsupplied run must refuse this request too, by name.
func TestRunMeasurement_PRGapBindingUnsuppliedRefusesByName(t *testing.T) {
	const build = "build123"
	candidate := httptest.NewServer(genericRESTStubHandler(t, build, nil))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(genericRESTStubHandler(t, "", nil)))
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
		timeout: 2 * time.Second, dryRun: true, reportPath: reportPath,
	}

	// See TestRunMeasurement_OperatorSuppliedBindingUnsuppliedRefusesByName
	// for why runMeasurement's own returned error is not asserted here.
	_ = captureStdout(t, func() {
		_ = runMeasurement(context.Background(), goapiproof.NewLegClient(0), f,
			staticCredentialForTest(), staticCredentialForTest(), build, nil, artifacts)
	})

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}

	var gapOutcome *outcome
	for i := range report.Outcomes {
		if report.Outcomes[i].Operation == "REST:GET:/api/v1/flame" && report.Outcomes[i].Request == "pr_gap_entity_id_bound_status_divergence" {
			gapOutcome = &report.Outcomes[i]
		}
	}
	if gapOutcome == nil {
		t.Fatalf("report has no outcome for REST:GET:/api/v1/flame/pr_gap_entity_id_bound_status_divergence among %d outcomes", len(report.Outcomes))
	}
	if gapOutcome.Admitted {
		t.Fatalf("pr gap outcome = %+v, want unadmitted (no -bind supplied)", gapOutcome)
	}
	if gapOutcome.Refusal != goapiproof.RESTRefusalIDBindingUnresolved {
		t.Fatalf("pr gap outcome Refusal = %q, want %q", gapOutcome.Refusal, goapiproof.RESTRefusalIDBindingUnresolved)
	}
	if !strings.Contains(gapOutcome.Detail, "supply -bind pr_gap_id=") {
		t.Fatalf("pr gap outcome Detail = %q, want it to name the flag to supply", gapOutcome.Detail)
	}
}

// TestRunMeasurement_IssueGapBindingUnsuppliedRefusesByName is
// TestRunMeasurement_DeploymentGapBindingUnsuppliedRefusesByName's own
// sibling for GET /api/v1/flame's "issue" operator-supplied gap producer:
// issue_gap_entity_id_bound_status_divergence's own IDBindings names
// issue_gap_id (restcorpus.go's own restOperatorSuppliedProducers) -- an
// unsupplied run must refuse this request too, by name.
func TestRunMeasurement_IssueGapBindingUnsuppliedRefusesByName(t *testing.T) {
	const build = "build123"
	candidate := httptest.NewServer(genericRESTStubHandler(t, build, nil))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(genericRESTStubHandler(t, "", nil)))
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
		timeout: 2 * time.Second, dryRun: true, reportPath: reportPath,
	}

	// See TestRunMeasurement_OperatorSuppliedBindingUnsuppliedRefusesByName
	// for why runMeasurement's own returned error is not asserted here.
	_ = captureStdout(t, func() {
		_ = runMeasurement(context.Background(), goapiproof.NewLegClient(0), f,
			staticCredentialForTest(), staticCredentialForTest(), build, nil, artifacts)
	})

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}

	var gapOutcome *outcome
	for i := range report.Outcomes {
		if report.Outcomes[i].Operation == "REST:GET:/api/v1/flame" && report.Outcomes[i].Request == "issue_gap_entity_id_bound_status_divergence" {
			gapOutcome = &report.Outcomes[i]
		}
	}
	if gapOutcome == nil {
		t.Fatalf("report has no outcome for REST:GET:/api/v1/flame/issue_gap_entity_id_bound_status_divergence among %d outcomes", len(report.Outcomes))
	}
	if gapOutcome.Admitted {
		t.Fatalf("issue gap outcome = %+v, want unadmitted (no -bind supplied)", gapOutcome)
	}
	if gapOutcome.Refusal != goapiproof.RESTRefusalIDBindingUnresolved {
		t.Fatalf("issue gap outcome Refusal = %q, want %q", gapOutcome.Refusal, goapiproof.RESTRefusalIDBindingUnresolved)
	}
	if !strings.Contains(gapOutcome.Detail, "supply -bind issue_gap_id=") {
		t.Fatalf("issue gap outcome Detail = %q, want it to name the flag to supply", gapOutcome.Detail)
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
	baseline := httptest.NewServer(referencePlane(genericRESTStubHandler(t, "", nil)))
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
		runErr = runMeasurement(context.Background(), goapiproof.NewLegClient(0), f,
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

// TestProveOneRESTRequest_StatusOnlyBaselineFailureProducesFromCandidateLeg
// proves the one exception ValidateRESTCorpus allows (goapiproof/
// restcorpus.go): a StatusOnly request whose two Want statuses diverge
// with the CANDIDATE's own want at 200 -- this route's baseline is
// declared failing in production -- still Produces an id, read from the
// CANDIDATE leg's decoded body through the real production decoder
// (DecodeRESTSnapshot), never a hand-built value, since the baseline is
// declared to carry no usable body at all.
func TestProveOneRESTRequest_StatusOnlyBaselineFailureProducesFromCandidateLeg(t *testing.T) {
	const build = "abc123def456"
	const wantID = "wi-777"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[{"work_item_id":"` + wantID + `"}]}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"detail":"Data unavailable"}`))
	})))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/drilldown/issues"}
	request := goapiproof.RESTRequest{
		Name: "default_window", WantCandidateStatus: 200, WantBaselineStatus: 503,
		StatusDivergenceReason: "constructed for this test",
		BodyMode:               goapiproof.RESTBodyModeStatusOnly,
		Produces:               []goapiproof.RESTIDProducer{{Name: "work_item_id", ListPath: "items", IDField: "work_item_id"}},
	}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/drilldown/issues", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if !out.Admitted {
		t.Fatalf("request refused: %s -- %s", out.Refusal, out.Detail)
	}
	if got := out.producedIDs["work_item_id"]; got != wantID {
		t.Fatalf("producedIDs[work_item_id] = %q, want %q (from the CANDIDATE leg)", got, wantID)
	}
	if len(writer.receipts) != 1 || writer.receipts[0].TerminalState != goapiproof.TerminalStateMatch {
		t.Fatalf("receipts = %+v, want exactly one Match receipt -- a StatusOnly request never compares bodies", writer.receipts)
	}
}

// TestProveOneRESTRequest_StatusOnlyBaselineFailureRefusedWhenCandidateBodyLacksTheDeclaredID
// proves the check this ticket adds is not decoration: a StatusOnly,
// declared-failing-baseline request whose CANDIDATE leg answers the
// declared 200 but whose body does not yield a declared Produces id --
// here, a real 200 body missing the "items" the declaration expects --
// refuses the request outright, by its own named reason, rather than
// leaving out.producedIDs silently short (the behaviour before this
// ticket) or writing a receipt.
func TestProveOneRESTRequest_StatusOnlyBaselineFailureRefusedWhenCandidateBodyLacksTheDeclaredID(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"entity":{"work_item_id":"w1"},"timeline":{"start":"2024-01-01T00:00:00Z","end":"2024-01-02T00:00:00Z"},"frames":[]}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"detail":"Data unavailable"}`))
	})))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/flame"}
	request := goapiproof.RESTRequest{
		Name: "issue_entity_id_bound_200", WantCandidateStatus: 200, WantBaselineStatus: 503,
		StatusDivergenceReason: "constructed for this test",
		BodyMode:               goapiproof.RESTBodyModeStatusOnly,
		Produces:               []goapiproof.RESTIDProducer{{Name: "issue_flame_frame_id", ListPath: "frames", IDField: "id"}},
	}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/flame", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if out.Admitted || out.Refusal != goapiproof.RESTRefusalCandidateProducerUnresolved {
		t.Fatalf("out = %+v, want a named RESTRefusalCandidateProducerUnresolved refusal, not an admitted request", out)
	}
	if len(out.producedIDs) != 0 {
		t.Fatalf("producedIDs = %v, want none: a refused request produces nothing", out.producedIDs)
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote %d receipts, want 0 for a refused request", len(writer.receipts))
	}
}

// TestProveOneRESTRequest_StatusOnlyBaselineFailureRefusedWhenBaselineRecovers
// proves the reversal is never silently absorbed: if the baseline this
// entry declares as failing starts answering the CANDIDATE's own
// declared status instead, admission refuses by name before this
// request's Produces logic ever runs, so no id is produced either.
func TestProveOneRESTRequest_StatusOnlyBaselineFailureRefusedWhenBaselineRecovers(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[{"work_item_id":"wi-777"}]}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The baseline has recovered: it now answers the CANDIDATE's own
		// declared 200, not the 503 this entry declares for it.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[{"work_item_id":"wi-777"}]}`))
	})))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/drilldown/issues"}
	request := goapiproof.RESTRequest{
		Name: "default_window", WantCandidateStatus: 200, WantBaselineStatus: 503,
		StatusDivergenceReason: "constructed for this test",
		BodyMode:               goapiproof.RESTBodyModeStatusOnly,
		Produces:               []goapiproof.RESTIDProducer{{Name: "work_item_id", ListPath: "items", IDField: "work_item_id"}},
	}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/drilldown/issues", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if out.Admitted || out.Refusal != goapiproof.RESTRefusalUnexpectedStatus {
		t.Fatalf("out = %+v, want a named RESTRefusalUnexpectedStatus refusal, not an admitted request", out)
	}
	if len(out.producedIDs) != 0 {
		t.Fatalf("producedIDs = %v, want none: a refused request produces nothing", out.producedIDs)
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote %d receipts, want 0 for a refused request", len(writer.receipts))
	}
}

// TestProveOneRESTRequest_OrdinaryRequestStillProducesFromBaselineLeg pins
// the unchanged case: a request whose two Want statuses agree (not the
// baseline-only-failure shape) still takes its Produces id from the
// BASELINE leg, exactly as before -- the candidate-leg exception above is
// scoped to the one divergent shape and must not leak into the ordinary
// path.
func TestProveOneRESTRequest_OrdinaryRequestStillProducesFromBaselineLeg(t *testing.T) {
	const build = "abc123def456"
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[{"work_item_id":"from-candidate"}]}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[{"work_item_id":"from-baseline"}]}`))
	})))
	defer baseline.Close()

	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/api/v1/drilldown/issues"}
	request := goapiproof.RESTRequest{
		Name: "default_window", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: goapiproof.RESTBodyModeJSON,
		Produces: []goapiproof.RESTIDProducer{{Name: "work_item_id", ListPath: "items", IDField: "work_item_id"}},
	}
	writer := &fakeReceiptWriter{}

	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/drilldown/issues", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if !out.Admitted {
		t.Fatalf("request refused: %s -- %s", out.Refusal, out.Detail)
	}
	if got := out.producedIDs["work_item_id"]; got != "from-baseline" {
		t.Fatalf("producedIDs[work_item_id] = %q, want %q (still the BASELINE leg for an ordinary request)", got, "from-baseline")
	}
}

// referencePlane wraps a test handler so every response carries the
// Python app's `server: uvicorn` stamp, the positive baseline identity
// RESTAdmit requires of a baseline leg.
func referencePlane(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", goapiproof.ReferencePlaneServer)
		handler.ServeHTTP(w, r)
	})
}
