package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"sort"
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
type fakeReceiptWriter struct {
	receipts []goapiproof.RESTReceipt
}

func (w *fakeReceiptWriter) WriteReceipt(_ context.Context, receipt goapiproof.RESTReceipt) (uuid.UUID, error) {
	w.receipts = append(w.receipts, receipt)
	return uuid.New(), nil
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
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, false)
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
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, true)
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
		staticCredentialForTest(), staticCredentialForTest(), "abc123", goapiproof.AuthContext{}, time.Now().UTC(), writer, false)
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
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, false)
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
		url.Values{"q": {"1"}}, map[string]any{"x": "y"}, staticCredentialForTest())
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
		poisonedCredential, poisonedCredential, build, goapiproof.AuthContext{}, time.Now().UTC(), nil, true)
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
