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
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, false, nil)
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
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, true, nil)
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
		staticCredentialForTest(), staticCredentialForTest(), "abc123", goapiproof.AuthContext{}, time.Now().UTC(), writer, false, nil)
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
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, false, nil)
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
		poisonedCredential, poisonedCredential, build, goapiproof.AuthContext{}, time.Now().UTC(), nil, true, nil)
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
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, false, nil)
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
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, false,
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
