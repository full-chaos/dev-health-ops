//go:build integration

package teamsidentity

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// linearDiscoverStub serves the real, captured, sanitized fixture under
// testdata/linear_discover_fixtures/ (README.md there has full provenance)
// at the one GraphQL path both planes POST to, and records every request it
// receives.
type linearDiscoverStub struct {
	server   *httptest.Server
	requests []venueOracleRequest
	fixture  []byte
}

func newLinearDiscoverStub(t *testing.T) *linearDiscoverStub {
	t.Helper()
	stub := &linearDiscoverStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)

	raw, err := os.ReadFile(filepath.Join("testdata", "linear_discover_fixtures", "teams.json"))
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	stub.fixture = raw
	return stub
}

func (s *linearDiscoverStub) handle(w http.ResponseWriter, r *http.Request) {
	headerNames := make([]string, 0, len(r.Header))
	for name := range r.Header {
		headerNames = append(headerNames, name)
	}
	sort.Strings(headerNames)

	resource := "UNKNOWN:" + r.URL.Path
	var body []byte
	if r.URL.Path == "/graphql" {
		// Both planes send one POST per page; this fixture has no
		// pageInfo.hasNextPage, so one request is the whole walk. The
		// request body (the GraphQL query + variables) is not inspected
		// here -- the response-body comparison below already proves the
		// two planes' queries produced the same discovered teams.
		_, _ = io.ReadAll(r.Body)
		resource = "graphql:teams"
		body = s.fixture
	}
	s.requests = append(s.requests, venueOracleRequest{
		Method: r.Method, Path: r.URL.Path, Resource: resource, HeaderNames: headerNames,
	})
	if body == nil {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"venue oracle stub: no fixture for ` + r.URL.Path + `"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func (s *linearDiscoverStub) requestSignatures() []string {
	out := make([]string, len(s.requests))
	for i, req := range s.requests {
		out[i] = req.Method + " " + req.Resource
	}
	sort.Strings(out)
	return out
}

// TestVenueOracleDiscoverLinearMatchesPython is CHAOS-6374's venue oracle
// for the Linear discovery route: ONE shared stub server, a fixture from a
// real captured Linear API response (see the fixture directory's own
// README for provenance), BOTH planes (this Go port and the real,
// unmodified Python TeamDiscoveryService.discover_linear) pointed at it,
// response bodies AND the stub's received request sequence compared.
//
// Opt-in only (DEV_HEALTH_LIVE_PYTHON_ORACLES=1, matching every other
// live-Python oracle in this repo, run through ci/check_go.sh
// live-python-oracles with -count=1).
func TestVenueOracleDiscoverLinearMatchesPython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR") == "" {
		t.Fatal("live Python oracle opt-in requires a proof directory from ci/check_go.sh")
	}
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(packageDir)))
	python := pyoracle.Resolve(t, repoRoot)

	stub := newLinearDiscoverStub(t)
	const apiKey = "venue-oracle-test-key"

	// --- Go plane --------------------------------------------------------
	credential := providerfoundation.NewCredential("linear", "venue-oracle-cred",
		map[string]string{},
		map[string]secrets.Value{"api_key": secrets.NewValue(apiKey)},
	)
	stubURL, err := url.Parse(stub.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	previousClient := discoveryHTTPClient
	discoveryHTTPClient = &http.Client{Transport: rewriteHostTransport{target: stubURL}}
	t.Cleanup(func() { discoveryHTTPClient = previousClient })

	goTeams, err := discoverLinear(t.Context(), credential)
	if err != nil {
		t.Fatalf("discoverLinear: %v", err)
	}
	goBody, err := pyjson.Marshal(teamDiscoverResponseJSON("linear", goTeams, false, nil))
	if err != nil {
		t.Fatal(err)
	}
	goSignatures := stub.requestSignatures()
	stub.requests = nil

	// --- Python plane ------------------------------------------------------
	//
	// LinearClient posts to a fixed module-level LINEAR_API_URL constant
	// (no url parameter on discover_linear/LinearClient, unlike gitlab/jira)
	// -- the script monkeypatches that constant, the same class of technique
	// the GitHub oracle uses for PyGithub's own fixed construction.
	scriptPath := filepath.Join(packageDir, "testdata", "venue_oracle_discover_linear.py")
	pyBody, err := exec.Command(python, scriptPath, stub.server.URL+"/graphql", apiKey).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("execute Python venue oracle for linear discovery: %v: %s", err, exitErr.Stderr)
		}
		t.Fatalf("execute Python venue oracle for linear discovery: %v", err)
	}
	pySignatures := stub.requestSignatures()

	// --- Compare the WHOLE response body -----------------------------------
	if string(goBody) != string(pyBody) {
		t.Errorf("response body diverges:\ngo:     %s\npython: %s", goBody, pyBody)
	}

	// --- Compare the stub's request sequence --------------------------------
	if !equalStrings(goSignatures, pySignatures) {
		t.Errorf("request sequence diverges:\ngo:\n%v\npython:\n%v", goSignatures, pySignatures)
	}
	t.Logf("go requests: %v", goSignatures)
	t.Logf("python requests: %v", pySignatures)

	venueoracle.WriteProof(t)
}
