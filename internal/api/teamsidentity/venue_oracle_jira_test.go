//go:build integration

package teamsidentity

import (
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// jiraDiscoverStub serves the real, captured, sanitized fixture under
// testdata/jira_discover_fixtures/ (README.md there has full provenance) by
// exact path, and records every request it receives.
type jiraDiscoverStub struct {
	server   *httptest.Server
	requests []venueOracleRequest
	fixture  []byte
}

func newJiraDiscoverStub(t *testing.T) *jiraDiscoverStub {
	t.Helper()
	stub := &jiraDiscoverStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)

	raw, err := os.ReadFile(filepath.Join("testdata", "jira_discover_fixtures", "project_search.json"))
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	stub.fixture = raw
	return stub
}

func (s *jiraDiscoverStub) handle(w http.ResponseWriter, r *http.Request) {
	headerNames := make([]string, 0, len(r.Header))
	for name := range r.Header {
		headerNames = append(headerNames, name)
	}
	sort.Strings(headerNames)

	resource := "UNKNOWN:" + r.URL.Path
	var body []byte
	if r.URL.Path == "/rest/api/3/project/search" {
		resource = "project_search"
		body = s.fixture
	}
	s.requests = append(s.requests, venueOracleRequest{
		Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Resource: resource, HeaderNames: headerNames,
	})
	if body == nil {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"venue oracle stub: no fixture for ` + r.URL.Path + `"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func (s *jiraDiscoverStub) requestSignatures() []string {
	out := make([]string, len(s.requests))
	for i, req := range s.requests {
		sig := req.Method + " " + req.Resource
		if req.Query != "" {
			sig += "?" + req.Query
		}
		out[i] = sig
	}
	sort.Strings(out)
	return out
}

// TestVenueOracleDiscoverJiraMatchesPython is CHAOS-6374's venue oracle for
// the Jira discovery route: ONE shared stub server, a fixture from a real
// captured Jira API response (see the fixture directory's own README for
// provenance), BOTH planes (this Go port and the real, unmodified Python
// TeamDiscoveryService.discover_jira) pointed at it, response bodies AND the
// stub's received request sequence compared.
//
// Opt-in only (DEV_HEALTH_LIVE_PYTHON_ORACLES=1, matching every other
// live-Python oracle in this repo, run through ci/check_go.sh
// live-python-oracles with -count=1).
func TestVenueOracleDiscoverJiraMatchesPython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR") == "" {
		t.Fatal("live Python oracle opt-in requires a proof directory from ci/check_go.sh")
	}
	_, currentFile, _, _ := moduleroot.Caller(0)
	packageDir := filepath.Dir(currentFile)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(packageDir)))
	python := pyoracle.Resolve(t, repoRoot)

	stub := newJiraDiscoverStub(t)
	const email = "venue-oracle@acme-corp.test"
	const apiToken = "venue-oracle-test-token"

	// --- Go plane --------------------------------------------------------
	//
	// Jira has no fixed default host (unlike GitHub/GitLab): a real
	// credential always carries its own base_url. Setting it to the stub's
	// own URL is exactly what a real credential config supplies, not a
	// production-logic bypass; the transport rewrite below is kept anyway
	// for defense in depth, matching the other two oracles.
	credential := providerfoundation.NewCredential("jira", "venue-oracle-cred",
		map[string]string{},
		map[string]secrets.Value{
			"email": secrets.NewValue(email), "api_token": secrets.NewValue(apiToken),
			"base_url": secrets.NewValue(stub.server.URL),
		},
	)
	stubURL, err := url.Parse(stub.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	previousClient := fakehttp.Client(discoveryHTTPClient)
	discoveryHTTPClient = &http.Client{Transport: rewriteHostTransport{target: stubURL}}
	t.Cleanup(func() { discoveryHTTPClient = fakehttp.Client(previousClient) })

	goTeams, err := discoverJira(t.Context(), credential)
	if err != nil {
		t.Fatalf("discoverJira: %v", err)
	}
	goBody, err := pyjson.Marshal(teamDiscoverResponseJSON("jira", goTeams, false, nil))
	if err != nil {
		t.Fatal(err)
	}
	goSignatures := stub.requestSignatures()
	stub.requests = nil

	// --- Python plane ------------------------------------------------------
	scriptPath := filepath.Join(packageDir, "testdata", "venue_oracle_discover_jira.py")
	pyBody, err := exec.Command(python, scriptPath, stub.server.URL, email, apiToken).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("execute Python venue oracle for jira discovery: %v: %s", err, exitErr.Stderr)
		}
		t.Fatalf("execute Python venue oracle for jira discovery: %v", err)
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
