//go:build integration

package teamsidentity

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// gitlabDiscoverStub serves the real, captured, sanitized fixtures under
// testdata/gitlab_discover_fixtures/ (README.md there has full provenance)
// by exact path, and records every request it receives so both planes'
// request sequences can be compared after each run. See the fixture
// directory's own README for the deliberate, named coverage gap: the
// captured group has no subgroups, so this stub cannot exercise the
// non-empty-subgroup-page code path.
type gitlabDiscoverStub struct {
	server   *httptest.Server
	requests []venueOracleRequest
	fixtures map[string][]byte
}

func newGitLabDiscoverStub(t *testing.T) *gitlabDiscoverStub {
	t.Helper()
	stub := &gitlabDiscoverStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)

	fixtureDir := filepath.Join("testdata", "gitlab_discover_fixtures")
	load := func(name string) []byte {
		raw, err := os.ReadFile(filepath.Join(fixtureDir, name))
		if err != nil {
			t.Fatalf("load fixture %s: %v", name, err)
		}
		return raw
	}
	group := load("group.json")
	subgroups := load("subgroups.json")
	// The flat include_subgroups=true walk answers the SAME response as the
	// plain per-group walk: with zero subgroups (the captured group has
	// none) GitLab's real API returns an identical project set either way
	// -- see the fixture README for why this is what the real API would
	// answer, not a stand-in.
	projects := load("projects.json")
	stub.fixtures = map[string][]byte{
		"/api/v4/groups/acme-corp": group,
		// Named asymmetry (observed live, not assumed): discoverGitLab
		// addresses the subgroups endpoint by the group PATH it was given
		// (url.PathEscape(groupPath)); discover_gitlab addresses it by the
		// NUMERIC id of the group object python-gitlab already fetched
		// (root_group.subgroups.list()). GitLab's API accepts either as the
		// same group's :id, and both forms are recorded under the same
		// canonical "subgroups" resource by canonicalGitLabResource below,
		// so this is proven equivalent, not papered over. discoverGitLab's
		// own project-list calls, unlike its subgroups call, already use
		// the numeric id (see discover_gitlab.go); only the subgroups call
		// has this inconsistency.
		"/api/v4/groups/acme-corp/subgroups": subgroups,
		"/api/v4/groups/9100001/subgroups":   subgroups,
		"/api/v4/groups/9100001/projects":    projects,
	}
	return stub
}

// canonicalGitLabResource maps a path this stub serves to the resource name
// used in the request-sequence comparison, folding the group-path/numeric-id
// addressing asymmetry documented above into one name.
func canonicalGitLabResource(path string) string {
	switch path {
	case "/api/v4/groups/acme-corp/subgroups", "/api/v4/groups/9100001/subgroups":
		return "/api/v4/groups/{id}/subgroups"
	default:
		return path
	}
}

func (s *gitlabDiscoverStub) handle(w http.ResponseWriter, r *http.Request) {
	headerNames := make([]string, 0, len(r.Header))
	for name := range r.Header {
		headerNames = append(headerNames, name)
	}
	sort.Strings(headerNames)

	// Fixture routing matches by PATH only: every call carries GitLab's own
	// pagination params (per_page/page, and include_subgroups=true on the
	// flat walk), which this single-page fixture set answers identically
	// either way (see the fixture README). The resource name recorded for
	// the request-sequence comparison keeps include_subgroups visible, so a
	// real divergence in which query either plane sends still shows up.
	body, ok := s.fixtures[r.URL.Path]
	resource := canonicalGitLabResource(r.URL.Path)
	if !ok {
		resource = "UNKNOWN:" + r.URL.Path
	} else if strings.Contains(strings.ToLower(r.URL.RawQuery), "include_subgroups=true") {
		resource += ":include_subgroups"
	}
	s.requests = append(s.requests, venueOracleRequest{
		Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Resource: resource, HeaderNames: headerNames,
	})

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"venue oracle stub: no fixture for ` + r.URL.Path + `"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func (s *gitlabDiscoverStub) requestSignatures() []string {
	out := make([]string, len(s.requests))
	for i, req := range s.requests {
		sig := req.Method + " " + req.Resource
		out[i] = sig
	}
	sort.Strings(out)
	return out
}

// TestVenueOracleDiscoverGitLabMatchesPython is CHAOS-6374's venue oracle for
// the GitLab discovery route: ONE shared stub server, fixtures from a real
// captured GitLab API response (see the fixture directory's own README for
// provenance and its named coverage gap), BOTH planes (this Go port and the
// real, unmodified Python TeamDiscoveryService.discover_gitlab) pointed at
// it, response bodies AND the stub's received request sequence compared.
//
// Opt-in only (DEV_HEALTH_LIVE_PYTHON_ORACLES=1, matching every other
// live-Python oracle in this repo, run through ci/check_go.sh
// live-python-oracles with -count=1).
func TestVenueOracleDiscoverGitLabMatchesPython(t *testing.T) {
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

	stub := newGitLabDiscoverStub(t)
	const token = "venue-oracle-test-token"
	const groupPath = "acme-corp"

	// --- Go plane --------------------------------------------------------
	credential := providerfoundation.NewCredential("gitlab", "venue-oracle-cred",
		map[string]string{},
		map[string]secrets.Value{"token": secrets.NewValue(token)},
	)
	stubURL, err := url.Parse(stub.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	previousClient := discoveryHTTPClient
	discoveryHTTPClient = &http.Client{Transport: rewriteHostTransport{target: stubURL}}
	t.Cleanup(func() { discoveryHTTPClient = previousClient })

	goTeams, goTruncated, goWarnings, err := discoverGitLab(t.Context(), credential, groupPath)
	if err != nil {
		t.Fatalf("discoverGitLab: %v", err)
	}
	goBody, err := pyjson.Marshal(teamDiscoverResponseJSON("gitlab", goTeams, goTruncated, goWarnings))
	if err != nil {
		t.Fatal(err)
	}
	goSignatures := stub.requestSignatures()
	stub.requests = nil

	// --- Python plane ------------------------------------------------------
	scriptPath := filepath.Join(packageDir, "testdata", "venue_oracle_discover_gitlab.py")
	pyBody, err := exec.Command(python, scriptPath, stub.server.URL, groupPath, token).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("execute Python venue oracle for gitlab discovery: %v: %s", err, exitErr.Stderr)
		}
		t.Fatalf("execute Python venue oracle for gitlab discovery: %v", err)
	}
	pySignatures := stub.requestSignatures()

	// --- Compare the WHOLE response body -----------------------------------
	if string(goBody) != string(pyBody) {
		t.Errorf("response body diverges:\ngo:     %s\npython: %s", goBody, pyBody)
	}

	// --- Compare the stub's request sequence --------------------------------
	if strings := goSignatures; !equalStrings(strings, pySignatures) {
		t.Errorf("request sequence diverges:\ngo:\n%v\npython:\n%v", goSignatures, pySignatures)
	}
	t.Logf("go requests: %v", goSignatures)
	t.Logf("python requests: %v", pySignatures)

	venueoracle.WriteProof(t)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
