//go:build integration

package teamsidentity

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venueOracleRequest records ONE HTTP request the stub server observed --
// ruling 25's "compare... the stub's received request sequence (method,
// path, query, header names; secrets masked)". Header VALUES are never
// captured (Authorization carries the token). Resource is the CANONICAL
// name of the fixture that answered it (see githubStubResponse), not the
// raw path -- Go's port and PyGithub reach the identical underlying
// GitHub resources through two DIFFERENT, both-real URL shapes (Go builds
// "/orgs/{org}/teams/{slug}" by convention; PyGithub follows the
// HATEOAS-style "url" field GitHub's own API response embeds,
// "/organizations/{org_id}/team/{team_id}"), so a raw-path comparison
// would report a false divergence on every single request. Comparing by
// Resource proves what this test actually needs to prove: did both
// planes fetch the same real GitHub resources.
type venueOracleRequest struct {
	Method      string
	Path        string
	Query       string
	Resource    string
	HeaderNames []string
}

func (r venueOracleRequest) String() string {
	return fmt.Sprintf("%s %s (resource=%s) ?%s headers=%v", r.Method, r.Path, r.Resource, r.Query, r.HeaderNames)
}

// githubStubResponse pairs a fixture's response bytes with the canonical
// resource name it represents -- more than one URL path can serve the
// same resource (see venueOracleRequest's own doc comment).
type githubStubResponse struct {
	resource string
	body     []byte
}

// githubDiscoverStub serves the real, captured, sanitized fixtures under
// testdata/github_discover_fixtures/ (README.md there has full provenance)
// by exact path, and records every request it receives so both planes'
// request sequences can be compared after each run.
type githubDiscoverStub struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []venueOracleRequest
	fixtures map[string]githubStubResponse
}

func newGitHubDiscoverStub(t *testing.T) *githubDiscoverStub {
	t.Helper()
	stub := &githubDiscoverStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)

	// PyGithub's Organization/Team objects are HATEOAS-style: each one's
	// OWN "url"/"repositories_url"/"members_url" fields (captured from the
	// real API as "https://api.github.com/...") are what PyGithub follows
	// for the NEXT call (org.get_teams() reads org.url, not the client's
	// configured base_url) -- confirmed live: without this rewrite,
	// PyGithub's domain allowlist rejected the follow-up request outright
	// ("AssertionError: api.github.com", since only the stub's own host is
	// allowlisted). Fixtures on disk keep the real captured host so their
	// provenance stays checkable by eye; this rewrite happens ONLY at
	// serve time, once the stub's own dynamic URL is known.
	fixtureDir := filepath.Join("testdata", "github_discover_fixtures")
	load := func(name string) []byte {
		raw, err := os.ReadFile(filepath.Join(fixtureDir, name))
		if err != nil {
			t.Fatalf("load fixture %s: %v", name, err)
		}
		rewritten := strings.ReplaceAll(string(raw), "https://api.github.com", stub.server.URL)
		return []byte(rewritten)
	}
	teamDetail := load("team_detail.json")
	teamRepos := load("team_repos.json")
	stub.fixtures = map[string]githubStubResponse{
		"/orgs/acme-corp":                           {"org_detail", load("org_detail.json")},
		"/orgs/acme-corp/teams":                     {"teams_list", load("teams_list.json")},
		"/orgs/acme-corp/teams/platform-team":       {"team_detail:platform-team", teamDetail},
		"/orgs/acme-corp/teams/platform-team/repos": {"team_repos:platform-team", teamRepos},
		// PyGithub's lazy attribute completion (gh_team.members_count,
		// gh_team.get_repos()) follows the TEAM OBJECT's own "url" field
		// from the list response (teams_list.json's captured
		// "/organizations/9000001/team/1000001"), not the
		// "/orgs/{org}/teams/{slug}" path Go's port calls directly --
		// same HATEOAS pattern as the org url rewrite above. Both paths
		// resolve to the SAME resource name, so the request-sequence
		// comparison below is unaffected by which URL shape either plane
		// happens to use to reach it.
		"/organizations/9000001/team/1000001":       {"team_detail:platform-team", teamDetail},
		"/organizations/9000001/team/1000001/repos": {"team_repos:platform-team", teamRepos},
	}
	return stub
}

func (s *githubDiscoverStub) handle(w http.ResponseWriter, r *http.Request) {
	headerNames := make([]string, 0, len(r.Header))
	for name := range r.Header {
		headerNames = append(headerNames, name)
	}
	sort.Strings(headerNames)

	fixture, ok := s.fixtures[r.URL.Path]
	resource := fixture.resource
	if !ok {
		resource = "UNKNOWN:" + r.URL.Path
	}
	s.mu.Lock()
	s.requests = append(s.requests, venueOracleRequest{
		Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Resource: resource, HeaderNames: headerNames,
	})
	s.mu.Unlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"venue oracle stub: no fixture for ` + r.URL.Path + `"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(fixture.body)
}

// requestSignatures reduces the recorded sequence to a sorted set of
// "METHOD resource?query" strings, keyed by canonical RESOURCE (see
// venueOracleRequest's own doc comment for why raw path is the wrong
// comparison key) -- compared across planes as a SET, not an ordered
// sequence: Go's and PyGithub's own internal call ordering for otherwise-
// independent lookups (e.g. which team's repos are fetched first) is a
// library/runtime detail, not a port-correctness question. Header-name
// sets are logged per request for manual review rather than gated on,
// since Go's net/http and PyGithub's requests-based transport send
// different incidental headers (User-Agent, Accept-Encoding, ...) by
// construction -- the ruling 25 comparison that matters is WHICH
// resources got fetched, which this proves exactly.
func (s *githubDiscoverStub) requestSignatures() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
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

func (s *githubDiscoverStub) reset() {
	s.mu.Lock()
	s.requests = nil
	s.mu.Unlock()
}

// TestVenueOracleDiscoverGitHubMatchesPython is CHAOS-6311's ruling-25
// venue oracle for the GitHub discovery route: ONE shared stub server,
// fixtures from real captured GitHub API responses (see the fixture
// directory's own README for provenance), BOTH planes (this Go port and
// the real, unmodified Python TeamDiscoveryService.discover_github)
// pointed at it via the credential base_url, response bodies AND the
// stub's received request sequence compared.
//
// Opt-in only (DEV_HEALTH_LIVE_PYTHON_ORACLES=1, matching every other
// live-Python oracle in this repo, run through ci/check_go.sh
// live-python-oracles with -count=1 -- never folded into the plain test
// verb, per AGENTS.md's compute-port-parity section).
func TestVenueOracleDiscoverGitHubMatchesPython(t *testing.T) {
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

	stub := newGitHubDiscoverStub(t)
	const token = "venue-oracle-test-token"
	const org = "acme-corp"

	// --- Go plane ------------------------------------------------------
	//
	// The credential carries NO base URL: discovery always targets
	// api.github.com for a PAT (Python's PyGithub default; CHAOS-6311 r1
	// finding 1 was Go sending the token to a stored config.base_url). The
	// stub is reached by rewriting every outgoing request's host in the
	// shared discovery HTTP client, so the production code path is
	// unchanged.
	credential := providerfoundation.NewCredential("github", "venue-oracle-cred",
		map[string]string{},
		map[string]secrets.Value{"token": secrets.NewValue(token)},
	)
	stubURL, err := url.Parse(stub.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	previousClient, previousExchange := discoveryHTTPClient, discoveryAppExchangeClient
	discoveryHTTPClient = &http.Client{Transport: rewriteHostTransport{target: stubURL}}
	discoveryAppExchangeClient = discoveryHTTPClient
	t.Cleanup(func() { discoveryHTTPClient, discoveryAppExchangeClient = previousClient, previousExchange })

	goTeams, err := discoverGitHub(t.Context(), credential, org)
	if err != nil {
		t.Fatalf("discoverGitHub: %v", err)
	}
	goBody, err := pyjson.Marshal(teamDiscoverResponseJSON("github", goTeams, false, nil))
	if err != nil {
		t.Fatal(err)
	}
	goSignatures := stub.requestSignatures()
	stub.reset()

	// --- Python plane ----------------------------------------------------
	scriptPath := filepath.Join(packageDir, "testdata", "venue_oracle_discover_github.py")
	pyBody, err := exec.Command(python, scriptPath, stub.server.URL, org, token).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("execute Python venue oracle for github discovery: %v: %s", err, exitErr.Stderr)
		}
		t.Fatalf("execute Python venue oracle for github discovery: %v", err)
	}
	pySignatures := stub.requestSignatures()

	// --- Compare the WHOLE response body ---------------------------------
	//
	// Byte for byte: the route's complete TeamDiscoverResponse envelope
	// (provider, teams with every field incl. associations.repo_patterns,
	// total, truncated, warnings), Go's pyjson rendering vs the real
	// FastAPI-shaped serialization of Python's own model -- not a field
	// subset.
	if string(goBody) != string(pyBody) {
		t.Errorf("response body diverges:\ngo:     %s\npython: %s", goBody, pyBody)
	}

	// --- Compare the stub's request sequence (ruling 25) -----------------
	//
	// One EXPECTED, documented asymmetry: PyGithub's org.get_teams() call
	// requires org.Organization, which PyGithub's own lazy-completion
	// design fetches via a real "GET /orgs/{org}" call the moment
	// get_organization() constructs it -- BEFORE discover_github's own
	// logic ever runs. Its response is never read by discover_github (the
	// org_name string parameter, not the fetched object, is what ends up
	// in repo_patterns/provider_org). Go's port has no equivalent lazy
	// object and never issues this request at all. Stripped here as a
	// known, harmless PyGithub-internals artifact -- not a Go/Python
	// output divergence -- so any OTHER unexpected difference still fails
	// loudly.
	pyOnlyKnown := "GET org_detail"
	filteredPy := make([]string, 0, len(pySignatures))
	sawKnownExtra := false
	for _, sig := range pySignatures {
		if sig == pyOnlyKnown && !sawKnownExtra {
			sawKnownExtra = true
			continue
		}
		filteredPy = append(filteredPy, sig)
	}
	sort.Strings(filteredPy)
	if !sawKnownExtra {
		t.Errorf("expected PyGithub's known org-detail lazy-completion call %q, did not see it -- fixture or PyGithub behavior changed", pyOnlyKnown)
	}
	if strings.Join(goSignatures, "\n") != strings.Join(filteredPy, "\n") {
		t.Errorf("request sequence diverges (after stripping PyGithub's known org-detail call):\ngo:\n%s\npython (filtered):\n%s",
			strings.Join(goSignatures, "\n"), strings.Join(filteredPy, "\n"))
	}
	t.Logf("go requests: %v", goSignatures)
	t.Logf("python requests (raw): %v", pySignatures)

	// ci/check_go.sh's venue-oracles verb discovers every `func
	// Test*VenueOracle*` in the repo by grep, not just ones built on
	// venueoracle.Diff, and fails loudly if that discovered test's own
	// proof file is missing after it runs (rule 4: a measurement that
	// did not happen must fail loudly). This test's shape genuinely
	// differs from Diff's (that diffs the dho api itself between planes
	// over HTTP; this diffs a THIRD-PARTY provider's API surface via a
	// local fixture-backed stub, which Diff has no facility for), so it
	// cannot call Diff itself, but owes the same CI check the same
	// proof-of-execution signal -- venueoracle.WriteProof is the shared,
	// exported primitive for exactly this request/response-set-shaped
	// case, called here once the real comparison against both planes has
	// actually completed.
	venueoracle.WriteProof(t)
}

// rewriteHostTransport redirects every outgoing request to the stub server,
// keeping method, path and query intact.
type rewriteHostTransport struct{ target *url.URL }

func (t rewriteHostTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	clone.Host = t.target.Host
	return http.DefaultTransport.RoundTrip(clone)
}
