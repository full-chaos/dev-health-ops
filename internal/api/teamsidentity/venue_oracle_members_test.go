//go:build integration

package teamsidentity

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// memberOracleStub serves GitHub and Linear member fixtures on one server
// and records "METHOD resource?canonical-query" per request.
type memberOracleStub struct {
	server *httptest.Server
	mu     sync.Mutex
	seen   []string
	files  map[string][]byte
	// linkBase is the origin the Link rel="next" header names. The Go plane
	// reaches the stub by rewriting requests to api.github.com, and its
	// client refuses a next URL on another host, so it is served
	// https://api.github.com; PyGithub is pointed at the stub through
	// base_url and refuses any host but the stub's.
	linkBase string
}

func newMemberOracleStub(t *testing.T) *memberOracleStub {
	t.Helper()
	stub := &memberOracleStub{files: map[string][]byte{}}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)
	stub.linkBase = stub.server.URL
	load := func(dir, name string) []byte {
		raw, err := os.ReadFile(filepath.Join("testdata", dir, name))
		if err != nil {
			t.Fatalf("load fixture %s/%s: %v", dir, name, err)
		}
		// PyGithub follows the "url" fields the payload embeds, so the
		// captured api.github.com host is rewritten to the stub at serve time.
		return []byte(strings.ReplaceAll(string(raw), "https://api.github.com", stub.server.URL))
	}
	stub.files["org"] = load("github_discover_fixtures", "org_detail.json")
	stub.files["team"] = load("github_discover_fixtures", "team_detail.json")
	stub.files["members"] = load("member_fixtures", "github_team_members.json")
	for i := 1; i <= 3; i++ {
		stub.files[fmt.Sprintf("user-%d", i)] = load("member_fixtures", fmt.Sprintf("github_user_%d.json", i))
	}
	return stub
}

func (s *memberOracleStub) setLinkBase(base string) {
	s.mu.Lock()
	s.linkBase = base
	s.mu.Unlock()
}

func (s *memberOracleStub) record(sig string) {
	s.mu.Lock()
	s.seen = append(s.seen, sig)
	s.mu.Unlock()
}

func (s *memberOracleStub) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.seen...)
	s.seen = nil
	sort.Strings(out)
	return out
}

func canonicalQueryString(values url.Values) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		for _, value := range values[key] {
			parts = append(parts, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}
	return strings.Join(parts, "&")
}

func (s *memberOracleStub) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := r.URL.Path
	switch {
	case path == "/orgs/acme-corp":
		s.record("GET org_detail")
		_, _ = w.Write(s.files["org"])
	case path == "/orgs/acme-corp/teams/platform-team":
		s.record("GET team_detail")
		_, _ = w.Write(s.files["team"])
	case path == "/organizations/9000001/team/1000001/members":
		s.record("GET team_members?" + canonicalQueryString(r.URL.Query()))
		var all []json.RawMessage
		_ = json.Unmarshal(s.files["members"], &all)
		if r.URL.Query().Get("page") == "2" {
			out, _ := json.Marshal(all[2:])
			_, _ = w.Write(out)
			return
		}
		s.mu.Lock()
		linkBase := s.linkBase
		s.mu.Unlock()
		w.Header().Set("Link", fmt.Sprintf(`<%s/organizations/9000001/team/1000001/members?page=2&per_page=100>; rel="next"`, linkBase))
		out, _ := json.Marshal(all[:2])
		_, _ = w.Write(out)
	case strings.HasPrefix(path, "/users/member-"):
		s.record("GET user " + strings.TrimPrefix(path, "/users/"))
		key := "user-" + strings.TrimPrefix(path, "/users/member-")
		if body, ok := s.files[key]; ok {
			_, _ = w.Write(body)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
	case path == "/graphql":
		s.linear(w, r)
	default:
		s.record("GET UNKNOWN " + path)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"venue oracle stub: no fixture"}`))
	}
}

// linear answers the two GraphQL operations the member discovery issues.
// Variables are recorded with null values dropped: Python sends
// "after": null on the first page, Go omits the key, and GraphQL treats the
// two the same.
func (s *memberOracleStub) linear(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var request struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	_ = json.Unmarshal(raw, &request)
	variables := map[string]any{}
	for key, value := range request.Variables {
		if value != nil {
			variables[key] = value
		}
	}
	encoded, _ := json.Marshal(variables)
	operation := "Teams"
	if strings.Contains(request.Query, "query TeamMembers") {
		operation = "TeamMembers"
	}
	s.record("POST graphql " + operation + " " + string(encoded))
	node := func(id, name, email string, active any) string {
		emailJSON := "null"
		if email != "" {
			emailJSON = fmt.Sprintf("%q", email)
		}
		activeJSON := "true"
		switch v := active.(type) {
		case bool:
			activeJSON = fmt.Sprint(v)
		case nil:
			return fmt.Sprintf(`{"id":%q,"name":%q,"email":%s}`, id, name, emailJSON)
		}
		return fmt.Sprintf(`{"id":%q,"name":%q,"email":%s,"active":%s}`, id, name, emailJSON, activeJSON)
	}
	page := func(nodes string, hasNext bool, cursor string) string {
		return fmt.Sprintf(`{"nodes":[%s],"pageInfo":{"hasNextPage":%t,"endCursor":%q}}`, nodes, hasNext, cursor)
	}
	team := func(id, key, members string) string {
		return fmt.Sprintf(`{"id":%q,"key":%q,"name":%q,"members":%s}`, id, key, key, members)
	}
	var body string
	switch {
	case operation == "TeamMembers" && variables["after"] == "mc1":
		body = `{"data":{"team":{"members":` + page(node("m3", "Linear Carol", "", true)+","+node("m4", "No Active Key", "m4@example.invalid", nil), false, "") + `}}}`
	case operation == "TeamMembers":
		body = `{"data":{"team":{"members":` + page(node("m1", "Lin B", "lin-b@example.invalid", true)+","+node("m2", "Gone", "gone@example.invalid", false), true, "mc1") + `}}}`
	case variables["after"] == "c1":
		eng := team("t-eng", "ENG", page(node("u1", "Lin A", "lin-a@example.invalid", true)+","+node("u2", "Inactive", "x@example.invalid", false)+","+node("u3", "No Email", "", true), false, ""))
		big := team("t-big", "BIG", page(node("m0", "Inline", "inline@example.invalid", true), true, "ic1"))
		body = `{"data":{"teams":` + page(eng+","+big, false, "") + `}}`
	default:
		body = `{"data":{"teams":` + page(team("t-oth", "OTH", page("", false, "")), true, "c1") + `}}`
	}
	_, _ = w.Write([]byte(body))
}

// memberIdentities loads testdata/member_fixtures/identities.json into the
// Go store type; the Python script builds ClickHouseIdentity from the same
// file.
func memberIdentities(t *testing.T) []Identity {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "member_fixtures", "identities.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		CanonicalID        string              `json:"canonical_id"`
		IdentityUUID       string              `json:"identity_uuid"`
		DisplayName        *string             `json:"display_name"`
		Email              *string             `json:"email"`
		ProviderIdentities map[string][]string `json:"provider_identities"`
		TeamIDs            []string            `json:"team_ids"`
		IsActive           bool                `json:"is_active"`
		UpdatedAt          time.Time           `json:"updated_at"`
		OrgID              string              `json:"org_id"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	out := make([]Identity, len(rows))
	for i, row := range rows {
		providers := pybody.NewOrderedStringListDict()
		keys := make([]string, 0, len(row.ProviderIdentities))
		for key := range row.ProviderIdentities {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			providers.Set(key, row.ProviderIdentities[key])
		}
		id := uuid.MustParse(row.IdentityUUID)
		out[i] = Identity{ID: id.String(), IdentityUUID: id, CanonicalID: row.CanonicalID, DisplayName: row.DisplayName, Email: row.Email,
			ProviderIdentities: providers, TeamIDs: row.TeamIDs, IsActive: row.IsActive, UpdatedAt: row.UpdatedAt, OrgID: row.OrgID}
	}
	return out
}

func runMemberOracle(t *testing.T, python string, args ...string) []byte {
	t.Helper()
	_, currentFile, _, _ := runtime.Caller(0)
	script := filepath.Join(filepath.Dir(currentFile), "testdata", "venue_oracle_members.py")
	out, err := exec.Command(python, append([]string{script}, args...)...).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("python member oracle %v: %v: %s", args[:2], err, exitErr.Stderr)
		}
		t.Fatalf("python member oracle: %v", err)
	}
	return out
}

func requireMemberOracleEnv(t *testing.T) string {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR") == "" {
		t.Fatal("live Python oracle opt-in requires a proof directory from ci/check_go.sh")
	}
	_, currentFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(currentFile))))
	python := pyoracle.Resolve(t, repoRoot)
	pyoracle.RequireDeployed(t, python)
	return python
}

func redirectDiscoveryClient(t *testing.T, stub *memberOracleStub) {
	t.Helper()
	stubURL, err := url.Parse(stub.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	previous := discoveryHTTPClient
	discoveryHTTPClient = &http.Client{Transport: rewriteHostTransport{target: stubURL}}
	t.Cleanup(func() { discoveryHTTPClient = previous })
}

func goMembersBody(t *testing.T, teamID, provider string, members []discoveredMember, err error) string {
	t.Helper()
	if err != nil {
		t.Fatalf("discover members (go): %v", err)
	}
	matches, err := matchMembers(members, memberIdentities(t))
	if err != nil {
		t.Fatal(err)
	}
	body, err := pyjson.Marshal(membersDiscoverJSON(teamID, provider, matches))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// TestVenueOracleDiscoverGitHubMembersMatchesPython runs discover_members_
// github + match_members (real Python) and the Go port against ONE stub
// serving sanitized public GitHub member payloads: whole response body and
// the provider requests are compared.
func TestVenueOracleDiscoverGitHubMembersMatchesPython(t *testing.T) {
	python := requireMemberOracleEnv(t)
	stub := newMemberOracleStub(t)
	redirectDiscoveryClient(t, stub)
	identities := filepath.Join("testdata", "member_fixtures", "identities.json")

	stub.setLinkBase("https://api.github.com")
	goMembers, err := discoverMembersGitHub(t.Context(), secrets.NewValue("venue-token"), "acme-corp", "platform-team")
	goBody := goMembersBody(t, "gh:platform-team", "github", goMembers, err)
	goRequests := stub.take()

	stub.setLinkBase(stub.server.URL)
	pyBody := runMemberOracle(t, python, "github", stub.server.URL, "acme-corp", "platform-team", "venue-token", identities)
	pyRequests := stub.take()

	if goBody != string(pyBody) {
		t.Errorf("response body diverges:\ngo:     %s\npython: %s", goBody, pyBody)
	}
	// PyGithub completes the lazily built Organization with GET /orgs/{org}
	// before it lists the team's members; Go has no such object. Every other
	// request must agree.
	filtered := make([]string, 0, len(pyRequests))
	sawOrg := false
	for _, sig := range pyRequests {
		if sig == "GET org_detail" && !sawOrg {
			sawOrg = true
			continue
		}
		filtered = append(filtered, sig)
	}
	if !sawOrg {
		t.Errorf("expected PyGithub's organization completion request; python saw %v", pyRequests)
	}
	if strings.Join(goRequests, "\n") != strings.Join(filtered, "\n") {
		t.Errorf("provider requests diverge:\ngo:\n%s\npython (organization completion removed):\n%s", strings.Join(goRequests, "\n"), strings.Join(filtered, "\n"))
	}
	if !strings.Contains(goBody, `"match_status":"matched"`) || !strings.Contains(goBody, `"suggested"`) || !strings.Contains(goBody, `"unmatched"`) {
		t.Errorf("the fixture set must reach all three match outcomes: %s", goBody)
	}
	t.Logf("go requests: %v", goRequests)
	venueoracle.WriteProof(t)
}

// TestVenueOracleDiscoverLinearMembersMatchesPython does the same for the
// Linear team members flow, including the inline member page, the follow-up
// TeamMembers query (hasNextPage), a "linear:" prefixed key, and a team key
// no page carries.
func TestVenueOracleDiscoverLinearMembersMatchesPython(t *testing.T) {
	python := requireMemberOracleEnv(t)
	stub := newMemberOracleStub(t)
	redirectDiscoveryClient(t, stub)
	identities := filepath.Join("testdata", "member_fixtures", "identities.json")

	for _, teamKey := range []string{"ENG", "linear:BIG", "NOPE"} {
		goMembers, err := discoverMembersLinear(t.Context(), secrets.NewValue("venue-key"), teamKey)
		goBody := goMembersBody(t, teamKey, "linear", goMembers, err)
		goRequests := stub.take()

		pyBody := runMemberOracle(t, python, "linear", stub.server.URL, "venue-key", teamKey, identities)
		pyRequests := stub.take()

		if goBody != string(pyBody) {
			t.Errorf("%s: response body diverges:\ngo:     %s\npython: %s", teamKey, goBody, pyBody)
		}
		if strings.Join(goRequests, "\n") != strings.Join(pyRequests, "\n") {
			t.Errorf("%s: provider requests diverge:\ngo:\n%s\npython:\n%s", teamKey, strings.Join(goRequests, "\n"), strings.Join(pyRequests, "\n"))
		}
		t.Logf("%s go body: %s", teamKey, goBody)
	}
	venueoracle.WriteProof(t)
}
