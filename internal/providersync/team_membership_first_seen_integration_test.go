//go:build integration

package providersync

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-9007: a membership is one open row per fact (org, provider, team,
// member, source) however many syncs run, and its valid_from is the first time
// the fact was observed. Each test below runs the REAL collector of one
// provider against real ClickHouse three times, then once more with a new
// member, and reads team_memberships back.

var membershipSyncAt = []time.Time{
	time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 1, 11, 0, 0, 0, time.UTC),
}

// openMembershipFacts is the open rows of a provider after FINAL, per fact
// "<team>|<member>": the valid_from of each open row of the fact.
func openMembershipFacts(ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider string) map[string][]time.Time {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT team_id, member_id, valid_from FROM team_memberships FINAL WHERE org_id = ? AND provider = ? AND valid_to IS NULL`, orgID, provider)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	facts := map[string][]time.Time{}
	for rows.Next() {
		var team, member string
		var from time.Time
		if err := rows.Scan(&team, &member, &from); err != nil {
			t.Fatal(err)
		}
		facts[team+"|"+member] = append(facts[team+"|"+member], from.UTC())
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return facts
}

// requireOneOpenRowPerFact holds the three properties of the rule over the
// facts read back after the runs: one open row per fact; an existing fact keeps
// its first valid_from byte for byte; a new member is stamped with its first run.
func requireOneOpenRowPerFact(t *testing.T, label string, facts map[string][]time.Time, existing []string, added string) {
	t.Helper()
	keys := make([]string, 0, len(facts))
	for key, rows := range facts {
		keys = append(keys, key)
		if len(rows) != 1 {
			t.Errorf("%s: fact %s has %d open rows %v, want 1", label, key, len(rows), rows)
		}
	}
	sort.Strings(keys)
	for _, key := range existing {
		rows, ok := facts[key]
		if !ok || len(rows) == 0 {
			t.Errorf("%s: existing fact %s has no open row (facts %v)", label, key, keys)
			continue
		}
		if !rows[0].Equal(membershipSyncAt[0]) {
			t.Errorf("%s: existing fact %s valid_from = %s, want its first run %s", label, key, rows[0], membershipSyncAt[0])
		}
	}
	rows, ok := facts[added]
	if !ok || len(rows) == 0 {
		t.Errorf("%s: the new member %s has no open row (facts %v)", label, added, keys)
	} else if !rows[0].Equal(membershipSyncAt[3]) {
		t.Errorf("%s: the new member %s valid_from = %s, want its first run %s", label, added, rows[0], membershipSyncAt[3])
	}
	if want := len(existing) + 1; len(facts) != want {
		t.Errorf("%s: %d facts %v, want %d", label, len(facts), keys, want)
	}
}

func TestGitHubMembershipsKeepOneOpenRowPerFactAcrossSyncs(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9007-github"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	sync := func(at time.Time, logins ...string) {
		t.Helper()
		members := "["
		for index, login := range logins {
			if index > 0 {
				members += ","
			}
			members += fmt.Sprintf(`{"login":%q}`, login)
		}
		members += "]"
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
			"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform","description":"Platform team"}]`,
			"/orgs/acme/teams/platform/repos":   `[{"name":"api"}]`,
			"/orgs/acme/teams/platform/members": members,
		}}
		adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
		if _, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)),
			TeamCatalogSelections{Teams: true, Members: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	for _, at := range membershipSyncAt[:3] {
		sync(at, "octocat", "hubot")
	}
	sync(membershipSyncAt[3], "octocat", "hubot", "monalisa")
	requireOneOpenRowPerFact(t, "github", openMembershipFacts(ctx, t, conn, orgID, "github"),
		[]string{"gh:platform|gh:octocat", "gh:platform|gh:hubot"}, "gh:platform|gh:monalisa")
}

func TestGitLabMembershipsKeepOneOpenRowPerFactAcrossSyncs(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9007-gitlab"
	fake := newGitLabMembersServer(t, "alice", "carol")
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	sync := func(at time.Time) {
		t.Helper()
		collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
			Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}, ScopeCensus: staticScopeCensus{}}
		ref := TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"}
		if _, err := collector.CollectTeamCatalog(ctx, ref, credential, gitlabTeamCatalogTestClient(t, fake.URL),
			TeamCatalogSelections{Teams: true, Members: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	for _, at := range membershipSyncAt[:3] {
		sync(at)
	}
	before := openMembershipFacts(ctx, t, conn, orgID, "gitlab")
	if len(before) == 0 {
		t.Fatal("the GitLab fixture wrote no membership: the test measures nothing")
	}
	for key, rows := range before {
		if len(rows) != 1 || !rows[0].Equal(membershipSyncAt[0]) {
			t.Errorf("gitlab: fact %s after 3 syncs = %v, want one open row at %s", key, rows, membershipSyncAt[0])
		}
	}
	// A new member: the fixture's group answers one more member.
	fake.setMembers("alice", "carol", "bob")
	sync(membershipSyncAt[3])
	after := openMembershipFacts(ctx, t, conn, orgID, "gitlab")
	existing := make([]string, 0, len(before))
	for key := range before {
		existing = append(existing, key)
	}
	var added string
	for key := range after {
		if _, ok := before[key]; !ok {
			added = key
		}
	}
	if added == "" {
		t.Fatalf("the new member wrote no fact: before %v after %v", before, after)
	}
	requireOneOpenRowPerFact(t, "gitlab", after, existing, added)
}

func TestLinearMembershipsKeepOneOpenRowPerFactAcrossSyncs(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	sync := func(at time.Time, members string) {
		t.Helper()
		doer := &linearWorkItemsDoer{responses: []string{
			`{"data":{"teams":{"nodes":[` +
				`{"id":"team-raw-eng","key":"ENG","name":"Provider Eng","members":{"nodes":[` + members + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}` +
				`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
			`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
			`{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		}}
		collector := LinearTeamCatalogCollector{
			ScopeCensus: staticScopeCensus{},
			Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10},
			Sink:        LinearReferenceCatalogClickHouseEffects{Conn: f.conn, Lease: carrySeamLease()},
		}
		claim := nativeTestClaim("linear", "work-items")
		claim.OrgID = f.orgID
		ref := teamCatalogRefFromClaim(claim)
		ref.Strict = true
		if _, err := collector.CollectTeamCatalog(f.ctx, ref,
			providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID}, linearWorkItemsClient(t, fakehttp.Client(doer)),
			TeamCatalogSelections{Teams: true, Members: true, Projects: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	alice := `{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}`
	bob := `{"id":"user-2","name":"Bob","email":"bob@example.com","active":true}`
	for _, at := range membershipSyncAt[:3] {
		sync(at, alice)
	}
	sync(membershipSyncAt[3], alice+","+bob)
	facts := openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear")
	var existing []string
	var added string
	for key := range facts {
		if len(facts[key]) > 0 && facts[key][0].Equal(membershipSyncAt[3]) {
			added = key
		} else {
			existing = append(existing, key)
		}
	}
	if added == "" || len(existing) != 1 {
		t.Fatalf("linear: facts %v: want one fact first seen at %s and one older", facts, membershipSyncAt[3])
	}
	requireOneOpenRowPerFact(t, "linear", facts, existing, added)
}

// gitlabMembersServer is a GitLab group tree of one subgroup, org/team-a,
// whose member list a test can change between runs. It answers no project.
type gitlabMembersServer struct {
	*httptest.Server
	mu      sync.Mutex
	members []string
	// endless answers every page of the team's member list with one member and a
	// next page, so the walk ends on its page budget, not on the provider's end.
	endless bool
	// raw, when set, is the exact member list answered (a node the collector
	// cannot use, for example).
	raw []map[string]any
	// noEndSignal leaves the end-of-list header off: the provider never says the list ended.
	noEndSignal bool
	// A direct lookup (GET /groups/org%2Fteam-a/members?query=<username>) is answered
	// from the provider's truth: lookupMembers (the list when nil). lookupStatus,
	// when set, is the status every lookup answers; lookups counts them.
	lookupMembers []string
	lookupStatus  int
	lookupNoEnd   bool // the lookup answer carries no end-of-list signal
	lookups       int
}

func newGitLabMembersServer(t *testing.T, usernames ...string) *gitlabMembersServer {
	t.Helper()
	fake := &gitlabMembersServer{members: usernames}
	fake.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		switch r.URL.EscapedPath() {
		case "/api/v4/groups/org":
			writeGitLabTeamCatalogJSON(t, w, map[string]any{"id": 1, "full_path": "org", "name": "Org", "description": nil})
		case "/api/v4/groups/org/subgroups":
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{{"id": 2, "full_path": "org/team-a", "name": "Team A", "description": nil}})
		case "/api/v4/groups/org/projects", "/api/v4/groups/org%2Fteam-a/projects":
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{})
		case "/api/v4/groups/org/members":
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{})
		case "/api/v4/groups/org%2Fteam-a/members":
			if query := r.URL.Query().Get("query"); query != "" {
				fake.lookups++
				if fake.lookupStatus != 0 {
					http.Error(w, "{}", fake.lookupStatus)
					return
				}
				truth := fake.members
				if fake.lookupMembers != nil {
					truth = fake.lookupMembers
				}
				out := []map[string]any{}
				for _, username := range truth {
					if strings.Contains(strings.ToLower(username), strings.ToLower(query)) { // GitLab's search is case-insensitive
						out = append(out, map[string]any{"username": username, "name": username})
					}
				}
				if !fake.lookupNoEnd {
					w.Header()["X-Next-Page"] = []string{""}
				}
				writeGitLabTeamCatalogJSON(t, w, out)
				return
			}
			if fake.endless {
				page, _ := strconv.Atoi(r.URL.Query().Get("page"))
				if page < 1 {
					page = 1
				}
				w.Header().Set("X-Next-Page", strconv.Itoa(page+1))
				writeGitLabTeamCatalogJSON(t, w, []map[string]any{{"username": fmt.Sprintf("endless-%d", page), "name": "x", "email": fmt.Sprintf("endless-%d@example.com", page)}})
				return
			}
			out := []map[string]any{}
			for _, username := range fake.members {
				out = append(out, map[string]any{"username": username, "name": username, "email": username + "@example.com"})
			}
			if fake.raw != nil {
				out = fake.raw
			}
			// GitLab's end of an offset listing: X-Next-Page sent and empty.
			if !fake.noEndSignal {
				w.Header()["X-Next-Page"] = []string{""}
			}
			writeGitLabTeamCatalogJSON(t, w, out)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	return fake
}

func (fake *gitlabMembersServer) setRaw(raw []map[string]any, noEndSignal bool) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.raw, fake.noEndSignal = raw, noEndSignal
}

// setLookup sets what the provider answers to a direct lookup: the members it
// says exist (nil: the list), and a status to answer instead (0: none).
func (fake *gitlabMembersServer) setLookup(members []string, status int) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.lookupMembers, fake.lookupStatus = members, status
}

// setLookupNoEnd makes the lookup answer leave GitLab's end-of-list signal off.
func (fake *gitlabMembersServer) setLookupNoEnd(noEnd bool) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.lookupNoEnd = noEnd
}

func (fake *gitlabMembersServer) lookupCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.lookups
}

func (fake *gitlabMembersServer) setEndless(endless bool) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.endless = endless
}

func (fake *gitlabMembersServer) setMembers(usernames ...string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.members = usernames
}
