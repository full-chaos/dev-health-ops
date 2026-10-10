//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

// A member who is absent from the list of a team is only a CANDIDATE for a
// close. The list of GitHub and GitLab is paged by offset: a member who leaves
// between two page requests moves every later member one place up, and one of
// them is on no page, although the last page carries the provider's end signal.
// A close needs the provider's own direct lookup to say "not a member"; a lookup
// that fails, is refused, or is over budget closes nothing.

type memberLogs struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (logs *memberLogs) Write(p []byte) (int, error) {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	return logs.buffer.Write(p)
}

// captureMemberLogs replaces the default logger for the test.
func captureMemberLogs(t *testing.T) *memberLogs {
	t.Helper()
	previous := slog.Default()
	logs := &memberLogs{}
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return logs
}

// lines are the log entries of the given messages.
func (logs *memberLogs) lines(messages ...string) []map[string]any {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(logs.buffer.String(), "\n") {
		var entry map[string]any
		if json.Unmarshal([]byte(line), &entry) != nil {
			continue
		}
		for _, message := range messages {
			if entry["msg"] == message {
				out = append(out, entry)
			}
		}
	}
	return out
}

func (logs *memberLogs) text() string {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	return logs.buffer.String()
}

type funcDoer func(*http.Request) (*http.Response, error)

func (doer funcDoer) Do(request *http.Request) (*http.Response, error) { return doer(request) }

func jsonResponse(request *http.Request, status int, body string, header http.Header) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	header.Set("Content-Type", "application/json")
	return &http.Response{StatusCode: status, Status: strconv.Itoa(status), Header: header,
		Body: io.NopCloser(strings.NewReader(body)), Request: request}
}

// seedMembership writes one team_memberships row as the catalog writers do.
func seedMembership(ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider, team, member, source string, from time.Time, to *time.Time, updated time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, valid_from, valid_to, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, orgID, provider, team, member, member, nil, []string{member}, source, from, to, updated); err != nil {
		t.Fatal(err)
	}
}

// ---- the offset shift, GitHub and GitLab --------------------------------------

func TestGitHubMemberWhoDidNotLeaveIsNotClosedWhenThePagesShift(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-github-shift"
	logs := captureMemberLogs(t)
	var mu sync.Mutex
	members := make([]string, 0, 150)
	for index := 0; index < 150; index++ {
		members = append(members, fmt.Sprintf("user-%03d", index))
	}
	removeFirstAfterPageOne := false
	lookups := 0
	doer := funcDoer(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case request.URL.Path == "/orgs/acme/teams":
			return jsonResponse(request, 200, `[{"slug":"platform","name":"Platform"}]`, nil), nil
		case request.URL.Path == "/orgs/acme/teams/platform/repos":
			return jsonResponse(request, 200, `[]`, nil), nil
		case strings.HasPrefix(request.URL.Path, "/orgs/acme/teams/platform/memberships/"):
			lookups++
			login := strings.TrimPrefix(request.URL.Path, "/orgs/acme/teams/platform/memberships/")
			for _, member := range members {
				if member == login {
					return jsonResponse(request, 200, `{"state":"active","role":"member"}`, nil), nil
				}
			}
			return jsonResponse(request, 404, `{"message":"Not Found"}`, nil), nil
		case request.URL.Path == "/orgs/acme/teams/platform/members":
			page, _ := strconv.Atoi(request.URL.Query().Get("page"))
			perPage, _ := strconv.Atoi(request.URL.Query().Get("per_page"))
			if page < 1 {
				page = 1
			}
			if perPage < 1 {
				perPage = 100
			}
			start, end := (page-1)*perPage, page*perPage
			if start > len(members) {
				start = len(members)
			}
			header := http.Header{}
			if end >= len(members) {
				end = len(members)
				header.Set("Link", `<https://api.github.com/orgs/acme/teams/platform/members?per_page=100&page=1>; rel="first"`)
			} else {
				header.Set("Link", fmt.Sprintf(`<https://api.github.com/orgs/acme/teams/platform/members?per_page=%d&page=%d>; rel="next"`, perPage, page+1))
			}
			var nodes []string
			for _, login := range members[start:end] {
				nodes = append(nodes, `{"login":"`+login+`"}`)
			}
			if page == 1 && removeFirstAfterPageOne {
				members = members[1:] // user-000 leaves while the walk is between pages
			}
			return jsonResponse(request, 200, `[`+strings.Join(nodes, ",")+`]`, header), nil
		}
		t.Fatalf("unexpected request %s", request.URL)
		return nil, nil
	})
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	run := func(at time.Time) TeamCatalogResult {
		t.Helper()
		adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
		result, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)), TeamCatalogSelections{Teams: true, Members: true}, at)
		if err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
		return result
	}
	run(departureAt[0])
	mu.Lock()
	removeFirstAfterPageOne = true
	mu.Unlock()
	result := run(departureAt[1])
	facts := openMembershipFacts(ctx, t, conn, orgID, "github")
	// user-100 never left: the shifted pages did not list it, the provider still does.
	requireOpen(t, "after the shifted walk", facts, "gh:platform|gh:user-100", departureAt[0])
	// user-000 was on page 1, read before it left: it is not a candidate yet.
	requireOpen(t, "after the shifted walk", facts, "gh:platform|gh:user-000", departureAt[0])
	if result.MembershipsClosed != 0 {
		t.Errorf("the shifted run closed %d members, want 0", result.MembershipsClosed)
	}
	mu.Lock()
	if lookups != 1 {
		t.Errorf("the provider was asked %d time(s), want one lookup, for the one member the pages lacked (user-100)", lookups)
	}
	removeFirstAfterPageOne = false
	mu.Unlock()
	// The next clean run keeps user-100 on its first-seen date and closes user-000,
	// who left: the provider's own lookup says so.
	result = run(departureAt[2])
	facts = openMembershipFacts(ctx, t, conn, orgID, "github")
	requireOpen(t, "after the next run", facts, "gh:platform|gh:user-100", departureAt[0])
	requireNotOpen(t, "after the next run", facts, "gh:platform|gh:user-000")
	if result.MembershipsClosed != 1 {
		t.Errorf("the next run closed %d members, want 1 (user-000)", result.MembershipsClosed)
	}
	// What the logs say: the close names the team and the provider, never a member.
	closedLines := logs.lines("team_membership_closed")
	if len(closedLines) != 1 || closedLines[0]["team"] != "platform" || closedLines[0]["provider"] != "github" || closedLines[0]["closed"] != float64(1) {
		t.Errorf("close log lines = %v, want one line of team=platform provider=github closed=1", closedLines)
	}
	if strings.Contains(logs.text(), "user-000") || strings.Contains(logs.text(), "user-100") {
		t.Error("a log line holds a member login")
	}
}

func TestGitLabMemberWhoDidNotLeaveIsNotClosedWhenThePagesShift(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-gitlab-shift"
	var mu sync.Mutex
	members := make([]string, 0, 150)
	for index := 0; index < 150; index++ {
		members = append(members, fmt.Sprintf("user-%03d", index))
	}
	removeFirstAfterPageOne := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.EscapedPath() {
		case "/api/v4/groups/org":
			writeGitLabTeamCatalogJSON(t, w, map[string]any{"id": 1, "full_path": "org", "name": "Org"})
		case "/api/v4/groups/org/subgroups", "/api/v4/groups/org/projects":
			w.Header()["X-Next-Page"] = []string{""}
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{})
		case "/api/v4/groups/org/members":
			if query := r.URL.Query().Get("query"); query != "" {
				out := []map[string]any{}
				for _, username := range members {
					if strings.Contains(username, query) {
						out = append(out, map[string]any{"username": username, "name": username})
					}
				}
				w.Header()["X-Next-Page"] = []string{""}
				writeGitLabTeamCatalogJSON(t, w, out)
				return
			}
			page, _ := strconv.Atoi(r.URL.Query().Get("page"))
			perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
			if page < 1 {
				page = 1
			}
			start, end := (page-1)*perPage, page*perPage
			if start > len(members) {
				start = len(members)
			}
			next := strconv.Itoa(page + 1)
			if end >= len(members) {
				end, next = len(members), ""
			}
			out := []map[string]any{}
			for _, username := range members[start:end] {
				out = append(out, map[string]any{"username": username, "name": username})
			}
			w.Header()["X-Next-Page"] = []string{next}
			writeGitLabTeamCatalogJSON(t, w, out)
			if page == 1 && removeFirstAfterPageOne {
				members = members[1:] // user-000 leaves while the walk is between pages
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	sync := func(at time.Time) TeamCatalogResult {
		t.Helper()
		collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
			Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}, ScopeCensus: staticScopeCensus{}}
		result, err := collector.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, gitlabTeamCatalogTestClient(t, server.URL), TeamCatalogSelections{Teams: true, Members: true}, at)
		if err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
		return result
	}
	sync(departureAt[0])
	mu.Lock()
	removeFirstAfterPageOne = true
	mu.Unlock()
	result := sync(departureAt[1])
	facts := openMembershipFacts(ctx, t, conn, orgID, "gitlab")
	requireOpen(t, "after the shifted walk", facts, "gl:org|gl:user-100", departureAt[0])
	requireOpen(t, "after the shifted walk", facts, "gl:org|gl:user-000", departureAt[0]) // read on page 1, before it left
	if result.MembershipsClosed != 0 {
		t.Errorf("the shifted run closed %d members, want 0", result.MembershipsClosed)
	}
	mu.Lock()
	removeFirstAfterPageOne = false
	mu.Unlock()
	result = sync(departureAt[2])
	facts = openMembershipFacts(ctx, t, conn, orgID, "gitlab")
	requireOpen(t, "after the next run", facts, "gl:org|gl:user-100", departureAt[0])
	requireNotOpen(t, "after the next run", facts, "gl:org|gl:user-000")
	if result.MembershipsClosed != 1 {
		t.Errorf("the next run closed %d members, want 1 (user-000)", result.MembershipsClosed)
	}
}

// ---- what a lookup can answer ---------------------------------------------------

func TestGitHubCandidateStaysOpenUnlessTheLookupSaysNotAMember(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-github-lookup"
	logs := captureMemberLogs(t)
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	// lookup is what the provider answers to GET .../memberships/hubot.
	type answer struct {
		status int
		body   string
		err    error
	}
	run := func(at time.Time, members string, lookup answer) TeamCatalogResult {
		t.Helper()
		doer := funcDoer(func(request *http.Request) (*http.Response, error) {
			switch request.URL.Path {
			case "/orgs/acme/teams":
				return jsonResponse(request, 200, `[{"slug":"platform","name":"Platform"}]`, nil), nil
			case "/orgs/acme/teams/platform/repos":
				return jsonResponse(request, 200, `[]`, nil), nil
			case "/orgs/acme/teams/platform/members":
				return jsonResponse(request, 200, members, nil), nil
			case "/orgs/acme/teams/platform/memberships/hubot":
				if lookup.err != nil {
					return nil, lookup.err
				}
				return jsonResponse(request, lookup.status, lookup.body, nil), nil
			}
			t.Fatalf("unexpected request %s", request.URL)
			return nil, nil
		})
		adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
		result, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)), TeamCatalogSelections{Teams: true, Members: true}, at)
		if err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
		return result
	}
	const hubot = "gh:platform|gh:hubot"
	both, only := `[{"login":"octocat"},{"login":"hubot"}]`, `[{"login":"octocat"}]`
	run(departureAt[0], both, answer{status: 200, body: `{"state":"active"}`})
	for index, c := range []struct {
		name   string
		lookup answer
		reason string
	}{
		{"the provider says the person is a member", answer{status: 200, body: `{"state":"active","role":"member"}`}, "provider_says_member"},
		{"the provider says the invitation is pending", answer{status: 200, body: `{"state":"pending"}`}, "provider_says_member"},
		{"the lookup answers 500", answer{status: 500, body: `{}`}, "lookup_not_proven"},
		{"the lookup is forbidden", answer{status: 403, body: `{"message":"Forbidden"}`}, "lookup_not_proven"},
		{"the lookup is rate limited", answer{status: 429, body: `{}`}, "lookup_not_proven"},
		{"the lookup answers a body that is not an answer", answer{status: 200, body: `{"message":"x"}`}, "lookup_not_proven"},
		{"the lookup times out", answer{err: io.ErrUnexpectedEOF}, "lookup_not_proven"},
	} {
		before := len(logs.lines("team_membership_close_skipped"))
		result := run(departureAt[1].Add(time.Duration(index)*time.Minute), only, c.lookup)
		requireOpen(t, c.name, openMembershipFacts(ctx, t, conn, orgID, "github"), hubot, departureAt[0])
		if result.MembershipsClosed != 0 {
			t.Errorf("%s: the run closed %d members, want 0", c.name, result.MembershipsClosed)
		}
		skipped := logs.lines("team_membership_close_skipped")
		if len(skipped) != before+1 {
			t.Fatalf("%s: %d skip lines, want one more than %d", c.name, len(skipped), before)
		}
		line := skipped[len(skipped)-1]
		if line[c.reason] != float64(1) || line["team"] != "platform" || line["provider"] != "github" {
			t.Errorf("%s: skip line = %v, want %s=1 for team=platform provider=github", c.name, line, c.reason)
		}
	}
	// The provider says "not a member" (404): closed.
	result := run(departureAt[3], only, answer{status: 404, body: `{"message":"Not Found"}`})
	requireNotOpen(t, "the lookup answers 404", openMembershipFacts(ctx, t, conn, orgID, "github"), hubot)
	if result.MembershipsClosed != 1 {
		t.Errorf("a 404 lookup closed %d members, want 1", result.MembershipsClosed)
	}
	if strings.Contains(logs.text(), "hubot") || strings.Contains(logs.text(), "octocat") {
		t.Error("a log line holds a member login")
	}
}

func TestGitLabCandidateStaysOpenUnlessTheLookupFindsNoSuchMember(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-gitlab-lookup"
	fake := newGitLabMembersServer(t, "alice", "carol")
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	sync := func(at time.Time) TeamCatalogResult {
		t.Helper()
		collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
			Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}, ScopeCensus: staticScopeCensus{}}
		result, err := collector.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, gitlabTeamCatalogTestClient(t, fake.URL), TeamCatalogSelections{Teams: true, Members: true}, at)
		if err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
		return result
	}
	const carol = "gl:org/team-a|gl:carol"
	sync(departureAt[0])
	// The list lacks carol. The provider's lookup still finds her: a member.
	fake.setMembers("alice")
	fake.setLookup([]string{"alice", "carol"}, 0)
	if result := sync(departureAt[1]); result.MembershipsClosed != 0 {
		t.Errorf("the lookup found carol and the run closed %d members", result.MembershipsClosed)
	}
	requireOpen(t, "the lookup finds her", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), carol, departureAt[0])
	// The lookup fails: nothing is proven.
	for index, status := range []int{500, 403, 429, 404} {
		fake.setLookup([]string{}, status)
		if result := sync(departureAt[1].Add(time.Duration(index+1) * time.Minute)); result.MembershipsClosed != 0 {
			t.Errorf("a lookup that answered %d closed %d members", status, result.MembershipsClosed)
		}
		requireOpen(t, "a failed lookup", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), carol, departureAt[0])
	}
	// An empty answer WITHOUT GitLab's end signal proves nothing.
	fake.setLookup([]string{}, 0)
	fake.setLookupNoEnd(true)
	if result := sync(departureAt[1].Add(10 * time.Minute)); result.MembershipsClosed != 0 {
		t.Errorf("an empty lookup answer with no end signal closed %d members", result.MembershipsClosed)
	}
	requireOpen(t, "no end signal", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), carol, departureAt[0])
	// The lookup answers an empty list with GitLab's end signal: not a member.
	fake.setLookupNoEnd(false)
	if result := sync(departureAt[3]); result.MembershipsClosed != 1 {
		t.Errorf("the lookup found nobody and the run closed %d members, want 1", result.MembershipsClosed)
	}
	requireNotOpen(t, "the lookup finds nobody", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), carol)
	if fake.lookupCount() == 0 {
		t.Error("the provider was never asked")
	}
}

// The budget: a run that finds more candidates than its lookups proves the
// first ones and leaves the rest open, and says so.
func TestAMembershipLookupBudgetThatEndsLeavesTheRestOpen(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-budget"
	logs := captureMemberLogs(t)
	for _, member := range []string{"gh:a", "gh:b", "gh:c"} {
		seedMembership(ctx, t, conn, orgID, "github", "gh:platform", member, "provider_access", departureAt[0], nil, departureAt[0])
	}
	asked := 0
	prover := &MembershipLookupBudget{Left: 1, Inner: absenceFunc(func(string, string) MembershipAbsence { asked++; return AbsenceProven })}
	closable := GitHubTeamMembershipKind([]string{"gh:platform"}).Snapshot(ScopeProof{stated: true}, ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}))
	rows, outcome, err := githubMembershipWriter.Snapshot(ctx, conn, orgID, nil, nil, departureAt[1], prover, closable)
	if err != nil {
		t.Fatal(err)
	}
	if asked != 1 || outcome.Closed != 1 || outcome.Skipped[membershipSkipOverBudget] != 2 || len(rows) != 1 {
		t.Errorf("asked=%d closed=%d skipped=%v rows=%d, want 1 lookup, 1 close, 2 left open for the budget", asked, outcome.Closed, outcome.Skipped, len(rows))
	}
	lines := logs.lines("team_membership_close_skipped")
	if len(lines) != 1 || lines[0][membershipSkipOverBudget] != float64(2) || lines[0]["team"] != "platform" {
		t.Errorf("skip lines = %v, want one line with %s=2 for team=platform", lines, membershipSkipOverBudget)
	}
}

type absenceFunc func(team, member string) MembershipAbsence

func (fn absenceFunc) Absence(_ context.Context, team, member string) MembershipAbsence {
	return fn(team, member)
}

// ---- Linear: no stable user id is stored ---------------------------------------------

func linearMembershipRun(t *testing.T, f carryFixture, at time.Time, teamResponse string) error {
	t.Helper()
	rest := []string{
		`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}
	collector := LinearTeamCatalogCollector{
		ScopeCensus: staticScopeCensus{},
		Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10},
		Sink:        LinearReferenceCatalogClickHouseEffects{Conn: f.conn, Lease: carrySeamLease()},
	}
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = f.orgID
	ref := teamCatalogRefFromClaim(claim)
	ref.Strict = true
	ref.IntegrationID = "integration-a"
	_, err := collector.CollectTeamCatalog(f.ctx, ref,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(&linearWorkItemsDoer{responses: append([]string{teamResponse}, rest...)})),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, at)
	return err
}

func linearTeamWith(members string) string {
	return `{"data":{"teams":{"nodes":[{"id":"team-raw-eng","key":"ENG","name":"Provider Eng","members":{"nodes":[` + members + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],` +
		`"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
}

// Linear keys a member by its email. A changed (or hidden) email is the same
// person, and no stable user id is stored to say so: nothing is closed, the
// log says so, and the member never gets a "left" interval.
func TestAChangedLinearEmailClosesNothingAndSaysSo(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	logs := captureMemberLogs(t)
	alice := `{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}`
	bob := `{"id":"user-2","name":"Bob","email":"bob@example.com","active":true}`
	robert := `{"id":"user-2","name":"Bob","email":"robert@example.com","active":true}`
	bobNoEmail := `{"id":"user-2","name":"Bob","active":true}`
	if err := linearMembershipRun(t, f, departureAt[0], linearTeamWith(alice+","+bob)); err != nil {
		t.Fatal(err)
	}
	for index, changed := range []string{robert, bobNoEmail} {
		if err := linearMembershipRun(t, f, departureAt[1].Add(time.Duration(index)*time.Hour), linearTeamWith(alice+","+changed)); err != nil {
			t.Fatal(err)
		}
		facts := openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear")
		requireOpen(t, "the old email", facts, "linear:ENG|linear:bob@example.com", departureAt[0])
		requireOpen(t, "alice", facts, "linear:ENG|linear:alice@example.com", departureAt[0])
		if len(closedMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear")) != 0 {
			t.Fatalf("a changed email closed %v", closedMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear"))
		}
	}
	lines := logs.lines("team_membership_close_skipped")
	if len(lines) != 2 || lines[0]["no_stable_user_id"] != float64(1) || lines[0]["team"] != "ENG" || lines[0]["provider"] != "linear" {
		t.Errorf("skip lines = %v, want two lines of no_stable_user_id=1 for team=ENG provider=linear", lines)
	}
	if strings.Contains(logs.text(), "bob@example.com") || strings.Contains(logs.text(), "robert@example.com") {
		t.Error("a log line holds an email")
	}
}

// A deactivated Linear user is returned by the provider with active = false:
// that IS the provider's statement, so the member (email-keyed too) is closed.
func TestADeactivatedLinearUserIsClosedEvenWhenKeyedByEmail(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	alice := `{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}`
	bob := `{"id":"user-2","name":"Bob","email":"bob@example.com","active":true}`
	bobInactive := `{"id":"user-2","name":"Bob","email":"bob@example.com","active":false}`
	if err := linearMembershipRun(t, f, departureAt[0], linearTeamWith(alice+","+bob)); err != nil {
		t.Fatal(err)
	}
	if err := linearMembershipRun(t, f, departureAt[1], linearTeamWith(alice+","+bobInactive)); err != nil {
		t.Fatal(err)
	}
	facts := openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear")
	requireNotOpen(t, "a deactivated user", facts, "linear:ENG|linear:bob@example.com")
	requireOpen(t, "alice", facts, "linear:ENG|linear:alice@example.com", departureAt[0])
}

// "nodes": null with a stated end is no answer to "who are the members".
func TestALinearNullMemberListClosesNothing(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	logs := captureMemberLogs(t)
	// Members with no email are keyed by the Linear user id: the list rule alone would close them.
	alice := `{"id":"user-1","name":"Alice","active":true}`
	bob := `{"id":"user-2","name":"Bob","active":true}`
	if err := linearMembershipRun(t, f, departureAt[0], linearTeamWith(alice+","+bob)); err != nil {
		t.Fatal(err)
	}
	nullNodes := `{"data":{"teams":{"nodes":[{"id":"team-raw-eng","key":"ENG","name":"Provider Eng","members":{"nodes":null,"pageInfo":{"hasNextPage":false,"endCursor":null}}}],` +
		`"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	if err := linearMembershipRun(t, f, departureAt[1], nullNodes); err != nil {
		t.Fatal(err)
	}
	facts := openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear")
	requireOpen(t, "after nodes null", facts, "linear:ENG|linear:user-1", departureAt[0])
	requireOpen(t, "after nodes null", facts, "linear:ENG|linear:user-2", departureAt[0])
	lines := logs.lines("linear_reference_catalog_member_unusable")
	if len(lines) != 1 || lines[0]["team"] != "ENG" || lines[0]["provider"] != "linear" || lines[0]["unusable_members"] != float64(1) {
		t.Errorf("unusable lines = %v, want one line of unusable_members=1 for team=ENG provider=linear", lines)
	}
}

// ---- two runs of one integration that overlap -----------------------------------------

// Run A (clock 09:00) read the team after hubot joined and wrote hubot open. Run
// B (clock 08:00) read it BEFORE hubot joined and writes after run A: its list
// is older than the row, so it cannot say hubot left.
func TestAnOlderRunThatWritesLastClosesNothingANewerRunWrote(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-stale-run"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	run := func(at time.Time, members string) TeamCatalogResult {
		t.Helper()
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
			"/orgs/acme/teams":                            `[{"slug":"platform","name":"Platform"}]`,
			"/orgs/acme/teams/platform/repos":             `[]`,
			"/orgs/acme/teams/platform/members":           members,
			"/orgs/acme/teams/platform/memberships/hubot": `{"message":"Not Found"}`,
		}, statuses: map[string]int{"/orgs/acme/teams/platform/memberships/hubot": http.StatusNotFound}}
		adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
		result, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)), TeamCatalogSelections{Teams: true, Members: true}, at)
		if err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
		return result
	}
	const hubot = "gh:platform|gh:hubot"
	run(departureAt[0].Add(-time.Hour), `[{"login":"octocat"}]`)
	run(departureAt[1], `[{"login":"octocat"},{"login":"hubot"}]`)
	result := run(departureAt[0], `[{"login":"octocat"}]`)
	requireOpen(t, "the older run wrote last", openMembershipFacts(ctx, t, conn, orgID, "github"), hubot, departureAt[1])
	if result.MembershipsClosed != 0 {
		t.Errorf("the older run closed %d members", result.MembershipsClosed)
	}
	// A run on the SAME clock as the row still closes it (the retraction is one tick newer).
	equal := run(departureAt[1], `[{"login":"octocat"}]`)
	requireNotOpen(t, "the same clock", openMembershipFacts(ctx, t, conn, orgID, "github"), hubot)
	if equal.MembershipsClosed != 1 {
		t.Errorf("a run on the clock of the row closed %d members, want 1", equal.MembershipsClosed)
	}
}

// ---- the scope of the open-row read, and what decides absence ---------------------------

// Rows of another org, another provider and another source are no part of this
// writer's read: a run closes none of them, whatever the provider returned.
func TestTheCloseReadsOnlyTheOpenRowsOfItsOwnOrgProviderAndSource(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-scope"
	seedMembership(ctx, t, conn, orgID, "github", "gh:platform", "gh:gone", "provider_access", departureAt[0], nil, departureAt[0])
	seedMembership(ctx, t, conn, "org-other", "github", "gh:platform", "gh:other-org", "provider_access", departureAt[0], nil, departureAt[0])
	seedMembership(ctx, t, conn, orgID, "gitlab", "gh:platform", "gh:other-provider", "provider_access", departureAt[0], nil, departureAt[0])
	seedMembership(ctx, t, conn, orgID, "github", "gh:platform", "gh:manual", "manual", departureAt[0], nil, departureAt[0])
	prover := absenceFunc(func(string, string) MembershipAbsence { return AbsenceProven })
	closable := GitHubTeamMembershipKind([]string{"gh:platform"}).Snapshot(ScopeProof{stated: true}, ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}))
	rows, outcome, err := githubMembershipWriter.Snapshot(ctx, conn, orgID, nil, nil, departureAt[1], prover, closable)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Closed != 1 || len(rows) != 1 || rows[0].MemberID != "gh:gone" {
		t.Errorf("closed %d rows %v, want only gh:gone of this org, provider and source", outcome.Closed, rows)
	}
}

// A member the conflict guard keeps out of the write is still OBSERVED: the
// run does not close it because the guard did not let it through. One test for
// each collector, through the real wiring.
func TestAMemberTheConflictGuardKeepsOutIsNotClosed(t *testing.T) {
	t.Run("github", func(t *testing.T) {
		ctx, conn := newWorkItemEffectsConn(t)
		const orgID = "org-9079-guard-github"
		credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
		run := func(at time.Time) TeamCatalogResult {
			doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
				"/orgs/acme/teams":                            `[{"slug":"platform","name":"Platform"}]`,
				"/orgs/acme/teams/platform/repos":             `[]`,
				"/orgs/acme/teams/platform/members":           `[{"login":"hubot"}]`,
				"/orgs/acme/teams/platform/memberships/hubot": `{"message":"Not Found"}`,
			}, statuses: map[string]int{"/orgs/acme/teams/platform/memberships/hubot": http.StatusNotFound}}
			adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
			result, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
				credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)), TeamCatalogSelections{Teams: true, Members: true}, at)
			if err != nil {
				t.Fatal(err)
			}
			return result
		}
		run(departureAt[0])
		seedMembership(ctx, t, conn, orgID, "github", "gh:other-team", "gh:hubot", "manual", departureAt[0], nil, departureAt[1])
		result := run(departureAt[2])
		if result.MembershipsSkippedManualConflict != 1 || result.MembershipsClosed != 0 {
			t.Errorf("skipped=%d closed=%d, want the guard to keep hubot out and nothing closed", result.MembershipsSkippedManualConflict, result.MembershipsClosed)
		}
		requireOpen(t, "kept out by the guard", openMembershipFacts(ctx, t, conn, orgID, "github"), "gh:platform|gh:hubot", departureAt[0])
	})
	t.Run("gitlab", func(t *testing.T) {
		ctx, conn := newWorkItemEffectsConn(t)
		const orgID = "org-9079-guard-gitlab"
		fake := newGitLabMembersServer(t, "alice")
		credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
		run := func(at time.Time) TeamCatalogResult {
			collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
				Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
			}, ScopeCensus: staticScopeCensus{}}
			result, err := collector.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
				credential, gitlabTeamCatalogTestClient(t, fake.URL), TeamCatalogSelections{Teams: true, Members: true}, at)
			if err != nil {
				t.Fatal(err)
			}
			return result
		}
		run(departureAt[0])
		seedMembership(ctx, t, conn, orgID, "gitlab", "gl:other-team", "gl:alice", "manual", departureAt[0], nil, departureAt[1])
		fake.setLookup([]string{}, 0) // the provider would answer "nobody": the guard decides nothing about absence
		result := run(departureAt[2])
		if result.MembershipsSkippedManualConflict != 1 || result.MembershipsClosed != 0 {
			t.Errorf("skipped=%d closed=%d, want the guard to keep alice out and nothing closed", result.MembershipsSkippedManualConflict, result.MembershipsClosed)
		}
		requireOpen(t, "kept out by the guard", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), "gl:org/team-a|gl:alice", departureAt[0])
	})
	t.Run("linear", func(t *testing.T) {
		ctx, conn := newWorkItemEffectsConn(t)
		f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
		alice := `{"id":"user-1","name":"Alice","active":true}`
		if err := linearMembershipRun(t, f, departureAt[0], linearTeamWith(alice)); err != nil {
			t.Fatal(err)
		}
		seedMembership(ctx, t, conn, f.orgID, "linear", "linear:OTHER", "linear:user-1", "manual", departureAt[0], nil, departureAt[1])
		if err := linearMembershipRun(t, f, departureAt[2], linearTeamWith(alice)); err != nil {
			t.Fatal(err)
		}
		requireOpen(t, "kept out by the guard", openMembershipFacts(ctx, t, conn, f.orgID, "linear"), "linear:ENG|linear:user-1", departureAt[0])
	})
}

// A read cut by the page budget writes no roster of that team: a member the
// cut page returned for the first time is not written either.
func TestAGitHubReadCutByThePageBudgetWritesNoMembershipOfThatTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-cut"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"}]`,
		"/orgs/acme/teams/platform/repos":   `[]`,
		"/orgs/acme/teams/platform/members": `[{"login":"newcomer"}]`,
	}, links: map[string]string{"/orgs/acme/teams/platform/members": `<https://api.github.com/orgs/acme/teams/platform/members?page=2>; rel="next"`}}
	adapter := GitHubTeamCatalogCollector{Client: GitHubTeamCatalogRouteHandler{MaxPages: 1},
		Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
	if _, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
		credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)), TeamCatalogSelections{Teams: true, Members: true}, departureAt[0]); err != nil {
		t.Fatal(err)
	}
	if facts := openMembershipFacts(ctx, t, conn, orgID, "github"); len(facts) != 0 {
		t.Errorf("a cut read wrote %v", facts)
	}
}

// The row that closes a member is written one tick newer than the row it
// replaces: both have the same sorting key, and the newer version wins. A run
// on the very clock of the row would otherwise tie with it.
func TestACloseIsWrittenOneTickNewerThanTheRowItReplaces(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-tick"
	seedMembership(ctx, t, conn, orgID, "github", "gh:platform", "gh:gone", "provider_access", departureAt[0], nil, departureAt[1])
	prover := absenceFunc(func(string, string) MembershipAbsence { return AbsenceProven })
	closable := GitHubTeamMembershipKind([]string{"gh:platform"}).Snapshot(ScopeProof{stated: true}, ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}))
	rows, _, err := githubMembershipWriter.Snapshot(ctx, conn, orgID, nil, nil, departureAt[1], prover, closable)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0].UpdatedAt.Equal(departureAt[1].Add(time.Millisecond)) {
		t.Errorf("rows = %v, want one closing row with updated_at = the row's updated_at + 1 ms", rows)
	}
}

// GitLab usernames are unique case-insensitively, so the lookup compares them
// that way: a username that matches in another case is the member. Reading it
// as "not a member" would close a person who is still in the group, and a
// close is the harmful side; the other side (a wrong "still a member") leaves
// a row open until the next run.
func TestGitLabLookupFindsTheMemberWhateverTheCaseOfTheUsername(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-gitlab-case"
	fake := newGitLabMembersServer(t, "alice", "carol")
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	sync := func(at time.Time) TeamCatalogResult {
		t.Helper()
		collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
			Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}, ScopeCensus: staticScopeCensus{}}
		result, err := collector.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, gitlabTeamCatalogTestClient(t, fake.URL), TeamCatalogSelections{Teams: true, Members: true}, at)
		if err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
		return result
	}
	sync(departureAt[0])
	fake.setMembers("alice")
	fake.setLookup([]string{"alice", "Carol"}, 0) // the provider answers the username in another case
	if result := sync(departureAt[1]); result.MembershipsClosed != 0 {
		t.Errorf("a username that matches in another case closed %d members", result.MembershipsClosed)
	}
	requireOpen(t, "another case", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), "gl:org/team-a|gl:carol", departureAt[0])
}

// A GitHub read that did not reach its own end signal (a full page and no Link)
// makes its team not closable: even a lookup that says "not a member" closes
// nothing for that team.
func TestAGitHubReadWithoutAnEndSignalClosesNothingEvenWhenTheLookupSaysNo(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-github-no-end"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	run := func(at time.Time, members string, perPage int) TeamCatalogResult {
		t.Helper()
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
			"/orgs/acme/teams":                            `[{"slug":"platform","name":"Platform"}]`,
			"/orgs/acme/teams/platform/repos":             `[]`,
			"/orgs/acme/teams/platform/members":           members,
			"/orgs/acme/teams/platform/memberships/hubot": `{"message":"Not Found"}`,
			"/orgs/acme/teams/platform/memberships/mona":  `{"message":"Not Found"}`,
		}, statuses: map[string]int{
			"/orgs/acme/teams/platform/memberships/hubot": http.StatusNotFound,
			"/orgs/acme/teams/platform/memberships/mona":  http.StatusNotFound,
		}}
		adapter := GitHubTeamCatalogCollector{Client: GitHubTeamCatalogRouteHandler{PerPage: perPage},
			Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
		result, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)), TeamCatalogSelections{Teams: true, Members: true}, at)
		if err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
		return result
	}
	const hubot = "gh:platform|gh:hubot"
	run(departureAt[0], `[{"login":"octocat"},{"login":"hubot"}]`, 0)
	// per_page 2 and two members back with no Link: the page is full, the read is not known to have ended.
	if result := run(departureAt[1], `[{"login":"octocat"},{"login":"mona"}]`, 2); result.MembershipsClosed != 0 {
		t.Errorf("a read with no end signal closed %d members", result.MembershipsClosed)
	}
	requireOpen(t, "no end signal", openMembershipFacts(ctx, t, conn, orgID, "github"), hubot, departureAt[0])
	// The same answer with fewer members than a page: the read ended, and the lookup closes.
	// (hubot, and mona, who joined in the run before and is gone now.)
	if result := run(departureAt[2], `[{"login":"octocat"}]`, 2); result.MembershipsClosed != 2 {
		t.Errorf("a read that ended closed %d members, want 2", result.MembershipsClosed)
	}
	requireNotOpen(t, "the read ended", openMembershipFacts(ctx, t, conn, orgID, "github"), hubot)
}

// A fact the run holds again with a later open row of the same fact: the later
// row is retired (closed at the run time) and the earliest stays open.
func TestALaterOpenRowOfAFactTheRunHoldsIsRetiredAndTheEarliestStaysOpen(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-duplicates"
	seedMembership(ctx, t, conn, orgID, "github", "gh:platform", "gh:octocat", "provider_access", departureAt[0], nil, departureAt[0])
	seedMembership(ctx, t, conn, orgID, "github", "gh:platform", "gh:octocat", "provider_access", departureAt[1], nil, departureAt[1])
	closable := GitHubTeamMembershipKind([]string{"gh:platform"}).Snapshot(ScopeProof{stated: true}, ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}))
	uid := "octocat"
	fresh := githubMembershipRow{OrgID: orgID, Provider: "github", TeamID: "gh:platform", MemberID: "gh:octocat", RawProviderUserID: &uid,
		IdentityFacets: []string{"octocat"}, Source: "provider_access", ValidFrom: departureAt[2], UpdatedAt: departureAt[2]}
	prover := absenceFunc(func(string, string) MembershipAbsence {
		t.Fatal("a held fact is never asked about")
		return AbsenceUnproven
	})
	rows, outcome, err := githubMembershipWriter.Snapshot(ctx, conn, orgID, []githubMembershipRow{fresh}, []githubMembershipRow{fresh}, departureAt[2], prover, closable)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.DuplicatesRetired != 1 || outcome.Closed != 0 {
		t.Fatalf("duplicates=%d closed=%d, want the later row retired and no departure", outcome.DuplicatesRetired, outcome.Closed)
	}
	var closedRows []githubMembershipRow
	for _, row := range rows {
		if row.ValidTo != nil {
			closedRows = append(closedRows, row)
		}
	}
	if len(closedRows) != 1 || !closedRows[0].ValidFrom.Equal(departureAt[1]) || !closedRows[0].ValidTo.Equal(departureAt[2]) {
		t.Errorf("closed rows = %v, want the row of %s closed at the run time %s", closedRows, departureAt[1], departureAt[2])
	}
	if len(rows) != 2 || !rows[0].ValidFrom.Equal(departureAt[0]) || rows[0].ValidTo != nil {
		t.Errorf("rows = %v, want the fresh row on the earliest stamp %s first", rows, departureAt[0])
	}
}

// The budget and the lookups count FACTS: a fact with many open rows costs one
// lookup, one answer closes every open row of the fact.
func TestAFactWithManyOpenRowsCostsOneLookupAndIsClosedWhole(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-per-fact"
	for index := 0; index < 5; index++ {
		seedMembership(ctx, t, conn, orgID, "github", "gh:platform", "gh:many", "provider_access", departureAt[0].Add(time.Duration(index)*time.Minute), nil, departureAt[0])
	}
	seedMembership(ctx, t, conn, orgID, "github", "gh:platform", "gh:other", "provider_access", departureAt[0], nil, departureAt[0])
	closable := GitHubTeamMembershipKind([]string{"gh:platform"}).Snapshot(ScopeProof{stated: true}, ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}))
	run := func(budget int) (map[string]int, []githubMembershipRow, MembershipSnapshotOutcome) {
		asked := map[string]int{}
		inner := absenceFunc(func(_, member string) MembershipAbsence { asked[member]++; return AbsenceProven })
		rows, outcome, err := githubMembershipWriter.Snapshot(ctx, conn, orgID, nil, nil, departureAt[1], &MembershipLookupBudget{Inner: inner, Left: budget}, closable)
		if err != nil {
			t.Fatal(err)
		}
		return asked, rows, outcome
	}
	// Enough budget: each fact is asked ONCE; the six rows of the two facts all close.
	asked, rows, outcome := run(10)
	if asked["gh:many"] != 1 || asked["gh:other"] != 1 || outcome.Closed != 2 || len(rows) != 6 {
		t.Errorf("lookups=%v closed=%d rows=%d, want one lookup per fact, 2 facts closed, 6 rows", asked, outcome.Closed, len(rows))
	}
	// A budget of ONE lookup: it is spent on one fact, whose rows all close; the other fact is left open for the budget.
	asked, rows, outcome = run(1)
	total := 0
	for _, n := range asked {
		total += n
	}
	if total != 1 || outcome.Closed != 1 || outcome.Skipped[membershipSkipOverBudget] != 1 {
		t.Fatalf("lookups=%v closed=%d skipped=%v, want 1 lookup, 1 fact closed, 1 fact left for the budget", asked, outcome.Closed, outcome.Skipped)
	}
	want := 5
	if asked["gh:other"] == 1 {
		want = 1
	}
	if len(rows) != want {
		t.Errorf("rows = %d, want every open row of the closed fact (%d)", len(rows), want)
	}
	for _, row := range rows {
		if row.ValidTo == nil || row.MemberID != rows[0].MemberID {
			t.Errorf("row %v: want only closed rows of one fact", row)
		}
	}
}
