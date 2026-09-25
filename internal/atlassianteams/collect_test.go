package atlassianteams

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/identityalias"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

const (
	teamA = "ari:cloud:identity::team/AAAAAAAA-0000-4000-8000-000000000001"
	teamB = "ari:cloud:identity::team/bbbbbbbb-0000-4000-8000-000000000002"
	teamC = "ari:cloud:identity::team/cccccccc-0000-4000-8000-000000000003"
)

type request struct {
	Operation string
	Variables map[string]any
	Header    http.Header
}

// gateway is a fake Atlassian GraphQL gateway serving the response shapes the
// vendored client's own generated decoders read.
type gateway struct {
	t      *testing.T
	server *httptest.Server

	mu       sync.Mutex
	requests []request
	// respond returns the JSON body (or a status) for one request.
	respond func(request) (int, any)
}

func newGateway(t *testing.T, respond func(request) (int, any)) *gateway {
	g := &gateway{t: t, respond: respond}
	g.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/gateway/api/graphql" {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var payload struct {
			OperationName string         `json:"operationName"`
			Variables     map[string]any `json:"variables"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		req := request{Operation: payload.OperationName, Variables: payload.Variables, Header: r.Header.Clone()}
		g.mu.Lock()
		g.requests = append(g.requests, req)
		g.mu.Unlock()
		status, response := g.respond(req)
		w.Header().Set("Content-Type", "application/json")
		if status == http.StatusTooManyRequests {
			// The client only reads an absolute time here (AGG sends one) and refuses a 429 without it.
			w.Header().Set("Retry-After", time.Now().Add(-time.Second).UTC().Format(time.RFC3339))
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(response)
	}))
	t.Cleanup(g.server.Close)
	return g
}

func (g *gateway) client() *graph.Client {
	return &graph.Client{
		BaseURL: g.server.URL + "/gateway/api",
		Auth:    atlassian.BasicAPITokenAuth{Email: "sync@example.test", Token: "gateway-secret"},
		Sleep:   func(time.Duration) {},
	}
}

func (g *gateway) count(operation, teamID string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, req := range g.requests {
		if req.Operation == operation && (teamID == "" || req.Variables["teamId"] == teamID) {
			n++
		}
	}
	return n
}

func teamNode(id, name, state string) map[string]any {
	return map[string]any{"team": map[string]any{"id": id, "displayName": name, "state": state}}
}

func searchPage(next string, nodes ...map[string]any) map[string]any {
	pageInfo := map[string]any{"hasNextPage": next != ""}
	if next != "" {
		pageInfo["endCursor"] = next
	}
	return map[string]any{"data": map[string]any{"team": map[string]any{"teamSearchV2": map[string]any{"pageInfo": pageInfo, "nodes": nodes}}}}
}

func ariNode(id, typename string, data map[string]any) map[string]any {
	data["__typename"] = typename
	return map[string]any{"__typename": "GraphStoreCypherQueryV2AriNode", "id": id, "data": data}
}

func userEdge(team, account string) map[string]any {
	return map[string]any{"node": map[string]any{"columns": []any{
		map[string]any{"key": "team", "value": ariNode(team, "TeamV2", map[string]any{"id": team, "displayName": "T"})},
		map[string]any{"key": "user", "value": ariNode("ari:cloud:identity::user/"+account, "AtlassianAccountUser", map[string]any{"id": "x", "accountId": account, "name": "N"})},
	}}}
}

func projectEdge(team, key string) map[string]any {
	data := map[string]any{"id": "p-" + key, "name": "Project " + key}
	if key != "" {
		data["key"] = key
	}
	return map[string]any{"node": map[string]any{"columns": []any{
		map[string]any{"key": "team", "value": ariNode(team, "TeamV2", map[string]any{"id": team, "displayName": "T"})},
		map[string]any{"key": "project", "value": ariNode("ari:cloud:jira::project/"+key, "JiraProject", data)},
	}}}
}

func connection(field, next string, edges ...map[string]any) map[string]any {
	pageInfo := map[string]any{"hasNextPage": next != ""}
	if next != "" {
		pageInfo["endCursor"] = next
	}
	return map[string]any{"data": map[string]any{field: map[string]any{"version": "1", "pageInfo": pageInfo, "edges": edges}}}
}

// standard serves: teams A (active), B (archived) on page one and C on page
// two; A has members alice, bob (page 2), alice again (a duplicate) and a
// project with a key and one without; C has one member and no projects.
func standard(req request) (int, any) {
	switch req.Operation {
	case "TeamSearchV2":
		if req.Variables["after"] == nil {
			return 200, searchPage("cursor-2", teamNode(teamA, "Platform", "ACTIVE"), teamNode(teamB, "Old", "ARCHIVED"))
		}
		return 200, searchPage("", teamNode(teamC, "Data", "ACTIVE"))
	case "TeamworkGraph_teamUsers":
		switch req.Variables["teamId"] {
		case teamA:
			if req.Variables["after"] == nil {
				return 200, connection("teamworkGraph_teamUsers", "u2", userEdge(teamA, "Alice-1"), userEdge(teamA, "alice-1"))
			}
			return 200, connection("teamworkGraph_teamUsers", "", userEdge(teamA, "bob-2"))
		case teamC:
			return 200, connection("teamworkGraph_teamUsers", "", userEdge(teamC, "carol-3"))
		}
	case "TeamworkGraph_teamActiveProjects":
		if req.Variables["teamId"] == teamA {
			return 200, connection("teamworkGraph_teamActiveProjects", "", projectEdge(teamA, "PLAT"), projectEdge(teamA, ""), projectEdge(teamA, "PLAT"))
		}
		return 200, connection("teamworkGraph_teamActiveProjects", "")
	}
	return 500, map[string]any{"errors": []any{map[string]any{"message": "unexpected " + req.Operation}}}
}

func params(selections Selections) Params {
	return Params{
		OrgID: "org-1", OrganizationID: "org-atlassian", SiteID: "site-uuid", Selections: selections,
		PageSize: 2, Now: time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC), Resolver: &identityalias.Resolver{AliasToCanonical: map[string]string{}},
	}
}

var everything = Selections{Structure: true, Members: true, Projects: true}

func TestCollectReadsTeamsMembersAndProjectsThroughTheRealClient(t *testing.T) {
	g := newGateway(t, standard)
	rows, err := Collect(context.Background(), g.client(), params(everything))
	if err != nil {
		t.Fatal(err)
	}

	if len(rows.Teams) != 3 {
		t.Fatalf("teams = %d, want 3 (active, archived, second page)", len(rows.Teams))
	}
	a, b, c := rows.Teams[0], rows.Teams[1], rows.Teams[2]
	if a.ID != "aaaaaaaa-0000-4000-8000-000000000001" || a.NativeTeamKey != teamA || a.Name != "Platform" || a.IsActive != 1 || a.Provider != "jira" || a.OrgID != "org-1" {
		t.Errorf("team A row = %+v", a)
	}
	if got := strings.Join(a.ProjectKeys, ","); got != "PLAT" {
		t.Errorf("team A project keys = %q, want PLAT (deduplicated, key-less link dropped)", got)
	}
	if b.IsActive != 0 || b.Name != "Old" {
		t.Errorf("archived team row = %+v, want inactive", b)
	}
	if c.ID != "cccccccc-0000-4000-8000-000000000003" {
		t.Errorf("second-page team = %+v", c)
	}
	if a.TeamUUID.String() == "" || a.TeamUUID == b.TeamUUID {
		t.Errorf("team uuids must be set and distinct: %v %v", a.TeamUUID, b.TeamUUID)
	}

	members := map[string][]string{}
	for _, m := range rows.Memberships {
		members[m.TeamID] = append(members[m.TeamID], m.MemberID)
		if m.Source != "native" || m.Provider != "jira" || m.IsPrimary != 1 || m.Specificity != 100 || m.Priority != 10 {
			t.Errorf("membership row %+v", m)
		}
		if m.RawProviderUserID == "" || len(m.IdentityFacets) == 0 || m.IdentityFacets[0] != m.RawProviderUserID {
			t.Errorf("membership identity %+v", m)
		}
	}
	if got := strings.Join(members["aaaaaaaa-0000-4000-8000-000000000001"], ","); got != "jira:alice-1,jira:bob-2" {
		t.Errorf("team A members = %q, want jira:alice-1,jira:bob-2 (lower-cased, deduplicated, both pages)", got)
	}
	if got := strings.Join(members["cccccccc-0000-4000-8000-000000000003"], ","); got != "jira:carol-3" {
		t.Errorf("team C members = %q", got)
	}
	if len(members["bbbbbbbb-0000-4000-8000-000000000002"]) != 0 {
		t.Errorf("an archived team has no members: %v", members["bbbbbbbb-0000-4000-8000-000000000002"])
	}

	if len(rows.Ownership) != 1 || rows.Ownership[0].ProjectID != "org-1:jira:PLAT" || rows.Ownership[0].ProjectKey != "PLAT" ||
		rows.Ownership[0].Source != "native" || rows.Ownership[0].Specificity != 110 || rows.Ownership[0].Priority != 10 {
		t.Errorf("ownership = %+v", rows.Ownership)
	}
	if rows.SkippedProjects != 1 {
		t.Errorf("skipped project links = %d, want 1", rows.SkippedProjects)
	}

	// The archived team was never asked for members or projects.
	if n := g.count("TeamworkGraph_teamUsers", teamB) + g.count("TeamworkGraph_teamActiveProjects", teamB); n != 0 {
		t.Errorf("%d graph reads for the archived team", n)
	}
}

func TestCollectSendsTheOrganizationSiteCredentialsAndOptIns(t *testing.T) {
	g := newGateway(t, standard)
	if _, err := Collect(context.Background(), g.client(), params(everything)); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, req := range g.requests {
		switch req.Operation {
		case "TeamSearchV2":
			if req.Variables["organizationId"] != "org-atlassian" || req.Variables["siteId"] != "site-uuid" {
				t.Errorf("search variables = %v", req.Variables)
			}
			if got := strings.Join(req.Header.Values("X-ExperimentalApi"), ","); !strings.Contains(got, "teams-beta") {
				t.Errorf("search opt-ins = %q", got)
			}
		case "TeamworkGraph_teamUsers", "TeamworkGraph_teamActiveProjects":
			if got := strings.Join(req.Header.Values("X-ExperimentalApi"), ","); !strings.Contains(got, "TeamworkGraphContextAPIs") {
				t.Errorf("%s opt-ins = %q", req.Operation, got)
			}
		}
		user, pass, ok := basicAuth(req.Header)
		if !ok || user != "sync@example.test" || pass != "gateway-secret" {
			t.Errorf("%s credentials not sent as basic auth", req.Operation)
		}
		seen[req.Operation] = true
	}
	for _, operation := range []string{"TeamSearchV2", "TeamworkGraph_teamUsers", "TeamworkGraph_teamActiveProjects"} {
		if !seen[operation] {
			t.Errorf("no %s request", operation)
		}
	}
}

func basicAuth(header http.Header) (string, string, bool) {
	r := &http.Request{Header: header}
	return r.BasicAuth()
}

func TestCollectReadsOnlyTheSelectedDimensions(t *testing.T) {
	g := newGateway(t, standard)
	rows, err := Collect(context.Background(), g.client(), params(Selections{Structure: true}))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Teams) != 3 || len(rows.Memberships) != 0 || len(rows.Ownership) != 0 {
		t.Fatalf("rows = %d teams %d memberships %d ownership", len(rows.Teams), len(rows.Memberships), len(rows.Ownership))
	}
	if n := g.count("TeamworkGraph_teamUsers", "") + g.count("TeamworkGraph_teamActiveProjects", ""); n != 0 {
		t.Errorf("%d graph reads for a structure-only run", n)
	}
}

func TestCollectIsAllOrNothing(t *testing.T) {
	failing := func(req request) (int, any) {
		if req.Operation == "TeamworkGraph_teamUsers" && req.Variables["teamId"] == teamC {
			return 200, map[string]any{"data": nil, "errors": []any{map[string]any{"message": "boom", "extensions": map[string]any{"classification": "InsufficientOAuthScopes"}}}}
		}
		return standard(req)
	}
	g := newGateway(t, failing)
	rows, err := Collect(context.Background(), g.client(), params(everything))
	if err == nil || !strings.Contains(err.Error(), "read members of team cccccccc-0000-4000-8000-000000000003") {
		t.Fatalf("err = %v, want the failing team named", err)
	}
	if len(rows.Teams)+len(rows.Memberships)+len(rows.Ownership) != 0 {
		t.Fatalf("a failed run returned rows: %+v", rows)
	}
}

func TestCollectRefusesARepeatedSearchCursor(t *testing.T) {
	g := newGateway(t, func(req request) (int, any) {
		return 200, searchPage("same", teamNode(teamA, "Platform", "ACTIVE"))
	})
	if _, err := Collect(context.Background(), g.client(), params(Selections{Structure: true})); err == nil {
		t.Fatal("a cursor that never advances must fail the run, not loop")
	}
	if n := g.count("TeamSearchV2", ""); n > 3 {
		t.Errorf("%d search requests before giving up", n)
	}
}

func TestCollectRetriesARateLimitedRequest(t *testing.T) {
	var once sync.Once
	g := newGateway(t, func(req request) (int, any) {
		limited := false
		once.Do(func() { limited = true })
		if limited {
			return http.StatusTooManyRequests, map[string]any{"errors": []any{map[string]any{"message": "slow down"}}}
		}
		return standard(req)
	})
	rows, err := Collect(context.Background(), g.client(), params(Selections{Structure: true}))
	if err != nil {
		t.Fatalf("a 429 must be retried by the client: %v", err)
	}
	if len(rows.Teams) != 3 {
		t.Fatalf("teams = %d", len(rows.Teams))
	}
}

func TestCollectRefusesATeamIdThatIsNotATeamARI(t *testing.T) {
	g := newGateway(t, func(req request) (int, any) {
		return 200, searchPage("", teamNode("not-an-ari", "X", "ACTIVE"))
	})
	if _, err := Collect(context.Background(), g.client(), params(Selections{Structure: true})); err == nil {
		t.Fatal("a team id that is not an ARI must fail the run")
	}
}

func TestCollectNeedsItsInputs(t *testing.T) {
	g := newGateway(t, standard)
	for name, mutate := range map[string]func(*Params){
		"org":          func(p *Params) { p.OrgID = "" },
		"organization": func(p *Params) { p.OrganizationID = "" },
		"site":         func(p *Params) { p.SiteID = "" },
		"selections":   func(p *Params) { p.Selections = Selections{} },
	} {
		p := params(everything)
		mutate(&p)
		if _, err := Collect(context.Background(), g.client(), p); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := Collect(context.Background(), nil, params(everything)); err == nil {
		t.Error("nil client: no error")
	}
	if n := len(g.requests); n != 0 {
		t.Errorf("%d requests for refused inputs", n)
	}
}

func TestIdentifiers(t *testing.T) {
	for in, want := range map[string]string{
		"ari:cloud:identity::team/ABC-1": "abc-1",
		" ari:cloud:identity::team/abc ": "abc",
	} {
		if got, err := teamID(in); err != nil || got != want {
			t.Errorf("teamID(%q) = %q, %v", in, got, err)
		}
	}
	for _, in := range []string{"", "team/abc", "ari:cloud:identity::team/", "abc"} {
		if _, err := teamID(in); err == nil {
			t.Errorf("teamID(%q): no error", in)
		}
	}
	if got, ok := accountID("ari:cloud:identity::user/557058:abc"); !ok || got != "557058:abc" {
		t.Errorf("accountID = %q %v", got, ok)
	}
	if got, ok := accountID("bare-account"); !ok || got != "bare-account" {
		t.Errorf("a bare account id is kept: %q %v", got, ok)
	}
	if _, ok := accountID("ari:cloud:identity::user/"); ok {
		t.Error("an empty account id must be dropped")
	}
	if got := memberID("  AbC "); got != "jira:abc" {
		t.Errorf("memberID = %q", got)
	}
}

// The attribution cascade ranks project_ownership candidates by is_primary,
// then specificity (higher first), then priority (lower first). The Jira
// catalog's project-as-team owner of a project is native 100/10; an Atlassian
// team working on the same project must outrank it, in either input order.
func TestAnAtlassianTeamOutranksTheProjectAsTeamOwnerInTheCascade(t *testing.T) {
	updated := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
	const projectTeamSpecificity, projectTeamPriority = 100, 10 // providersync jira_team_catalog native ownership
	projectTeam := teamattribution.GithubWorkItemDerivationCandidateFromFact(
		"project_ownership", "PLAT", "Platform project", "project_ownership=org-1:jira:PLAT", 1, projectTeamSpecificity, projectTeamPriority, updated)
	atlassianTeam := teamattribution.GithubWorkItemDerivationCandidateFromFact(
		"project_ownership", "aaaaaaaa-0000-4000-8000-000000000001", "Platform", "project_ownership=org-1:jira:PLAT", 1, OwnershipSpecificity, OwnershipPriority, updated)
	for name, input := range map[string][]teamattribution.GithubWorkItemDerivationCandidate{
		"project team first":   {projectTeam, atlassianTeam},
		"atlassian team first": {atlassianTeam, projectTeam},
	} {
		ranked := teamattribution.RankDerivationCandidates(append([]teamattribution.GithubWorkItemDerivationCandidate(nil), input...))
		if got := teamattribution.GithubWorkItemDerivationStringValue(ranked[0].TeamID); got != "aaaaaaaa-0000-4000-8000-000000000001" {
			t.Errorf("%s: primary owner = %q, want the Atlassian team", name, got)
		}
	}
	if OwnershipSpecificity <= projectTeamSpecificity {
		t.Errorf("Atlassian ownership specificity %d must exceed the project-as-team %d", OwnershipSpecificity, projectTeamSpecificity)
	}
}
