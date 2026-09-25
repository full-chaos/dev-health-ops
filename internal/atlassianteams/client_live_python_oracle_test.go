package atlassianteams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	osexec "os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"
	"atlassian/atlassian/graph/gen"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// The reference is the full-chaos/atlassian Python Teams client at the sha
// ops vendors the Go client from (third_party/vendor/atlassian/PROVENANCE.md):
// testdata/atlassian_python_cb7c3665.zip holds its python/atlassian tree
// unmodified (sha256 below), put first on PYTHONPATH so it shadows the older
// atlassian-gen-client the ops venv pins, which has no Teams API. It needs
// only httpx and graphql-core, which the ops venv already has.
const (
	pythonClientZip    = "testdata/atlassian_python_cb7c3665.zip"
	pythonClientSHA256 = "c87b9781b0117895c42afe610c1bda5238a219ac5487ef001847da7f3cf34f76"
)

// The page corpus is built from the vendored client's generated response
// types (graph/gen), marshalled into the gateway JSON both clients parse: it
// is derived from the schema, not captured from a tenant. A corpus captured
// from a live tenant is added when the live probe runs.

func str(value string) *string { return &value }

// teamPage is one teamSearchV2 answer: the generated TeamSearchConnection in
// gen.TeamSearchV2Data's envelope, plus
// top-level GraphQL errors when errs is set.
func teamPage(nodes []gen.TeamNode, hasNext bool, cursor *string, errs bool) []byte {
	connection := &gen.TeamSearchConnection{PageInfo: gen.TeamPageInfo{HasNextPage: hasNext, EndCursor: cursor}}
	for index := range nodes {
		connection.Nodes = append(connection.Nodes, gen.TeamSearchResultNode{Team: &nodes[index]})
	}
	// gen.TeamSearchV2Data's envelope: {"team": {"teamSearchV2": <connection>}}.
	return envelope(map[string]any{"team": map[string]any{"teamSearchV2": connection}}, errs)
}

// value flattens a generated Cypher value union into the JSON the gateway
// sends: its __typename and the fields of the one variant it holds, the
// inverse of the generated UnmarshalJSON.
func value(v gen.GraphStoreCypherQueryV2Value) map[string]any {
	out := map[string]any{"__typename": v.Typename}
	var variant any
	switch {
	case v.AriNode != nil:
		variant = v.AriNode
	case v.NodeList != nil:
		variant = v.NodeList
	case v.StringObject != nil:
		variant = v.StringObject
	}
	if variant != nil {
		raw, _ := json.Marshal(variant)
		var fields map[string]any
		_ = json.Unmarshal(raw, &fields)
		for key, field := range fields {
			out[key] = field
		}
	}
	return out
}

// graphPage is one Teamwork Graph answer for field (teamworkGraph_teamUsers or
// teamworkGraph_teamActiveProjects): the generated connection in its generated
// data envelope (gen.TeamUsersData or gen.TeamActiveProjectsData), marshalled.
// The generated Cypher value union has no MarshalJSON, so each column value is
// replaced by its gateway form (value). The page must decode, through the
// client's own decoder, back to the connection it was built from: a flattening
// that is not the inverse of the generated UnmarshalJSON panics here.
func graphPage(field string, rows [][]gen.GraphStoreCypherQueryV2Column, hasNext bool, cursor *string, errs bool) []byte {
	// The query selects hasPreviousPage, so a gateway always answers it.
	noPrevious := false
	connection := gen.GraphStoreCypherQueryV2Connection{
		PageInfo: gen.GraphStoreCypherQueryV2PageInfo{HasNextPage: hasNext, HasPreviousPage: &noPrevious, EndCursor: cursor},
		Edges:    []gen.GraphStoreCypherQueryV2Edge{},
		Version:  "1",
	}
	for _, row := range rows {
		connection.Edges = append(connection.Edges, gen.GraphStoreCypherQueryV2Edge{Node: gen.GraphStoreCypherQueryV2Node{Columns: row}})
	}
	var typed any
	decode := gen.DecodeTeamUsers
	switch field {
	case usersField:
		typed = gen.TeamUsersData{Result: &connection}
	case projectsField:
		typed = gen.TeamActiveProjectsData{Result: &connection}
		decode = gen.DecodeTeamActiveProjects
	default:
		panic("graphPage: unknown field " + field)
	}
	raw, err := json.Marshal(typed)
	if err != nil {
		panic(err)
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		panic(err)
	}
	edges := data[field].(map[string]any)["edges"].([]any)
	for i, edge := range connection.Edges {
		columns := edges[i].(map[string]any)["node"].(map[string]any)["columns"].([]any)
		for j, column := range edge.Node.Columns {
			if column.Value != nil {
				columns[j].(map[string]any)["value"] = value(*column.Value)
			}
		}
	}
	decoded, err := decode(data)
	if err != nil {
		panic(fmt.Sprintf("graphPage: the client cannot decode the page: %v", err))
	}
	if !reflect.DeepEqual(*decoded, connection) {
		panic(fmt.Sprintf("graphPage: the page decodes to %+v, not to the connection it was built from", *decoded))
	}
	return envelope(data, errs)
}

func envelope(data any, errs bool) []byte {
	body := map[string]any{"data": data}
	if errs {
		body["errors"] = []map[string]any{{"message": "partial failure", "path": []string{"team"}}}
	}
	raw, _ := json.Marshal(body)
	return raw
}

func cypherNode(id, typename string, data *gen.GraphStoreCypherQueryV2AriNodeData) *gen.GraphStoreCypherQueryV2Value {
	if data != nil {
		data.Typename = typename
	}
	return &gen.GraphStoreCypherQueryV2Value{Typename: "GraphStoreCypherQueryV2AriNode",
		AriNode: &gen.GraphStoreCypherQueryV2AriNode{ID: id, Data: data}}
}

func column(key string, v *gen.GraphStoreCypherQueryV2Value) gen.GraphStoreCypherQueryV2Column {
	return gen.GraphStoreCypherQueryV2Column{Key: key, Value: v}
}

// scenario is one self-contained gateway: its own organization id, so one
// fake gateway serves every scenario, and the team ids whose users and
// projects both clients read.
type scenario struct {
	name    string
	strict  bool
	teams   [][]byte // teamSearchV2 pages, in order; the cursor names the next index
	graph   map[string][][]byte
	teamIDs []string
}

const (
	usersField    = "teamworkGraph_teamUsers"
	projectsField = "teamworkGraph_teamActiveProjects"
)

// teamARI is a team ARI whose id is a uuid, as the gateway issues them,
// derived from the scenario so every scenario's teams are distinct.
func teamARI(scenarioName, suffix string) string {
	return "ari:cloud:identity::team/" + uuid.NewSHA1(uuid.NameSpaceURL, []byte(scenarioName+"/"+suffix)).String()
}

func userARI(account string) string { return "ari:cloud:identity::user/" + account }

func corpus() []scenario {
	var out []scenario
	add := func(s scenario) {
		if s.graph == nil {
			s.graph = map[string][][]byte{}
		}
		out = append(out, s)
	}
	{
		// The configuration the sync runs: the Go client Strict behind
		// CompletePagesOnly, the Python client strict.
		strict := true
		name := func(base string) string { return base }

		// Two team pages, an archived team, whitespace to trim, optional
		// fields present and absent.
		n := name("paged")
		t1, t2, t3 := teamARI(n, "1"), teamARI(n, "2"), teamARI(n, "3")
		add(scenario{name: n, strict: strict, teamIDs: []string{t1, t2},
			teams: [][]byte{
				teamPage([]gen.TeamNode{
					{ID: t1, DisplayName: str("Platform"), State: str("ACTIVE"), SmallAvatarImageURL: str("https://a.example/1.png")},
					{ID: t2, DisplayName: str("  Payments  "), State: str("ACTIVE")},
				}, true, str("c1"), false),
				teamPage([]gen.TeamNode{{ID: t3, DisplayName: str("Old"), State: str("ARCHIVED"), SmallAvatarImageURL: str("   ")}}, false, nil, false),
			},
			graph: map[string][][]byte{
				usersField + " " + t1: {
					graphPage(usersField, [][]gen.GraphStoreCypherQueryV2Column{
						{column("user", cypherNode(userARI("acc-1"), "AtlassianAccountUser", &gen.GraphStoreCypherQueryV2AriNodeData{AccountID: str("acc-1"), Name: str("One")})),
							column("team", cypherNode(t1, "TeamV2", nil))},
						// No keyed user column: the user is found by its ARI.
						{column("x", cypherNode(userARI("acc-2"), "Other", nil)), column("teamId", cypherNode(t1, "TeamV2", nil))},
					}, true, str("u1"), false),
					graphPage(usersField, [][]gen.GraphStoreCypherQueryV2Column{
						// A node list holding the user.
						{column("member", &gen.GraphStoreCypherQueryV2Value{Typename: "GraphStoreCypherQueryV2NodeList",
							NodeList: &gen.GraphStoreCypherQueryV2NodeList{Nodes: []gen.GraphStoreCypherQueryV2AriNode{
								{ID: userARI("acc-3"), Data: &gen.GraphStoreCypherQueryV2AriNodeData{Typename: "AtlassianAccountUser"}}}}}),
							column("team", cypherNode(t1, "TeamV2", nil))},
					}, false, nil, false),
				},
				projectsField + " " + t1: {
					graphPage(projectsField, [][]gen.GraphStoreCypherQueryV2Column{
						{column("team", cypherNode(t1, "TeamV2", nil)),
							column("project", cypherNode("ari:cloud:jira:site:project/10001", "JiraProject", &gen.GraphStoreCypherQueryV2AriNodeData{Key: str("PAY"), Name: str("Payments")}))},
						// A project named by displayName, key padded.
						{column("teamId", cypherNode(t1, "TeamV2", nil)),
							column("projectId", cypherNode("ari:cloud:jira:site:project/10002", "JiraProject", &gen.GraphStoreCypherQueryV2AriNodeData{Key: str("  WEB "), DisplayName: str("Web")}))},
						// A Townsquare project with no key.
						{column("team", cypherNode(t1, "TeamV2", nil)),
							column("project", cypherNode("ari:cloud:townsquare:site:project/7", "TownsquareProject", &gen.GraphStoreCypherQueryV2AriNodeData{Name: str("Goal")}))},
					}, false, nil, false),
				},
				usersField + " " + t2:    {graphPage(usersField, nil, false, nil, false)},
				projectsField + " " + t2: {graphPage(projectsField, nil, false, nil, false)},
			},
		})

		// A team with no display name: a required field is missing.
		n = name("no-name")
		add(scenario{name: n, strict: strict,
			teams: [][]byte{teamPage([]gen.TeamNode{{ID: teamARI(n, "1"), State: str("ACTIVE")}}, false, nil, false)}})

		// GraphQL errors next to valid data, on the search and on a graph read.
		n = name("errors-with-data")
		e1 := teamARI(n, "1")
		add(scenario{name: n, strict: strict, teamIDs: []string{e1},
			teams: [][]byte{teamPage([]gen.TeamNode{{ID: e1, DisplayName: str("Ops"), State: str("ACTIVE")}}, false, nil, true)},
			graph: map[string][][]byte{
				usersField + " " + e1: {graphPage(usersField, [][]gen.GraphStoreCypherQueryV2Column{
					{column("user", cypherNode(userARI("acc-9"), "AtlassianAccountUser", nil))}}, false, nil, true)},
				projectsField + " " + e1: {graphPage(projectsField, nil, false, nil, true)},
			}})

		// A page that promises more but gives no cursor.
		n = name("missing-cursor")
		m1 := teamARI(n, "1")
		add(scenario{name: n, strict: strict, teamIDs: []string{m1},
			teams: [][]byte{teamPage([]gen.TeamNode{{ID: m1, DisplayName: str("Solo"), State: str("ACTIVE")}}, true, nil, false)},
			graph: map[string][][]byte{
				usersField + " " + m1: {graphPage(usersField, [][]gen.GraphStoreCypherQueryV2Column{
					{column("user", cypherNode(userARI("acc-5"), "AtlassianAccountUser", nil))}}, true, nil, false)},
				projectsField + " " + m1: {graphPage(projectsField, nil, true, nil, false)},
			}})

		// A member row with no team column.
		n = name("member-without-team")
		w1 := teamARI(n, "1")
		add(scenario{name: n, strict: strict, teamIDs: []string{w1},
			teams: [][]byte{teamPage([]gen.TeamNode{{ID: w1, DisplayName: str("Loose"), State: str("ACTIVE")}}, false, nil, false)},
			graph: map[string][][]byte{
				usersField + " " + w1: {graphPage(usersField, [][]gen.GraphStoreCypherQueryV2Column{
					{column("user", cypherNode(userARI("acc-7"), "AtlassianAccountUser", nil))}}, false, nil, false)},
				projectsField + " " + w1: {graphPage(projectsField, nil, false, nil, false)},
			}})

		// A graph row with no user at all, and a project row with no project.
		n = name("unmappable-rows")
		r1 := teamARI(n, "1")
		add(scenario{name: n, strict: strict, teamIDs: []string{r1},
			teams: [][]byte{teamPage([]gen.TeamNode{{ID: r1, DisplayName: str("Edge"), State: str("ACTIVE")}}, false, nil, false)},
			graph: map[string][][]byte{
				usersField + " " + r1: {graphPage(usersField, [][]gen.GraphStoreCypherQueryV2Column{
					{column("team", cypherNode(r1, "TeamV2", nil))}}, false, nil, false)},
				projectsField + " " + r1: {graphPage(projectsField, [][]gen.GraphStoreCypherQueryV2Column{
					{column("team", cypherNode(r1, "TeamV2", nil)), column("project", nil)}}, false, nil, false)},
			}})
	}
	return out
}

// oracleGateway serves every scenario: teamSearchV2 by organization id and page
// cursor, the graph reads by team id and cursor ("" is the first page, "cN"
// or "uN" the N-th). It records any request it could not answer.
type oracleGateway struct {
	byOrg   map[string]scenario
	byTeam  map[string]scenario
	mu      sync.Mutex
	strange []string
}

func newOracleGateway(scenarios []scenario) *oracleGateway {
	g := &oracleGateway{byOrg: map[string]scenario{}, byTeam: map[string]scenario{}}
	for _, s := range scenarios {
		g.byOrg["org-"+s.name] = s
		for key := range s.graph {
			_, team, _ := strings.Cut(key, " ")
			g.byTeam[team] = s
		}
	}
	return g
}

func pageIndex(after any) int {
	cursor, _ := after.(string)
	if cursor == "" {
		return 0
	}
	index, err := strconv.Atoi(cursor[1:])
	if err != nil {
		return -1
	}
	return index
}

func (g *oracleGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var request struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	_ = json.Unmarshal(raw, &request)
	fail := func(reason string) {
		g.mu.Lock()
		g.strange = append(g.strange, reason)
		g.mu.Unlock()
		http.Error(w, reason, http.StatusBadRequest)
	}
	var pages [][]byte
	switch {
	case strings.Contains(request.Query, "teamSearchV2"):
		org, _ := request.Variables["organizationId"].(string)
		s, ok := g.byOrg[org]
		if !ok {
			fail("unknown organization " + org)
			return
		}
		pages = s.teams
	case strings.Contains(request.Query, usersField), strings.Contains(request.Query, projectsField):
		field := usersField
		if strings.Contains(request.Query, projectsField) {
			field = projectsField
		}
		team, _ := request.Variables["teamId"].(string)
		s, ok := g.byTeam[team]
		if !ok {
			fail("unknown team " + team)
			return
		}
		pages = s.graph[field+" "+team]
	default:
		fail("unexpected query")
		return
	}
	index := pageIndex(request.Variables["after"])
	if index < 0 || index >= len(pages) {
		fail(fmt.Sprintf("no page %v", request.Variables["after"]))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(pages[index])
}

// leaf is a typed value: {"t": type, "v": text}, so no bare JSON number or
// null is compared loosely.
type leaf struct {
	T string `json:"t"`
	V string `json:"v"`
}

func typedString(value string) leaf { return leaf{T: "str", V: value} }

func typedOptional(value *string) leaf {
	if value == nil {
		return leaf{T: "null"}
	}
	return typedString(*value)
}

func typedInt(value *int) leaf {
	if value == nil {
		return leaf{T: "null"}
	}
	return leaf{T: "int", V: strconv.Itoa(*value)}
}

// record is one mapped model: its fields in declaration order.
type record [][2]any

// outcome is one read: "ok" and the records, or "error" (the error text is
// language-specific and is not compared).
type outcome struct {
	Status  string   `json:"status"`
	Records []record `json:"records"`
	// Detail is the error text, shown in a failure and never compared.
	Detail string `json:"-"`
}

func teamRecords(teams []atlassian.AtlassianTeam) []record {
	var out []record
	for _, t := range teams {
		out = append(out, record{{"id", typedString(t.ID)}, {"display_name", typedString(t.DisplayName)},
			{"state", typedString(t.State)}, {"description", typedOptional(t.Description)},
			{"avatar_url", typedOptional(t.AvatarURL)}, {"member_count", typedInt(t.MemberCount)}})
	}
	return out
}

func userRecords(users []atlassian.TeamworkUserRelation) []record {
	var out []record
	for _, u := range users {
		out = append(out, record{{"subject_user_id", typedString(u.SubjectUserID)}, {"relation_type", typedString(u.RelationType)},
			{"team_id", typedOptional(u.TeamID)}, {"related_user_id", typedOptional(u.RelatedUserID)}})
	}
	return out
}

func projectRecords(projects []atlassian.TeamworkProject) []record {
	var out []record
	for _, p := range projects {
		out = append(out, record{{"team_id", typedString(p.TeamID)}, {"project_id", typedString(p.ProjectID)},
			{"project_key", typedOptional(p.ProjectKey)}, {"project_name", typedOptional(p.ProjectName)}})
	}
	return out
}

func toOutcome(records []record, err error) outcome {
	if err != nil {
		return outcome{Status: "error", Detail: err.Error()}
	}
	if records == nil {
		records = []record{}
	}
	return outcome{Status: "ok", Records: records}
}

// goReads runs the vendored Go client over every scenario: "<scenario> teams",
// "<scenario> users <team>", "<scenario> projects <team>".
func goReads(ctx context.Context, base string, scenarios []scenario) map[string]outcome {
	out := map[string]outcome{}
	for _, s := range scenarios {
		// As synccli builds it: Strict, and CompletePagesOnly on the transport.
		client := &graph.Client{BaseURL: base, Auth: atlassian.BasicAPITokenAuth{Email: "oracle@example.test", Token: "t"}, Strict: s.strict,
			HTTPClient: &http.Client{Transport: CompletePagesOnly(nil)}}
		teams, err := client.SearchTeams(ctx, "org-"+s.name, "site-"+s.name, "", 2)
		out[s.name+" teams"] = toOutcome(teamRecords(teams), err)
		for _, team := range s.teamIDs {
			users, err := client.IterTeamUsers(ctx, team, 2)
			out[s.name+" users "+team] = toOutcome(userRecords(users), err)
			projects, err := client.IterTeamActiveProjects(ctx, team, 2)
			out[s.name+" projects "+team] = toOutcome(projectRecords(projects), err)
		}
	}
	return out
}

const pythonClientProgram = `
import json, sys
from atlassian.auth import BasicApiTokenAuth
from atlassian.graph.client import GraphQLClient
from atlassian.graph.api.teams import iter_teams
from atlassian.graph.api.teamwork_graph import iter_team_users, iter_team_active_projects

def s(v): return {"t": "str", "v": v}
def o(v): return {"t": "null", "v": ""} if v is None else s(v)
def i(v): return {"t": "null", "v": ""} if v is None else {"t": "int", "v": str(v)}

def read(fn, to_record):
    try:
        return {"status": "ok", "records": [to_record(x) for x in fn()]}
    except Exception as exc:
        # The text is language-specific: kept for the failure message only.
        return {"status": "error", "records": None, "detail": type(exc).__name__ + ": " + str(exc)[:300]}

req = json.loads(sys.stdin.read())
out = {}
for sc in req["scenarios"]:
    client = GraphQLClient(req["base"], BasicApiTokenAuth("oracle@example.test", "t"), strict=sc["strict"], max_retries_429=0)
    name = sc["name"]
    out[name + " teams"] = read(lambda: iter_teams(client, "org-" + name, "site-" + name, first=2),
        lambda t: [["id", s(t.id)], ["display_name", s(t.display_name)], ["state", s(t.state)],
                   ["description", o(t.description)], ["avatar_url", o(t.avatar_url)], ["member_count", i(t.member_count)]])
    for team in sc["teams"] or []:
        out[name + " users " + team] = read(lambda: iter_team_users(client, team, first=2),
            lambda u: [["subject_user_id", s(u.subject_user_id)], ["relation_type", s(u.relation_type)],
                       ["team_id", o(u.team_id)], ["related_user_id", o(u.related_user_id)]])
        out[name + " projects " + team] = read(lambda: iter_team_active_projects(client, team, first=2),
            lambda p: [["team_id", s(p.team_id)], ["project_id", s(p.project_id)],
                       ["project_key", o(p.project_key)], ["project_name", o(p.project_name)]])
print(json.dumps(out))
`

func pythonReads(t *testing.T, root, base string, scenarios []scenario) map[string]outcome {
	t.Helper()
	python := pyoracle.Resolve(t, root)
	zipPath := filepath.Join(root, "internal", "atlassianteams", pythonClientZip)
	content, err := os.ReadFile(zipPath)
	if err != nil {
		t.Fatalf("read the pinned Python client: %v", err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(content)); got != pythonClientSHA256 {
		t.Fatalf("%s has sha256 %s, want %s: the pinned reference changed", pythonClientZip, got, pythonClientSHA256)
	}
	type sc struct {
		Name   string   `json:"name"`
		Strict bool     `json:"strict"`
		Teams  []string `json:"teams"`
	}
	var request struct {
		Base      string `json:"base"`
		Scenarios []sc   `json:"scenarios"`
	}
	request.Base = base
	for _, s := range scenarios {
		request.Scenarios = append(request.Scenarios, sc{Name: s.name, Strict: s.strict, Teams: s.teamIDs})
	}
	input, _ := json.Marshal(request)
	command := osexec.Command(python, "-c", pythonClientProgram)
	// The pinned client first, so `atlassian` is the reference, not the
	// venv's older package.
	command.Env = append(os.Environ(), "PYTHONPATH="+zipPath)
	command.Dir = t.TempDir()
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &decoded); err != nil {
		t.Fatalf("decode python output: %v\n%s", err, output)
	}
	out := map[string]outcome{}
	for key, raw := range decoded {
		var o outcome
		if err := json.Unmarshal(raw, &o); err != nil {
			t.Fatalf("decode %s: %v", key, err)
		}
		var detail struct {
			Detail string `json:"detail"`
		}
		_ = json.Unmarshal(raw, &detail)
		o.Detail = detail.Detail
		out[key] = o
	}
	return out
}

// normalize renders an outcome for comparison; records keep their order.
func normalize(o outcome) string {
	raw, _ := json.Marshal(o)
	var generic any
	_ = json.Unmarshal(raw, &generic)
	canonical, _ := json.Marshal(generic)
	return string(canonical)
}

// excludedFields are the model fields Collect never reads, so neither side
// compares them; each carries its reason. The compared set is exactly what
// Collect reads: a team's id, displayName, state and description; a user
// relation's subject and type; a project link's key. Two clients disagree on
// excluded fields, which is why they are named rather than compared:
//
//   - avatar_url: Go maps smallAvatarImageUrl, Python leaves it null.
//   - project_name: Go falls back to displayName, Python reads only name.
var excludedFields = map[string]string{
	"avatar_url":      "Collect does not store a team avatar",
	"member_count":    "Collect counts members from the Teamwork Graph, not this field",
	"team_id":         "Collect already knows the team it asked about",
	"related_user_id": "Collect reads TEAM_MEMBER relations only, which have no related user",
	"project_id":      "Collect builds the project id from the key",
	"project_name":    "Collect never stores a project name",
}

func withoutExcluded(o outcome) outcome {
	out := outcome{Status: o.Status, Detail: o.Detail}
	if o.Records == nil {
		return out
	}
	out.Records = []record{}
	for _, r := range o.Records {
		var kept record
		for _, field := range r {
			if name, _ := field[0].(string); excludedFields[name] == "" {
				kept = append(kept, field)
			}
		}
		out.Records = append(out.Records, kept)
	}
	return out
}

// staleExclusions names the excluded fields no record carries: an exclusion
// that matches nothing is a stale one.
func staleExclusions(reads map[string]outcome) []string {
	seen := map[string]bool{}
	for _, o := range reads {
		for _, r := range o.Records {
			for _, field := range r {
				name, _ := field[0].(string)
				seen[name] = true
			}
		}
	}
	var stale []string
	for name := range excludedFields {
		if !seen[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	return stale
}

// compare returns one line per read whose outcomes differ.
func compare(python, goSide map[string]outcome) []string {
	keys := map[string]bool{}
	for key := range python {
		keys[key] = true
	}
	for key := range goSide {
		keys[key] = true
	}
	var sorted []string
	for key := range keys {
		sorted = append(sorted, key)
	}
	sort.Strings(sorted)
	var mismatches []string
	for _, key := range sorted {
		py, pyOK := python[key]
		gv, goOK := goSide[key]
		if !pyOK || !goOK {
			mismatches = append(mismatches, key+": read on one side only")
			continue
		}
		py, gv = withoutExcluded(py), withoutExcluded(gv)
		if normalize(py) != normalize(gv) {
			mismatches = append(mismatches, fmt.Sprintf("%s:\n python %s %s\n go     %s %s", key, normalize(py), py.Detail, normalize(gv), gv.Detail))
		}
	}
	return mismatches
}

// TestAtlassianTeamsClientMatchesLivePython compares the vendored Go
// atlassian client the sync reads through (SearchTeams, IterTeamUsers,
// IterTeamActiveProjects) with the full-chaos/atlassian Python Teams client
// at the same sha (iter_teams, iter_team_users, iter_team_active_projects):
// both read one fake gateway that serves a corpus built from the client's
// generated response types, in the strict configuration the sync runs, and every mapped model (typed
// leaves) or the fact of an error must match.
//
// The known-defect gate plants one divergence in each compared family (a
// team field, a user relation, a project link, an outcome) and requires the
// comparison to report it.
func TestAtlassianTeamsClientMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	scenarios := corpus()
	gw := newOracleGateway(scenarios)
	server := httptest.NewServer(gw)
	defer server.Close()

	python := pythonReads(t, root, server.URL, scenarios)
	goSide := goReads(context.Background(), server.URL, scenarios)
	if len(gw.strange) > 0 {
		t.Fatalf("the gateway could not answer %d request(s): %v", len(gw.strange), gw.strange)
	}
	statuses := map[string]int{}
	records := 0
	for _, o := range goSide {
		statuses[o.Status]++
		records += len(o.Records)
	}
	if statuses["ok"] == 0 || statuses["error"] == 0 || records == 0 {
		t.Fatalf("the corpus must reach both outcomes and some records: %v, %d records", statuses, records)
	}
	if stale := staleExclusions(goSide); len(stale) > 0 {
		t.Fatalf("excluded fields no record carries (stale exclusions): %v", stale)
	}
	mismatches := compare(python, goSide)
	for _, m := range mismatches {
		t.Error(m)
	}
	if len(mismatches) > 0 {
		t.Fatalf("%d of %d reads differ", len(mismatches), len(goSide))
	}

	// Known-defect gate: each planted divergence must be found.
	plant := func(family string, mutate func(map[string]outcome) bool) {
		t.Helper()
		copied := map[string]outcome{}
		for key, o := range goSide {
			records := make([]record, len(o.Records))
			for index, r := range o.Records {
				records[index] = append(record(nil), r...)
			}
			copied[key] = outcome{Status: o.Status, Records: records}
		}
		if !mutate(copied) {
			t.Fatalf("known-defect gate %s: the corpus has nothing to plant it in", family)
		}
		if len(compare(python, copied)) == 0 {
			t.Errorf("known-defect gate %s: the planted divergence was not found", family)
		}
	}
	setField := func(field string, to leaf) func(map[string]outcome) bool {
		return func(reads map[string]outcome) bool {
			for key, o := range reads {
				for _, r := range o.Records {
					for index := range r {
						if r[index][0] == field {
							r[index][1] = to
							reads[key] = o
							return true
						}
					}
				}
			}
			return false
		}
	}
	plant("team state", setField("state", typedString("ARCHIVED-OR-NOT")))
	plant("team name", setField("display_name", typedString("Planted")))
	plant("team description", setField("description", typedString("planted")))
	plant("relation type", setField("relation_type", typedString("MANAGES")))
	plant("user subject", setField("subject_user_id", typedString("ari:cloud:identity::user/planted")))
	plant("project key", setField("project_key", leaf{T: "null"}))
	plant("outcome", func(reads map[string]outcome) bool {
		for key, o := range reads {
			if o.Status == "ok" {
				reads[key] = outcome{Status: "error"}
				return true
			}
		}
		return false
	})
	plant("record dropped", func(reads map[string]outcome) bool {
		for key, o := range reads {
			if len(o.Records) > 1 {
				o.Records = o.Records[1:]
				reads[key] = o
				return true
			}
		}
		return false
	})
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "atlassianteams-python-client"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d scenarios, %d reads compared (%v), %d records; 0 mismatches; 8 planted divergences found", len(scenarios), len(goSide), statuses, records)
}
