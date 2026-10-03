package atlassianteams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"
	"atlassian/atlassian/graph/gen"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
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
// the generated gen.TeamSearchV2Data envelope, plus
// top-level GraphQL errors when errs is set.
func teamPage(nodes []gen.TeamNode, hasNext bool, cursor *string, errs bool) []byte {
	connection := &gen.TeamSearchConnection{Nodes: []gen.TeamSearchResultNode{}, PageInfo: gen.TeamPageInfo{HasNextPage: hasNext, EndCursor: cursor}}
	for index := range nodes {
		connection.Nodes = append(connection.Nodes, gen.TeamSearchResultNode{Team: &nodes[index]})
	}
	data := gen.TeamSearchV2Data{Team: &struct {
		Search *gen.TeamSearchConnection `json:"teamSearchV2"`
	}{Search: connection}}
	return envelope(data, errs)
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

	// The call arguments and the client configuration; the zero value is the
	// configuration the sync runs ("org-"+name, "site-"+name, pages of 2).
	orgArg, siteArg *string
	first           *int
	teamArgs        []string // read these teams instead of teamIDs
	config          clientConfig
	// faults answers the Nth request (per client) for a key -- "teams" or
	// "<field> <team>" -- with the fault instead of the page.
	faults map[string][]fault
}

// clientConfig is the part of the two clients' configuration both expose.
type clientConfig struct {
	retries        int  // Python max_retries_429; Go -1 when 0 (Go reads 0 as "default 2")
	defaultRetries bool // Go MaxRetries429 0 (default 2) against Python 2
	maxWait        int  // seconds, > 0; 0 leaves the default (60)
	throttling     bool
	baseSuffix     string  // appended to the gateway address
	baseURL        *string // replaces the gateway address
	noGuard        bool    // the Go client without CompletePagesOnly
	userAgent      string
	noAuth         bool // an empty email and token
}

func (f fault) isZero() bool {
	return f.status == 0 && len(f.header) == 0 && f.body == "" && !f.closeConn && !f.truncate && f.padTo == 0
}

// fault is one scripted gateway answer.
type fault struct {
	status    int
	header    map[string]string
	body      string
	closeConn bool // hang up without an answer
	truncate  bool // promise more bytes than are sent
	padTo     int  // pad the page with spaces to this many bytes
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
		// fields present and absent. The member rows of this scenario keep a
		// SYNTHETIC team column: the pinned Python client refuses a member row
		// without one, and a refused read would end the scenario before its
		// pagination, node-list and project reads are compared. The real shape
		// (one user column) is the "member-without-team" scenario below.
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

		// A member row with no team column: the REAL gateway shape (one user column per edge, measured by a structure-only probe on
		// 2026-10-02; the team is the request variable). The pinned Python client refuses it ("TEAM_MEMBER relation requires team node",
		// mappers/teams.py:237-239 via iter_team_users, teamwork_graph.py:108-128); the Go client serves it with the team of the request
		// (CHAOS-7902). This read is pinned as a Known divergence, "Go serves, the reference fails" (see knownDivergences).
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
	return append(out, extraCorpusA()...)
}

// oracleGateway serves every scenario: teamSearchV2 by organization id and page
// cursor, the graph reads by team id and cursor ("" is the first page, "cN"
// or "uN" the N-th). It records any request it could not answer.
type oracleGateway struct {
	byOrg   map[string]scenario
	byTeam  map[string]scenario
	mu      sync.Mutex
	strange []string
	seen    map[string]int // requests so far per client kind and key
}

func newOracleGateway(scenarios []scenario) *oracleGateway {
	g := &oracleGateway{byOrg: map[string]scenario{}, byTeam: map[string]scenario{}, seen: map[string]int{}}
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
	// A cursor of only whitespace (cursor-blank) names the second page.
	if cursor = strings.TrimSpace(cursor); cursor == "" {
		return 1
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
	var scen scenario
	var key string
	switch {
	case strings.Contains(request.Query, "teamSearchV2"):
		org, _ := request.Variables["organizationId"].(string)
		s, ok := g.byOrg[org]
		if !ok {
			fail("unknown organization " + org)
			return
		}
		pages, scen, key = s.teams, s, "teams"
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
		pages, scen, key = s.graph[field+" "+team], s, field+" "+team
	default:
		fail("unexpected query")
		return
	}
	// Every fault is per client: the Python reads ran at record time, the Go
	// reads run now, and neither may consume the other's answers.
	kind := "python"
	if strings.HasPrefix(r.Header.Get("User-Agent"), "atlassian-go") || r.Header.Get("User-Agent") == goOracleUserAgent {
		kind = "go"
	}
	g.mu.Lock()
	ordinal := g.seen[kind+"|"+scen.name+"|"+key]
	g.seen[kind+"|"+scen.name+"|"+key] = ordinal + 1
	g.mu.Unlock()
	if faults := scen.faults[key]; ordinal < len(faults) && !faults[ordinal].isZero() {
		serveFault(w, faults[ordinal], pageFor(pages, request.Variables["after"]))
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

// goOracleUserAgent is the user agent the Go reads send, so the gateway can
// tell the two clients apart without a header only one of them sets.
const goOracleUserAgent = "atlassian-go/0.1.0"

func pageFor(pages [][]byte, after any) []byte {
	index := pageIndex(after)
	if index < 0 || index >= len(pages) {
		return []byte("{}")
	}
	return pages[index]
}

func serveFault(w http.ResponseWriter, f fault, page []byte) {
	for name, value := range f.header {
		w.Header().Set(name, value)
	}
	if f.closeConn {
		if hijacker, ok := w.(http.Hijacker); ok {
			if conn, _, err := hijacker.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		return
	}
	body := []byte(f.body)
	if f.body == "" && f.padTo > 0 {
		body = append(append([]byte(nil), page...), make([]byte, 0)...)
		for len(body) < f.padTo {
			body = append(body, ' ')
		}
	}
	if f.truncate {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)+64))
	}
	status := f.status
	if status == 0 {
		status = http.StatusOK
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
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
// language-specific and is not compared, its class is).
type outcome struct {
	Status  string   `json:"status"`
	Records []record `json:"records"`
	// Class is the family of the error: graphql, transport, ratelimit or
	// other ("" when the read succeeded).
	Class string `json:"class"`
	// Requests are the requests the client sent, in order; Sleeps the waits
	// it asked for.
	Requests []requestMeta `json:"requests"`
	Sleeps   []string      `json:"sleeps"`
	// Detail is the error text, shown in a failure and never compared.
	Detail string `json:"-"`
}

// requestMeta is what one request carried that both clients must agree on:
// the operation, its variables, the normalised query, the opt-in headers and
// the fixed headers; the user agent only as "default" or the custom value.
type requestMeta struct {
	Operation    string   `json:"operation"`
	Variables    string   `json:"variables"`
	Query        string   `json:"query"`
	Experimental []string `json:"experimental"`
	ContentType  string   `json:"content_type"`
	Accept       string   `json:"accept"`
	Auth         string   `json:"auth"`
	UserAgent    string   `json:"user_agent"`
	Path         string   `json:"path"`
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
		return outcome{Status: "error", Class: errorClass(err), Detail: err.Error()}
	}
	if records == nil {
		records = []record{}
	}
	return outcome{Status: "ok", Records: records}
}

// errorClass is the family of a Go error, named as the Python exception
// families are: GraphQLOperationError, TransportError (an HTTP status),
// RateLimitError; everything else is "other".
func errorClass(err error) string {
	var operation *atlassian.GraphQLOperationError
	var transport *atlassian.TransportError
	var limited *atlassian.RateLimitError
	switch {
	case errors.As(err, &operation):
		return "graphql"
	case errors.As(err, &transport):
		return "transport"
	case errors.As(err, &limited):
		return "ratelimit"
	}
	return "other"
}

// requestRecorder is the transport under the guard: it keeps what each
// request carried, as the Python request hook does.
type requestRecorder struct {
	next     http.RoundTripper
	mu       sync.Mutex
	requests []requestMeta
}

func (r *requestRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	raw, _ := io.ReadAll(req.Body)
	req.Body = io.NopCloser(bytes.NewReader(raw))
	r.mu.Lock()
	r.requests = append(r.requests, metaOf(req.Header.Get, req.Header.Values("X-ExperimentalApi"), req.URL.Path, raw, goOracleUserAgent))
	r.mu.Unlock()
	return r.next.RoundTrip(req)
}

func (r *requestRecorder) take() []requestMeta {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.requests
	r.requests = nil
	if out == nil {
		out = []requestMeta{}
	}
	return out
}

// metaOf renders one request the way the Python program renders it.
func metaOf(header func(string) string, experimental []string, path string, body []byte, defaultAgent string) requestMeta {
	var payload struct {
		Query         string         `json:"query"`
		OperationName string         `json:"operationName"`
		Variables     map[string]any `json:"variables"`
	}
	_ = json.Unmarshal(body, &payload)
	variables, _ := json.Marshal(payload.Variables)
	if payload.Variables == nil {
		variables = []byte("null")
	}
	sorted := append([]string{}, experimental...)
	sort.Strings(sorted)
	auth, _, _ := strings.Cut(header("Authorization"), " ")
	agent := header("User-Agent")
	if agent == defaultAgent {
		agent = "default"
	}
	return requestMeta{
		Operation: payload.OperationName, Variables: string(variables),
		Query:        fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(strings.Fields(payload.Query), " ")))),
		Experimental: sorted, ContentType: header("Content-Type"), Accept: header("Accept"), Auth: auth, UserAgent: agent, Path: path,
	}
}

// oracleNow is the instant both clients read as "now": Retry-After values in
// the corpus are written against it.
var oracleNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// goReads runs the vendored Go client over every scenario: "<scenario> teams",
// "<scenario> users <team>", "<scenario> projects <team>".
func goReads(ctx context.Context, base string, scenarios []scenario) map[string]outcome {
	out := map[string]outcome{}
	for _, s := range scenarios {
		recorder := &requestRecorder{next: http.DefaultTransport}
		var sleeps []string
		auth := atlassian.BasicAPITokenAuth{Email: "oracle@example.test", Token: "t"}
		if s.config.noAuth {
			auth = atlassian.BasicAPITokenAuth{}
		}
		// As synccli builds it: Strict, and CompletePagesOnly on the transport.
		baseURL := base + s.config.baseSuffix
		if s.config.baseURL != nil {
			baseURL = *s.config.baseURL
		}
		var transport http.RoundTripper = CompletePagesOnly(recorder)
		if s.config.noGuard {
			transport = recorder
		}
		client := &graph.Client{BaseURL: baseURL, Auth: auth, Strict: s.strict,
			HTTPClient: &http.Client{Transport: transport},
			Now:        func() time.Time { return oracleNow },
			Sleep:      func(d time.Duration) { sleeps = append(sleeps, fmt.Sprintf("%.3f", d.Seconds())) },
		}
		switch {
		case s.config.defaultRetries:
			client.MaxRetries429 = 0
		case s.config.retries == 0:
			client.MaxRetries429 = -1
		default:
			client.MaxRetries429 = s.config.retries
		}
		if s.config.maxWait > 0 {
			client.MaxWait = time.Duration(s.config.maxWait) * time.Second
		}
		client.EnableLocalThrottling = s.config.throttling
		if s.config.userAgent != "" {
			client.UserAgent = s.config.userAgent
		}
		finish := func(o outcome) outcome {
			o.Requests = recorder.take()
			o.Sleeps = append([]string{}, sleeps...)
			sleeps = nil
			return o
		}
		org, site, first := "org-"+s.name, "site-"+s.name, 2
		if s.orgArg != nil {
			org = *s.orgArg
		}
		if s.siteArg != nil {
			site = *s.siteArg
		}
		if s.first != nil {
			first = *s.first
		}
		teams, err := client.SearchTeams(ctx, org, site, "", first)
		out[s.name+" teams"] = finish(toOutcome(teamRecords(teams), err))
		for _, team := range s.readTeams() {
			users, err := client.IterTeamUsers(ctx, team, first)
			out[s.name+" users "+team] = finish(toOutcome(userRecords(users), err))
			projects, err := client.IterTeamActiveProjects(ctx, team, first)
			out[s.name+" projects "+team] = finish(toOutcome(projectRecords(projects), err))
		}
	}
	return out
}

// readTeams is the teams whose users and projects are read.
func (s scenario) readTeams() []string {
	if s.teamArgs != nil {
		return s.teamArgs
	}
	return s.teamIDs
}

const pythonClientProgram = `
import hashlib, json, os, sys
from datetime import datetime, timezone
import httpx
# The pinned client first, so "atlassian" is the reference, not the venv's older
# package. The zip is part of the pinned checkout the program runs in.
sys.path.insert(0, os.path.join(os.getcwd(), "internal", "atlassianteams", "testdata", "atlassian_python_cb7c3665.zip"))
from atlassian.auth import BasicApiTokenAuth
from atlassian.graph.client import GraphQLClient
from atlassian.graph.api.teams import iter_teams
from atlassian.graph.api.teamwork_graph import iter_team_users, iter_team_active_projects

NOW = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
DEFAULT_AGENT = "atlassian-graphql-python/0.1.0"

def s(v): return {"t": "str", "v": v}
def o(v): return {"t": "null", "v": ""} if v is None else s(v)
def i(v): return {"t": "null", "v": ""} if v is None else {"t": "int", "v": str(v)}

CLASSES = {"GraphQLOperationError": "graphql", "TransportError": "transport", "RateLimitError": "ratelimit"}

requests = []
sleeps = []

def record_request(request):
    body = json.loads(request.content.decode())
    variables = body.get("variables")
    agent = request.headers.get("user-agent", "")
    requests.append({
        "operation": body.get("operationName", ""),
        "variables": "null" if variables is None else json.dumps(variables, sort_keys=True, separators=(",", ":")),
        "query": hashlib.sha256(" ".join(body.get("query", "").split()).encode()).hexdigest(),
        "experimental": sorted(request.headers.get_list("x-experimentalapi")),
        "content_type": request.headers.get("content-type", ""),
        "accept": request.headers.get("accept", ""),
        "auth": request.headers.get("authorization", "").split(" ")[0],
        "user_agent": "default" if agent == DEFAULT_AGENT else agent,
        "path": request.url.path,
    })

def read(fn, to_record):
    del requests[:]
    del sleeps[:]
    try:
        out = {"status": "ok", "records": [to_record(x) for x in fn()], "class": ""}
    except Exception as exc:
        # The text is language-specific: kept for the failure message only.
        cls = CLASSES.get(type(exc).__name__, "other")
        if cls == "transport" and getattr(exc, "status_code", None) == 0:
            cls = "other"  # the connection failed before any HTTP status
        out = {"status": "error", "records": None, "class": cls,
               "detail": (type(exc).__name__ + ": " + str(exc)).replace(BASE, "<gateway>")[:300]}
    out["requests"] = list(requests)
    out["sleeps"] = list(sleeps)
    return out

# The gateway address is of one run: it reaches the program by name, and nothing
# the program prints holds it.
BASE = os.environ["ATLASSIAN_ORACLE_GATEWAY"]
req = json.loads(sys.stdin.read())
out = {}
for sc in req["scenarios"]:
    name = sc["name"]
    cfg = sc["config"]
    http_client = httpx.Client(timeout=15.0, event_hooks={"request": [record_request]})
    try:
        auth = BasicApiTokenAuth("", "") if cfg["no_auth"] else BasicApiTokenAuth("oracle@example.test", "t")
        kwargs = {"strict": sc["strict"], "max_retries_429": 2 if cfg["default_retries"] else cfg["retries"],
                  "sleeper": lambda seconds: sleeps.append("%.3f" % seconds), "time_provider": lambda: NOW,
                  "enable_local_throttling": cfg["throttling"], "http_client": http_client}
        if cfg["max_wait"] > 0:
            kwargs["max_wait_seconds"] = cfg["max_wait"]
        if cfg["user_agent"]:
            kwargs["user_agent"] = cfg["user_agent"]
        client = GraphQLClient(cfg["base_url"] if cfg["base_url"] is not None else BASE + cfg["base_suffix"], auth, **kwargs)
        failure = None
    except Exception as exc:
        client, failure = None, exc
    def go(fn, to_record):
        if failure is not None:
            return {"status": "error", "records": None, "class": CLASSES.get(type(failure).__name__, "other"),
                    "detail": (type(failure).__name__ + ": " + str(failure))[:300], "requests": [], "sleeps": []}
        return read(fn, to_record)
    org = sc["org"] if sc["org"] is not None else "org-" + name
    site = sc["site"] if sc["site"] is not None else "site-" + name
    first = sc["first"] if sc["first"] is not None else 2
    out[name + " teams"] = go(lambda: iter_teams(client, org, site, first=first),
        lambda t: [["id", s(t.id)], ["display_name", s(t.display_name)], ["state", s(t.state)],
                   ["description", o(t.description)], ["avatar_url", o(t.avatar_url)], ["member_count", i(t.member_count)]])
    for team in sc["teams"] or []:
        out[name + " users " + team] = go(lambda: iter_team_users(client, team, first=first),
            lambda u: [["subject_user_id", s(u.subject_user_id)], ["relation_type", s(u.relation_type)],
                       ["team_id", o(u.team_id)], ["related_user_id", o(u.related_user_id)]])
        out[name + " projects " + team] = go(lambda: iter_team_active_projects(client, team, first=first),
            lambda p: [["team_id", s(p.team_id)], ["project_id", s(p.project_id)],
                       ["project_key", o(p.project_key)], ["project_name", o(p.project_name)]])
print(json.dumps(out))
`

func pythonReads(t *testing.T, root, base string, scenarios []scenario) map[string]outcome {
	t.Helper()
	zipPath := filepath.Join(root, "internal", "atlassianteams", pythonClientZip)
	content, err := os.ReadFile(zipPath)
	if err != nil {
		t.Fatalf("read the pinned Python client: %v", err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(content)); got != pythonClientSHA256 {
		t.Fatalf("%s has sha256 %s, want %s: the pinned reference changed", pythonClientZip, got, pythonClientSHA256)
	}
	type cfg struct {
		Retries        int     `json:"retries"`
		DefaultRetries bool    `json:"default_retries"`
		MaxWait        int     `json:"max_wait"`
		Throttling     bool    `json:"throttling"`
		BaseSuffix     string  `json:"base_suffix"`
		BaseURL        *string `json:"base_url"`
		UserAgent      string  `json:"user_agent"`
		NoAuth         bool    `json:"no_auth"`
	}
	type sc struct {
		Name   string   `json:"name"`
		Strict bool     `json:"strict"`
		Teams  []string `json:"teams"`
		Org    *string  `json:"org"`
		Site   *string  `json:"site"`
		First  *int     `json:"first"`
		Config cfg      `json:"config"`
	}
	var request struct {
		Scenarios []sc `json:"scenarios"`
	}
	for _, s := range scenarios {
		request.Scenarios = append(request.Scenarios, sc{Name: s.name, Strict: s.strict, Teams: s.readTeams(), Org: s.orgArg, Site: s.siteArg, First: s.first,
			Config: cfg{Retries: s.config.retries, DefaultRetries: s.config.defaultRetries, MaxWait: s.config.maxWait, Throttling: s.config.throttling,
				BaseSuffix: s.config.baseSuffix, BaseURL: s.config.baseURL, UserAgent: s.config.userAgent, NoAuth: s.config.noAuth}})
	}
	input, _ := json.Marshal(request)
	output := frozenPython(t, "teams-client.golden.json", programoracle.Program{
		Name: "teams client", Text: pythonClientProgram, Stdin: input,
		PerRun:      func() map[string]string { return map[string]string{"ATLASSIAN_ORACLE_GATEWAY": base} },
		PerRunNames: []string{"ATLASSIAN_ORACLE_GATEWAY"},
	})[0]
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
	out := outcome{Status: o.Status, Class: o.Class, Sleeps: o.Sleeps, Detail: o.Detail}
	// The two clients name the operation and word the query differently (the
	// Go generator names TeamworkGraphTeamUsers where the Python one names
	// TeamworkGraph_teamUsers); neither is a behaviour the sync reads, so they
	// are recorded and not compared. Everything else a request carries is.
	for _, request := range o.Requests {
		request.Operation, request.Query = "", ""
		out.Requests = append(out.Requests, request)
	}
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
// knownDivergences pins the reads where Go intentionally serves what the pinned reference client refuses ("Go serves, the reference
// fails"): the key prefix is "<scenario> users ", the reference must still answer the named error, and Go must answer the named
// members. Anything else on such a read is a mismatch, so the pin cannot hide a wrong Go answer.
var knownDivergences = map[string]struct {
	pythonDetail string
	goRecords    []string // every non-excluded field of every Go record, "name=value", in order
}{
	"member-without-team users ": {pythonDetail: "TEAM_MEMBER relation requires team node",
		goRecords: []string{"subject_user_id=ari:cloud:identity::user/acc-7", "relation_type=TEAM_MEMBER"}},
}

func knownDivergenceFor(key string) (string, bool) {
	for prefix := range knownDivergences {
		if strings.HasPrefix(key, prefix) {
			return prefix, true
		}
	}
	return "", false
}

// checkKnownDivergence reports why a pinned read does NOT hold, or "" when it does.
func checkKnownDivergence(prefix string, py, gv outcome) string {
	known := knownDivergences[prefix]
	if py.Status != "error" || !strings.Contains(py.Detail, known.pythonDetail) {
		return fmt.Sprintf("the reference no longer answers %q (status %s, %q)", known.pythonDetail, py.Status, py.Detail)
	}
	if gv.Status != "ok" {
		return fmt.Sprintf("Go no longer serves the members (status %s, %q)", gv.Status, gv.Detail)
	}
	var got []string
	for _, r := range withoutExcluded(gv).Records {
		for _, f := range r {
			name, _ := f[0].(string)
			value, _ := f[1].(leaf)
			got = append(got, name+"="+value.V)
		}
	}
	if strings.Join(got, ",") != strings.Join(known.goRecords, ",") {
		return fmt.Sprintf("Go serves %v, want %v", got, known.goRecords)
	}
	return ""
}

// brief renders an outcome to the fields a declared divergence pins: the
// status and error family, the compared record fields, the number of requests,
// the page size and cursor of the first one, and the waits.
func brief(o outcome) string {
	var records []string
	for _, r := range withoutExcluded(o).Records {
		var fields []string
		for _, f := range r {
			name, _ := f[0].(string)
			var value leaf
			raw, _ := json.Marshal(f[1])
			_ = json.Unmarshal(raw, &value)
			fields = append(fields, name+"="+value.T+":"+value.V)
		}
		records = append(records, strings.Join(fields, ","))
	}
	first := "-"
	if len(o.Requests) > 0 {
		var vars map[string]any
		_ = json.Unmarshal([]byte(o.Requests[0].Variables), &vars)
		first = fmt.Sprintf("first=%v,after=%v", vars["first"], vars["after"])
	}
	text := fmt.Sprintf("%s/%s records=[%s] requests=%d %s sleeps=%s", o.Status, o.Class, strings.Join(records, ";"), len(o.Requests), first, strings.Join(o.Sleeps, ","))
	return teamARIPattern.ReplaceAllString(text, "<team>")
}

var teamARIPattern = regexp.MustCompile(`ari:cloud:identity::team/[0-9a-f-]{36}`)

// declared pins one read where the vendored Go client and the pinned Python
// client differ and the difference is decided (see declaredDivergences): the
// recorded Python answer and Go's own answer, both as brief renders them.
type declared struct{ python, goSide string }

func declaredDivergenceFor(key string) (string, bool) {
	for prefix := range declaredDivergences {
		if strings.HasPrefix(key, prefix) {
			return prefix, true
		}
	}
	return "", false
}

func checkDeclaredDivergence(prefix string, py, gv outcome) string {
	want := declaredDivergences[prefix]
	if normalize(withoutExcluded(py)) == normalize(withoutExcluded(gv)) {
		return "the two clients now agree"
	}
	if gotPy, gotGo := brief(py), brief(gv); gotPy != want.python || gotGo != want.goSide {
		return fmt.Sprintf("pins python %q / go %q, got python %q / go %q", want.python, want.goSide, gotPy, gotGo)
	}
	return ""
}

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
		if prefix, declared := declaredDivergenceFor(key); declared {
			if why := checkDeclaredDivergence(prefix, py, gv); why != "" {
				mismatches = append(mismatches, key+": the declared divergence does not hold: "+why)
			}
			continue
		}
		if prefix, known := knownDivergenceFor(key); known {
			if why := checkKnownDivergence(prefix, py, gv); why != "" {
				mismatches = append(mismatches, key+": the pinned divergence does not hold: "+why)
			}
			continue
		}
		py, gv = withoutExcluded(py), withoutExcluded(gv)
		if normalize(py) != normalize(gv) {
			mismatches = append(mismatches, fmt.Sprintf("%s:\n python %s %s\n go     %s %s", key, normalize(py), py.Detail, normalize(gv), gv.Detail))
		}
	}
	return mismatches
}

// TestAtlassianTeamsClientMatchesFrozenPython compares the vendored Go
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
func TestAtlassianTeamsClientMatchesFrozenPython(t *testing.T) {
	_, file, _, _ := moduleroot.Caller(0)
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

	// Known-defect gate: each planted divergence must be found. A plant is a pure change of ONE read; the gate applies it to EVERY
	// eligible read in turn (sorted keys, no random pick) and requires, for each: the unplanted copy has 0 mismatches, the planted
	// read is among the mismatches, and no other read is.
	sortedKeys := make([]string, 0, len(goSide))
	for key := range goSide {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)
	copyOf := func(o outcome) outcome {
		// A nil record list stays nil (an error read has none): turning it into an empty list would make the copy differ from the
		// recorded reference on every error read, and compare() would report mismatches for ANY plant, or for none.
		var records []record
		if o.Records != nil {
			records = make([]record, len(o.Records))
			for index, r := range o.Records {
				records[index] = append(record(nil), r...)
			}
		}
		return outcome{Status: o.Status, Records: records, Class: o.Class, Requests: append([]requestMeta{}, o.Requests...), Sleeps: append([]string{}, o.Sleeps...), Detail: o.Detail}
	}
	withRead := func(key string, o outcome) map[string]outcome {
		copied := map[string]outcome{}
		for k, v := range goSide {
			copied[k] = copyOf(v)
		}
		copied[key] = o
		return copied
	}
	if unplanted := compare(python, withRead(sortedKeys[0], copyOf(goSide[sortedKeys[0]]))); len(unplanted) != 0 {
		t.Fatalf("known-defect gate: the unplanted copy already differs from the reference: %v", unplanted)
	}
	plant := func(family string, mutate func(outcome) (outcome, bool)) {
		t.Helper()
		eligible := 0
		for _, key := range sortedKeys {
			changed, ok := mutate(copyOf(goSide[key]))
			if !ok {
				continue
			}
			eligible++
			found := compare(python, withRead(key, changed))
			planted := false
			for _, m := range found {
				if strings.HasPrefix(m, key+":") {
					planted = true
				} else {
					t.Errorf("known-defect gate %s on %q: another read is reported: %s", family, key, m)
				}
			}
			if !planted {
				t.Errorf("known-defect gate %s: the planted divergence in %q was not found (%d mismatches)", family, key, len(found))
			}
		}
		if eligible == 0 {
			t.Errorf("known-defect gate %s: the corpus has nothing to plant it in", family)
		}
	}
	setField := func(field string, to leaf) func(outcome) (outcome, bool) {
		return func(o outcome) (outcome, bool) {
			for _, r := range o.Records {
				for index := range r {
					if r[index][0] == field {
						if value, _ := r[index][1].(leaf); value == to {
							return o, false // already that value: not a change
						}
						r[index][1] = to
						return o, true
					}
				}
			}
			return o, false
		}
	}
	plant("team state", setField("state", typedString("ARCHIVED-OR-NOT")))
	plant("team name", setField("display_name", typedString("Planted")))
	plant("team description", setField("description", typedString("planted")))
	plant("relation type", setField("relation_type", typedString("MANAGES")))
	plant("user subject", setField("subject_user_id", typedString("ari:cloud:identity::user/planted")))
	plant("project key", setField("project_key", leaf{T: "null"}))
	plant("outcome", func(o outcome) (outcome, bool) {
		if o.Status == "ok" {
			return outcome{Status: "error"}, true
		}
		return o, false
	})
	plant("record dropped", func(o outcome) (outcome, bool) {
		if len(o.Records) > 1 {
			o.Records = o.Records[1:]
			return o, true
		}
		return o, false
	})
	t.Logf("%d scenarios, %d reads compared (%v), %d records; 0 mismatches; 8 planted divergence kinds found on every eligible read (the pinned read included)", len(scenarios), len(goSide), statuses, records)
}

// declaredDivergences are the reads where the two clients differ and the
// difference is decided: key prefix -> the recorded Python answer and Go's
// answer, each pinned. Anything else on such a read is a mismatch, and a read
// where the clients agree again is one too.
var declaredDivergences = map[string]declared{
	"base-only-slash projects ":     {python: "error/other records=[] requests=1 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=0 - sleeps="},
	"base-only-slash users ":        {python: "error/other records=[] requests=1 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=0 - sleeps="},
	"base-only-slash teams":         {python: "error/other records=[] requests=1 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=0 - sleeps="},
	"base-invalid-escape projects ": {python: "error/other records=[] requests=1 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=0 - sleeps="},
	"base-invalid-escape users ":    {python: "error/other records=[] requests=1 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=0 - sleeps="},
	"base-invalid-escape teams":     {python: "error/other records=[] requests=1 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=0 - sleeps="},
	"base-blank projects ":          {python: "error/other records=[] requests=1 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=0 - sleeps="},
	"base-blank users ":             {python: "error/other records=[] requests=1 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=0 - sleeps="},
	"base-blank teams":              {python: "error/other records=[] requests=1 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=0 - sleeps="},
	"cursor-blank teams":            {python: "ok/ records=[id=str:<team>,display_name=str:A,state=str:ACTIVE,description=null:;id=str:<team>,display_name=str:B,state=str:ACTIVE,description=null:] requests=2 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=1 first=2,after=<nil> sleeps="},
	"cursor-blank users ":           {python: "ok/ records=[] requests=2 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=1 first=2,after=<nil> sleeps="},
	"cursor-blank projects ":        {python: "ok/ records=[] requests=2 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=1 first=2,after=<nil> sleeps="},
	"oversized-answer teams":        {python: "ok/ records=[id=str:<team>,display_name=str:Mini,state=str:ACTIVE,description=null:] requests=1 first=2,after=<nil> sleeps=", goSide: "error/other records=[] requests=1 first=2,after=<nil> sleeps="},
}
