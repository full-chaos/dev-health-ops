//go:build integration

package synccli

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

//go:embed testdata/teams_sync_oracle_linear.py
var teamsSyncOracleLinearProgram string

// fakeLinear is the one Linear GraphQL endpoint both planes read: python's
// LinearClient (repointed at this server via LINEAR_API_URL, which its module
// hardcodes with no env override -- a named difference from GitHub/GitLab)
// and the Go catalog (repointed via the CLI's own LINEAR_URL, a Go-only
// addition for testability). Both send one POST /graphql; this fake ignores
// the query TEXT (it never varies field selection in a way either consumer
// cares about) and dispatches on the request's variables shape instead:
// "teamId" present -> a team-members page; absent -> a teams listing page.
type fakeLinear struct {
	server *httptest.Server
	mu     sync.Mutex

	token   string
	teams   []fakeLinearTeam
	failing bool // true: every request gets a GraphQL error
}

type fakeLinearMember struct {
	ID     string
	Name   string
	Email  string // "" = no email (identity falls back to ID)
	Active bool
}

type fakeLinearTeam struct {
	Key         string
	Name        string
	Description *string
	Archived    bool // python skips archivedAt teams; Go's team query never even asks for it
	Members     []fakeLinearMember
}

func newFakeLinear(t *testing.T) *fakeLinear {
	t.Helper()
	f := &fakeLinear{}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeLinear) base() string { return f.server.URL }

func (f *fakeLinear) set(token string, teams []fakeLinearTeam) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.token, f.teams, f.failing = token, teams, false
}

type linearGraphQLRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

func (f *fakeLinear) memberJSON(m fakeLinearMember) map[string]any {
	var email any
	if m.Email != "" {
		email = m.Email
	}
	return map[string]any{"id": m.ID, "name": m.Name, "email": email, "active": m.Active}
}

func (f *fakeLinear) teamJSON(team fakeLinearTeam) map[string]any {
	var description any
	if team.Description != nil {
		description = *team.Description
	}
	var archivedAt any
	if team.Archived {
		archivedAt = "2026-01-01T00:00:00.000Z"
	}
	members := make([]any, 0, len(team.Members))
	for i, m := range team.Members {
		if i >= 10 {
			break // page 1 only, matching both queries' members(first:10)
		}
		members = append(members, f.memberJSON(m))
	}
	return map[string]any{
		"id": "id-" + team.Key, "key": team.Key, "name": team.Name, "description": description,
		"archivedAt": archivedAt,
		"members": map[string]any{
			"nodes":    members,
			"pageInfo": map[string]any{"hasNextPage": len(team.Members) > 10, "endCursor": nil},
		},
	}
}

func (f *fakeLinear) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.token != "" && r.Header.Get("Authorization") != f.token {
		writeJSON(w, http.StatusOK, map[string]any{"errors": []any{map[string]any{"message": "Authentication required."}}})
		return
	}
	if f.failing {
		writeJSON(w, http.StatusOK, map[string]any{"errors": []any{map[string]any{"message": "injected failure"}}})
		return
	}
	var req linearGraphQLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"errors": []any{map[string]any{"message": "bad request"}}})
		return
	}
	if teamID, ok := req.Variables["teamId"].(string); ok {
		key := strings.TrimPrefix(teamID, "id-")
		var members []any
		for _, team := range f.teams {
			if team.Key != key {
				continue
			}
			for _, m := range team.Members {
				members = append(members, f.memberJSON(m))
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{
			"team": map[string]any{"members": map[string]any{"nodes": members, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}}},
		}})
		return
	}
	nodes := make([]any, 0, len(f.teams))
	for _, team := range f.teams {
		if team.Archived {
			// Go's team query never requests archivedAt at all, so its own
			// walk includes an archived team unconditionally (a named
			// divergence -- python's providers.teams.py skips it). Serve it
			// to BOTH: the difference is in what each CONSUMER does with the
			// field, not in what the fake returns.
		}
		nodes = append(nodes, f.teamJSON(team))
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{
		"teams": map[string]any{"nodes": nodes, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}},
	}})
}

type linearRun struct {
	pythonStage string
	pythonCode  string
	goCode      int
	goStdout    string
	goStderr    string
}

func (o *teamsOracle) runLinear(fake *fakeLinear, orgID, token string, sc *linearScenario, extra ...string) linearRun {
	o.t.Helper()
	o.truncateAll()
	viaEnv := strings.HasPrefix(token, "env:")
	token = strings.TrimPrefix(token, "env:")
	argv := []string{"--org", orgID, "sync", "teams", "--provider", "linear"}
	pythonEnv := map[string]string{"CLICKHOUSE_URI": o.pythonHTTPDSN, "LINEAR_API_KEY": token}
	argv = append(argv, extra...)
	answer := o.ask(map[string]any{"argv": argv, "env": pythonEnv, "linear_base": fake.base()})
	value := func(key string) string {
		if item, ok := answer[key].(map[string]any); ok {
			return fmt.Sprint(item["v"])
		}
		return ""
	}
	run := linearRun{pythonStage: value("stage"), pythonCode: value("code")}
	run.goCode, run.goStdout, run.goStderr = o.runGoLinear(fake, orgID, token, viaEnv, extra...)
	return run
}

func (o *teamsOracle) runGoLinear(fake *fakeLinear, orgID, token string, viaEnv bool, extra ...string) (int, string, string) {
	o.t.Helper()
	env := map[string]string{"CLICKHOUSE_URI": o.goNativeDSN, "LINEAR_URL": fake.base()}
	args := []string{"--provider", "linear", "--org", orgID}
	if viaEnv {
		env["LINEAR_API_KEY"] = token
	} else {
		args = append(args, "--auth", token)
	}
	lookup := func(key string) (string, bool) { v, ok := env[key]; return v, ok }
	args = append(args, extra...)
	var stdout, stderr strings.Builder
	d := defaultDeps()
	d.now = func() time.Time { return time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC) }
	d.doer = http.DefaultClient
	d.openStore = func(context.Context, string) (driver.Conn, error) { return &keepOpen{o.goConn}, nil }
	code := runTeams(o.ctx, cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}, d)
	return code, stdout.String(), stderr.String()
}

type linearScenario struct {
	name  string
	token string
	teams []fakeLinearTeam
	extra []string
	// wantExit is the exit code both planes must end with.
	wantExit int
	// wantRows is python's written team count.
	wantRows int
	// goDiffers marks a scenario where the two planes end differently by
	// design (the archived-team divergence): the two databases are not
	// compared column by column and `after` says what each must hold.
	goDiffers bool
	goExit    int
	goRows    int
	after     func(t *testing.T, o *teamsOracle, sc *linearScenario, run linearRun, python, goRows []map[string]string)
}

func (sc *linearScenario) wantToken() string { return strings.TrimPrefix(sc.token, "env:") }

type linearTeamsRule struct {
	same  bool
	check func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string
	why   string
}

func parseLinearList(text string) []string {
	text = strings.TrimSpace(text)
	if text == "[]" || text == "" {
		return nil
	}
	text = strings.TrimSuffix(strings.TrimPrefix(text, "["), "]")
	var out []string
	for _, part := range strings.Split(text, "','") {
		out = append(out, strings.Trim(part, "'"))
	}
	return out
}

func linearTeamsRules() map[string]linearTeamsRule {
	same := linearTeamsRule{same: true}
	return map[string]linearTeamsRule{
		"is_active": same, "org_id": same, "manual_members": {same: true, why: "neither scenario seeds an admin override"},
		"project_keys":   {same: true, why: "CHAOS-4530: a team key is not a project key; neither plane populates it for Linear"},
		"parent_team_id": same, "source_id": same, "repo_patterns": {same: true, why: "no repository ownership concept for Linear"},
		"id": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			if py["id"] != "linear:"+team.Key || gr["id"] != team.Key {
				return fmt.Sprintf("id: python %q (want linear:%s), go %q (want %s)", py["id"], team.Key, gr["id"], team.Key)
			}
			return ""
		}, why: "legacy: 'linear:<key>'; catalog: the bare team key (both consistently ordered, so index-matched comparison still holds)"},
		"name": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			want := team.Name
			if strings.TrimSpace(want) == "" {
				want = team.Key
			}
			if py["name"] != want || gr["name"] != want {
				return fmt.Sprintf("name: want %q, python %q go %q", want, py["name"], gr["name"])
			}
			return ""
		}, why: "both take the provider's name (falling back to the key when blank)"},
		"description": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			if team.Description != nil && *team.Description != "" {
				if py["description"] != *team.Description || gr["description"] != *team.Description {
					return fmt.Sprintf("a provider description is carried by both: python %q go %q", py["description"], gr["description"])
				}
				return ""
			}
			if py["description"] != "<NULL>" && py["description"] != "" {
				return fmt.Sprintf("python: want NULL/empty for no description, got %q", py["description"])
			}
			if gr["description"] != "<NULL>" && gr["description"] != "" {
				return fmt.Sprintf("go: want NULL for no description, got %q", gr["description"])
			}
			return ""
		}, why: "neither plane invents a description for Linear (unlike GitHub/GitLab's legacy fallback text)"},
		"members": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			var logins []string
			for _, entry := range parseLinearList(gr["members"]) {
				if strings.HasPrefix(entry, "linear:") {
					logins = append(logins, strings.TrimPrefix(entry, "linear:"))
				}
			}
			var want []string
			for _, m := range team.Members {
				if !m.Active {
					continue
				}
				identity := m.Email
				if identity == "" {
					identity = m.ID
				}
				want = append(want, strings.ToLower(identity))
			}
			sort.Strings(logins)
			sort.Strings(want)
			if strings.Join(logins, ",") != strings.Join(want, ",") {
				return fmt.Sprintf("members: the identities differ: want %v, go %v (from %s)", want, logins, gr["members"])
			}
			var pyWant []string
			for _, m := range team.Members {
				if !m.Active {
					continue
				}
				identity := m.Email
				if identity == "" {
					identity = m.Name
				}
				if identity != "" {
					pyWant = append(pyWant, identity)
				}
			}
			pyGot := parseLinearList(py["members"])
			sort.Strings(pyWant)
			sort.Strings(pyGot)
			if strings.Join(pyGot, ",") != strings.Join(pyWant, ",") {
				return fmt.Sprintf("members: python identities differ: want %v, got %v", pyWant, pyGot)
			}
			return ""
		}, why: "legacy: email-or-name identities; catalog: provider-scoped identity facets (email-or-id, lower-cased)"},
		"team_uuid": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			wantGo := uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+team.Key)).String()
			parsed, err := uuid.Parse(py["team_uuid"])
			if err != nil || parsed.Version() != 4 {
				return fmt.Sprintf("team_uuid: python's is a random uuid4, got %q", py["team_uuid"])
			}
			if gr["team_uuid"] != wantGo {
				return fmt.Sprintf("team_uuid: go's is uuid5(URL, \"team:<key>\") = %s, got %s", wantGo, gr["team_uuid"])
			}
			return ""
		}, why: "legacy: a random uuid4 per run; catalog: uuid5 of the bare team key, stable across runs"},
		"provider": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			if py["provider"] != "" || gr["provider"] != "linear" {
				return fmt.Sprintf("provider: python %q (want empty), go %q (want linear)", py["provider"], gr["provider"])
			}
			return ""
		}, why: "legacy rows carry no provider; catalog rows are provider_access rows of 'linear'"},
		"native_team_key": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			if py["native_team_key"] != "<NULL>" || gr["native_team_key"] != team.Key {
				return fmt.Sprintf("native_team_key: python %q (want NULL), go %q (want %q)", py["native_team_key"], gr["native_team_key"], team.Key)
			}
			return ""
		}, why: "legacy rows carry no native key; the catalog keys the team by its key"},
		"updated_at": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			if py["updated_at"] == "" || gr["updated_at"] != "2026-09-26 12:00:00.000000" {
				return fmt.Sprintf("updated_at: python %q, go %q (want the run's clock)", py["updated_at"], gr["updated_at"])
			}
			return ""
		}, why: "time of the run (python: its own clock)"},
		"last_synced": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			if py["last_synced"] == "" || gr["last_synced"] == "" {
				return "last_synced is empty"
			}
			return ""
		}, why: "time of the write (DEFAULT now() on both databases)"},
	}
}

func (o *teamsOracle) compareLinear(sc *linearScenario, fake *fakeLinear) {
	t := o.t
	t.Helper()
	fake.set(sc.wantToken(), sc.teams)
	run := o.runLinear(fake, "org-1", sc.token, sc, sc.extra...)
	if run.pythonStage != "ok" && run.pythonStage != "exit" {
		t.Fatalf("%s: python ended %s (%s)", sc.name, run.pythonStage, run.pythonCode)
	}
	wantGoExit, wantGoRows := sc.wantExit, sc.wantRows
	if sc.goDiffers {
		wantGoExit, wantGoRows = sc.goExit, sc.goRows
	}
	wantPython := strconv.Itoa(sc.wantExit)
	if run.pythonCode != wantPython || run.goCode != wantGoExit {
		t.Fatalf("%s: exit codes: python %s (stage %s, want %d), go %d (want %d)\ngo stderr: %s", sc.name, run.pythonCode, run.pythonStage, sc.wantExit, run.goCode, wantGoExit, run.goStderr)
	}
	python := o.rows(o.pythonDatabase, "teams", "org-1")
	goRows := o.rows(o.goDatabase, "teams", "org-1")
	if len(python) != sc.wantRows || len(goRows) != wantGoRows {
		t.Fatalf("%s: teams written: python %d (want %d), go %d (want %d)", sc.name, len(python), sc.wantRows, len(goRows), wantGoRows)
	}
	rules := linearTeamsRules()
	cols, err := o.admin.Query(o.ctx, "SELECT name FROM system.columns WHERE database = ? AND table = 'teams' ORDER BY position", o.pythonDatabase)
	if err != nil {
		t.Fatal(err)
	}
	var columns []string
	for cols.Next() {
		var name string
		if err := cols.Scan(&name); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	_ = cols.Close()
	for _, column := range columns {
		if _, ok := rules[column]; !ok {
			t.Fatalf("teams has a column %q that the linear comparison does not classify: add a rule for it", column)
		}
	}
	byKey := map[string]fakeLinearTeam{}
	for _, team := range sc.teams {
		byKey[team.Key] = team
	}
	if !sc.goDiffers {
		for index := range python {
			py, gr := python[index], goRows[index]
			key := strings.TrimPrefix(py["id"], "linear:")
			team := byKey[key]
			for _, column := range columns {
				rule := rules[column]
				switch {
				case rule.same:
					if py[column] != gr[column] {
						t.Errorf("%s team %s column %s: python %q go %q", sc.name, py["id"], column, py[column], gr[column])
					}
				default:
					if message := rule.check(sc, team, py, gr); message != "" {
						t.Errorf("%s team %s column %s: %s", sc.name, py["id"], column, message)
					}
				}
			}
		}
	}
	if sc.after != nil {
		sc.after(t, o, sc, run, python, goRows)
	}
}

func TestSyncTeamsLinearVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	o := newTeamsOracleFor(t, teamsSyncOracleLinearProgram)
	fake := newFakeLinear(t)

	two := []fakeLinearTeam{
		{Key: "ENG", Name: "Engineering", Description: strPtr("The eng team"), Members: []fakeLinearMember{
			{ID: "u1", Name: "Alice", Email: "alice@example.com", Active: true},
			{ID: "u2", Name: "Bob", Active: true}, // no email: identity falls back to the provider id
		}},
		{Key: "OPS", Name: "Operations", Members: []fakeLinearMember{
			{ID: "u3", Name: "Carol", Email: "carol@example.com", Active: true},
			{ID: "u4", Name: "Dave", Email: "dave@example.com", Active: false}, // inactive: excluded by both planes
		}},
	}

	scenarios := []*linearScenario{
		{name: "two teams, one member without an email, one inactive member excluded", token: "tok", teams: two, wantExit: 0, wantRows: 2},
		{name: "the token from LINEAR_API_KEY", token: "env:tok", teams: two, wantExit: 0, wantRows: 2},
		{name: "a rejected token", token: "wrong", teams: two, wantExit: 1, wantRows: 0},
		{name: "an empty workspace is an error", token: "tok", teams: nil, wantExit: 1, wantRows: 0},
		{name: "an empty workspace with --allow-empty", token: "tok", teams: nil, extra: []string{"--allow-empty"}, wantExit: 0, wantRows: 0},
		{name: "an archived team: python skips it, go's catalog does not", token: "tok", wantExit: 0, wantRows: 1,
			teams:     []fakeLinearTeam{{Key: "ENG", Name: "Engineering", Members: []fakeLinearMember{{ID: "u1", Name: "Alice", Email: "alice@example.com", Active: true}}}, {Key: "OLD", Name: "Retired", Archived: true, Members: []fakeLinearMember{{ID: "u5", Name: "Eve", Email: "eve@example.com", Active: true}}}},
			goDiffers: true, goExit: 0, goRows: 2,
			after: func(t *testing.T, o *teamsOracle, sc *linearScenario, run linearRun, python, goRows []map[string]string) {
				var pythonIDs, goIDs []string
				for _, row := range python {
					pythonIDs = append(pythonIDs, row["id"])
				}
				for _, row := range goRows {
					goIDs = append(goIDs, row["id"])
				}
				sort.Strings(pythonIDs)
				sort.Strings(goIDs)
				if strings.Join(pythonIDs, ",") != "linear:ENG" {
					t.Errorf("python wrote %v, want only linear:ENG (archived team skipped)", pythonIDs)
				}
				if strings.Join(goIDs, ",") != "ENG,OLD" {
					t.Errorf("go wrote %v, want both ENG and OLD (the catalog's team query never reads archivedAt)", goIDs)
				}
			}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			o.t = t
			o.compareLinear(sc, fake)
		})
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}
