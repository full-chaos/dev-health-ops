//go:build integration

package synccli

import (
	"context"
	_ "embed"
	"encoding/json"
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

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

//go:embed testdata/teams_sync_oracle_linear.py
var teamsSyncOracleLinearProgram string

// fakeLinear is the one Linear GraphQL endpoint both planes read: python's
// LinearClient (repointed at this server via LINEAR_API_URL, which its module
// hardcodes with no env override -- a named difference from GitHub/GitLab)
// and the Go catalog (repointed via the CLI's own LINEAR_URL, a Go-only
// addition for testability). Both send one POST /graphql; this fake dispatches
// on the request's VARIABLES shape (see handle()'s dispatch comment for the
// three query shapes and how they're told apart), never the query text.
//
// It does NOT emulate real GraphQL field selection: every response includes
// every field this file's payload structs define, regardless of what the
// query actually asked for. This was already a design tradeoff (chosen for
// simplicity over building a real selection-set interpreter) but it is a
// FOOTGUN, not a free simplification: codex r1 (CHAOS-6908) caught a case
// where it fabricated a divergence that cannot occur in production -- an
// "archivedAt" field python's own TEAMS_QUERY never selects at all, so a
// conforming server would never send it, but this fake did, making python's
// dead `if t.get("archivedAt")` check look alive. Never add a field to a
// response struct/JSON here that the corresponding real query does not
// actually select -- check the query constant in client.go (python) and the
// route file (Go) first.
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
	members := make([]any, 0, len(team.Members))
	for i, m := range team.Members {
		if i >= 10 {
			break // page 1 only, matching both queries' members(first:10)
		}
		members = append(members, f.memberJSON(m))
	}
	// codex r1 relaunch, CHAOS-6908 (P2): no "archivedAt" key here, ever --
	// this fake previously injected one to simulate an "archived team"
	// divergence, but python's real TEAMS_QUERY (client.py:258-287) never
	// SELECTS that field at all. A real GraphQL server omits an unselected
	// field entirely (it does not return it as null); a fake that returns it
	// anyway lets python's `t.get("archivedAt")` check see data no
	// conforming server would ever hand it, fabricating a divergence that
	// cannot occur in production. Confirmed by removing the field and
	// re-running the live oracle: python includes the "archived" team same
	// as Go, because its own filter is dead code against the real query.
	return map[string]any{
		"id": "id-" + team.Key, "key": team.Key, "name": team.Name, "description": description,
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
	// codex r1, CHAOS-6908 (P3): the collector issues THREE query shapes with
	// no "teamId" key on two of them -- routing on "teamId" alone (the only
	// check this fake originally had) misrouted both to the teams-listing
	// branch below, which answers with `data.teams` instead of `data.cycles`/
	// `data.projects`; the Go client's JSON decode of that mismatched shape
	// failed with "provider pagination response is invalid" on every
	// non-empty scenario (an executed repro confirmed this against the
	// unmodified fake). Dispatch on the actual distinguishing variable each
	// query sends instead of assuming "no teamId" means "teams listing":
	//   - "filter": {"team": {"id": {"eq": <id>}}}      -> cycles (collectLinearCycles)
	//   - "teamId": "<id>"                                -> members for one team
	//   - "includeArchived": true                         -> native projects
	//   - none of the above                                -> teams listing
	// Neither cycles nor projects affect anything this oracle compares
	// (python's legacy verb never fetches either), so both get a clean empty
	// answer.
	if _, isCycles := req.Variables["filter"]; isCycles {
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{
			"cycles": map[string]any{"nodes": []any{}, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}},
		}})
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
	if includeArchived, ok := req.Variables["includeArchived"].(bool); ok && includeArchived {
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{
			"projects": map[string]any{"nodes": []any{}, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}},
		}})
		return
	}
	nodes := make([]any, 0, len(f.teams))
	for _, team := range f.teams {
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
	o.python(sc.name, map[string]any{"argv": argv, "env": pythonEnv, "linear_base": fake.base()})
	run := linearRun{pythonStage: o.cur.Stage, pythonCode: o.cur.Code}
	if o.recording {
		return run // the recording asks Python only: the Go plane runs in the comparison
	}
	run.goCode, run.goStdout, run.goStderr = o.runGoLinear(fake, orgID, token, viaEnv, extra...)
	return run
}

func (o *teamsOracle) runGoLinear(fake *fakeLinear, orgID, token string, viaEnv bool, extra ...string) (int, string, string) {
	o.t.Helper()
	env := map[string]string{"CLICKHOUSE_URI": o.goNativeDSN, "LINEAR_URL": fake.base()}
	args := []string{"--provider", "linear", "--org", orgID}
	if viaEnv {
		env["LINEAR_API_KEY"] = token
	} else if token != "" { // a scenario with no token sends no --auth
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
	// serverToken, when set, is the token the fake actually requires --
	// distinct from token (what the client sends). Needed for a genuine
	// rejected-token scenario: leaving it unset made the fake accept
	// whatever token the client happened to send, so "a rejected token"
	// never actually rejected anything (codex r1, CHAOS-6908, P3).
	serverToken string
	teams       []fakeLinearTeam
	extra       []string
	// wantExit is the exit code both planes must end with.
	wantExit int
	// wantRows is python's written team count.
	wantRows int
	// goDiffers marks a scenario where the two planes end differently by
	// design (the inactive-member divergence): the two databases are not
	// compared column by column and `after` says what each must hold.
	goDiffers bool
	goExit    int
	goRows    int
	// wantGoCode, when set, is the error code the Go verb must print on stderr: the CLI's own refusal, told from a
	// later failure that also ends in exit 1 and no rows.
	wantGoCode string
	after      func(t *testing.T, o *teamsOracle, sc *linearScenario, run linearRun, python, goRows []map[string]string)
}

// wantToken is the token the fake actually requires: serverToken when the
// scenario sets one (a genuine mismatch case), else the token the client
// itself sends.
func (sc *linearScenario) wantToken() string {
	if sc.serverToken != "" {
		return sc.serverToken
	}
	return strings.TrimPrefix(sc.token, "env:")
}

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
			if py["id"] != "linear:"+team.Key || gr["id"] != "linear:"+team.Key {
				return fmt.Sprintf("id: python %q, go %q, want both linear:%s", py["id"], gr["id"], team.Key)
			}
			return ""
		}, why: "both 'linear:<key>': CHAOS-8939 gave the catalog the provider prefix the legacy writer already had"},
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
			if gr["members"] != "" && gr["members"] != "[]" {
				return fmt.Sprintf("members: the roster column is not written (CHAOS-9087), go wrote %s", gr["members"])
			}
			// codex r1, CHAOS-6908 (P1): providers/teams.py's inline path (a
			// team's page-1 members(first:10), which every row-compared
			// scenario here fits inside) never filters on `active` at all --
			// only the FULL-pagination path (LinearClient.get_team_members)
			// does. No row-compared scenario has an inactive member (that
			// divergence is asserted directly, not through this equality
			// check); if one is ever added here, python's expectation must
			// NOT filter it out.
			var pyWant []string
			for _, m := range team.Members {
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
			wantGo := uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:linear:"+team.Key)).String()
			if py["team_uuid"] != "<uuid4>" { // the golden stores python's random uuid4 as its kind
				return fmt.Sprintf("team_uuid: python's is a random uuid4 (stored as <uuid4>), got %q", py["team_uuid"])
			}
			if gr["team_uuid"] != wantGo {
				return fmt.Sprintf("team_uuid: go's is uuid5(URL, \"team:linear:<key>\") = %s, got %s", wantGo, gr["team_uuid"])
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
		"created_at": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			if py["created_at"] != "" && py["created_at"] != "<NULL>" || gr["created_at"] == "" || gr["created_at"] > gr["updated_at"] {
				return fmt.Sprintf("created_at: python %q (no such column), go %q (want a time no later than its updated_at %q)", py["created_at"], gr["created_at"], gr["updated_at"])
			}
			return ""
		}, why: "legacy rows carry no creation time; the catalog stamps a new team with its own updated_at and keeps the oldest stored version's time for an existing one"},
		"updated_at": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			if py["updated_at"] != "<time>" || gr["updated_at"] != "2026-09-26 12:00:00.000000" {
				return fmt.Sprintf("updated_at: python %q, go %q (want the run's clock)", py["updated_at"], gr["updated_at"])
			}
			return ""
		}, why: "time of the run (python: its own clock)"},
		"last_synced": {check: func(sc *linearScenario, team fakeLinearTeam, py, gr map[string]string) string {
			if py["last_synced"] != "<time>" || gr["last_synced"] == "" {
				return "last_synced is empty"
			}
			return ""
		}, why: "time of the write (DEFAULT now() on both databases)"},
	}
}

// arrangeAndRunLinear sets the fake and runs the scenario: both planes when comparing, the legacy verb alone when
// recording.
func (o *teamsOracle) arrangeAndRunLinear(sc *linearScenario, fake *fakeLinear) linearRun {
	o.t.Helper()
	fake.set(sc.wantToken(), sc.teams)
	return o.runLinear(fake, "org-1", sc.token, sc, sc.extra...)
}

func (o *teamsOracle) compareLinear(sc *linearScenario, fake *fakeLinear) {
	t := o.t
	t.Helper()
	run := o.arrangeAndRunLinear(sc, fake)
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
	if sc.wantGoCode != "" && !strings.Contains(run.goStderr, `"code":"`+sc.wantGoCode+`"`) {
		t.Fatalf("%s: go stderr %q does not carry the refusal code %q", sc.name, run.goStderr, sc.wantGoCode)
	}
	python := o.pythonRows("teams")
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

func linearScenarios() []*linearScenario {
	two := []fakeLinearTeam{
		{Key: "ENG", Name: "Engineering", Description: strPtr("The eng team"), Members: []fakeLinearMember{
			{ID: "u1", Name: "Alice", Email: "alice@example.com", Active: true},
			{ID: "u2", Name: "Bob", Active: true}, // no email: identity falls back to the provider id
		}},
		{Key: "OPS", Name: "Operations", Members: []fakeLinearMember{
			{ID: "u3", Name: "Carol", Email: "carol@example.com", Active: true},
			{ID: "u4", Name: "Dave", Email: "dave@example.com", Active: true},
		}},
	}

	return []*linearScenario{
		{name: "two teams, one member without an email", token: "tok", teams: two, wantExit: 0, wantRows: 2},
		{name: "the token from LINEAR_API_KEY", token: "env:tok", teams: two, wantExit: 0, wantRows: 2},
		{name: "no token anywhere is refused", token: "", teams: two, wantExit: 1, wantRows: 0, wantGoCode: "token_required"},
		{name: "a team with no name takes its key", token: "tok", wantExit: 0, wantRows: 1,
			teams: []fakeLinearTeam{{Key: "ENG", Name: "", Members: []fakeLinearMember{{ID: "u1", Name: "Alice", Email: "alice@example.com", Active: true}}}}},
		// A named divergence (recorded, not argued): Python skips a team that has no key (providers/teams.py:677-679)
		// and writes the others, exit 0; the catalog refuses the whole run (linear_reference_catalog.go:303-305,
		// ErrInvalidConfiguration -> sync_failed, exit 1) and writes nothing. Linear gives every team a key, so the
		// case does not occur on real data; it is pinned so a change on either side is seen.
		{name: "a team with no key: python skips it, the catalog refuses the run", token: "tok", wantExit: 0, wantRows: 1, goDiffers: true, goExit: 1, goRows: 0,
			after: func(t *testing.T, o *teamsOracle, sc *linearScenario, run linearRun, python, goRows []map[string]string) {
				if len(python) != 1 || python[0]["id"] != "linear:ENG" {
					t.Errorf("python wrote %v, want only the team that has a key (linear:ENG)", python)
				}
				// Both planes are pinned: python's answer above (the recorded exit 0 and the one row), go's here: the
				// exact refusal text today, which names no cause (a known gap, ticketed): a change on either side fails.
				if len(goRows) != 0 || !strings.Contains(run.goStderr, `"code":"sync_failed"`) || !strings.Contains(run.goStderr, "provider sync configuration is invalid") {
					t.Errorf("go wrote %d rows, stderr %q: want a refused run (sync_failed, provider sync configuration is invalid) and no row", len(goRows), run.goStderr)
				}
				if run.pythonCode != "0" {
					t.Errorf("python exit %s: the recorded answer is exit 0 with the keyless team skipped", run.pythonCode)
				}
			},
			teams: []fakeLinearTeam{
				{Key: "", Name: "Keyless", Members: []fakeLinearMember{{ID: "u9", Name: "Zed", Email: "zed@example.com", Active: true}}},
				{Key: "ENG", Name: "Engineering", Members: []fakeLinearMember{{ID: "u1", Name: "Alice", Email: "alice@example.com", Active: true}}},
			}},
		{name: "a rejected token", token: "wrong", serverToken: "tok", teams: two, wantExit: 1, wantRows: 0},
		{name: "an empty workspace is an error", token: "tok", teams: nil, wantExit: 1, wantRows: 0},
		{name: "an empty workspace with --allow-empty", token: "tok", teams: nil, extra: []string{"--allow-empty"}, wantExit: 0, wantRows: 0},
		{name: "an inactive member: legacy keeps it inline, dho's catalog excludes it", token: "tok", wantExit: 0, wantRows: 1,
			teams: []fakeLinearTeam{{Key: "ENG", Name: "Engineering", Members: []fakeLinearMember{
				{ID: "u1", Name: "Alice", Email: "alice@example.com", Active: true},
				{ID: "u2", Name: "Bob", Email: "bob@example.com", Active: false},
			}}},
			// codex r1, CHAOS-6908 (P1): for a team with <=10 inline members,
			// providers/teams.py never filters on `active` at all (only the
			// FULL-pagination path, LinearClient.get_team_members, does) --
			// the legacy verb keeps an inactive member in `teams.members`;
			// dho's catalog always excludes one from its membership rows.
			// Real, undocumented-until-now output divergence: not a row-by-row
			// comparison, `after` asserts each plane's actual content directly.
			goDiffers: true, goExit: 0, goRows: 1,
			after: func(t *testing.T, o *teamsOracle, sc *linearScenario, run linearRun, python, goRows []map[string]string) {
				if len(python) != 1 || len(goRows) != 1 {
					t.Fatalf("want exactly one team row on each plane, got python %d go %d", len(python), len(goRows))
				}
				pyMembers := parseLinearList(python[0]["members"])
				sort.Strings(pyMembers)
				if strings.Join(pyMembers, ",") != "alice@example.com,bob@example.com" {
					t.Errorf("python members = %v, want both alice and the inactive bob kept (inline path never filters active)", pyMembers)
				}
				// dho's catalog stores no roster (CHAOS-9087): the membership rows below are where a member lands.
				// The membership rows are the other place an inactive member could land: only alice has one.
				var membershipUsers []string
				for _, row := range o.rows(o.goDatabase, "team_memberships", "org-1") {
					membershipUsers = append(membershipUsers, row["member_id"])
				}
				sort.Strings(membershipUsers)
				if strings.Join(membershipUsers, ",") != "linear:alice@example.com" {
					t.Errorf("go wrote team_memberships for %v, want only linear:alice@example.com (an inactive member has no membership)", membershipUsers)
				}
			}},
		// codex r1 relaunch, CHAOS-6908 (P2): NOT a divergence, corrected from
		// an earlier draft that claimed one. providers/teams.py's `archivedAt`
		// check (teams.py) reads a field its own TEAMS_QUERY never selects
		// (client.py:258-287) -- against the real Linear API that check is
		// dead code, always false, and python includes every team regardless
		// of archived status, same as dho's catalog (whose query never asks
		// for archivedAt either). Verified directly: injecting an
		// "archivedAt" value into this fake's response (as an earlier draft
		// did) made python skip the team -- but that only proves python
		// trusts a field a conforming GraphQL server would never send for
		// this query; a real server omits an unselected field entirely
		// rather than sending it null. This scenario proves the NULL
		// hypothesis instead: with no archivedAt in the response at all, both
		// planes write every team.
		{name: "an org-archived team is included by both planes (teams.py's archivedAt check is dead code against the real query)",
			token: "tok", wantExit: 0, wantRows: 2,
			teams: []fakeLinearTeam{
				{Key: "ENG", Name: "Engineering", Members: []fakeLinearMember{{ID: "u1", Name: "Alice", Email: "alice@example.com", Active: true}}},
				{Key: "OLD", Name: "Retired", Members: []fakeLinearMember{{ID: "u5", Name: "Eve", Email: "eve@example.com", Active: true}}},
			}},
	}
}

// linearCorpusKey is the golden's request key: every scenario's inputs.
func linearCorpusKey() []byte {
	type entry struct {
		Name, Token, ServerToken string
		Teams                    []fakeLinearTeam
		Extra                    []string
	}
	var entries []entry
	for _, sc := range linearScenarios() {
		entries = append(entries, entry{sc.name, sc.token, sc.serverToken, sc.teams, sc.extra})
	}
	return teamsCorpusKey("linear", entries)
}

// TestSyncTeamsLinearMatchesFrozenPython compares `dho sync teams --provider linear` (the Go team catalog) with what
// the REAL legacy verb (LinearClient pointed at a fake) did over the same fake Linear workspace: how each run ended
// and, column by column, the `teams` rows it wrote. Executed once on teamsPythonBuild and frozen in
// testdata/golden/teams_linear.json.
func TestSyncTeamsLinearMatchesFrozenPython(t *testing.T) {
	frozen, golden := openTeamsGolden(t, "linear", "TestSyncTeamsLinearMatchesFrozenPython", "748704d189fd6ceb08ba5b0b3ae36d5089d52592b514e3a81f2aad54554895f3", teamsSyncOracleLinearProgram, linearCorpusKey(),
		func(t *testing.T, producer *venueoracle.Producer) []teamsFrozen {
			o := newTeamsOracle(t)
			o.startPython(producer, teamsSyncOracleLinearProgram)
			fake := newFakeLinear(t)
			for _, sc := range linearScenarios() {
				o.arrangeAndRunLinear(sc, fake)
			}
			return o.recorded
		})
	o := newTeamsOracle(t)
	o.frozen = frozen
	fake := newFakeLinear(t)
	for _, sc := range linearScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			o.t = t
			o.compareLinear(sc, fake)
		})
	}
	if o.next != len(frozen) {
		t.Fatalf("the golden holds %d runs, the corpus runs %d", len(frozen), o.next)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}
