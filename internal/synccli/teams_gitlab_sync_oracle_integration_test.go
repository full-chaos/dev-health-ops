//go:build integration

package synccli

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

//go:embed testdata/teams_sync_oracle_gitlab.py
var teamsSyncOracleGitLabProgram string

// fakeGitLab is the one GitLab both planes read: python-gitlab (the real Python
// verb, pointed here via GITLAB_URL, which providers/teams.py's gitlab branch
// already reads -- no client monkeypatch needed, unlike the GitHub oracle) and
// the Go catalog. It serves a single group (no subgroups): the subgroup-naming
// divergence (python IDs a subgroup by its bare path, "gl:<leaf>"; the Go
// catalog IDs it by full_path, "gl:<parent>/<leaf>") is proven separately in
// TestGitLabSubgroupIDsDivergeByDesign, not through this row-by-row harness.
type fakeGitLab struct {
	server *httptest.Server
	mu     sync.Mutex

	token    string
	group    fakeGitLabGroup
	projects []fakeGitLabProject
	users    map[string]string // username -> email ("" = private/none)
	failing  map[string]int    // escaped path -> status to answer instead
}

type fakeGitLabGroup struct {
	ID          int
	FullPath    string
	Name        string
	Description *string
	Members     []string
}

type fakeGitLabProject struct {
	ID                int
	PathWithNamespace string
	Name              string
	Archived          bool
}

func newFakeGitLab(t *testing.T) *fakeGitLab {
	t.Helper()
	f := &fakeGitLab{users: map[string]string{}, failing: map[string]int{}}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeGitLab) base() string { return f.server.URL }

func (f *fakeGitLab) set(token string, group fakeGitLabGroup, projects []fakeGitLabProject, users map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.token, f.group, f.projects, f.users = token, group, projects, users
	f.failing = map[string]int{}
}

func writeGitLabJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (f *fakeGitLab) groupJSON() map[string]any {
	var description any
	if f.group.Description != nil {
		description = *f.group.Description
	}
	return map[string]any{"id": f.group.ID, "full_path": f.group.FullPath, "path": f.group.FullPath, "name": f.group.Name, "description": description}
}

func (f *fakeGitLab) memberJSON(username string) map[string]any {
	email, known := f.users[username]
	m := map[string]any{"id": 1000 + len(username), "username": username, "name": "Name of " + username}
	if known && email != "" {
		m["email"] = email
	} else {
		m["email"] = nil
	}
	return m
}

func (f *fakeGitLab) projectJSON(p fakeGitLabProject) map[string]any {
	return map[string]any{
		"id": p.ID, "path_with_namespace": p.PathWithNamespace, "name": p.Name,
		"archived": p.Archived, "web_url": f.base() + "/" + p.PathWithNamespace,
	}
}

func (f *fakeGitLab) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.EscapedPath()
	if f.token != "" && r.Header.Get("PRIVATE-TOKEN") != f.token {
		writeGitLabJSON(w, http.StatusUnauthorized, map[string]any{"message": "401 Unauthorized"})
		return
	}
	if status, ok := f.failing[path]; ok {
		writeGitLabJSON(w, status, map[string]any{"message": "injected failure"})
		return
	}
	// codex r1, CHAOS-6907 (P2): a group's sub-resources are reachable by BOTH
	// the group's numeric id and its full path -- real GitLab accepts either
	// as the ":id" segment. The Go catalog requests them by path
	// (providerRelativePath(..., group.FullPath)); python-gitlab's bound
	// sub-managers (group.members, group.subgroups) always request them by
	// the group's numeric id instead (an executed client trace confirmed
	// "/api/v4/groups/1/members", never "/api/v4/groups/acme/members"). The
	// fake originally recognized only the path form, so every python-gitlab
	// request 404'd while the Go client's own (path-form) request succeeded.
	groupBase := "/api/v4/groups/" + url.PathEscape(f.group.FullPath)
	groupByID := fmt.Sprintf("/api/v4/groups/%d", f.group.ID)
	isGroupRoot := path == groupBase || path == groupByID
	hasSuffix := func(suffix string) bool {
		return path == groupBase+suffix || path == groupByID+suffix
	}
	switch {
	case isGroupRoot:
		writeGitLabJSON(w, http.StatusOK, f.groupJSON())
	case hasSuffix("/subgroups"):
		writeGitLabJSON(w, http.StatusOK, []any{})
	case hasSuffix("/projects"):
		items := make([]any, 0, len(f.projects))
		for _, p := range f.projects {
			items = append(items, f.projectJSON(p))
		}
		writeGitLabJSON(w, http.StatusOK, items)
	case hasSuffix("/members"):
		items := make([]any, 0, len(f.group.Members))
		for _, username := range f.group.Members {
			items = append(items, f.memberJSON(username))
		}
		writeGitLabJSON(w, http.StatusOK, items)
	default:
		writeGitLabJSON(w, http.StatusNotFound, map[string]any{"message": "404 Group Not Found"})
	}
}

type gitlabRun struct {
	pythonStage string
	pythonCode  string
	goCode      int
	goStdout    string
	goStderr    string
}

// runGitLab runs both planes on the same scenario.
func (o *teamsOracle) runGitLab(fake *fakeGitLab, orgID, owner, token string, sc *gitlabScenario, extra ...string) gitlabRun {
	o.t.Helper()
	o.truncateAll()
	viaEnv := strings.HasPrefix(token, "env:")
	token = strings.TrimPrefix(token, "env:")
	argv := []string{"--org", orgID, "sync", "teams", "--provider", "gitlab"}
	if owner != "" { // a scenario with no owner omits the flag in both planes
		argv = append(argv, "--owner", owner)
	}
	pythonEnv := map[string]string{"CLICKHOUSE_URI": o.pythonHTTPDSN, "GITLAB_URL": fake.base()}
	if sc != nil && sc.envToken != "" {
		pythonEnv["GITLAB_TOKEN"] = sc.envToken
	}
	if viaEnv {
		pythonEnv["GITLAB_TOKEN"] = token
	} else if token != "" { // a scenario with no token sends no --auth in either plane
		argv = append(argv, "--auth", token)
	}
	argv = append(argv, extra...)
	o.python(sc.name, map[string]any{"argv": argv, "env": pythonEnv})
	run := gitlabRun{pythonStage: o.cur.Stage, pythonCode: o.cur.Code}
	if o.recording {
		return run // the recording asks Python only: the Go plane runs in the comparison
	}
	run.goCode, run.goStdout, run.goStderr = o.runGoGitLab(fake, orgID, owner, token, viaEnv, sc, extra...)
	return run
}

func (o *teamsOracle) runGoGitLab(fake *fakeGitLab, orgID, owner, token string, viaEnv bool, sc *gitlabScenario, extra ...string) (int, string, string) {
	o.t.Helper()
	env := map[string]string{"CLICKHOUSE_URI": o.goNativeDSN, "GITLAB_URL": fake.base()}
	if sc != nil && sc.envToken != "" {
		env["GITLAB_TOKEN"] = sc.envToken
	}
	lookup := func(key string) (string, bool) { v, ok := env[key]; return v, ok }
	args := []string{"--provider", "gitlab", "--org", orgID}
	if owner != "" {
		args = append(args, "--owner", owner)
	}
	if viaEnv {
		env["GITLAB_TOKEN"] = token
	} else if token != "" {
		args = append(args, "--auth", token)
	}
	args = append(args, extra...)
	var stdout, stderr strings.Builder
	d := defaultDeps()
	d.now = func() time.Time { return time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC) }
	d.doer = http.DefaultClient
	d.openStore = func(context.Context, string) (driver.Conn, error) { return &keepOpen{o.goConn}, nil }
	code := runTeams(o.ctx, cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}, d)
	return code, stdout.String(), stderr.String()
}

type gitlabScenario struct {
	name     string
	owner    string
	token    string
	envToken string
	// serverToken, when set, is the token the fake actually requires --
	// distinct from token/envToken (what the client sends). Needed for a
	// genuine rejected-token scenario: leaving it unset made the fake accept
	// whatever token the client happened to send, so "a rejected token"
	// never actually rejected anything (codex r1, CHAOS-6907, P2).
	serverToken string
	group       fakeGitLabGroup
	projects    []fakeGitLabProject
	users       map[string]string
	prepare     func(o *teamsOracle, fake *fakeGitLab)
	extra       []string
	failPath    map[string]int
	// wantExit is the exit code both planes must end with.
	wantExit int
	// wantRows is how many teams both planes must have written.
	wantRows int
	// goDiffers marks a scenario where the two planes end differently by design
	// (a sync-policy guard, for instance): the two databases are not compared
	// column by column and `after` says what each must hold instead.
	goDiffers bool
	goExit    int
	goRows    int
	// wantGoCode, when set, is the error code the Go verb must print on stderr: the CLI's own refusal, told from a
	// later failure that also ends in exit 1 and no rows.
	wantGoCode string
	after      func(t *testing.T, o *teamsOracle, fake *fakeGitLab, sc *gitlabScenario, run gitlabRun, python, goRows []map[string]string)
}

// wantToken is the token the fake actually requires: serverToken when the
// scenario sets one (a genuine mismatch case), else the token the client
// itself sends.
func (sc *gitlabScenario) wantToken() string {
	if sc.serverToken != "" {
		return sc.serverToken
	}
	return strings.TrimPrefix(sc.token, "env:")
}

// gitlabTeamsRule is teamsRule's gitlab-shaped counterpart: what one column of
// `teams` must hold, given the scenario's fake group. Every column of the real
// table needs one (the set is read from the schema in compareGitLab, so a
// column added later fails until it is classified here).
type gitlabTeamsRule struct {
	same  bool
	check func(sc *gitlabScenario, py, gr map[string]string) string
	why   string
}

// parseGitLabList reads ClickHouse's toString of an Array(String): ['a','b'].
func parseGitLabList(text string) []string {
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

func gitlabTeamsRules() map[string]gitlabTeamsRule {
	same := gitlabTeamsRule{same: true}
	return map[string]gitlabTeamsRule{
		// A top-level group's python id ("gl:"+group.path) and go's
		// ("gl:"+full_path) are identical -- no namespace prefix. The
		// subgroup case where they diverge is proven separately, not here.
		"id": same, "is_active": same, "org_id": same,
		"manual_members": {same: true, why: "neither scenario here seeds an admin override"},
		"parent_team_id": same, "source_id": same,
		"repo_patterns": {same: true, why: "GitLab ownership is team_project_ownership, not repo_patterns; neither plane writes it"},
		"name": {check: func(sc *gitlabScenario, py, gr map[string]string) string {
			// Every scenario here gives the group a real, unpadded name, so both
			// planes' actual behavior (python: the raw group.name, no fallback;
			// go: trimmed, falling back to the team id when empty) agree. The
			// fallback branch is not exercised.
			want := sc.group.Name
			if py["name"] != want || gr["name"] != want {
				return fmt.Sprintf("name: want %q, python %q go %q", want, py["name"], gr["name"])
			}
			return ""
		}, why: "both take the provider's name (a blank name is a named, untested divergence: go falls back to the team id, python does not)"},
		"description": {check: func(sc *gitlabScenario, py, gr map[string]string) string {
			if sc.group.Description != nil && *sc.group.Description != "" {
				if py["description"] != *sc.group.Description || gr["description"] != *sc.group.Description {
					return fmt.Sprintf("a provider description is carried by both: python %q go %q", py["description"], gr["description"])
				}
				return ""
			}
			wantPy := "GitLab group " + sc.group.FullPath
			if py["description"] != wantPy {
				return fmt.Sprintf("python's fallback is %q, got %q", wantPy, py["description"])
			}
			if gr["description"] != "<NULL>" && gr["description"] != "" {
				return fmt.Sprintf("go carries the provider's empty description, got %q", gr["description"])
			}
			return ""
		}, why: "legacy: 'GitLab group <full_path>' when the provider has none; catalog: the provider's value"},
		"members": {check: func(sc *gitlabScenario, py, gr map[string]string) string {
			var logins []string
			for _, entry := range parseGitLabList(gr["members"]) {
				if strings.HasPrefix(entry, "gitlab:") {
					logins = append(logins, strings.TrimPrefix(entry, "gitlab:"))
				}
			}
			want := parseGitLabList(py["members"])
			sort.Strings(logins)
			sort.Strings(want)
			if strings.Join(logins, ",") != strings.Join(want, ",") {
				return fmt.Sprintf("members: the usernames differ: python %v, go %v (from %s)", want, logins, gr["members"])
			}
			return ""
		}, why: "legacy: bare usernames; catalog: provider-scoped identity facets"},
		"team_uuid": {check: func(sc *gitlabScenario, py, gr map[string]string) string {
			wantGo := uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+py["id"])).String()
			if py["team_uuid"] != "<uuid4>" { // the golden stores python's random uuid4 as its kind
				return fmt.Sprintf("team_uuid: python's is a random uuid4 (stored as <uuid4>), got %q", py["team_uuid"])
			}
			if gr["team_uuid"] != wantGo {
				return fmt.Sprintf("team_uuid: go's is uuid5(URL, \"team:<id>\") = %s, got %s", wantGo, gr["team_uuid"])
			}
			return ""
		}, why: "legacy: a random uuid4 per run; catalog: uuid5 of the team id, stable across runs"},
		"provider": {check: func(sc *gitlabScenario, py, gr map[string]string) string {
			if py["provider"] != "" || gr["provider"] != "gitlab" {
				return fmt.Sprintf("provider: python %q (want empty), go %q (want gitlab)", py["provider"], gr["provider"])
			}
			return ""
		}, why: "legacy rows carry no provider; catalog rows are provider_access rows of 'gitlab'"},
		"native_team_key": {check: func(sc *gitlabScenario, py, gr map[string]string) string {
			if py["native_team_key"] != "<NULL>" || gr["native_team_key"] != sc.group.FullPath {
				return fmt.Sprintf("native_team_key: python %q (want NULL), go %q (want %q)", py["native_team_key"], gr["native_team_key"], sc.group.FullPath)
			}
			return ""
		}, why: "legacy rows carry no native key; the catalog keys the team by its full_path"},
		"project_keys": {check: func(sc *gitlabScenario, py, gr map[string]string) string {
			if py["project_keys"] != "[]" {
				return fmt.Sprintf("project_keys: python %q (want [])", py["project_keys"])
			}
			var want []string
			for _, p := range sc.projects {
				want = append(want, p.PathWithNamespace)
			}
			sort.Strings(want)
			got := parseGitLabList(gr["project_keys"])
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				return fmt.Sprintf("project_keys: go %v, want %v", got, want)
			}
			return ""
		}, why: "legacy: none; catalog: the group's own projects (its path_with_namespace values)"},
		"created_at": {check: func(sc *gitlabScenario, py, gr map[string]string) string {
			if py["created_at"] != "" && py["created_at"] != "<NULL>" || gr["created_at"] != gr["updated_at"] {
				return fmt.Sprintf("created_at: python %q (no such column), go %q (want its updated_at %q)", py["created_at"], gr["created_at"], gr["updated_at"])
			}
			return ""
		}, why: "legacy rows carry no creation time; the catalog stamps a new team with its own updated_at"},
		"updated_at": {check: func(sc *gitlabScenario, py, gr map[string]string) string {
			if py["updated_at"] != "<time>" || gr["updated_at"] != "2026-09-26 12:00:00.000000" {
				return fmt.Sprintf("updated_at: python %q, go %q (want the run's clock)", py["updated_at"], gr["updated_at"])
			}
			return ""
		}, why: "time of the run (python: its own clock)"},
		"last_synced": {check: func(sc *gitlabScenario, py, gr map[string]string) string {
			if py["last_synced"] != "<time>" || gr["last_synced"] == "" {
				return "last_synced is empty"
			}
			return ""
		}, why: "time of the write (DEFAULT now() on both databases)"},
	}
}

// arrangeAndRunGitLab sets the fake, the failures and the seeds the scenario names and runs it: both planes when
// comparing, the legacy verb alone when recording.
func (o *teamsOracle) arrangeAndRunGitLab(sc *gitlabScenario, fake *fakeGitLab) gitlabRun {
	o.t.Helper()
	fake.set(sc.wantToken(), sc.group, sc.projects, sc.users)
	for path, status := range sc.failPath {
		fake.failing[path] = status
	}
	if sc.prepare != nil {
		sc.prepare(o, fake)
	}
	return o.runGitLab(fake, "org-1", sc.owner, sc.token, sc, sc.extra...)
}

// compareGitLab checks one scenario: exit codes, then the `teams` rows column by column under
// gitlabTeamsRules, then the tables only the catalog writes.
func (o *teamsOracle) compareGitLab(sc *gitlabScenario, fake *fakeGitLab) {
	t := o.t
	t.Helper()
	run := o.arrangeAndRunGitLab(sc, fake)
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
	rules := gitlabTeamsRules()
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
			t.Fatalf("teams has a column %q that the gitlab comparison does not classify: add a rule for it", column)
		}
	}
	if !sc.goDiffers {
		for index := range python {
			py, gr := python[index], goRows[index]
			for _, column := range columns {
				rule := rules[column]
				switch {
				case rule.same:
					if py[column] != gr[column] {
						t.Errorf("%s team %s column %s: python %q go %q", sc.name, py["id"], column, py[column], gr[column])
					}
				default:
					if message := rule.check(sc, py, gr); message != "" {
						t.Errorf("%s team %s column %s: %s", sc.name, py["id"], column, message)
					}
				}
			}
		}
	}
	if sc.after != nil {
		sc.after(t, o, fake, sc, run, python, goRows)
	}
	// Python wrote nothing but `teams`; the catalog also writes ownership and memberships.
	for _, table := range []string{"team_project_ownership", "team_memberships", "projects"} {
		if rows := o.pythonRows(table); len(rows) != 0 {
			t.Errorf("%s: python wrote %d %s rows", sc.name, len(rows), table)
		}
	}
	if sc.wantExit == 0 && sc.wantRows > 0 && !sc.goDiffers {
		wantMemberships := len(sc.group.Members)
		memberships := o.rows(o.goDatabase, "team_memberships", "org-1")
		if len(memberships) != wantMemberships {
			t.Errorf("%s: go wrote %d team_memberships rows, the fake has %d members", sc.name, len(memberships), wantMemberships)
		}
		for _, row := range memberships {
			username := strings.TrimPrefix(row["raw_provider_user_id"], "gitlab:")
			want := fake.users[username]
			if want == "" {
				want = "<NULL>"
			}
			if row["raw_email"] != want {
				t.Errorf("%s: membership of %s carries email %q, the fake's is %q", sc.name, username, row["raw_email"], want)
			}
			if row["member_id"] != "gl:"+username || row["source"] != "provider_access" || row["provider"] != "gitlab" || row["is_primary"] != "0" {
				t.Errorf("%s: membership of %s has member_id %q source %q provider %q is_primary %q", sc.name, username, row["member_id"], row["source"], row["provider"], row["is_primary"])
			}
		}
		wantOwnership := len(sc.projects)
		if got := len(o.rows(o.goDatabase, "team_project_ownership", "org-1")); got != wantOwnership {
			t.Errorf("%s: go wrote %d team_project_ownership rows, the fake has %d projects", sc.name, got, wantOwnership)
		}
	}
}

func gitlabScenarios() []*gitlabScenario {
	return []*gitlabScenario{
		{name: "one top-level group with members and projects", owner: "acme", token: "tok", wantExit: 0, wantRows: 1,
			group:    fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Description: strPtr("The Acme group"), Members: []string{"alice", "bob"}},
			projects: []fakeGitLabProject{{ID: 501, PathWithNamespace: "acme/api", Name: "api"}, {ID: 502, PathWithNamespace: "acme/web", Name: "web"}},
			users:    map[string]string{"alice": "alice@example.com", "bob": ""}},
		{name: "no description", owner: "acme", token: "tok", wantExit: 0, wantRows: 1,
			group: fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Description: nil, Members: []string{"alice"}}, users: map[string]string{"alice": "a@example.com"}},
		{name: "a group with no projects and one member", owner: "acme", token: "tok", wantExit: 0, wantRows: 1,
			group: fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Members: []string{"alice"}}, users: map[string]string{"alice": ""}},
		{name: "the token from GITLAB_TOKEN", owner: "acme", token: "env:tok", wantExit: 0, wantRows: 1,
			group: fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Members: []string{"alice"}}, users: map[string]string{"alice": ""}},
		{name: "--auth wins over GITLAB_TOKEN", owner: "acme", token: "tok", envToken: "not-the-token", wantExit: 0, wantRows: 1,
			group: fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Members: []string{"alice"}}, users: map[string]string{"alice": ""}},
		{name: "no --owner is refused", owner: "", token: "tok", wantExit: 1, wantRows: 0, wantGoCode: "owner_required",
			group: fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Members: []string{"alice"}}},
		{name: "no token anywhere is refused", owner: "acme", token: "", wantExit: 1, wantRows: 0, wantGoCode: "token_required",
			group: fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Members: []string{"alice"}}},
		{name: "a padded --owner", owner: " acme ", token: "tok", wantExit: 1, wantRows: 0, goDiffers: true, goExit: 0, goRows: 1,
			group: fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Members: []string{"alice"}}},
		{name: "a rejected token", owner: "acme", token: "wrong", serverToken: "tok", wantExit: 1, wantRows: 0,
			group: fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Members: []string{"alice"}}},
		{name: "an unknown group", owner: "nope", token: "tok", wantExit: 1, wantRows: 0,
			group: fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Members: []string{"alice"}}},
		{name: "a member roster that cannot be read stops the run", owner: "acme", token: "tok", wantExit: 1, wantRows: 0,
			group: fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Members: []string{"alice"}},
			// Go requests /members by path; python-gitlab's bound group.members
			// manager requests it by the group's numeric id (id=1) -- both must
			// fail for both planes to stop on this.
			failPath: map[string]int{"/api/v4/groups/acme/members": 500, "/api/v4/groups/1/members": 500}},
	}
}

// gitlabCorpusKey is the golden's request key: every scenario's inputs.
func gitlabCorpusKey() []byte {
	type entry struct {
		Name, Owner, Token, EnvToken, ServerToken string
		Group                                     fakeGitLabGroup
		Projects                                  []fakeGitLabProject
		Users                                     map[string]string
		Extra                                     []string
		FailPath                                  map[string]int
	}
	var entries []entry
	for _, sc := range gitlabScenarios() {
		entries = append(entries, entry{sc.name, sc.owner, sc.token, sc.envToken, sc.serverToken, sc.group, sc.projects, sc.users, sc.extra, sc.failPath})
	}
	return teamsCorpusKey("gitlab", entries)
}

// TestSyncTeamsGitLabMatchesFrozenPython compares `dho sync teams --provider gitlab` (the Go team catalog) with what
// the REAL legacy verb (python-gitlab pointed at a fake via GITLAB_URL) did over the same fake GitLab: how each run
// ended and, column by column, the `teams` rows it wrote. Executed once on teamsPythonBuild and frozen in
// testdata/golden/teams_gitlab.json.
func TestSyncTeamsGitLabMatchesFrozenPython(t *testing.T) {
	frozen, golden := openTeamsGolden(t, "gitlab", "TestSyncTeamsGitLabMatchesFrozenPython", "2ce74c0408dd8981d23f61e1e28ba27e7a79aa1a7d2de510b036553b46a82c2e", teamsSyncOracleGitLabProgram, gitlabCorpusKey(),
		func(t *testing.T, producer *venueoracle.Producer) []teamsFrozen {
			o := newTeamsOracle(t)
			o.startPython(producer, teamsSyncOracleGitLabProgram)
			fake := newFakeGitLab(t)
			for _, sc := range gitlabScenarios() {
				o.arrangeAndRunGitLab(sc, fake)
			}
			return o.recorded
		})
	o := newTeamsOracle(t)
	o.frozen = frozen
	fake := newFakeGitLab(t)
	for _, sc := range gitlabScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			o.t = t
			o.compareGitLab(sc, fake)
		})
	}
	if o.next != len(frozen) {
		t.Fatalf("the golden holds %d runs, the corpus runs %d", len(frozen), o.next)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// The subgroup id divergence this harness deliberately does not exercise
// (python: "gl:" + the group's own path; go: "gl:" + full_path) is proven in
// internal/providersync/gitlab_team_catalog_subgroup_id_test.go, next to
// gitlabTeamID itself.

// TestMembersOnlyGitLabRunIsNotReportedEmpty is the codex r1 regression proof
// (CHAOS-6907, P1): `--members` alone (no --structure) selects Members but not
// Teams, so the catalog writes real team_memberships rows while TeamsWritten
// stays zero -- the empty-result check (runCatalogTeams, teamscatalog.go)
// used to count only team-shaped outcomes, so this exact run committed a
// membership row and still exited 1 with "No teams found/generated." No
// python or venue oracle needed: this is Go-only CLI behavior (python's `sync
// teams` argparse has no --structure/--members/--projects flags at all), so
// it runs on every `-tags=integration` pass, not gated behind
// DEV_HEALTH_VENUE_ORACLES.
func TestMembersOnlyGitLabRunIsNotReportedEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	fake := newFakeGitLab(t)
	fake.set("tok", fakeGitLabGroup{ID: 1, FullPath: "acme", Name: "Acme", Members: []string{"alice"}}, nil, map[string]string{"alice": "alice@example.com"})

	var stdout, stderr strings.Builder
	d := defaultDeps()
	d.now = func() time.Time { return time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC) }
	d.doer = http.DefaultClient
	d.openStore = func(context.Context, string) (driver.Conn, error) { return &keepOpen{conn}, nil }
	args := []string{"--provider", "gitlab", "--org", "org-1", "--owner", "acme", "--auth", "tok", "--members"}
	env := map[string]string{"CLICKHOUSE_URI": instance.URI, "GITLAB_URL": fake.base()}
	lookup := func(key string) (string, bool) { v, ok := env[key]; return v, ok }
	code := runTeams(ctx, cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}, d)
	if code != cli.ExitOK {
		t.Fatalf("--members alone: exit %d (want 0), stderr %s", code, stderr.String())
	}
	rows, err := conn.Query(ctx, "SELECT count() FROM team_memberships FINAL WHERE org_id = ?", "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var membershipCount uint64
	if rows.Next() {
		if err := rows.Scan(&membershipCount); err != nil {
			t.Fatal(err)
		}
	}
	if membershipCount == 0 {
		t.Fatalf("expected a real team_memberships row to have been written, got %d", membershipCount)
	}
	teamRows, err := conn.Query(ctx, "SELECT count() FROM teams FINAL WHERE org_id = ?", "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer teamRows.Close()
	var teamCount uint64
	if teamRows.Next() {
		if err := teamRows.Scan(&teamCount); err != nil {
			t.Fatal(err)
		}
	}
	if teamCount != 0 {
		t.Fatalf("--structure was not selected: expected 0 teams rows, got %d", teamCount)
	}
}
