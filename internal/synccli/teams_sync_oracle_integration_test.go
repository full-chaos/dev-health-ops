//go:build integration

package synccli

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

//go:embed testdata/teams_sync_oracle.py
var teamsSyncOracleProgram string

// fakeGitHub is the one GitHub both planes read: PyGithub (the real Python verb) and the
// Go catalog. It serves every path either of them asks for from one data set.
type fakeGitHub struct {
	server *httptest.Server
	mu     sync.Mutex

	token   string // the token every request must carry ("" = none required)
	org     string
	teams   []fakeTeam
	users   map[string]string // login -> email ("" = private)
	failing map[string]int    // path -> status to answer instead
	hits    map[string]int
}

type fakeTeam struct {
	ID          int
	Slug, Name  string
	Description *string
	Members     []string
	Repos       []string
}

func newFakeGitHub(t *testing.T) *fakeGitHub {
	t.Helper()
	f := &fakeGitHub{users: map[string]string{}, failing: map[string]int{}, hits: map[string]int{}}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeGitHub) base() string { return f.server.URL }

func (f *fakeGitHub) set(org, token string, teams []fakeTeam, users map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.org, f.token, f.teams, f.users = org, token, teams, users
	f.failing = map[string]int{}
	f.hits = map[string]int{}
}

func (f *fakeGitHub) page(w http.ResponseWriter, r *http.Request, items []any) {
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	if perPage <= 0 || perPage > 100 {
		perPage = 30
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page <= 0 {
		page = 1
	}
	start, end := (page-1)*perPage, page*perPage
	if start > len(items) {
		start = len(items)
	}
	if end > len(items) {
		end = len(items)
	}
	if end < len(items) {
		next := *r.URL
		query := next.Query()
		query.Set("page", strconv.Itoa(page+1))
		next.RawQuery = query.Encode()
		w.Header().Set("Link", fmt.Sprintf(`<%s%s>; rel="next"`, f.base(), next.RequestURI()))
	}
	slice := items[start:end]
	if slice == nil {
		slice = []any{}
	}
	writeJSON(w, http.StatusOK, slice)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (f *fakeGitHub) teamJSON(team fakeTeam) map[string]any {
	teamURL := fmt.Sprintf("%s/organizations/1/team/%d", f.base(), team.ID)
	var description any
	if team.Description != nil {
		description = *team.Description
	}
	return map[string]any{
		"id": team.ID, "node_id": "T_" + team.Slug, "slug": team.Slug, "name": team.Name, "description": description,
		"privacy": "closed", "permission": "pull", "url": teamURL, "html_url": f.base() + "/orgs/" + f.org + "/teams/" + team.Slug,
		"members_url": teamURL + "/members{/member}", "repositories_url": teamURL + "/repos", "parent": nil,
	}
}

func (f *fakeGitHub) userJSON(login string) map[string]any {
	return map[string]any{"login": login, "id": 1000 + len(login), "type": "User", "url": f.base() + "/users/" + login}
}

func (f *fakeGitHub) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	f.hits[path]++
	if f.token != "" && r.Header.Get("Authorization") != "token "+f.token && r.Header.Get("Authorization") != "Bearer "+f.token {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "Bad credentials"})
		return
	}
	if status, ok := f.failing[path]; ok {
		writeJSON(w, status, map[string]any{"message": "injected failure"})
		return
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case len(parts) == 2 && parts[0] == "orgs" && parts[1] == f.org:
		writeJSON(w, http.StatusOK, map[string]any{"login": f.org, "id": 1, "type": "Organization", "url": f.base() + "/orgs/" + f.org})
	case len(parts) == 3 && parts[0] == "orgs" && parts[1] == f.org && parts[2] == "teams":
		items := make([]any, 0, len(f.teams))
		for _, team := range f.teams {
			items = append(items, f.teamJSON(team))
		}
		f.page(w, r, items)
	case len(parts) == 5 && parts[0] == "orgs" && parts[1] == f.org && parts[2] == "teams" && (parts[4] == "members" || parts[4] == "repos"):
		f.teamList(w, r, parts[3], parts[4])
	case len(parts) == 5 && parts[0] == "organizations" && parts[2] == "team" && parts[4] == "members":
		id, _ := strconv.Atoi(parts[3])
		for _, team := range f.teams {
			if team.ID == id {
				f.teamList(w, r, team.Slug, "members")
				return
			}
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
	case len(parts) == 2 && parts[0] == "users":
		email, known := f.users[parts[1]]
		if !known {
			writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
			return
		}
		user := f.userJSON(parts[1])
		user["name"] = "Name of " + parts[1]
		if email != "" {
			user["email"] = email
		} else {
			user["email"] = nil
		}
		writeJSON(w, http.StatusOK, user)
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found: " + path})
	}
}

func (f *fakeGitHub) teamList(w http.ResponseWriter, r *http.Request, slug, kind string) {
	for _, team := range f.teams {
		if team.Slug != slug {
			continue
		}
		var items []any
		if kind == "members" {
			for _, login := range team.Members {
				items = append(items, f.userJSON(login))
			}
		} else {
			for _, name := range team.Repos {
				items = append(items, map[string]any{"id": 5000 + len(name), "name": name, "full_name": f.org + "/" + name, "private": false})
			}
		}
		f.page(w, r, items)
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
}

// teamsOracle is the two planes: a real Python verb in a long-lived child writing one
// ClickHouse database (migrated by the real chain), and the Go verb writing a clone of it.
type teamsOracle struct {
	t                *testing.T
	ctx              context.Context
	admin            driver.Conn
	pythonDatabase   string
	goDatabase       string
	pythonHTTPDSN    string
	goNativeDSN      string
	goConn           driver.Conn
	ask              func(map[string]any) map[string]any
	tablesWrittenAny []string
}

func newTeamsOracle(t *testing.T) *teamsOracle {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	nativeURL, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	pythonDatabase := strings.TrimPrefix(nativeURL.Path, "/")
	const goDatabase = "dho_teams_go"
	admin, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+goDatabase); err != nil {
		t.Fatal(err)
	}
	tables, err := admin.Query(ctx, "SELECT name FROM system.tables WHERE database = ? AND engine NOT LIKE '%View' AND NOT startsWith(name, '.')", pythonDatabase)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for tables.Next() {
		var name string
		if err := tables.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	_ = tables.Close()
	for _, name := range names {
		if err := admin.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s AS %s.%s", goDatabase, name, pythonDatabase, name)); err != nil {
			t.Fatalf("clone %s: %v", name, err)
		}
	}
	httpDSN, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	goURL := *nativeURL
	goURL.Path = "/" + goDatabase
	goConn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(goURL.String()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = goConn.Close() })

	command := exec.Command(python, "-c", teamsSyncOracleProgram)
	command.Env = append(os.Environ(), "PYTHONHASHSEED=0", "PYTHONPATH="+filepath.Join(root, "src"))
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = command.Wait() })
	reader := bufio.NewReaderSize(stdout, 1<<20)
	ask := func(request map[string]any) map[string]any {
		raw, _ := json.Marshal(request)
		if _, err := io.WriteString(stdin, string(raw)+"\n"); err != nil {
			t.Fatalf("python stdin: %v", err)
		}
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatalf("python answer: %v\n%s", err, stderr.String())
		}
		var answer map[string]any
		if err := json.Unmarshal(line, &answer); err != nil {
			t.Fatalf("python answer %q: %v", line, err)
		}
		return answer
	}
	return &teamsOracle{t: t, ctx: ctx, admin: admin, pythonDatabase: pythonDatabase, goDatabase: goDatabase,
		pythonHTTPDSN: httpDSN, goNativeDSN: goURL.String(), goConn: goConn, ask: ask, tablesWrittenAny: names}
}

func (o *teamsOracle) truncateAll() {
	o.t.Helper()
	for _, database := range []string{o.pythonDatabase, o.goDatabase} {
		for _, table := range []string{"teams", "team_memberships", "team_repo_ownership", "team_project_ownership", "team_drift_changes", "jira_project_ops_team_links", "projects", "identities"} {
			if err := o.admin.Exec(o.ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s", database, table)); err != nil {
				o.t.Fatal(err)
			}
		}
	}
}

// rows reads every row of a table (FINAL) as column -> text.
func (o *teamsOracle) rows(database, table, orgID string) []map[string]string {
	o.t.Helper()
	cols, err := o.admin.Query(o.ctx, "SELECT name FROM system.columns WHERE database = ? AND table = ? ORDER BY position", database, table)
	if err != nil {
		o.t.Fatal(err)
	}
	var names []string
	for cols.Next() {
		var name string
		if err := cols.Scan(&name); err != nil {
			o.t.Fatal(err)
		}
		names = append(names, name)
	}
	_ = cols.Close()
	selects := make([]string, len(names))
	for i, name := range names {
		selects[i] = fmt.Sprintf("ifNull(toString(%s), '<NULL>')", name)
	}
	query := fmt.Sprintf("SELECT %s FROM %s.%s FINAL WHERE org_id = ? ORDER BY 1", strings.Join(selects, ", "), database, table)
	data, err := o.admin.Query(o.ctx, query, orgID)
	if err != nil {
		o.t.Fatalf("%s.%s: %v", database, table, err)
	}
	defer data.Close()
	var out []map[string]string
	for data.Next() {
		values := make([]string, len(names))
		pointers := make([]any, len(names))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := data.Scan(pointers...); err != nil {
			o.t.Fatal(err)
		}
		row := map[string]string{}
		for i, name := range names {
			row[name] = values[i]
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["id"] < out[j]["id"] })
	return out
}

type githubRun struct {
	pythonStage string
	pythonCode  string
	goCode      int
	goStdout    string
	goStderr    string
}

// runGitHub runs both planes on the same scenario.
func (o *teamsOracle) runGitHub(fake *fakeGitHub, orgID, owner, token string, prepare func(), sc *teamsScenario, extra ...string) githubRun {
	o.t.Helper()
	o.truncateAll()
	if prepare != nil {
		prepare()
	}
	viaEnv := strings.HasPrefix(token, "env:")
	token = strings.TrimPrefix(token, "env:")
	argv := []string{"--org", orgID, "sync", "teams", "--provider", "github", "--owner", owner}
	pythonEnv := map[string]string{"CLICKHOUSE_URI": o.pythonHTTPDSN}
	if sc != nil && sc.envToken != "" {
		pythonEnv["GITHUB_TOKEN"] = sc.envToken
	}
	if viaEnv {
		pythonEnv["GITHUB_TOKEN"] = token
	} else {
		argv = append(argv, "--auth", token)
	}
	argv = append(argv, extra...)
	answer := o.ask(map[string]any{"argv": argv, "github_base": fake.base(), "env": pythonEnv})
	value := func(key string) string {
		if item, ok := answer[key].(map[string]any); ok {
			return fmt.Sprint(item["v"])
		}
		return ""
	}
	run := githubRun{pythonStage: value("stage"), pythonCode: value("code")}

	baseKey := "GITHUB_BASE_URL"
	if sc != nil && sc.baseEnv != "" {
		baseKey = sc.baseEnv
	}
	env := map[string]string{"CLICKHOUSE_URI": o.goNativeDSN, baseKey: fake.base()}
	if sc != nil && sc.envToken != "" {
		env["GITHUB_TOKEN"] = sc.envToken
	}
	lookup := func(key string) (string, bool) { v, ok := env[key]; return v, ok }
	args := []string{"--provider", "github", "--org", orgID, "--owner", owner}
	if viaEnv {
		env["GITHUB_TOKEN"] = token
	} else {
		args = append(args, "--auth", token)
	}
	args = append(args, extra...)
	var stdout, stderr strings.Builder
	d := defaultDeps()
	d.now = func() time.Time { return time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC) }
	d.doer = http.DefaultClient
	d.openStore = func(context.Context, string) (driver.Conn, error) { return &keepOpen{o.goConn}, nil }
	run.goCode = runTeams(o.ctx, cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}, d)
	run.goStdout, run.goStderr = stdout.String(), stderr.String()
	return run
}

// keepOpen lets the verb close "its" connection without closing the test's.
type keepOpen struct{ driver.Conn }

func (keepOpen) Close() error { return nil }

func strPtr(s string) *string { return &s }

// teamsRule says what the comparison requires of one column of `teams` (every column of the
// table must have one: the set is read from the schema, so a column added later fails until it is
// classified). Python's `teams` row is the legacy dev-hops writer's; Go's is the team catalog's
// (ops/docs/architecture/team-attribution.md §0, CHAOS-2600): a column that differs does so
// by design, and the rule asserts HOW.
type teamsRule struct {
	// same: the two planes' values are equal text. Otherwise check runs on both rows.
	same  bool
	check func(sc *teamsScenario, team fakeTeam, python, goRow map[string]string) string
	why   string
}

type teamsScenario struct {
	name  string
	org   string
	owner string
	token string
	// envToken, when set, is GITHUB_TOKEN in both planes' environment (the --auth token must win).
	envToken string
	// baseEnv is the environment variable that carries the fake's URL to the Go plane
	// (default GITHUB_BASE_URL; the other spelling is GITHUB_URL).
	baseEnv string
	// serverToken is the token the fake demands when it is not the one sent (a rejected token).
	serverToken string
	teams       []fakeTeam
	users       map[string]string
	prepare     func(o *teamsOracle, fake *fakeGitHub)
	extra       []string
	failPath    map[string]int // path -> status, injected before the run
	// wantExit is the exit code both planes must end with (Python's: 0 or 1).
	wantExit int
	// wantRows is how many teams both planes must have written.
	wantRows int
	// after asserts more about the rows both planes wrote.
	after func(t *testing.T, python, goRows []map[string]string)
}

// wantToken is the token the fake demands: the scenario's, without the "env:" marker.
func (sc *teamsScenario) wantToken() string {
	if sc.serverToken != "" {
		return sc.serverToken
	}
	return strings.TrimPrefix(sc.token, "env:")
}

func quoteList(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = "'" + item + "'"
	}
	return "[" + strings.Join(quoted, ",") + "]"
}

// parseList reads ClickHouse's toString of an Array(String): ['a','b'].
func parseList(text string) []string {
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

func teamsRules() map[string]teamsRule {
	same := teamsRule{same: true}
	return map[string]teamsRule{
		"id": same, "name": same, "is_active": same, "org_id": same,
		"manual_members": {same: true, why: "both preserve an admin's override (CHAOS-4321)"},
		"project_keys":   {same: true, why: "neither writes project keys for a GitHub team"},
		"parent_team_id": same, "source_id": same,
		"description": {check: func(sc *teamsScenario, team fakeTeam, py, gr map[string]string) string {
			if team.Description != nil && *team.Description != "" {
				if py["description"] != gr["description"] {
					return fmt.Sprintf("a provider description is carried by both: python %q go %q", py["description"], gr["description"])
				}
				return ""
			}
			// No description: the legacy verb invented one, the catalog keeps the provider's silence.
			if py["description"] != "GitHub team "+team.Slug {
				return fmt.Sprintf("python's fallback is %q, got %q", "GitHub team "+team.Slug, py["description"])
			}
			if gr["description"] != "<NULL>" && gr["description"] != "" {
				return fmt.Sprintf("go carries the provider's empty description, got %q", gr["description"])
			}
			return ""
		}, why: "legacy: 'GitHub team <slug>' when the provider has none; catalog: the provider's value"},
		"members": {check: func(sc *teamsScenario, team fakeTeam, py, gr map[string]string) string {
			// The catalog's roster is identity facets: 'github:<login>' for each member (plus the member's
			// public email); the legacy roster is the bare login. The logins are what must agree.
			var logins []string
			for _, entry := range parseList(gr["members"]) {
				if strings.HasPrefix(entry, "github:") {
					logins = append(logins, strings.TrimPrefix(entry, "github:"))
				}
			}
			want := parseList(py["members"])
			sort.Strings(logins)
			sort.Strings(want)
			if strings.Join(logins, ",") != strings.Join(want, ",") {
				return fmt.Sprintf("members: the logins differ: python %v, go %v (from %s)", want, logins, gr["members"])
			}
			return ""
		}, why: "legacy: bare logins; catalog: provider-scoped identity facets"},
		"team_uuid": {check: func(sc *teamsScenario, team fakeTeam, py, gr map[string]string) string {
			wantGo := uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+py["id"])).String()
			parsed, err := uuid.Parse(py["team_uuid"])
			if err != nil || parsed.Version() != 4 {
				return fmt.Sprintf("team_uuid: python's is a random uuid4, got %q", py["team_uuid"])
			}
			if gr["team_uuid"] != wantGo {
				return fmt.Sprintf("team_uuid: go's is uuid5(URL, \"team:<id>\") = %s, got %s", wantGo, gr["team_uuid"])
			}
			return ""
		}, why: "legacy: a random uuid4 per run; catalog: uuid5 of the team id, stable across runs"},
		"provider": {check: func(sc *teamsScenario, team fakeTeam, py, gr map[string]string) string {
			if py["provider"] != "" || gr["provider"] != "github" {
				return fmt.Sprintf("provider: python %q (want empty), go %q (want github)", py["provider"], gr["provider"])
			}
			return ""
		}, why: "legacy rows carry no provider; catalog rows are provider_access rows of 'github'"},
		"native_team_key": {check: func(sc *teamsScenario, team fakeTeam, py, gr map[string]string) string {
			if py["native_team_key"] != "<NULL>" || gr["native_team_key"] != team.Slug {
				return fmt.Sprintf("native_team_key: python %q (want NULL), go %q (want %q)", py["native_team_key"], gr["native_team_key"], team.Slug)
			}
			return ""
		}, why: "legacy rows carry no native key; the catalog keys the team by its slug"},
		"repo_patterns": {check: func(sc *teamsScenario, team fakeTeam, py, gr map[string]string) string {
			var want []string
			for _, repo := range team.Repos {
				want = append(want, sc.owner+"/"+repo)
			}
			sort.Strings(want)
			got := parseList(gr["repo_patterns"])
			sort.Strings(got)
			if py["repo_patterns"] != "[]" || strings.Join(got, ",") != strings.Join(want, ",") {
				return fmt.Sprintf("repo_patterns: python %q (want []), go %v (want %v)", py["repo_patterns"], got, want)
			}
			return ""
		}, why: "legacy: none; catalog: the team's repositories as owner/name (its ownership grants)"},
		"updated_at": {check: func(sc *teamsScenario, team fakeTeam, py, gr map[string]string) string {
			if py["updated_at"] == "" || gr["updated_at"] != "2026-09-26 12:00:00.000000" {
				return fmt.Sprintf("updated_at: python %q, go %q (want the run's clock)", py["updated_at"], gr["updated_at"])
			}
			return ""
		}, why: "time of the run (python: its own clock)"},
		"last_synced": {check: func(sc *teamsScenario, team fakeTeam, py, gr map[string]string) string {
			if py["last_synced"] == "" || gr["last_synced"] == "" {
				return "last_synced is empty"
			}
			return ""
		}, why: "time of the write"},
	}
}

// compareGitHub checks one scenario: exit codes, then the `teams` rows column by column under teamsRules,
// then the tables only the catalog writes.
func (o *teamsOracle) compareGitHub(sc *teamsScenario, fake *fakeGitHub) {
	t := o.t
	t.Helper()
	fake.set(sc.org, sc.wantToken(), sc.teams, sc.users)
	for path, status := range sc.failPath {
		fake.failing[path] = status
	}
	var prepare func()
	if sc.prepare != nil {
		prepare = func() { sc.prepare(o, fake) }
	}
	run := o.runGitHub(fake, "org-1", sc.owner, sc.token, prepare, sc, sc.extra...)
	if run.pythonStage != "ok" && run.pythonStage != "exit" {
		t.Fatalf("%s: python ended %s (%s)", sc.name, run.pythonStage, run.pythonCode)
	}
	wantPython := strconv.Itoa(sc.wantExit)
	if run.pythonCode != wantPython || run.goCode != sc.wantExit {
		t.Fatalf("%s: exit codes: python %s (stage %s), go %d, want %d\ngo stderr: %s", sc.name, run.pythonCode, run.pythonStage, run.goCode, sc.wantExit, run.goStderr)
	}
	python := o.rows(o.pythonDatabase, "teams", "org-1")
	goRows := o.rows(o.goDatabase, "teams", "org-1")
	if len(python) != sc.wantRows || len(goRows) != sc.wantRows {
		t.Fatalf("%s: teams written: python %d, go %d, want %d", sc.name, len(python), len(goRows), sc.wantRows)
	}
	// Every column of the real table has a rule.
	rules := teamsRules()
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
			t.Fatalf("teams has a column %q that the comparison does not classify: add a rule for it", column)
		}
	}
	byID := map[string]fakeTeam{}
	for _, team := range sc.teams {
		byID["gh:"+team.Slug] = team
	}
	for index := range python {
		py, gr := python[index], goRows[index]
		team := byID[py["id"]]
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
	if sc.after != nil {
		sc.after(t, python, goRows)
	}
	// Python wrote nothing but `teams`; the catalog also writes memberships and repository ownership.
	for _, table := range []string{"team_memberships", "team_repo_ownership"} {
		if rows := o.rows(o.pythonDatabase, table, "org-1"); len(rows) != 0 {
			t.Errorf("%s: python wrote %d %s rows", sc.name, len(rows), table)
		}
	}
	wantMemberships, wantOwnership := 0, 0
	for _, team := range sc.teams {
		wantMemberships += len(team.Members)
		wantOwnership += len(team.Repos)
	}
	if sc.wantExit == 0 && sc.wantRows > 0 {
		memberships := o.rows(o.goDatabase, "team_memberships", "org-1")
		if len(memberships) != wantMemberships {
			t.Errorf("%s: go wrote %d team_memberships rows, the fake has %d members", sc.name, len(memberships), wantMemberships)
		}
		// The member's public email is resolved (GET /users/{login}) and kept on the membership.
		for _, row := range memberships {
			login := strings.TrimPrefix(row["raw_provider_user_id"], "github:")
			want := fake.users[login]
			if want == "" {
				want = "<NULL>"
			}
			if row["raw_email"] != want {
				t.Errorf("%s: membership of %s carries email %q, the fake's is %q", sc.name, login, row["raw_email"], want)
			}
		}
		if got := len(o.rows(o.goDatabase, "team_repo_ownership", "org-1")); got != wantOwnership {
			t.Errorf("%s: go wrote %d team_repo_ownership rows, the fake has %d repositories", sc.name, got, wantOwnership)
		}
	}
}

func githubScenarios() []*teamsScenario {
	members := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("%s%03d", prefix, i)
		}
		return out
	}
	emails := func(logins ...string) map[string]string {
		m := map[string]string{}
		for i, login := range logins {
			if i%2 == 0 {
				m[login] = login + "@example.com"
			} else {
				m[login] = ""
			}
		}
		return m
	}
	two := []fakeTeam{
		{ID: 11, Slug: "platform", Name: "Platform", Description: strPtr("The platform team"), Members: []string{"alice", "bob"}, Repos: []string{"api", "web"}},
		{ID: 12, Slug: "data", Name: "Data & ML", Description: nil, Members: []string{"carol"}, Repos: nil},
	}
	var many []fakeTeam
	for i := 0; i < 130; i++ {
		many = append(many, fakeTeam{ID: 100 + i, Slug: fmt.Sprintf("team-%03d", i), Name: fmt.Sprintf("Team %03d", i), Description: strPtr("d"), Members: []string{"alice"}})
	}
	return []*teamsScenario{
		{name: "two teams, one without a description", org: "acme", owner: "acme", token: "tok", teams: two, users: emails("alice", "bob", "carol"), wantRows: 2},
		{name: "empty description and odd names", org: "acme", owner: "acme", token: "tok", users: emails("alice"), wantRows: 3, teams: []fakeTeam{
			{ID: 21, Slug: "blank", Name: "Blank", Description: strPtr(""), Members: []string{"alice"}},
			{ID: 22, Slug: "unicode", Name: "Ünïcode Wörks", Description: strPtr("ünï"), Members: []string{"alice"}, Repos: []string{"r"}},
			{ID: 23, Slug: "no-members", Name: "Nobody", Description: strPtr("empty"), Members: nil, Repos: []string{"a", "b", "c"}},
		}},
		{name: "a team with a hundred and thirty members and a second page of teams", org: "acme", owner: "acme", token: "tok", wantRows: 131,
			teams: append([]fakeTeam{{ID: 1, Slug: "big", Name: "Big", Description: strPtr("b"), Members: members("m", 130), Repos: members("r", 105)}}, many...),
			users: emails(append(members("m", 130), "alice")...)},
		{name: "the token from GITHUB_TOKEN", org: "acme", owner: "acme", token: "env:tok", teams: two, users: emails("alice", "bob", "carol"), wantRows: 2},
		{name: "--auth wins over GITHUB_TOKEN", org: "acme", owner: "acme", token: "tok", envToken: "not-the-token", teams: two, users: emails("alice", "bob", "carol"), wantRows: 2},
		{name: "the GITHUB_URL spelling of the base URL", org: "acme", owner: "acme", token: "tok", baseEnv: "GITHUB_URL", teams: two, users: emails("alice", "bob", "carol"), wantRows: 2},
		{name: "a rejected token", org: "acme", owner: "acme", token: "wrong", serverToken: "tok", teams: two, users: emails("alice", "bob", "carol"), wantExit: 1, wantRows: 0},
		{name: "no teams is an error", org: "acme", owner: "acme", token: "tok", teams: nil, wantExit: 1, wantRows: 0},
		{name: "no teams with --allow-empty", org: "acme", owner: "acme", token: "tok", teams: nil, extra: []string{"--allow-empty"}, wantExit: 0, wantRows: 0},
		{name: "unknown organization", org: "acme", owner: "nope", token: "tok", teams: two, users: emails("alice", "bob", "carol"), wantExit: 1, wantRows: 0},
		{name: "a member roster that cannot be read stops the run", org: "acme", owner: "acme", token: "tok", teams: two, users: emails("alice", "bob", "carol"),
			failPath: map[string]int{"/orgs/acme/teams/platform/members": 500, "/organizations/1/team/11/members": 500}, wantExit: 1, wantRows: 0},
		{name: "an admin override survives", org: "acme", owner: "acme", token: "tok", teams: two, users: emails("alice", "bob", "carol"), wantRows: 2,
			prepare: func(o *teamsOracle, fake *fakeGitHub) {
				for _, database := range []string{o.pythonDatabase, o.goDatabase} {
					if err := o.admin.Exec(o.ctx, fmt.Sprintf(`INSERT INTO %s.teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider) VALUES ('gh:platform', generateUUIDv4(), 'Platform', 'old', ['old'], ['override@example.com'], [], [], 1, now64(6) - INTERVAL 1 DAY, now64(6) - INTERVAL 1 DAY, 'org-1', 'github')`, database)); err != nil {
						o.t.Fatal(err)
					}
				}
			},
			after: func(t *testing.T, python, goRows []map[string]string) {
				for name, rows := range map[string][]map[string]string{"python": python, "go": goRows} {
					for _, row := range rows {
						want := "[]"
						if row["id"] == "gh:platform" {
							want = "['override@example.com']"
						}
						if row["manual_members"] != want {
							t.Errorf("%s: %s manual_members = %s, want %s (an admin override must survive a sync)", name, row["id"], row["manual_members"], want)
						}
					}
				}
			}},
	}
}

// TestSyncTeamsGitHubVenueOracleMatchesThePythonProducer runs `dev-hops sync teams --provider github` (the real verb, PyGithub
// pointed at a fake) and `dho sync teams --provider github` (the Go team catalog) over one fake GitHub and
// compares exit codes and, column by column, the `teams` rows each wrote. The rows differ by design in the
// columns teamsRules names; everything else must be equal.
func TestSyncTeamsGitHubVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	o := newTeamsOracle(t)
	fake := newFakeGitHub(t)
	for _, sc := range githubScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			o.t = t
			o.compareGitHub(sc, fake)
		})
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}
