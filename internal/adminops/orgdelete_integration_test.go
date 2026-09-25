//go:build integration

package adminops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// `admin orgs delete` is compared with the real Python verb on real PostgreSQL
// and ClickHouse: per scenario, the exit code, the printed plan and the rows
// each organization has left in every table.

const (
	targetOrg  = "aaaaaaaa-0000-4000-8000-00000000000a"
	controlOrg = "bbbbbbbb-0000-4000-8000-00000000000b"
)

type deleteScenario struct {
	name string
	args []string
	// clickhouse points the verb at the ClickHouse container.
	clickhouse bool
	// pagerduty seeds a PagerDuty OAuth credential for the target org.
	pagerduty bool
}

var deleteScenarios = []deleteScenario{
	{name: "dry run, no ClickHouse", args: []string{"orgs", "delete", "--org-id", targetOrg, "--dry-run"}},
	{name: "real delete, no ClickHouse", args: []string{"orgs", "delete", "--org-id", targetOrg}},
	{name: "unknown organization", args: []string{"orgs", "delete", "--org-id", "cccccccc-0000-4000-8000-00000000000c"}},
	{name: "id in braces and capitals", args: []string{"orgs", "delete", "--org-id", "{AAAAAAAA-0000-4000-8000-00000000000A}", "--dry-run"}},
	{name: "id without hyphens", args: []string{"orgs", "delete", "--org-id", strings.ReplaceAll(targetOrg, "-", "")}},
	{name: "malformed id", args: []string{"orgs", "delete", "--org-id", "not-a-uuid"}},
	{name: "no id", args: []string{"orgs", "delete"}},
	{name: "PagerDuty grant without configuration", args: []string{"orgs", "delete", "--org-id", targetOrg}, pagerduty: true},
	{name: "PagerDuty grant, dry run", args: []string{"orgs", "delete", "--org-id", targetOrg, "--dry-run"}, pagerduty: true},
	{name: "dry run, ClickHouse", args: []string{"orgs", "delete", "--org-id", targetOrg, "--dry-run"}, clickhouse: true},
	{name: "real delete, ClickHouse", args: []string{"orgs", "delete", "--org-id", targetOrg}, clickhouse: true},
	{name: "unknown organization, ClickHouse", args: []string{"orgs", "delete", "--org-id", "cccccccc-0000-4000-8000-00000000000c"}, clickhouse: true},
}

type deleteResult struct {
	Name   string `json:"name"`
	Exit   int    `json:"exit"`
	Stdout string `json:"stdout"`
	State  string `json:"state"`
}

var timestampLine = regexp.MustCompile(`"timestamp": "[^"]*"`)

// staleWarning is org_deletion.py's own migration-history staleness (five tables
// its migration-file regex still names): the accepted, separately proven
// divergence of the deletion route (TestClickHouseOrgTableDiscoveryMatchesThePythonMigrationRegex).
var staleWarning = regexp.MustCompile(`^ClickHouse table (ci_daily_rollup|commit_daily_rollup|deployment_daily_rollup|ai_attribution_new|statement) missing or has no org_id column; skipped\.$`)

// normalize masks the timestamp and, for a ClickHouse scenario, removes the
// five stale-table warnings from a parsed plan before it is written back in
// Python's layout.
func normalize(t *testing.T, stdout string, clickhouse bool) string {
	t.Helper()
	stdout = timestampLine.ReplaceAllString(stdout, `"timestamp": "<time>"`)
	if !clickhouse || !strings.HasPrefix(stdout, "{") {
		return stdout
	}
	var plan map[string]any
	if err := json.Unmarshal([]byte(stdout), &plan); err != nil {
		t.Fatalf("plan is not JSON: %v\n%s", err, stdout)
	}
	var kept []any
	for _, item := range plan["warnings"].([]any) {
		if !staleWarning.MatchString(item.(string)) {
			kept = append(kept, item)
		}
	}
	if kept == nil {
		kept = []any{}
	}
	plan["warnings"] = kept
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(plan); err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(out.String(), "\n") + "\n"
}

// pgSeed makes the target and the control organization the same rows.
func (db *database) seedOrganizations(t *testing.T, pagerduty bool) {
	t.Helper()
	if _, err := db.conn.Exec(context.Background(), "SET session_replication_role = replica"); err != nil {
		t.Fatal(err)
	}
	db.reset(t)
	if _, err := db.conn.Exec(context.Background(), "TRUNCATE scheduled_jobs, job_runs, settings, integration_credentials, sso_providers, dev_conversations, provider_oauth_credentials, provider_oauth_revocations CASCADE"); err != nil {
		t.Fatal(err)
	}
	for index, org := range []string{targetOrg, controlOrg} {
		db.insertRow(t, "organizations", map[string]string{"id": quote(org) + "::uuid", "slug": quote(fmt.Sprintf("org-%d", index)), "name": quote("Org"), "tier": quote("community"), "managed_by": quote("stripe"), "is_active": "true"})
		for round := 0; round < 2; round++ {
			job := uuid.NewString()
			db.insertRow(t, "scheduled_jobs", map[string]string{"id": quote(job) + "::uuid", "org_id": quote(org), "name": quote(job), "status": "0", "is_running": "true", "next_run_at": "now()"})
			db.insertRow(t, "job_runs", map[string]string{"job_id": quote(job) + "::uuid"})
		}
		db.insertRow(t, "settings", map[string]string{"org_id": quote(org), "is_encrypted": "true"})
		db.insertRow(t, "integration_credentials", map[string]string{"org_id": quote(org)})
		db.insertRow(t, "sso_providers", map[string]string{"org_id": quote(org) + "::uuid", "encrypted_secrets": quote(`{"k": "v"}`)})
		db.insertRow(t, "memberships", map[string]string{"org_id": quote(org) + "::uuid"})
		db.insertRow(t, "dev_conversations", map[string]string{"org_id": quote(org) + "::uuid"})
	}
	if pagerduty {
		db.insertRow(t, "provider_oauth_credentials", map[string]string{"org_id": quote(targetOrg), "provider": quote("pagerduty"), "token_encrypted": quote("not-a-token")})
	}
}

// insertRow is insert() of the users test's sibling helper: the named columns,
// every other NOT NULL column without a default filled neutrally.
func (db *database) insertRow(t *testing.T, table string, values map[string]string) {
	t.Helper()
	ctx := context.Background()
	rows, err := db.conn.Query(ctx, `SELECT column_name, data_type, is_nullable = 'YES', column_default IS NOT NULL, is_identity = 'YES' OR is_generated = 'ALWAYS'
FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position`, table)
	if err != nil {
		t.Fatal(err)
	}
	var names, expressions []string
	seen := map[string]bool{}
	for rows.Next() {
		var name, dataType string
		var nullable, hasDefault, generated bool
		if err := rows.Scan(&name, &dataType, &nullable, &hasDefault, &generated); err != nil {
			t.Fatal(err)
		}
		seen[name] = true
		if generated {
			continue
		}
		if expression, ok := values[name]; ok {
			names, expressions = append(names, name), append(expressions, expression)
		} else if !nullable && !hasDefault {
			db.seq++
			names = append(names, name)
			expressions = append(expressions, neutral(dataType, fmt.Sprintf("00000000-0000-4000-8000-%012d", db.seq)))
		}
	}
	rows.Close()
	for name := range values {
		if !seen[name] {
			t.Fatalf("%s has no column %s", table, name)
		}
	}
	statement := fmt.Sprintf("INSERT INTO public.%s (%s) VALUES (%s)", table, strings.Join(names, ", "), strings.Join(expressions, ", "))
	if _, err := db.conn.Exec(ctx, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

func neutral(dataType, id string) string {
	switch dataType {
	case "uuid":
		return "'" + id + "'::uuid"
	case "text", "character varying", "character":
		return "'x'"
	case "timestamp with time zone", "timestamp without time zone":
		return "'2000-01-01 00:00:00+00'"
	case "integer", "bigint", "smallint", "numeric", "double precision", "real":
		return "0"
	case "boolean":
		return "false"
	case "json", "jsonb", "ARRAY":
		return "'{}'"
	case "date":
		return "'2000-01-01'"
	}
	return "NULL"
}

// orgRowState is, for every public table with an org column, the rows each
// organization has, plus the scheduled jobs' own state.
func (db *database) orgRowState(t *testing.T) map[string]any {
	t.Helper()
	ctx := context.Background()
	rows, err := db.conn.Query(ctx, `SELECT table_name, column_name FROM information_schema.columns
WHERE table_schema = 'public' AND column_name IN ('org_id', 'target_org_id') AND table_name IN (SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE') ORDER BY 1, 2`)
	if err != nil {
		t.Fatal(err)
	}
	type tc struct{ table, column string }
	var columns []tc
	for rows.Next() {
		var c tc
		if err := rows.Scan(&c.table, &c.column); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, c)
	}
	rows.Close()
	state := map[string]any{}
	for _, c := range columns {
		counts, err := db.conn.Query(ctx, fmt.Sprintf("SELECT %s::text, count(*) FROM %s GROUP BY 1 ORDER BY 1", c.column, c.table))
		if err != nil {
			t.Fatal(err)
		}
		perOrg := map[string]int64{}
		for counts.Next() {
			var org *string
			var n int64
			if err := counts.Scan(&org, &n); err != nil {
				t.Fatal(err)
			}
			key := "<null>"
			if org != nil {
				key = *org
			}
			perOrg[key] = n
		}
		counts.Close()
		state[c.table+"."+c.column] = perOrg
	}
	var organizations []string
	orgRows, err := db.conn.Query(ctx, "SELECT id::text FROM organizations ORDER BY 1")
	if err != nil {
		t.Fatal(err)
	}
	for orgRows.Next() {
		var id string
		_ = orgRows.Scan(&id)
		organizations = append(organizations, id)
	}
	orgRows.Close()
	state["organizations"] = organizations
	var jobRuns int64
	_ = db.conn.QueryRow(ctx, "SELECT count(*) FROM job_runs").Scan(&jobRuns)
	state["job_runs"] = jobRuns
	jobs, err := db.conn.Query(ctx, "SELECT org_id, status, is_running, next_run_at IS NULL FROM scheduled_jobs ORDER BY org_id, id")
	if err != nil {
		t.Fatal(err)
	}
	var jobStates []string
	for jobs.Next() {
		var org string
		var status int
		var running, noNext bool
		_ = jobs.Scan(&org, &status, &running, &noNext)
		jobStates = append(jobStates, fmt.Sprintf("%s/%d/%v/%v", org, status, running, noNext))
	}
	jobs.Close()
	state["scheduled_jobs"] = jobStates
	return state
}

func TestOrgsDeleteCommandLineRefusals(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runOrgsDelete(context.Background(), cli.Env{Args: nil, Stdout: &stdout, Stderr: &stderr, Lookup: func(string) (string, bool) { return "", false }})
	if code != cli.ExitUsage || stdout.Len() != 0 {
		t.Fatalf("no --org-id: exit %d, stdout %q", code, stdout.String())
	}
}

func runDeleteScenarios(t *testing.T, run func(*testing.T, *database, *chFixture, deleteScenario) deleteResult) []deleteResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	t.Cleanup(cancel)
	db := startDatabase(t)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	httpURI, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	fixture := &chFixture{nativeURI: instance.URI, httpURI: httpURI, exec: func(sql string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("clickhouse: %v\n%s", err, sql)
		}
	}, counts: func() map[string]map[string]uint64 {
		t.Helper()
		tables, err := admin.DiscoverClickHouseOrgTables(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]map[string]uint64{}
		for _, table := range tables {
			condition := "toString(org_id)"
			rows, err := conn.Query(ctx, fmt.Sprintf("SELECT %s, count() FROM `%s` GROUP BY 1 ORDER BY 1", condition, table.Name))
			if err != nil {
				t.Fatal(err)
			}
			perOrg := map[string]uint64{}
			for rows.Next() {
				var org string
				var n uint64
				if err := rows.Scan(&org, &n); err != nil {
					t.Fatal(err)
				}
				perOrg[org] = n
			}
			rows.Close()
			if len(perOrg) > 0 {
				out[table.Name] = perOrg
			}
		}
		return out
	}}
	var results []deleteResult
	for _, scenario := range deleteScenarios {
		db.seedOrganizations(t, scenario.pagerduty)
		fixture.reseed()
		results = append(results, run(t, db, fixture, scenario))
	}
	return results
}

type chFixture struct {
	nativeURI, httpURI string
	exec               func(string, ...any)
	counts             func() map[string]map[string]uint64
}

func (f *chFixture) reseed() {
	f.exec("TRUNCATE TABLE git_blame")
	f.exec("TRUNCATE TABLE git_blame_dirty_paths")
	f.exec("TRUNCATE TABLE backfill_log")
	for _, org := range []string{targetOrg, controlOrg} {
		f.exec(`INSERT INTO git_blame (org_id, repo_id, path, line_no, author_email, last_synced) VALUES (?, ?, 'main.go', 1, 'a@example.com', now64(3,'UTC'))`, org, uuid.New())
		f.exec(`INSERT INTO backfill_log (job_id, org_id, chunk_index, chunk_since, chunk_before, provider, items_synced, duration_ms, status) VALUES (?, ?, 0, today(), today(), 'github', 1, 1, 'complete')`, uuid.NewString(), org)
	}
}

func snapshot(t *testing.T, db *database, fixture *chFixture, scenario deleteScenario) string {
	t.Helper()
	state := map[string]any{"postgres": db.orgRowState(t)}
	if scenario.clickhouse {
		state["clickhouse"] = fixture.counts()
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func deleteEnv(fixture *chFixture, scenario deleteScenario, python bool) map[string]string {
	env := map[string]string{}
	if scenario.clickhouse {
		env["CLICKHOUSE_URI"] = fixture.nativeURI
		if python {
			env["CLICKHOUSE_URI"] = fixture.httpURI
		}
	}
	return env
}

func goDelete(t *testing.T, db *database, fixture *chFixture, scenario deleteScenario) deleteResult {
	code, stdout := goVerbEnv(t, db, deleteEnv(fixture, scenario, false), scenario.args)
	return deleteResult{Name: scenario.name, Exit: code, Stdout: normalize(t, stdout, scenario.clickhouse), State: snapshot(t, db, fixture, scenario)}
}

func pythonDelete(t *testing.T, db *database, fixture *chFixture, scenario deleteScenario) deleteResult {
	code, stdout := pythonVerbEnv(t, db, deleteEnv(fixture, scenario, true), scenario.args)
	return deleteResult{Name: scenario.name, Exit: code, Stdout: normalize(t, stdout, scenario.clickhouse), State: snapshot(t, db, fixture, scenario)}
}

const deleteGolden = "testdata/orgdelete_golden.json"

// deleteGoldenSHA256 pins testdata/orgdelete_golden.json (R24): what the real
// `dev-hops admin orgs delete` printed and left for every scenario. The producer
// is deleted with the Python CLI, so this is a rot guard: the file is only
// rewritten by TestOrgsDeleteVenueOracleMatchesThePythonProducer with
// DHO_ORGDELETE_GOLDEN_UPDATE=1, then this digest is updated.
const deleteGoldenSHA256 = "6a6ebbfe47ef3219c2ef4809b7436d9f1fc29051346598cbf91e5f5c1dce002d"

func TestOrgsDeleteGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(deleteGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != deleteGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", deleteGolden, got, deleteGoldenSHA256)
	}
}

func compareDeletes(t *testing.T, got, want []deleteResult, wantName string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d scenarios, %s has %d", len(got), wantName, len(want))
	}
	for index := range got {
		g, w := got[index], want[index]
		if g.Exit != w.Exit {
			t.Errorf("%s: exit %d, %s exit %d", g.Name, g.Exit, wantName, w.Exit)
		}
		if g.Stdout != w.Stdout {
			t.Errorf("%s: stdout\n%s\n%s stdout\n%s", g.Name, g.Stdout, wantName, w.Stdout)
		}
		if g.State != w.State {
			t.Errorf("%s: rows\n%s\n%s rows\n%s", g.Name, g.State, wantName, w.State)
		}
	}
}

// TestOrgsDeleteMatchesTheFrozenPythonOutput runs every scenario and compares it
// with what the real Python verb did (frozen; no Python needed).
func TestOrgsDeleteMatchesTheFrozenPythonOutput(t *testing.T) {
	raw, err := os.ReadFile(deleteGolden)
	if err != nil {
		t.Fatal(err)
	}
	var frozen []deleteResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	got := runDeleteScenarios(t, goDelete)
	compareDeletes(t, got, frozen, "frozen Python")
	// A golden in which nothing was deleted passes for any implementation.
	deleted := 0
	for _, item := range frozen {
		if item.Exit == 0 && strings.Contains(item.Stdout, `"dry_run": false`) && !strings.Contains(item.Stdout, `"total": 0,`) {
			deleted++
		}
	}
	if deleted < 2 {
		t.Fatalf("the golden has %d real deletions that removed rows: it measures too little", deleted)
	}
}

// TestOrgsDeleteVenueOracleMatchesThePythonProducer runs every scenario through
// the real Python verb and through dho. With DHO_ORGDELETE_GOLDEN_UPDATE=1 it
// rewrites the frozen file.
func TestOrgsDeleteVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	py := runDeleteScenarios(t, pythonDelete)
	got := runDeleteScenarios(t, goDelete)
	compareDeletes(t, got, py, "python")
	if os.Getenv("DHO_ORGDELETE_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(py, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(deleteGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}

func quote(text string) string { return "'" + strings.ReplaceAll(text, "'", "''") + "'" }
