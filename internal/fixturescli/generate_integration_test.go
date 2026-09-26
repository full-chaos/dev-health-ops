//go:build integration

package fixturescli

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	worldFreezeEnv   = "DHO_GENERATE_FREEZE"
	worldProducerEnv = "DHO_GENERATE_PRODUCER"
)

// generateParameterSets are the `fixtures generate` runs the callers make (CHAOS-6468): the ops CI
// scripts, the web and acr end-to-end suites. Each is frozen for the organization it is frozen with;
// the verb rewrites the organization.
var generateParameterSets = []struct {
	Org    string
	Params GenerateParams
}{
	{"11111111-2222-4333-8444-555555555555", GenerateParams{
		Provider: "synthetic", RepoName: "acme/live-e2e", RepoCount: 1, Days: 14, CommitsPerDay: 6, PRCount: 24, TeamCount: 10,
		Seed: 20260219, WithMetrics: true, WithWorkGraph: true,
	}},
}

// pythonGenerate runs the real `dev-hops fixtures generate` against the ClickHouse at dsn as a caller
// does, with no PostgreSQL configured, and returns the instant it started.
func pythonGenerate(t *testing.T, dsn, org string, p GenerateParams) time.Time {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	args := []string{"-c", program, "fixtures", "generate", "--sink", dsn, "--db-type", "clickhouse", "--repo-name", p.RepoName,
		"--repo-count", fmt.Sprint(p.RepoCount), "--provider", p.Provider, "--days", fmt.Sprint(p.Days),
		"--commits-per-day", fmt.Sprint(p.CommitsPerDay), "--pr-count", fmt.Sprint(p.PRCount),
		"--team-count", fmt.Sprint(p.TeamCount), "--seed", fmt.Sprint(p.Seed)}
	if p.WithMetrics {
		args = append(args, "--with-metrics")
	}
	if p.WithWorkGraph {
		args = append(args, "--with-work-graph")
	}
	command := exec.Command(python, args...)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "ORG_ID="+org, "OTEL_ENABLED=false")
	// No PostgreSQL: the analytics rows only.
	command.Env = append(command.Env, "DATABASE_URI=", "POSTGRES_URI=", "DATABASE_URL=")
	started := time.Now().UTC()
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("the Python fixtures generate failed: %v", pyoracle.RunError(python, err, output))
	}
	return started
}

// worldColumns reads a table's columns in order. The server must run in UTC, so a DateTime without
// a zone is UTC too; an alias or ephemeral column would not round-trip and is refused, a materialized one is recomputed by the insert.
func worldColumns(t *testing.T, dsn, table string) []FrozenColumn {
	t.Helper()
	body := clickHouseHTTP(t, dsn, "SELECT name, type, default_kind FROM system.columns WHERE database = currentDatabase() AND table = '"+table+"' ORDER BY position FORMAT JSONCompactEachRow")
	var columns []FrozenColumn
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		var row []string
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("columns of %s: %v", table, err)
		}
		// A MATERIALIZED column is computed from the others when a row is inserted: SELECT * leaves it
		// out and so does the INSERT. An alias or ephemeral column has no stored value to carry.
		if row[2] == "MATERIALIZED" {
			continue
		}
		if row[2] == "ALIAS" || row[2] == "EPHEMERAL" {
			t.Fatalf("%s.%s is a %s column; SELECT * would not carry it", table, row[0], row[2])
		}
		if _, _, err := shiftLayout(row[1]); err != nil {
			t.Fatalf("%s.%s: %v", table, row[0], err)
		}
		columns = append(columns, FrozenColumn{Name: row[0], Type: row[1]})
	}
	return columns
}

func dumpWorldTable(t *testing.T, dsn, table string) FrozenTable {
	t.Helper()
	frozen := FrozenTable{Name: table, Columns: worldColumns(t, dsn, table)}
	body := strings.TrimSpace(clickHouseHTTP(t, dsn, "SELECT * FROM `"+table+"` FORMAT JSONCompactEachRow"))
	if body == "" {
		return frozen
	}
	lines := strings.Split(body, "\n")
	sort.Strings(lines)
	for _, line := range lines {
		var row []any
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&row); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		frozen.Rows = append(frozen.Rows, row)
	}
	return frozen
}

// baseTables lists every table that is not a view.
func baseTables(t *testing.T, dsn string) []string {
	t.Helper()
	list := clickHouseHTTP(t, dsn, "SELECT name FROM system.tables WHERE database = currentDatabase() AND engine NOT LIKE '%View' ORDER BY name FORMAT TSV")
	return strings.Fields(list)
}

var viewTarget = regexp.MustCompile("(?s)MATERIALIZED VIEW [^\\n]* TO (?:[`\\w]+\\.)?`?(\\w+)`?")

// derivedTables are the tables a materialized view writes into.
func derivedTables(t *testing.T, dsn string) map[string]bool {
	t.Helper()
	derived := map[string]bool{}
	body := clickHouseHTTP(t, dsn, "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND engine = 'MaterializedView' FORMAT JSONCompactEachRow")
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if line == "" {
			continue
		}
		var row []string
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		if match := viewTarget.FindStringSubmatch(row[0]); match != nil {
			derived[match[1]] = true
		}
	}
	return derived
}

// stopMerges keeps ClickHouse from merging parts in the background: the tables of a world are
// ReplacingMergeTree tables with several versions of a row, and how many of them a raw SELECT sees
// depends on which merges have run. With merges stopped, a database holds exactly the rows inserted
// into it, in the capture and after a load alike.
func stopMerges(t *testing.T, dsn string) {
	t.Helper()
	clickHouseHTTP(t, dsn, "SYSTEM STOP MERGES")
}

func requireUTCServer(t *testing.T, dsn string) {
	t.Helper()
	if zone := strings.TrimSpace(clickHouseHTTP(t, dsn, "SELECT timezone() FORMAT TSV")); zone != "UTC" {
		t.Fatalf("the server runs in %s: a column without a zone is only known to be UTC when the server is", zone)
	}
}

// TestFreezeGenerateWorlds writes the frozen worlds from the real Python producer. It is not a check:
// it runs only with DHO_GENERATE_FREEZE=1, needs the full project Python environment, and rewrites
// testdata/generate.
func TestFreezeGenerateWorlds(t *testing.T) {
	if os.Getenv(worldFreezeEnv) != "1" {
		t.Skip("set " + worldFreezeEnv + "=1 (and " + worldProducerEnv + "=<commit>) to re-freeze the generate worlds from the live Python producer")
	}
	producer := os.Getenv(worldProducerEnv)
	if len(producer) != 40 {
		t.Fatalf("%s must name the commit whose Python generator runs", worldProducerEnv)
	}
	if err := os.MkdirAll("testdata/generate", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, set := range generateParameterSets {
		ch := startClickHouse(t)
		requireUTCServer(t, ch.httpDSN)
		stopMerges(t, ch.httpDSN)
		before := rowCounts(t, ch.httpDSN)
		startedAt := pythonGenerate(t, ch.httpDSN, set.Org, set.Params)
		after := rowCounts(t, ch.httpDSN)
		derived := derivedTables(t, ch.httpDSN)
		world := FrozenWorld{Producer: producer, FrozenAt: startedAt.Format(time.RFC3339Nano), OrgID: set.Org, Params: set.Params}
		orgLeaks := 0
		for _, name := range baseTables(t, ch.httpDSN) {
			if before[name] == after[name] {
				continue
			}
			dumped := dumpWorldTable(t, ch.httpDSN, name)
			if len(dumped.Rows) == 0 {
				continue
			}
			// The organization is replaced by value in String and UUID columns only: every other
			// place it appears would keep the frozen organization.
			for _, row := range dumped.Rows {
				for column, value := range row {
					text, ok := value.(string)
					if ok && strings.Contains(text, set.Org) && (!holdsOrg(dumped.Columns[column].Type) || text != set.Org) {
						orgLeaks++
					}
				}
			}
			world.Tables = append(world.Tables, WorldTable{FrozenTable: dumped, Derived: derived[name]})
		}
		if orgLeaks > 0 {
			t.Fatalf("the organization appears in %d value(s) that are not a whole String/UUID column value: substituting it would leave it behind", orgLeaks)
		}
		if len(world.Tables) == 0 {
			t.Fatal("the run wrote no rows")
		}
		var buffer bytes.Buffer
		writer, err := gzip.NewWriterLevel(&buffer, gzip.BestCompression)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(writer).Encode(world); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(WorldFile(set.Params), buffer.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("froze %s: %d table(s), %d bytes", set.Params, len(world.Tables), buffer.Len())
	}
}

func openNative(t *testing.T, ch clickHouse) (func(context.Context, FrozenWorld, string, time.Time) (map[string]int, error), func()) {
	t.Helper()
	conn, err := chstorage.Open(context.Background(), chstorage.DefaultConfig(ch.instance.URI))
	if err != nil {
		t.Fatalf("open the native client: %v", err)
	}
	return func(ctx context.Context, world FrozenWorld, org string, now time.Time) (map[string]int, error) {
		return LoadWorld(ctx, conn, world, org, now)
	}, func() { _ = conn.Close() }
}

// distinctRows is the rows without repeats.
func distinctRows(rows [][]any) [][]any {
	seen := map[string]bool{}
	var out [][]any
	for _, row := range rows {
		raw, _ := json.Marshal(row)
		if !seen[string(raw)] {
			seen[string(raw)] = true
			out = append(out, row)
		}
	}
	return out
}

// canonicalRows is the rows as sorted JSON text.
func canonicalRows(rows [][]any) []string {
	out := make([]string, len(rows))
	for index, row := range rows {
		raw, _ := json.Marshal(row)
		out[index] = string(raw)
	}
	sort.Strings(out)
	return out
}

// Loading a frozen world into a real ClickHouse at the migration head reproduces the capture: on the
// day it was frozen and for its own organization, every table (the ones a materialized view fills
// included) holds exactly the frozen rows; moved by whole days and given another organization, every
// table holds the frozen rows moved and rewritten, and nothing else changed.
func TestLoadWorldReproducesTheRecordedCapture(t *testing.T) {
	for _, set := range generateParameterSets {
		world, err := LoadFrozenWorld(set.Params)
		if err != nil {
			t.Fatal(err)
		}
		frozenAt, err := time.Parse(time.RFC3339Nano, world.FrozenAt)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(set.Params.RepoName, func(t *testing.T) {
			ch := startClickHouse(t)
			stopMerges(t, ch.httpDSN)
			load, closeConn := openNative(t, ch)
			defer closeConn()
			names := map[string]bool{}
			for _, table := range world.Tables {
				names[table.Name] = true
			}
			before := rowCounts(t, ch.httpDSN)
			counts, err := load(context.Background(), world, world.OrgID, frozenAt)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			for _, table := range world.Tables {
				if table.Derived {
					if _, inserted := counts[table.Name]; inserted {
						t.Fatalf("the derived table %s was inserted into", table.Name)
					}
				} else if counts[table.Name] != len(table.Rows) {
					t.Fatalf("%s: the loader reported %d row(s), frozen %d", table.Name, counts[table.Name], len(table.Rows))
				}
				got := dumpWorldTable(t, ch.httpDSN, table.Name)
				if !reflect.DeepEqual(got.Columns, table.Columns) {
					t.Fatalf("%s: the schema's columns differ from the frozen ones", table.Name)
				}
				// A materialized view stamps its rows with the clock of the insert and writes one row per
				// insert block (git_blame_dirty_paths marks a path dirty each time rows for it arrive):
				// the rows a view fills agree as a set, ignoring their timestamps; everything else agrees
				// exactly.
				wantRows, gotRows := table.Rows, got.Rows
				if table.Derived {
					wantRows, gotRows = distinctRows(maskTimes(table.FrozenTable)), distinctRows(maskTimes(got))
				}
				if diff := rowsDiff(wantRows, gotRows); diff != "" {
					t.Fatalf("%s: the loaded rows differ from the capture:\n%s", table.Name, diff)
				}
			}
			for table, count := range rowCounts(t, ch.httpDSN) {
				if !names[table] && count != before[table] {
					t.Fatalf("the load changed %s (%s -> %s), which the world does not write", table, before[table], count)
				}
			}

			// Another day, another organization: the same rows, moved and rewritten.
			other := startClickHouse(t)
			stopMerges(t, other.httpDSN)
			loadOther, closeOther := openNative(t, other)
			defer closeOther()
			org := "99999999-8888-4777-8666-555555555555"
			now := frozenAt.Add(9*24*time.Hour + 2*time.Minute)
			if _, err := loadOther(context.Background(), world, org, now); err != nil {
				t.Fatalf("load: %v", err)
			}
			days, err := world.WholeDays(now)
			if err != nil || days != 9 {
				t.Fatalf("whole days = %d, %v; want 9", days, err)
			}
			for _, table := range world.Tables {
				got := dumpWorldTable(t, other.httpDSN, table.Name)
				back, err := WorldTable{FrozenTable: got}.Transform(-days, org, world.OrgID)
				if err != nil {
					t.Fatal(err)
				}
				wantRows, backRows := table.Rows, back
				if table.Derived {
					wantRows = distinctRows(maskTimes(table.FrozenTable))
					backRows = distinctRows(maskTimes(FrozenTable{Name: table.Name, Columns: table.Columns, Rows: back}))
				}
				if diff := rowsDiff(wantRows, backRows); diff != "" {
					t.Fatalf("%s: the moved and rewritten rows are not the capture moved back:\n%s", table.Name, diff)
				}
				// And no row kept the frozen organization.
				for _, row := range got.Rows {
					for _, value := range row {
						if text, ok := value.(string); ok && text == world.OrgID {
							t.Fatalf("%s: a row kept the frozen organization", table.Name)
						}
					}
				}
			}
		})
	}
}

// The differential guard against the REAL producer while it exists: a fresh run of the Python verb
// for each frozen parameter set writes the same tables, with the same columns and the same number of
// rows in each, as the world holds (the ids and timestamps of two runs never agree, the shape does).
// A change of the generator that would make the frozen worlds stale turns this red. It needs the full
// project Python environment; CI runs it in the venue-oracles job.
func TestGenerateVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	compared := 0
	for _, set := range generateParameterSets {
		world, err := LoadFrozenWorld(set.Params)
		if err != nil {
			t.Fatal(err)
		}
		ch := startClickHouse(t)
		stopMerges(t, ch.httpDSN)
		before := rowCounts(t, ch.httpDSN)
		pythonGenerate(t, ch.httpDSN, set.Org, set.Params)
		after := rowCounts(t, ch.httpDSN)
		changed := map[string]int{}
		for name, count := range after {
			if before[name] != count {
				var n int
				fmt.Sscan(count, &n)
				changed[name] = n
			}
		}
		frozen := map[string]int{}
		for _, table := range world.Tables {
			frozen[table.Name] = len(table.Rows)
			columns := worldColumns(t, ch.httpDSN, table.Name)
			if !reflect.DeepEqual(columns, table.Columns) {
				t.Fatalf("%s: the schema's columns differ from the frozen ones", table.Name)
			}
		}
		if !reflect.DeepEqual(changed, frozen) {
			t.Fatalf("%s: the live Python producer wrote %v, the frozen world holds %v: the world is stale; re-freeze it (TestFreezeGenerateWorlds)", set.Params, changed, frozen)
		}
		for _, count := range frozen {
			compared += count
		}
	}
	if compared == 0 {
		t.Fatal("the comparison measured nothing")
	}
	t.Logf("compared the shape of %d row(s) across %d parameter set(s)", compared, len(generateParameterSets))
	venueoracle.WriteProof(t)
}

// runGenerateVerb runs `dho fixtures generate` as a caller does.
func runGenerateVerb(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr strings.Builder
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	code := runGenerate(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	return code, stdout.String(), stderr.String()
}

func acrArgs(sink string, extra ...string) []string {
	return append([]string{"--sink", sink, "--db-type", "clickhouse", "--repo-name", "acme/live-e2e", "--provider", "synthetic",
		"--days", "14", "--commits-per-day", "6", "--pr-count", "24", "--seed", "20260219", "--with-metrics", "--with-work-graph"}, extra...)
}

// The verb, as the acr end-to-end run calls it: the frozen world lands in ClickHouse for the
// caller's organization, moved so it ends today, on the native protocol and over HTTP alike; an
// organization that already holds synced data is refused and written to not at all.
func TestGenerateVerbLoadsTheWorldForTheCallersOrganization(t *testing.T) {
	set := generateParameterSets[0]
	world, err := LoadFrozenWorld(set.Params)
	if err != nil {
		t.Fatal(err)
	}
	const org = "99999999-8888-4777-8666-555555555555"
	httpURI := func(ch clickHouse) string {
		parsed, err := url.Parse(ch.httpDSN)
		if err != nil {
			t.Fatal(err)
		}
		parsed.Scheme = "http"
		return parsed.String()
	}
	for name, sink := range map[string]func(clickHouse) string{
		"native": func(ch clickHouse) string { return ch.instance.URI },
		"http":   httpURI,
	} {
		t.Run(name, func(t *testing.T) {
			ch := startClickHouse(t)
			stopMerges(t, ch.httpDSN)
			env := map[string]string{"ORG_ID": org}
			code, stdout, stderr := runGenerateVerb(t, env, acrArgs(sink(ch))...)
			if code != cli.ExitOK {
				t.Fatalf("exit %d: %s", code, stderr)
			}
			var result struct {
				OrgID string         `json:"org_id"`
				Rows  map[string]int `json:"rows"`
			}
			if err := json.Unmarshal([]byte(stdout), &result); err != nil || result.OrgID != org {
				t.Fatalf("stdout %q: %v", stdout, err)
			}
			total := 0
			for _, table := range world.Tables {
				if table.Derived {
					continue
				}
				if result.Rows[table.Name] != len(table.Rows) {
					t.Fatalf("%s: reported %d row(s), the world holds %d", table.Name, result.Rows[table.Name], len(table.Rows))
				}
				total += len(table.Rows)
			}
			if total < 20000 {
				t.Fatalf("the verb loaded %d row(s): the run measures too little", total)
			}
			// Every organization column holds the caller's, none the frozen one.
			for _, probe := range []string{"git_commits", "work_items", "repos", "teams", "dora_metrics_daily"} {
				body := clickHouseHTTP(t, ch.httpDSN, "SELECT groupUniqArray(org_id) FROM "+probe+" FORMAT JSONCompactEachRow")
				if strings.TrimSpace(body) != `[["`+org+`"]]` {
					t.Fatalf("%s holds the organizations %s, want only %s", probe, strings.TrimSpace(body), org)
				}
			}
			// The window ends today (a run across UTC midnight may end yesterday).
			last := strings.TrimSpace(clickHouseHTTP(t, ch.httpDSN, "SELECT toString(max(day)) FROM work_graph_edges FORMAT TSV"))
			now := time.Now().UTC()
			if last != now.Format("2006-01-02") && last != now.Add(-24*time.Hour).Format("2006-01-02") {
				t.Fatalf("the last day of the loaded world is %s, want today (%s)", last, now.Format("2006-01-02"))
			}

			// An organization holding synced data is refused, and nothing is written for it.
			other := "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
			clickHouseHTTP(t, ch.httpDSN, "INSERT INTO repos (id, repo, ref, created_at, settings, tags, last_synced, org_id, provider) "+
				"SELECT generateUUIDv4(), 'real/repo', 'main', now64(3), NULL, NULL, now64(3), '"+other+"', 'github'")
			before := rowCounts(t, ch.httpDSN)
			code, stdout, stderr = runGenerateVerb(t, map[string]string{"ORG_ID": other}, acrArgs(sink(ch))...)
			if code != cli.ExitRefused || stdout != "" || !strings.Contains(stderr, "mixed_org") || !strings.Contains(stderr, "github") {
				t.Fatalf("a synced org: exit %d stdout %q stderr %q", code, stdout, stderr)
			}
			if after := rowCounts(t, ch.httpDSN); !reflect.DeepEqual(before, after) {
				t.Fatalf("the refused run wrote rows: %v -> %v", before, after)
			}
			if code, _, stderr := runGenerateVerb(t, map[string]string{"ORG_ID": other}, acrArgs(sink(ch), "--allow-mixed-org")...); code != cli.ExitOK {
				t.Fatalf("--allow-mixed-org: exit %d %s", code, stderr)
			}
			// Without an organization the default demo organization is used.
			fresh := startClickHouse(t)
			stopMerges(t, fresh.httpDSN)
			if code, _, stderr := runGenerateVerb(t, nil, acrArgs(sink(fresh))...); code != cli.ExitOK {
				t.Fatalf("no organization: exit %d %s", code, stderr)
			}
			if body := clickHouseHTTP(t, fresh.httpDSN, "SELECT groupUniqArray(org_id) FROM git_commits FORMAT JSONCompactEachRow"); strings.TrimSpace(body) != `[["`+defaultOrg+`"]]` {
				t.Fatalf("no organization: git_commits holds %s, want the default demo organization %s", strings.TrimSpace(body), defaultOrg)
			}
			// A missing ClickHouse is reported without its credentials.
			code, _, stderr = runGenerateVerb(t, env, acrArgs("clickhouse://user:s3cretpw@127.0.0.1:1/db")...)
			if code != cli.ExitFailure || strings.Contains(stderr, "s3cretpw") {
				t.Fatalf("an unreachable ClickHouse: exit %d %s", code, stderr)
			}
		})
	}
}

// A ClickHouse the migrations never ran on has none of the guard's tables: the guard treats that as
// nothing synced, and the run then fails on the first table it writes, naming the load, not the guard.
func TestGenerateVerbOnAnUnmigratedClickHouseFailsOnTheLoadNotTheGuard(t *testing.T) {
	instance, err := containers.StartClickHouse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	code, _, stderr := runGenerateVerb(t, nil, acrArgs(instance.URI)...)
	if code != cli.ExitFailure || !strings.Contains(stderr, "load_failed") || strings.Contains(stderr, "guard_failed") {
		t.Fatalf("an unmigrated ClickHouse: exit %d %s", code, stderr)
	}
}

// A table missing later in the frozen insert order (a partially migrated database, as opposed to an
// entirely unmigrated one) is caught by the preflight before any row is written, naming the table and
// leaving every other table exactly as it was, not merely the tables ahead of the missing one in
// frozen order.
func TestGenerateVerbOnAPartiallyMigratedClickHouseWritesNothing(t *testing.T) {
	set := generateParameterSets[0]
	world, err := LoadFrozenWorld(set.Params)
	if err != nil {
		t.Fatal(err)
	}
	var dropped string
	for i := len(world.Tables) - 1; i >= 0; i-- {
		if !world.Tables[i].Derived {
			dropped = world.Tables[i].Name
			break
		}
	}
	if dropped == "" {
		t.Fatal("no non-derived table to drop")
	}
	ch := startClickHouse(t)
	stopMerges(t, ch.httpDSN)
	clickHouseHTTP(t, ch.httpDSN, "DROP TABLE `"+dropped+"`")
	before := rowCounts(t, ch.httpDSN)
	code, stdout, stderr := runGenerateVerb(t, nil, acrArgs(ch.instance.URI)...)
	if code != cli.ExitFailure || stdout != "" || !strings.Contains(stderr, "load_failed") || !strings.Contains(stderr, dropped) {
		t.Fatalf("a table missing later in the frozen order: exit %d stdout %q stderr %q", code, stdout, stderr)
	}
	if after := rowCounts(t, ch.httpDSN); !reflect.DeepEqual(before, after) {
		t.Fatalf("a refused partial-schema load wrote rows: %v -> %v", before, after)
	}
}

// The frozen worlds hold Date/DateTime/DateTime64 values with no zone suffix, captured against a
// server the freezer required to run in UTC. Loading them into a server whose default zone is not UTC
// must refuse before writing anything, not reinterpret every shifted value at the server's offset.
func TestGenerateVerbRefusesANonUTCServer(t *testing.T) {
	t.Setenv(containers.ClickHouseTimezoneEnv, "America/Los_Angeles")
	ch := startClickHouse(t)
	before := rowCounts(t, ch.httpDSN)
	code, stdout, stderr := runGenerateVerb(t, nil, acrArgs(ch.instance.URI)...)
	if code != cli.ExitRefused || stdout != "" || !strings.Contains(stderr, "non_utc_server") || !strings.Contains(stderr, "America/Los_Angeles") {
		t.Fatalf("a non-UTC server: exit %d stdout %q stderr %q", code, stdout, stderr)
	}
	if after := rowCounts(t, ch.httpDSN); !reflect.DeepEqual(before, after) {
		t.Fatalf("a refused non-UTC load wrote rows: %v -> %v", before, after)
	}
}
