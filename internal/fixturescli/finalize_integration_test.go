//go:build integration

package fixturescli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

const (
	testOrg      = "c0ffee00-dead-4bee-8bad-f00dfeedface"
	testRepoName = "ci-metrics-executed-proof/repo"
	goldenUpdate = "DHO_SYNTHETIC_FINALIZE_GOLDEN_UPDATE"
	goldenPath   = "testdata/finalize_synthetic_golden.json"
)

var (
	testSince  = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	testBefore = time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)
)

// livePythonProgram is the producer: the real
// dev_health_ops.processors.sync._complete_synthetic_sync_run, which the
// (deleted-by-S10i) `dev-hops sync finalize-synthetic-sync` calls, driven with
// the same fixed window the Go side gets. It finalizes through Python's own
// finalize_sync_run.
const livePythonProgram = `
import json, sys
from datetime import datetime
from dev_health_ops.processors.sync import _complete_synthetic_sync_run

spec = json.load(sys.stdin)
for target in spec["targets"]:
    _complete_synthetic_sync_run(
        org_id=spec["org_id"], repo_full_name=spec["repo_name"], target=target,
        since_at=datetime.fromisoformat(spec["since"]), before_at=datetime.fromisoformat(spec["before"]),
    )
`

// golden is the frozen output of the Python producer (R24): the rows one
// finalize of every target adds to a fresh database at the PostgreSQL head,
// normalized (uuids and free timestamps masked).
type golden struct {
	Producer string              `json:"producer"`
	Targets  []string            `json:"targets"`
	Added    map[string][]string `json:"added"`
}

func startDatabase(t *testing.T) (*containers.Instance, *pgx.Conn) {
	t.Helper()
	instance, err := containers.StartPostgres(context.Background())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := pgx.Connect(context.Background(), instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return instance, conn
}

// freshDatabase creates a database and applies the PostgreSQL head baseline
// to it: the schema and seed rows the real chain builds.
func freshDatabase(t *testing.T, instance *containers.Instance, admin *pgx.Conn) string {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "dho_synthetic_" + hex.EncodeToString(suffix)
	if _, err := admin.Exec(context.Background(), "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	uri := parsed.String()

	conn, err := pgx.Connect(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain); err != nil {
		t.Fatalf("apply the baseline: %v", err)
	}
	return uri
}

// snapshot reads every row of every public table as jsonb text, plus the text
// of each json column (jsonb loses the stored spacing and key order, which the
// Python producer's json.dumps text carries).
func snapshot(t *testing.T, uri string) map[string][]string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tables, err := conn.Query(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	names, err := pgx.CollectRows(tables, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]string{}
	for _, table := range names {
		columns, err := conn.Query(ctx, `SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND data_type = 'json' ORDER BY 1`, table)
		if err != nil {
			t.Fatal(err)
		}
		jsonColumns, err := pgx.CollectRows(columns, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		expression := "to_jsonb(t)"
		for _, column := range jsonColumns {
			expression += fmt.Sprintf(" || jsonb_build_object('%s__text', t.%s::text)", column, pgx.Identifier{column}.Sanitize())
		}
		rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT (%s)::text FROM public.%s t`, expression, pgx.Identifier{table}.Sanitize()))
		if err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		texts, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		out[table] = texts
	}
	return out
}

var (
	uuidPattern      = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	timestampPattern = regexp.MustCompile(`^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d`)
)

// mask replaces every uuid with <uuid> and every timestamp other than the two
// instants both sides are given with <ts>. Ids and wall-clock stamps are what
// legitimately differ between two runs; the fixed window must not.
func mask(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = mask(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = mask(item)
		}
		return out
	case string:
		switch {
		case uuidPattern.MatchString(typed):
			return "<uuid>"
		case timestampPattern.MatchString(typed):
			parsed, err := time.Parse(time.RFC3339Nano, strings.Replace(typed, " ", "T", 1))
			if err == nil && (parsed.Equal(testSince) || parsed.Equal(testBefore)) {
				return parsed.UTC().Format(time.RFC3339)
			}
			return "<ts>"
		}
	}
	return value
}

// added is the multiset of rows in after and not in before, masked, per table,
// as sorted canonical JSON text.
func added(t *testing.T, before, after map[string][]string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for table, rows := range after {
		remaining := map[string]int{}
		for _, row := range before[table] {
			remaining[row]++
		}
		for _, row := range rows {
			if remaining[row] > 0 {
				remaining[row]--
				continue
			}
			var decoded any
			if err := json.Unmarshal([]byte(row), &decoded); err != nil {
				t.Fatalf("%s: %v", table, err)
			}
			if object, ok := decoded.(map[string]any); ok && table == "sync_runs" {
				object["result__text"] = renderAsJSONDumps(t, object["result__text"])
			}
			canonical, err := json.Marshal(mask(decoded))
			if err != nil {
				t.Fatal(err)
			}
			out[table] = append(out[table], string(canonical))
		}
		sort.Strings(out[table])
	}
	return out
}

func goRun(t *testing.T, uri string, targets []string) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	for _, target := range targets {
		params := Params{OrgID: testOrg, RepoName: testRepoName, Target: target, Since: testSince, Before: testBefore}
		if _, err := Finalize(context.Background(), pool, logger, params); err != nil {
			t.Fatalf("finalize %s: %v", target, err)
		}
	}
}

func pythonRun(t *testing.T, uri string, targets []string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	input, err := json.Marshal(map[string]any{
		"targets": targets, "org_id": testOrg, "repo_name": testRepoName,
		"since": testSince.Format(time.RFC3339), "before": testBefore.Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	// SQLAlchemy has no "postgres" dialect: the container's DSN names the scheme
	// the way libpq does.
	uri = strings.Replace(uri, "postgres://", "postgresql://", 1)
	command := exec.Command(python, "-c", livePythonProgram)
	command.Stdin = bytes.NewReader(input)
	command.Env = append(os.Environ(),
		"PYTHONPATH="+filepath.Join(root, "src"),
		"POSTGRES_URI="+uri, "DATABASE_URI="+uri, "OTEL_ENABLED=false",
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("the Python producer failed: %v", pyoracle.RunError(python, err, output))
	}
}

func loadGolden(t *testing.T) golden {
	t.Helper()
	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var frozen golden
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	return frozen
}

// requireNonVacuous fails unless the run wrote the rows the verb exists to
// write: a comparison of two empty sets passes for any implementation.
func requireNonVacuous(t *testing.T, side string, rows map[string][]string, targets []string) {
	t.Helper()
	want := map[string]int{
		"integrations":               1,
		"integration_sources":        1,
		"integration_datasets":       len(targets),
		"sync_runs":                  len(targets),
		"sync_run_units":             len(targets),
		"sync_executed_proof_ledger": len(targets),
		"sync_run_post_dispatches":   len(targets),
	}
	for table, count := range want {
		if len(rows[table]) != count {
			t.Fatalf("%s wrote %d %s row(s), want %d: %v", side, len(rows[table]), table, count, rows[table])
		}
	}
	for _, row := range rows["sync_runs"] {
		if !strings.Contains(row, `"status":"success"`) || !strings.Contains(row, `"completed_units":1`) {
			t.Fatalf("%s left a run that is not a finalized success: %s", side, row)
		}
	}
	for _, row := range rows["sync_executed_proof_ledger"] {
		if !strings.Contains(row, `"proven_at":null`) || strings.Contains(row, `"attempted_at":null`) {
			t.Fatalf("%s: the ledger row must be attempted and never proven: %s", side, row)
		}
	}
}

// The Go verb writes, for a fresh database at the PostgreSQL head, exactly the
// rows the Python producer wrote (frozen in the golden, R24): every table,
// every column, in the same JSON text the producer's json.dumps wrote.
func TestFinalizeSyntheticWritesTheFrozenPythonRows(t *testing.T) {
	instance, admin := startDatabase(t)
	uri := freshDatabase(t, instance, admin)
	frozen := loadGolden(t)

	before := snapshot(t, uri)
	goRun(t, uri, frozen.Targets)
	got := added(t, before, snapshot(t, uri))

	requireNonVacuous(t, "Go", got, frozen.Targets)
	assertRelations(t, uri)
	if !reflect.DeepEqual(got, frozen.Added) {
		t.Fatalf("the rows the Go verb wrote differ from the frozen Python rows:\n%s", diffTables(frozen.Added, got))
	}
}

// The same comparison against the live producer, while it still exists. It
// needs the full project Python environment (the producer imports the API
// app), so it runs by hand, not in a CI shard:
//
//	DEV_HEALTH_LIVE_PYTHON_ORACLES=1 DEV_HEALTH_PYTHON=<full venv python> \
//	  go test -tags=integration -run TestFinalizeSyntheticMatchesTheLivePythonProducer ./internal/fixturescli
//
// DHO_SYNTHETIC_FINALIZE_GOLDEN_UPDATE=1 rewrites the golden from the
// producer's rows. It is not a freshness check of the golden: the producer is
// deleted with the Python CLI (S10i).
func TestFinalizeSyntheticMatchesTheLivePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	instance, admin := startDatabase(t)
	frozen := loadGolden(t)
	targets := frozen.Targets
	if len(targets) == 0 {
		targets = Targets
	}

	pythonURI := freshDatabase(t, instance, admin)
	pythonBefore := snapshot(t, pythonURI)
	pythonRun(t, pythonURI, targets)
	pythonRows := added(t, pythonBefore, snapshot(t, pythonURI))

	goURI := freshDatabase(t, instance, admin)
	goBefore := snapshot(t, goURI)
	goRun(t, goURI, targets)
	goRows := added(t, goBefore, snapshot(t, goURI))

	requireNonVacuous(t, "Python", pythonRows, targets)
	requireNonVacuous(t, "Go", goRows, targets)
	if os.Getenv(goldenUpdate) == "1" {
		raw, err := json.MarshalIndent(golden{Producer: frozen.Producer, Targets: targets, Added: pythonRows}, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(goRows, pythonRows) {
		t.Fatalf("the Go verb wrote different rows than the live Python producer:\n%s", diffTables(pythonRows, goRows))
	}
}

// assertRelations checks what masking hides: every foreign key column points
// at the row it names, in the database itself.
func assertRelations(t *testing.T, uri string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for name, query := range map[string]string{
		"units point at their run, integration and source": `SELECT count(*) FROM sync_run_units u
			JOIN sync_runs r ON r.id = u.sync_run_id AND r.integration_id = u.integration_id
			JOIN integration_sources s ON s.id = u.source_id AND s.integration_id = u.integration_id
			JOIN integration_datasets d ON d.integration_id = u.integration_id AND d.dataset_key = u.dataset_key`,
		"one post dispatch per run": `SELECT count(*) FROM sync_run_post_dispatches p JOIN sync_runs r ON r.id = p.sync_run_id AND r.org_id = p.org_id`,
	} {
		var count int
		if err := conn.QueryRow(ctx, query).Scan(&count); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if count != len(Targets) {
			t.Fatalf("%s: %d row(s), want %d", name, count, len(Targets))
		}
	}
}

func diffTables(want, got map[string][]string) string {
	var out strings.Builder
	tables := map[string]bool{}
	for table := range want {
		tables[table] = true
	}
	for table := range got {
		tables[table] = true
	}
	names := make([]string, 0, len(tables))
	for table := range tables {
		names = append(names, table)
	}
	sort.Strings(names)
	for _, table := range names {
		if reflect.DeepEqual(want[table], got[table]) {
			continue
		}
		fmt.Fprintf(&out, "table %s: want %d row(s), got %d\n", table, len(want[table]), len(got[table]))
		for _, row := range want[table] {
			fmt.Fprintf(&out, "  - %s\n", row)
		}
		for _, row := range got[table] {
			fmt.Fprintf(&out, "  + %s\n", row)
		}
	}
	return out.String()
}

// renderAsJSONDumps applies the ONE named difference between the two planes'
// stored JSON text: the native Go finalize writes sync_runs.result compact,
// Python's json.dumps writes ", " and ": " separators. The Go text is re-rendered
// as json.dumps would write it, keeping its key order, so a difference in
// values, keys or key order still fails. (The same class the admin venue
// oracles name for audit_logs; no reader of the column parses its text.)
func renderAsJSONDumps(t *testing.T, text any) any {
	t.Helper()
	raw, ok := text.(string)
	if !ok {
		return text
	}
	value, err := pyjson.DecodeString(raw)
	if err != nil {
		t.Fatalf("sync_runs.result is not JSON: %q: %v", raw, err)
	}
	rendered, err := pyjson.Dumps(value)
	if err != nil {
		t.Fatal(err)
	}
	return rendered
}

// The verb itself, as CI runs it: the database and organization from the
// environment, one line on stdout naming the run it finalized, and the run is
// a finalized success in the database. A second call mints a second run (the
// verb is not idempotent, as the Python verb was not), and both runs share the
// one integration, source and dataset.
func TestFinalizeSyntheticVerbFinalizesARunFromTheEnvironment(t *testing.T) {
	instance, admin := startDatabase(t)
	uri := freshDatabase(t, instance, admin)
	env := map[string]string{
		"MIGRATION_DATABASE_URI": uri, "ORG_ID": testOrg, AllowEnvVar: "1",
	}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	args := []string{"--target", "cicd", "--repo-name", testRepoName, "--backfill", "7"}

	var runIDs []string
	for range 2 {
		var stdout, stderr bytes.Buffer
		code := runFinalizeSynthetic(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		if code != cli.ExitOK {
			t.Fatalf("exit %d, stderr:\n%s", code, stderr.String())
		}
		runID := strings.TrimSpace(stdout.String())
		if !uuidPattern.MatchString(runID) || strings.Contains(stdout.String()[:len(stdout.String())-1], "\n") {
			t.Fatalf("stdout = %q, want one run id line", stdout.String())
		}
		if strings.Contains(stderr.String(), uri) {
			t.Fatalf("stderr carries the database URI:\n%s", stderr.String())
		}
		runIDs = append(runIDs, runID)
	}
	if runIDs[0] == runIDs[1] {
		t.Fatalf("two calls returned the same run %s", runIDs[0])
	}

	conn, err := pgx.Connect(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var runs, successes, integrations, sources, datasets int
	if err := conn.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM sync_runs WHERE id = ANY($1::uuid[])),
		(SELECT count(*) FROM sync_runs WHERE id = ANY($1::uuid[]) AND status = 'success' AND completed_at IS NOT NULL),
		(SELECT count(*) FROM integrations WHERE org_id = $2),
		(SELECT count(*) FROM integration_sources WHERE org_id = $2),
		(SELECT count(*) FROM integration_datasets WHERE org_id = $2)`, runIDs, testOrg).Scan(&runs, &successes, &integrations, &sources, &datasets); err != nil {
		t.Fatal(err)
	}
	if runs != 2 || successes != 2 || integrations != 1 || sources != 1 || datasets != 1 {
		t.Fatalf("runs %d (finalized %d), integrations %d, sources %d, datasets %d; want 2 finalized runs sharing 1 integration, 1 source, 1 dataset", runs, successes, integrations, sources, datasets)
	}

	// Without the throwaway-database word nothing is written.
	delete(env, AllowEnvVar)
	var stdout, stderr bytes.Buffer
	if code := runFinalizeSynthetic(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}); code != cli.ExitRefused || stdout.Len() != 0 {
		t.Fatalf("without %s: exit %d, stdout %q", AllowEnvVar, code, stdout.String())
	}
	var after int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM sync_runs`).Scan(&after); err != nil || after != 2 {
		t.Fatalf("a refused call left %d run(s) (err %v), want 2", after, err)
	}
}
