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
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/cacheinvalidation"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	testOrg      = "c0ffee00-dead-4bee-8bad-f00dfeedface"
	testRepoName = "ci-metrics-executed-proof/repo"

	// finalizePythonBuild is a build that still carried the Python producer
	// (processors/sync.py _complete_synthetic_sync_run and Python's own
	// finalize_sync_run).
	finalizePythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"
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

// pythonRun runs the producer against uri through the harness launcher: the
// pinned interpreter in the closed environment, with the address of the run's
// own database as the only entries made for this run.
func pythonRun(t *testing.T, producer *venueoracle.Producer, uri string, input []byte) {
	t.Helper()
	// SQLAlchemy has no "postgres" dialect: the container's DSN names the scheme
	// the way libpq does.
	uri = strings.Replace(uri, "postgres://", "postgresql://", 1)
	command, err := producer.Command(context.Background(), nil, []string{"POSTGRES_URI=" + uri, "DATABASE_URI=" + uri}, "-c", livePythonProgram)
	if err != nil {
		t.Fatal(err)
	}
	command.Stdin = bytes.NewReader(input)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("the Python producer failed: %v", pyoracle.RunError(command.Path, err, output))
	}
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
// rows the Python producer wrote: every table, every column, in the same JSON
// text the producer's json.dumps wrote (uuids and free timestamps masked).
//
// The producer's rows were executed once on finalizePythonBuild by the record
// verb, against a database this test migrates to the PostgreSQL head, and are
// frozen in testdata/golden/finalize_synthetic.json. The producer's text and
// its input (targets, org, repository, window) are the golden's key.
//
// NOT pinned: the schema the producer wrote into. A later migration that adds a
// column to one of these tables changes the Go rows and not the frozen ones:
// the test then fails on that column, and the frozen rows are amended by hand
// with the column's default once the producer is gone.
func TestFinalizeSyntheticWritesTheFrozenPythonRows(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/finalize_synthetic.json",
		PythonBuild: finalizePythonBuild,
		SHA256:      "892a51290a37d0c16672821bebb87ed97e87721cb0246ef28dd81a1f369fde0c",
		Recipe: "git worktree add --detach $DIR " + finalizePythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/fixturescli/ -test '^TestFinalizeSyntheticWritesTheFrozenPythonRows$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)
	instance, admin := startDatabase(t)
	targets := Targets
	input, err := json.Marshal(map[string]any{
		"targets": targets, "org_id": testOrg, "repo_name": testRepoName,
		"since": testSince.Format(time.RFC3339), "before": testBefore.Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("finalize every target", livePythonProgram, input, nil)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		uri := freshDatabase(t, instance, admin)
		before := snapshot(t, uri)
		pythonRun(t, producer, uri, input)
		rows := added(t, before, snapshot(t, uri))
		requireNonVacuous(t, "Python", rows, targets)
		body, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: string(body)}}
	})
	golden.Consumed(t, answers...)
	var frozen map[string][]string
	if err := json.Unmarshal([]byte(answers[0].Body), &frozen); err != nil {
		t.Fatalf("decode the frozen Python rows: %v", err)
	}
	// A golden of empty tables would pass for any implementation.
	requireNonVacuous(t, "the frozen Python producer", frozen, targets)

	uri := freshDatabase(t, instance, admin)
	before := snapshot(t, uri)
	goRun(t, uri, targets)
	got := added(t, before, snapshot(t, uri))

	requireNonVacuous(t, "Go", got, targets)
	assertRelations(t, uri)
	if !reflect.DeepEqual(got, frozen) {
		t.Fatalf("the rows the Go verb wrote differ from the frozen Python rows:\n%s", diffTables(frozen, got))
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

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

// With VALKEY_URI configured, the verb bumps the org's coverage-cache epoch
// exactly as the River worker's finalize does (a cached filter-scoped view of
// the org becomes unreachable); without it the run still finalizes, the skip is
// logged at Info, and no cache failure is logged as a warning.
func TestFinalizeSyntheticBumpsTheCoverageCacheEpochWhenValkeyIsConfigured(t *testing.T) {
	instance, admin := startDatabase(t)
	uri := freshDatabase(t, instance, admin)
	valkey, err := containers.StartValkey(context.Background())
	if err != nil {
		t.Fatalf("start valkey: %v", err)
	}
	t.Cleanup(func() { _ = valkey.Close(context.Background()) })

	epoch := func() (int64, bool) {
		client, err := valkeystore.Open(context.Background(), valkeystore.DefaultConfig(valkey.URI))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		value, err := client.Do(context.Background(), client.B().Get().Key(cacheinvalidation.OrgCacheEpochKey(testOrg)).Build()).AsInt64()
		if valkeygo.IsValkeyNil(err) {
			return 0, false
		}
		if err != nil {
			t.Fatal(err)
		}
		return value, true
	}
	runVerb := func(env map[string]string) (int, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
		code := runFinalizeSynthetic(context.Background(), cli.Env{
			Args:   []string{"--target", "cicd", "--repo-name", testRepoName, "--backfill", "7"},
			Lookup: lookup, Stdout: &stdout, Stderr: &stderr,
		})
		return code, stderr.String()
	}
	base := map[string]string{"MIGRATION_DATABASE_URI": uri, "ORG_ID": testOrg, AllowEnvVar: "1"}

	code, stderr := runVerb(base)
	if code != cli.ExitOK {
		t.Fatalf("without VALKEY_URI: exit %d\n%s", code, stderr)
	}
	if _, present := epoch(); present {
		t.Fatal("the epoch was bumped with no VALKEY_URI configured")
	}
	if strings.Contains(stderr, "coverage_cache_invalidation_failed") || !strings.Contains(stderr, `"msg":"coverage cache invalidation skipped"`) {
		t.Fatalf("without VALKEY_URI the skip must be one Info line and no failure warning:\n%s", stderr)
	}

	withValkey := map[string]string{"VALKEY_URI": valkey.URI}
	for key, value := range base {
		withValkey[key] = value
	}
	code, stderr = runVerb(withValkey)
	if code != cli.ExitOK {
		t.Fatalf("with VALKEY_URI: exit %d\n%s", code, stderr)
	}
	if got, present := epoch(); !present || got != 1 {
		t.Fatalf("epoch after a finalize with VALKEY_URI = %d (present %v), want 1", got, present)
	}
	if strings.Contains(stderr, "coverage_cache_invalidation_failed") || strings.Contains(stderr, valkey.URI) {
		t.Fatalf("with VALKEY_URI the run logged a cache failure or the URI:\n%s", stderr)
	}

	withValkey["VALKEY_URI"] = "redis://127.0.0.1:1/1"
	if code, stderr = runVerb(withValkey); code == cli.ExitOK || !strings.Contains(stderr, "valkey_unavailable") {
		t.Fatalf("an unreachable VALKEY_URI must fail the verb naming valkey_unavailable (exit %d):\n%s", code, stderr)
	}
}
