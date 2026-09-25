//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// revisionScenario is one state of alembic_version and the verb run on it.
type revisionScenario struct {
	name string
	// setup edits alembic_version after a full upgrade ("" keeps both heads).
	setup string
	verb  string
}

var revisionScenarios = []revisionScenario{
	{name: "heads", verb: "heads"},
	{name: "current at the head", verb: "current"},
	{name: "current without the cutover head", setup: "DELETE FROM alembic_version WHERE version_num = '0066'", verb: "current"},
	{name: "current below the head", setup: "UPDATE alembic_version SET version_num = '0139' WHERE version_num = '0140'", verb: "current"},
	{name: "current of an empty version table", setup: "DELETE FROM alembic_version", verb: "current"},
	{name: "current without a version table", setup: "DROP TABLE alembic_version", verb: "current"},
}

const revisionsGolden = "testdata/revisions_golden.json"

// revisionsGoldenSHA256 pins testdata/revisions_golden.json (R24): what the real
// `dev-hops migrate postgres current|heads` (Alembic) printed for every scenario.
// The producer is deleted with the Python CLI, so this is a rot guard, not a
// freshness check: the file is only rewritten by
// TestRevisionsVenueOracleMatchesAlembic with DHO_REVISIONS_GOLDEN_UPDATE=1,
// then this digest is updated.
const revisionsGoldenSHA256 = "af1cb89d596dfe5ba9866f48217237b40ce8e478cf68a5ee53e864e70f641f1f"

type revisionResult struct {
	Name   string `json:"name"`
	Stdout string `json:"stdout"`
}

func goRevisions(t *testing.T, uri, verb string) string {
	t.Helper()
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(resolve).Children {
		if child.Name == verb {
			run = child.Run
		}
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), cli.Env{Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &stderr})
	if code != cli.ExitOK {
		t.Fatalf("dho migrate postgres %s: exit %d, stderr %s", verb, code, stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), "postgres://") {
		t.Fatal("the output carries the DSN")
	}
	return stdout.String()
}

func pythonRevisions(t *testing.T, uri, verb string) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	command := exec.Command(python, "-c", program, "migrate", "postgres", verb)
	pyURI := strings.Replace(uri, "postgres://", "postgresql://", 1)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "POSTGRES_URI="+pyURI, "DATABASE_URI="+pyURI, "OTEL_ENABLED=false")
	command.Env = removeEnv(command.Env, "MIGRATION_DATABASE_URI")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("the Python producer failed: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
	}
	return stdout.String()
}

func removeEnv(environ []string, key string) []string {
	kept := environ[:0:0]
	for _, entry := range environ {
		if !strings.HasPrefix(entry, key+"=") {
			kept = append(kept, entry)
		}
	}
	return kept
}

// revisionsDatabase is a database at the head (both alembic heads recorded).
func revisionsDatabase(t *testing.T) (uri string, admin func(string)) {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	adminConn := connect(t, instance.URI)
	database := scratchDatabase(t, adminConn)
	uri = databaseURI(t, instance.URI, database)
	conn := connect(t, uri)
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	return uri, func(statement string) {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

// TestRevisionsMatchTheFrozenAlembicOutput runs the verbs on a real PostgreSQL
// and compares their text with what Alembic printed for the same states (frozen,
// no Python needed).
func TestRevisionsMatchTheFrozenAlembicOutput(t *testing.T) {
	raw, err := os.ReadFile(revisionsGolden)
	if err != nil {
		t.Fatal(err)
	}
	var frozen []revisionResult
	if err := json.Unmarshal(goldenBody(raw), &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen) != len(revisionScenarios) {
		t.Fatalf("golden has %d scenarios, the test defines %d", len(frozen), len(revisionScenarios))
	}
	// Every scenario edits alembic_version in turn, so each restarts from the
	// head: one database, the version table saved and restored around the edit.
	uri, exec := revisionsDatabase(t)
	exec("CREATE TABLE alembic_version_saved AS SELECT * FROM alembic_version")
	for index, scenario := range revisionScenarios {
		if scenario.setup != "" {
			exec(scenario.setup)
		}
		got := goRevisions(t, uri, scenario.verb)
		if !sameRevisionText(scenario.verb, got, frozen[index].Stdout) {
			t.Errorf("%s: dho printed %q, Alembic printed %q", scenario.name, got, frozen[index].Stdout)
		}
		if frozen[index].Stdout == "" && scenario.verb == "heads" {
			t.Errorf("%s: the frozen heads are empty: the comparison measures nothing", scenario.name)
		}
		exec("DROP TABLE IF EXISTS alembic_version")
		exec("CREATE TABLE alembic_version AS SELECT * FROM alembic_version_saved")
	}
}

func TestRevisionsGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(revisionsGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != revisionsGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", revisionsGolden, got, revisionsGoldenSHA256)
	}
}

// TestRevisionsVenueOracleMatchesAlembic runs the real `dev-hops migrate postgres
// current|heads` for every scenario and compares its text with dho's. With
// DHO_REVISIONS_GOLDEN_UPDATE=1 it rewrites the frozen file.
func TestRevisionsVenueOracleMatchesAlembic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	uri, exec := revisionsDatabase(t)
	exec("CREATE TABLE alembic_version_saved AS SELECT * FROM alembic_version")
	var frozen []revisionResult
	for _, scenario := range revisionScenarios {
		if scenario.setup != "" {
			exec(scenario.setup)
		}
		want := pythonRevisions(t, uri, scenario.verb)
		got := goRevisions(t, uri, scenario.verb)
		if !sameRevisionText(scenario.verb, got, want) {
			t.Errorf("%s: dho printed %q, Alembic printed %q", scenario.name, got, want)
		}
		frozen = append(frozen, revisionResult{Name: scenario.name, Stdout: want})
		exec("DROP TABLE IF EXISTS alembic_version")
		exec("CREATE TABLE alembic_version AS SELECT * FROM alembic_version_saved")
	}
	if os.Getenv("DHO_REVISIONS_GOLDEN_UPDATE") == "1" {
		out, err := json.MarshalIndent(frozen, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		out = append([]byte("# written by TestRevisionsVenueOracleMatchesAlembic; `current` lines are compared as a set: "+
			"Alembic prints alembic_version in table (heap) order, which an UPDATE or VACUUM can change\n"), out...)
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(revisionsGolden, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}

// goldenBody is the golden's JSON: its leading "#" header lines removed.
func goldenBody(raw []byte) []byte {
	for bytes.HasPrefix(raw, []byte("#")) {
		end := bytes.IndexByte(raw, '\n')
		if end < 0 {
			return nil
		}
		raw = raw[end+1:]
	}
	return raw
}

// sameRevisionText compares a verb's output. `heads` is ordered (both sides
// sort it). `current` prints alembic_version in table order, which is not a
// contract (an UPDATE or VACUUM moves a row), so its lines compare as a set.
func sameRevisionText(verb, got, want string) bool {
	if verb != "current" {
		return got == want
	}
	return strings.Join(sortedLines(got), "\n") == strings.Join(sortedLines(want), "\n")
}

func sortedLines(text string) []string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	sort.Strings(lines)
	return lines
}
