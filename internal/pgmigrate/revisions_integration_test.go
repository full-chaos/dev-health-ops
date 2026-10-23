//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
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
	{name: "current below the head", setup: "UPDATE alembic_version SET version_num = '0140' WHERE version_num = '0141'", verb: "current"},
	{name: "current of an empty version table", setup: "DELETE FROM alembic_version", verb: "current"},
	{name: "current without a version table", setup: "DROP TABLE alembic_version", verb: "current"},
}

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

// revisionsPythonBuild is the build whose Python CLI (Alembic) answered the scenarios: a build that still
// carried the Python CLI.
const revisionsPythonBuild = "de4bc686e35308fb20670ad6ce8333952e2b43e6"

// TestRevisionsMatchTheFrozenAlembicOutput runs the verbs on a real PostgreSQL and compares their text with
// what Alembic printed for the same states. The answers were executed once on revisionsPythonBuild (the real
// `dev-hops migrate postgres current|heads` against a database in each state) and are frozen in
// testdata/golden/revisions.json (the recipe regenerates them by execution); the scenarios are part of the
// golden's key.
func TestRevisionsMatchTheFrozenAlembicOutput(t *testing.T) {
	pgmigrate.UseFrozenChain(t) // the frozen goldens end at pgmigrate.FrozenHead
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/revisions.json",
		PythonBuild: revisionsPythonBuild,
		SHA256:      "2a739c531a37758783463f25e1ac489375fbb3d99d5f90b38953511e9d2101ce",
	})

	scenarios := make([]map[string]string, len(revisionScenarios))
	for index, scenario := range revisionScenarios {
		scenarios[index] = map[string]string{"name": scenario.name, "setup": scenario.setup, "verb": scenario.verb}
	}
	input, err := json.Marshal(scenarios)
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("revisions scenarios", input)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	var frozen []revisionResult
	if err := json.Unmarshal([]byte(answers[0].Body), &frozen); err != nil {
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
		if frozen[index].Name != scenario.name {
			t.Fatalf("scenario %d is %q, the golden has %q", index, scenario.name, frozen[index].Name)
		}
		if scenario.verb == "current" && strings.Join(sortedLines(got), "\n") != strings.TrimRight(got, "\n") {
			t.Errorf("%s: dho printed %q, want its revisions sorted", scenario.name, got)
		}
		if !sameRevisionText(scenario.verb, got, frozen[index].Stdout) {
			t.Errorf("%s: dho printed %q, Alembic printed %q", scenario.name, got, frozen[index].Stdout)
		}
		if frozen[index].Stdout == "" && scenario.verb == "heads" {
			t.Errorf("%s: the frozen heads are empty: the comparison measures nothing", scenario.name)
		}
		exec("DROP TABLE IF EXISTS alembic_version")
		exec("CREATE TABLE alembic_version AS SELECT * FROM alembic_version_saved")
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// sameRevisionText compares a verb's output. `heads` is ordered (both sides
// sort it). Alembic's `current` order for two heads is unstable across runs
// while dho sorts it, so `current` lines compare as a set.
func sameRevisionText(verb, got, want string) bool {
	if verb != "current" {
		return got == want
	}
	return strings.Join(sortedLines(got), "\n") == strings.Join(sortedLines(want), "\n")
}

// setOrdered fixes the line order of the recorded `current` text. Alembic prints the current heads in the
// row order of its version table (an unordered SELECT), which differs between two fresh databases with two
// heads, so the recorder's two-run determinism check failed at random. `current` is a set of heads
// (sameRevisionText compares it as one), so the golden holds its lines sorted; `heads` is already ordered.
func setOrdered(verb, text string) string {
	if verb != "current" || text == "" {
		return text
	}
	return strings.Join(sortedLines(text), "\n") + "\n"
}

func sortedLines(text string) []string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	sort.Strings(lines)
	return lines
}
