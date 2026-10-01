//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// `history` is compared with the real `dev-hops migrate postgres history`
// (Alembic): the text for the environments that could change the walk (the River
// cutover setting, Python's hash seed, which orders Alembic's sets). `downgrade` is
// the one verb dho refuses; both sides are pinned: Alembic downgrades a database at
// the head by one revision, dho refuses with exit 3 and leaves the database as it
// was.

type historyScenario struct {
	name string
	env  []string
}

var historyScenarios = []historyScenario{
	{name: "cutover set", env: []string{"DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1", "PYTHONHASHSEED=0"}},
	{name: "cutover unset", env: []string{"PYTHONHASHSEED=1"}},
	{name: "another hash seed", env: []string{"DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1", "PYTHONHASHSEED=12345"}},
}

type historyGoldenFile struct {
	History   map[string]string `json:"history"`
	Downgrade struct {
		Exit          int      `json:"exit"`
		VersionsAfter []string `json:"versionsAfter"`
	} `json:"downgrade"`
}

func historyChild(t *testing.T, name string) func(context.Context, cli.Env) int {
	t.Helper()
	for _, child := range pgmigrate.Command(nil).Children {
		if child.Name == name {
			return child.Run
		}
	}
	t.Fatalf("no %s verb", name)
	return nil
}

func goHistory(t *testing.T) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if code := historyChild(t, "history")(context.Background(), cli.Env{Stdout: &stdout, Stderr: &stderr}); code != cli.ExitOK {
		t.Fatalf("dho migrate postgres history: exit %d, stderr %s", code, stderr.String())
	}
	return stdout.String()
}

// historyPythonBuild is the build whose Python CLI (Alembic) answered the scenarios: a build that still
// carried it.
const historyPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// TestHistoryMatchesTheFrozenAlembicOutput compares dho's `history` text with what the REAL `dev-hops migrate
// postgres history` printed in each scenario, and the one verb dho refuses (`downgrade`) with what Alembic did
// to a database at the head: dho refuses with exit 3 and leaves the database as it was. The answers were
// executed once on historyPythonBuild and are frozen in testdata/golden/history.json (the recipe regenerates
// them by execution); the scenarios are part of the golden's key.
func TestHistoryMatchesTheFrozenAlembicOutput(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/history.json",
		PythonBuild: historyPythonBuild,
		SHA256:      "070ce3be31bd17bb8e1245db998ed4822e7c1c0517c4690b2b8bc5e5a7a7baba",
		Recipe: "git worktree add --detach $DIR " + historyPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/pgmigrate/ -test '^TestHistoryMatchesTheFrozenAlembicOutput$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)

	scenarios := make([]map[string]any, len(historyScenarios))
	for index, scenario := range historyScenarios {
		scenarios[index] = map[string]any{"name": scenario.name, "env": scenario.env}
	}
	input, err := json.Marshal(map[string]any{"history": scenarios, "downgrade": []string{"downgrade", "0139"}, "downgradeEnv": []string{"DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1"}})
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"OTEL_ENABLED": "false"}
	request := venueoracle.ProgramRequest("history scenarios", pythonCLIProgram, input, env)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(root string, _ []venueoracle.Request) []venueoracle.Response {
		produced := historyGoldenFile{History: map[string]string{}}
		for _, scenario := range historyScenarios {
			code, text := pythonMigrate(t, root, scenario.env, "", "history")
			if code != 0 {
				t.Fatalf("%s: alembic history exited %d", scenario.name, code)
			}
			produced.History[scenario.name] = text
		}
		uri, _ := revisionsDatabase(t)
		code, _ := pythonMigrate(t, root, []string{"DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1"}, uri, "downgrade", "0139")
		produced.Downgrade.Exit = code
		produced.Downgrade.VersionsAfter = recordedVersions(t, uri)
		body, err := json.Marshal(produced)
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: string(body)}}
	})
	golden.Consumed(t, answers...)
	var frozen historyGoldenFile
	if err := json.Unmarshal([]byte(answers[0].Body), &frozen); err != nil {
		t.Fatal(err)
	}
	got := goHistory(t)
	if len(frozen.History) != len(historyScenarios) {
		t.Fatalf("golden has %d scenarios, the test defines %d", len(frozen.History), len(historyScenarios))
	}
	for _, scenario := range historyScenarios {
		want := frozen.History[scenario.name]
		if want == "" || strings.Count(want, "\n") < 100 {
			t.Errorf("%s: the frozen history is %d lines: the comparison measures nothing", scenario.name, strings.Count(want, "\n"))
		}
		if got != want {
			t.Errorf("%s: dho printed\n%.400s\nAlembic printed\n%.400s", scenario.name, got, want)
		}
	}
	if frozen.Downgrade.Exit != 0 || !reflect.DeepEqual(frozen.Downgrade.VersionsAfter, []string{"0066", "0139"}) {
		t.Errorf("the frozen downgrade is %+v, want Alembic to have downgraded to 0139 (versions 0066, 0139)", frozen.Downgrade)
	}

	// downgrade: dho refuses a target outside its ported range and leaves the database (the ported targets
	// are compared in TestDowngradeVenueOracleMatchesPythonDowngrade).
	uri, _ := revisionsDatabase(t)
	before := recordedVersions(t, uri)
	for _, target := range []string{"base", "0066"} {
		var stdout, stderr bytes.Buffer
		code := historyChild(t, "downgrade")(context.Background(), cli.Env{Args: []string{target}, Stdout: &stdout, Stderr: &stderr})
		if code != cli.ExitRefused || stdout.Len() != 0 || !strings.Contains(stderr.String(), `"error"`) {
			t.Errorf("dho downgrade %s: exit %d stdout %q stderr %q, want a refusal (exit 3)", target, code, stdout.String(), stderr.String())
		}
		if after := recordedVersions(t, uri); !reflect.DeepEqual(after, before) {
			t.Fatalf("dho downgrade %s changed alembic_version: %v -> %v", target, before, after)
		}
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// pythonCLIProgram is the entry point the producer runs: the real dev-hops CLI, in process.
const pythonCLIProgram = "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"

func pythonMigrate(t *testing.T, root string, env []string, uri string, args ...string) (int, string) {
	t.Helper()
	return pythonCLI(t, root, env, uri, append([]string{"migrate", "postgres"}, args...)...)
}

// pythonCLI runs `dev-hops ARGS` (the real entry point, in process) and returns its
// exit code and stdout.
func pythonCLI(t *testing.T, root string, env []string, uri string, cliArgs ...string) (int, string) {
	t.Helper()
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, append([]string{"-c", pythonCLIProgram}, cliArgs...)...)
	command.Env = removeEnv(removeEnv(os.Environ(), "MIGRATION_DATABASE_URI"), "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER")
	command.Env = append(command.Env, "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_ENABLED=false", "PYTHONDONTWRITEBYTECODE=1")
	if uri != "" {
		// The async engine (migrate status) takes asyncpg's own query names.
		pyURI := strings.Replace(strings.Replace(uri, "postgres://", "postgresql+asyncpg://", 1), "sslmode=", "ssl=", 1)
		command.Env = append(command.Env, "POSTGRES_URI="+pyURI, "DATABASE_URI="+pyURI)
	}
	command.Env = append(command.Env, env...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	code := 0
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("the Python producer did not run: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
		}
		code = exitErr.ExitCode()
	}
	return code, stdout.String()
}

func recordedVersions(t *testing.T, uri string) []string {
	t.Helper()
	conn := connect(t, uri)
	rows, err := conn.Query(context.Background(), "SELECT version_num FROM alembic_version ORDER BY version_num")
	if err != nil {
		t.Fatal(err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return versions
}

// TestHistoryGraphIsTheAlembicChain regenerates the embedded walk from the Python
// scripts (in the integration shard, next to the baseline capture) and fails when
// baseline/history.json differs. With DHO_HISTORY_UPDATE=1
// it rewrites the file.
func TestHistoryGraphIsTheAlembicChain(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	const program = `
import json, sys
from alembic import util
from alembic.script import ScriptDirectory
from dev_health_ops.migrate import _make_alembic_config
script = ScriptDirectory.from_config(_make_alembic_config(None))
out = []
for sc in script.walk_revisions(base="base", head="heads"):
    entry = {"revision": sc.revision, "down": sc._format_down_revision()}
    if sc.dependencies:
        entry["dependencies"] = util.format_as_comma(sc.dependencies)
    if sc.branch_labels:
        entry["labels"] = util.format_as_comma(sc.branch_labels)
    if sc._is_real_head:
        entry["realHead"] = True
    if sc.is_head:
        entry["head"] = True
    if sc.is_branch_point:
        entry["branchPoint"] = True
    if sc.is_merge_point:
        entry["mergePoint"] = True
    entry["doc"] = sc.doc
    out.append(entry)
print(json.dumps(out, indent=1, ensure_ascii=False))
`
	command := exec.Command(python, "-c", program)
	command.Env = append(removeEnv(os.Environ(), "MIGRATION_DATABASE_URI"), "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_ENABLED=false", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("the generator failed: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
	}
	if os.Getenv("DHO_HISTORY_UPDATE") == "1" {
		if err := os.WriteFile("baseline/history.json", stdout.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var want, got []pgmigrate.HistoryEntry
	if err := json.Unmarshal(stdout.Bytes(), &want); err != nil {
		t.Fatalf("decode the generated walk: %v", err)
	}
	got, err = pgmigrate.LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("baseline/history.json is not what the Alembic scripts walk to (%d entries embedded, %d generated): regenerate it with DHO_HISTORY_UPDATE=1 go test -tags integration -run TestHistoryGraphIsTheAlembicChain ./internal/pgmigrate", len(got), len(want))
	}
}
