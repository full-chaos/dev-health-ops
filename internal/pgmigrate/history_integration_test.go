//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

const historyGolden = "testdata/history_golden.json"

// historyGoldenSHA256 pins testdata/history_golden.json (R24): what the real
// `dev-hops migrate postgres history` printed for each scenario, and what the real
// `dev-hops migrate postgres downgrade 0139` did to a database at the head. The
// producer is deleted with the Python CLI, so this is a rot guard: the file is only
// rewritten by TestHistoryVenueOracleMatchesAlembic with DHO_HISTORY_GOLDEN_UPDATE=1,
// then this digest is updated.
const historyGoldenSHA256 = "d7cbf47c4fcbb9386250aa3af3e79ffb6b12da88778ae734d403269b0e81e6a8"

type historyGoldenFile struct {
	History   map[string]string `json:"history"`
	Downgrade struct {
		Exit          int      `json:"exit"`
		VersionsAfter []string `json:"versionsAfter"`
	} `json:"downgrade"`
}

func TestHistoryGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(historyGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != historyGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", historyGolden, got, historyGoldenSHA256)
	}
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

func TestHistoryMatchesTheFrozenAlembicOutput(t *testing.T) {
	raw, err := os.ReadFile(historyGolden)
	if err != nil {
		t.Fatal(err)
	}
	var frozen historyGoldenFile
	if err := json.Unmarshal(raw, &frozen); err != nil {
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
}

func pythonMigrate(t *testing.T, env []string, uri string, args ...string) (int, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	command := exec.Command(python, append([]string{"-c", program, "migrate", "postgres"}, args...)...)
	command.Env = removeEnv(removeEnv(os.Environ(), "MIGRATION_DATABASE_URI"), "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER")
	command.Env = append(command.Env, "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_ENABLED=false")
	if uri != "" {
		pyURI := strings.Replace(uri, "postgres://", "postgresql://", 1)
		command.Env = append(command.Env, "POSTGRES_URI="+pyURI, "DATABASE_URI="+pyURI)
	}
	command.Env = append(command.Env, env...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
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

// TestHistoryVenueOracleMatchesAlembic runs the real `dev-hops migrate postgres
// history` in each scenario and compares its text with dho's; then downgrades a
// database at the head with Alembic and asks dho to do the same. With
// DHO_HISTORY_GOLDEN_UPDATE=1 it rewrites the frozen file.
func TestHistoryVenueOracleMatchesAlembic(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	frozen := historyGoldenFile{History: map[string]string{}}
	got := goHistory(t)
	for _, scenario := range historyScenarios {
		code, want := pythonMigrate(t, scenario.env, "", "history")
		if code != 0 {
			t.Errorf("%s: alembic history exited %d", scenario.name, code)
		}
		if got != want {
			t.Errorf("%s: dho printed\n%.400s\nAlembic printed\n%.400s", scenario.name, got, want)
		}
		frozen.History[scenario.name] = want
	}

	// downgrade: dho refuses and leaves the database; Alembic then downgrades it.
	uri, _ := revisionsDatabase(t)
	before := recordedVersions(t, uri)
	for _, target := range []string{"0139", "-1", "base"} {
		var stdout, stderr bytes.Buffer
		code := historyChild(t, "downgrade")(context.Background(), cli.Env{Args: []string{target}, Stdout: &stdout, Stderr: &stderr})
		if code != cli.ExitRefused || stdout.Len() != 0 || !strings.Contains(stderr.String(), "forward_only") {
			t.Errorf("dho downgrade %s: exit %d stdout %q stderr %q, want a refusal (exit 3)", target, code, stdout.String(), stderr.String())
		}
		if after := recordedVersions(t, uri); !reflect.DeepEqual(after, before) {
			t.Fatalf("dho downgrade %s changed alembic_version: %v -> %v", target, before, after)
		}
	}
	code, _ := pythonMigrate(t, []string{"DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1"}, uri, "downgrade", "0139")
	frozen.Downgrade.Exit = code
	frozen.Downgrade.VersionsAfter = recordedVersions(t, uri)
	if code != 0 || !reflect.DeepEqual(frozen.Downgrade.VersionsAfter, []string{"0066", "0139"}) {
		t.Errorf("alembic downgrade 0139: exit %d, versions %v, want exit 0 and 0066, 0139: the refusal no longer contrasts with what Alembic does", code, frozen.Downgrade.VersionsAfter)
	}
	if os.Getenv("DHO_HISTORY_GOLDEN_UPDATE") == "1" {
		body, err := json.MarshalIndent(frozen, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(historyGolden, append(body, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}
