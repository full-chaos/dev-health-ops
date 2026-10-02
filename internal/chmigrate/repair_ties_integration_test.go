//go:build integration

package chmigrate_test

import (
	"context"
	"fmt"
	"maps"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// repairTiesPythonBuild is the build whose `dev-hops migrate clickhouse repair` answered the tie scenarios: a build
// that still carried the Python CLI.
const repairTiesPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// repairTiesProducerEnv is the environment of the producer's runs that shapes its answers: part of every request's
// key, with the ordering contract of the database the scenario repairs (the contract-2 head).
var repairTiesProducerEnv = map[string]string{
	chmigrate.OrderingContractEnv: "2", "OTEL_ENABLED": "false", "PYTHONHASHSEED": "0", "PYTHONUTF8": "1",
}

// tieRows are the rows of repos the tie scenarios repair (id, repo, org, last_synced): the active row of an id is
// argMax(org_id, last_synced), so a tie on the newest last_synced leaves the verb's own choice between two
// organizations. They are part of every request's key.
var tieRows = [][4]string{
	{"a0000000-0000-4000-8000-000000000006", "acme/tie-two", orgOne, "2026-03-01 10:00:00.123"},
	{"a0000000-0000-4000-8000-000000000006", "acme/tie-two", orgTwo, "2026-03-01 10:00:00.123"},
	{"a0000000-0000-4000-8000-000000000007", "acme/tie-three", orgOne, "2026-02-01 00:00:00"},
	{"a0000000-0000-4000-8000-000000000007", "acme/tie-three", orgTwo, "2026-03-15 00:00:00"},
	{"a0000000-0000-4000-8000-000000000007", "acme/tie-three", orgThree, "2026-03-15 00:00:00"},
}

// seedTies writes the tie rows into an empty repos table.
func (db repairDB) seedTies(t *testing.T) {
	t.Helper()
	db.do(t, "TRUNCATE TABLE repos")
	for _, row := range tieRows {
		db.do(t, fmt.Sprintf("INSERT INTO repos (id, repo, ref, created_at, settings, tags, last_synced, org_id) VALUES ('%s', '%s', NULL, toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), NULL, NULL, toDateTime64('%s', 3, 'UTC'), '%s')",
			row[0], row[1], row[3], row[2]))
	}
}

var tieScenarios = []repairScenario{
	{name: "dry run with a tie on last_synced", args: nil},
	{name: "apply with a tie on last_synced", args: []string{"--apply"}},
	{name: "dry run for one org with a tie on last_synced", args: []string{"--org", orgTwo}},
	{name: "apply for one org with a tie on last_synced", args: []string{"--apply", "--org", orgTwo}},
}

// tieRequest is a scenario as a golden request: its path holds the producer's environment, the seeded rows and the
// argv, so all of them are compared exactly.
func tieRequest(s repairScenario) venueoracle.Request {
	path := ""
	for _, key := range slices.Sorted(maps.Keys(repairTiesProducerEnv)) {
		path += key + "=" + repairTiesProducerEnv[key] + " "
	}
	return venueoracle.Request{Name: s.name, Method: "CLI", Path: path + fmt.Sprintf("rows=%q dev-hops migrate clickhouse repair %q", tieRows, s.args)}
}

// TestRepairTiesMatchTheFrozenPythonProducer is the differential oracle of `dho migrate clickhouse repair` on ids whose
// newest rows tie on last_synced: the report and the rows left, for the dry run and for --apply, with and without
// --org. The producer ran once on repairTiesPythonBuild (CHAOS-7832) and its answers are frozen in
// testdata/golden/repair-ties.json.
func TestRepairTiesMatchTheFrozenPythonProducer(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/repair-ties.json",
		PythonBuild: repairTiesPythonBuild,
		SHA256:      "PIN:repair-ties",
		Recipe: "git worktree add --detach $DIR " + repairTiesPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/chmigrate/ -test '^TestRepairTiesMatchTheFrozenPythonProducer$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)
	// The producer's own ClickHouse exists only while recording.
	var python repairDB
	if golden.Recording() {
		python = startRepairDB(t)
	}
	scenarios := map[string]repairScenario{}
	produce := func(producer *venueoracle.Producer, requests []venueoracle.Request) []venueoracle.Response {
		answers := make([]venueoracle.Response, len(requests))
		for index, request := range requests {
			s, ok := scenarios[request.Path]
			if !ok {
				t.Fatalf("no scenario for %s", request.Path)
			}
			python.seedTies(t)
			command, err := producer.Command(context.Background(), maps.Clone(repairTiesProducerEnv), []string{"CLICKHOUSE_URI=" + python.httpDSN},
				append([]string{"-m", "dev_health_ops.cli", "migrate", "clickhouse", "repair"}, s.args...)...)
			if err != nil {
				t.Fatal(err)
			}
			var stdout, stderr strings.Builder
			command.Stdout, command.Stderr = &stdout, &stderr
			code := 0
			if err := command.Run(); err != nil {
				exit, ok := err.(*exec.ExitError)
				if !ok {
					t.Fatalf("run python: %v", err)
				}
				code = exit.ExitCode()
			}
			answers[index] = venueoracle.Response{Status: code, Body: stdout.String()}
		}
		return answers
	}

	golang := startRepairDB(t)
	for _, s := range tieScenarios {
		request := tieRequest(s)
		scenarios[request.Path] = s
		answer := golden.Produce(t, root, []venueoracle.Request{request}, produce)[0]
		golden.Consumed(t, answer)
		golang.seedTies(t)
		got := golang.goRun(t, s)
		pythonState := golden.CompareRows(t, "state after "+s.name, func() string { return python.state(t) }, got.State)
		if answer.Status != 0 || answer.Body != got.Stdout {
			t.Errorf("%s: python (exit %d)\n%s\ndho\n%s", s.name, answer.Status, answer.Body, got.Stdout)
		}
		if answer.Body == "" || pythonState == "" {
			t.Errorf("%s: the producer's report %q or rows %q are empty: the comparison measures nothing", s.name, answer.Body, pythonState)
		}
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}
