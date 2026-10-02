//go:build integration

package chmigrate_test

import (
	"context"
	"fmt"
	"maps"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
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

// Two groups of rows of repos, both part of every request's key (id, repo, org, last_synced).
//
// tiedRows: the newest rows of an id share one last_synced in two or three organisations. argMax(org_id, last_synced)
// over an equal last_synced has no defined winner on either plane (the recorded Python answer flipped between two
// recordings of the same rows, CHAOS-7832), so what the tie scenarios record is NOT MEASURED: the exact report and the
// rows left are not compared. What does not depend on the winner is compared (how many rows are listed and deleted, how
// many rows of each id are left): that is what the `org_id` clause of the DELETE decides.
var tiedRows = [][4]string{
	{"a0000000-0000-4000-8000-000000000006", "acme/tie-two", orgOne, "2026-03-01 10:00:00.123"},
	{"a0000000-0000-4000-8000-000000000006", "acme/tie-two", orgTwo, "2026-03-01 10:00:00.123"},
	{"a0000000-0000-4000-8000-000000000007", "acme/tie-three", orgOne, "2026-02-01 00:00:00"},
	{"a0000000-0000-4000-8000-000000000007", "acme/tie-three", orgTwo, "2026-03-15 00:00:00"},
	{"a0000000-0000-4000-8000-000000000007", "acme/tie-three", orgThree, "2026-03-15 00:00:00"},
}

// clearRows: no tie, the newest row is in the organisation with the SMALLEST id (argMax by last_synced, not the
// largest org id). Compared exactly.
var clearRows = [][4]string{
	{"a0000000-0000-4000-8000-000000000008", "acme/small-org-newest", orgOne, "2026-03-20 00:00:00"},
	{"a0000000-0000-4000-8000-000000000008", "acme/small-org-newest", orgTwo, "2026-03-10 00:00:00"},
	{"a0000000-0000-4000-8000-000000000009", "acme/small-org-newest-of-three", orgTwo, "2026-02-10 00:00:00"},
	{"a0000000-0000-4000-8000-000000000009", "acme/small-org-newest-of-three", orgThree, "2026-02-20 00:00:00"},
	{"a0000000-0000-4000-8000-000000000009", "acme/small-org-newest-of-three", orgOne, "2026-03-30 12:00:00.5"},
}

// seedRows writes rows into an empty repos table.
func (db repairDB) seedRows(t *testing.T, rows [][4]string) {
	t.Helper()
	db.do(t, "TRUNCATE TABLE repos")
	for _, row := range rows {
		db.do(t, fmt.Sprintf("INSERT INTO repos (id, repo, ref, created_at, settings, tags, last_synced, org_id) VALUES ('%s', '%s', NULL, toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), NULL, NULL, toDateTime64('%s', 3, 'UTC'), '%s')",
			row[0], row[1], row[3], row[2]))
	}
}

// ruleScenario is a scenario over one group of rows; measured is false where the winner of the rows is undefined.
type ruleScenario struct {
	repairScenario
	rows     [][4]string
	measured bool
}

// No --org scenario over tiedRows: the filter is on the ACTIVE org, so which rows a filtered run lists is the winner's
// choice too (the first recording of such a scenario differed between the planes).
var ruleScenarios = []ruleScenario{
	{repairScenario{name: "dry run with a tie on last_synced"}, tiedRows, false},
	{repairScenario{name: "apply with a tie on last_synced", args: []string{"--apply"}}, tiedRows, false},
	{repairScenario{name: "dry run, newest rows in the smallest org"}, clearRows, true},
	{repairScenario{name: "apply, newest rows in the smallest org", args: []string{"--apply"}}, clearRows, true},
	{repairScenario{name: "dry run for the smallest org that owns the newest rows", args: []string{"--org", orgOne}}, clearRows, true},
	{repairScenario{name: "apply for the smallest org that owns the newest rows", args: []string{"--apply", "--org", orgOne}}, clearRows, true},
	{repairScenario{name: "dry run for the org that owns only older rows of those ids", args: []string{"--org", orgThree}}, clearRows, true},
}

// ruleRequest is a scenario as a golden request: its path holds the producer's environment, the seeded rows and the
// argv, so all of them are compared exactly.
func ruleRequest(s ruleScenario) venueoracle.Request {
	path := ""
	for _, key := range slices.Sorted(maps.Keys(repairTiesProducerEnv)) {
		path += key + "=" + repairTiesProducerEnv[key] + " "
	}
	return venueoracle.Request{Name: s.name, Method: "CLI", Path: path + fmt.Sprintf("rows=%q dev-health-ops migrate clickhouse repair %q", s.rows, s.args)}
}

var reportCount = regexp.MustCompile(`(?m)^(Found|Deleted) (\d+) stale duplicate row`)

// winnerFree is what a report and the rows left say whatever the winner of a tie is: the counts the report states and
// how many rows of each id are left.
func winnerFree(stdout, state string) string {
	var facts []string
	for _, match := range reportCount.FindAllStringSubmatch(stdout, -1) {
		count, _ := strconv.Atoi(match[2])
		facts = append(facts, match[1]+" "+strconv.Itoa(count))
	}
	perID := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(state), "\n") {
		if id, _, found := strings.Cut(line, "\t"); found {
			perID[id]++
		}
	}
	for _, id := range slices.Sorted(maps.Keys(perID)) {
		facts = append(facts, fmt.Sprintf("%s rows=%d", id, perID[id]))
	}
	return strings.Join(facts, "; ")
}

// TestRepairRulesMatchTheFrozenPythonProducer is the differential oracle of `dho migrate clickhouse repair` on ids
// whose newest row is in the smallest organisation id (compared exactly) and on ids whose newest rows tie on
// last_synced (NOT MEASURED, see tiedRows; only the winner-free facts are compared): the report and the rows left,
// for the dry run and for --apply, with and without --org. The producer ran once on repairTiesPythonBuild
// (CHAOS-7832) and its answers are frozen in testdata/golden/repair-ties.json.
func TestRepairRulesMatchTheFrozenPythonProducer(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/repair-ties.json",
		PythonBuild: repairTiesPythonBuild,
		SHA256:      "8d5603e1944366ac7d92224932d888bfb79a887ef81109fdf31b6092b5c1f27e",
		Recipe: "git worktree add --detach $DIR " + repairTiesPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/chmigrate/ -test '^TestRepairRulesMatchTheFrozenPythonProducer$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)
	// The producer's own ClickHouse exists only while recording.
	var python repairDB
	if golden.Recording() {
		python = startRepairDB(t)
	}
	scenarios := map[string]ruleScenario{}
	produce := func(producer *venueoracle.Producer, requests []venueoracle.Request) []venueoracle.Response {
		answers := make([]venueoracle.Response, len(requests))
		for index, request := range requests {
			s, ok := scenarios[request.Path]
			if !ok {
				t.Fatalf("no scenario for %s", request.Path)
			}
			python.seedRows(t, s.rows)
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
	compared, notMeasured := 0, 0
	for _, s := range ruleScenarios {
		request := ruleRequest(s)
		scenarios[request.Path] = s
		answer := golden.Produce(t, root, []venueoracle.Request{request}, produce)[0]
		golden.Consumed(t, answer)
		golang.seedRows(t, s.rows)
		got := golang.goRun(t, s.repairScenario)
		name := "state after " + s.name
		var pythonState string
		if s.measured {
			pythonState = golden.CompareRows(t, name, func() string { return python.state(t) }, got.State)
			if answer.Status != 0 || answer.Body != got.Stdout {
				t.Errorf("%s: python (exit %d)\n%s\ndho\n%s", s.name, answer.Status, answer.Body, got.Stdout)
			}
			compared++
		} else {
			pythonState = golden.InspectRows(t, name, func() string { return python.state(t) })
			if want, have := winnerFree(answer.Body, pythonState), winnerFree(got.Stdout, got.State); answer.Status != 0 || want != have {
				t.Errorf("%s: winner-free facts differ: python (exit %d) %q, dho %q", s.name, answer.Status, want, have)
			}
			notMeasured++
		}
		if answer.Body == "" || pythonState == "" {
			t.Errorf("%s: the producer's report %q or rows %q are empty: the comparison measures nothing", s.name, answer.Body, pythonState)
		}
	}
	t.Logf("%d scenarios: %d compared exactly, %d NOT MEASURED (an equal last_synced has no defined winner on either plane; the recorded Python answer flipped between two recordings; only the winner-free facts are compared)",
		len(ruleScenarios), compared, notMeasured)
	golden.SkipDiff(t)
	golden.Finish(t)
}
