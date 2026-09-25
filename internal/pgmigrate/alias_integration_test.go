//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The flat verbs of `dev-hops migrate` (current, heads, history, status) are compared
// with the real Python verbs on a real PostgreSQL, in every state the Python status
// distinguishes: at the head, with the River cutover authorized or not, below the head,
// with the cutover revision missing, an empty version table, no version table, and a
// revision the release does not contain.

type aliasScenario struct {
	name string
	// setup edits alembic_version after a full upgrade ("" keeps both heads); it may
	// name {app} and {below} (the application head and the revision under it).
	setup   string
	cutover bool
	args    []string
}

var aliasScenarios = []aliasScenario{
	{name: "heads", args: []string{"heads"}},
	{name: "history", args: []string{"history"}},
	{name: "current at the head", args: []string{"current"}},
	{name: "status at the head", args: []string{"status"}, cutover: true},
	{name: "status at the head without the cutover", args: []string{"status"}},
	{name: "status check at the head", args: []string{"status", "--check"}, cutover: true},
	{name: "status check at the head without the cutover", args: []string{"status", "--check"}},
	{name: "status without the cutover head", setup: "DELETE FROM alembic_version WHERE version_num = '0066'", args: []string{"status"}, cutover: true},
	{name: "status check without the cutover head", setup: "DELETE FROM alembic_version WHERE version_num = '0066'", args: []string{"status", "--check"}, cutover: true},
	{name: "status check without the cutover head, cutover not authorized", setup: "DELETE FROM alembic_version WHERE version_num = '0066'", args: []string{"status", "--check"}},
	{name: "status below the head", setup: "UPDATE alembic_version SET version_num = '{below}' WHERE version_num = '{app}'", args: []string{"status"}, cutover: true},
	{name: "status check below the head", setup: "UPDATE alembic_version SET version_num = '{below}' WHERE version_num = '{app}'", args: []string{"status", "--check"}},
	{name: "status of an empty version table", setup: "DELETE FROM alembic_version", args: []string{"status"}, cutover: true},
	{name: "status check of an empty version table", setup: "DELETE FROM alembic_version", args: []string{"status", "--check"}},
	{name: "status without a version table", setup: "DROP TABLE alembic_version", args: []string{"status"}, cutover: true},
	{name: "status check without a version table", setup: "DROP TABLE alembic_version", args: []string{"status", "--check"}},
	{name: "status only the cutover head", setup: "DELETE FROM alembic_version WHERE version_num <> '0066'", args: []string{"status", "--check"}, cutover: true},
	{name: "status of a revision the release does not contain", setup: "UPDATE alembic_version SET version_num = '9999' WHERE version_num = '{app}'", args: []string{"status"}, cutover: true},
}

const aliasGolden = "testdata/alias_golden.json"

// aliasGoldenSHA256 pins testdata/alias_golden.json (R24): what the real `dev-hops
// migrate heads|history|current|status` printed and exited with in every scenario.
// The producer is deleted with the Python CLI, so this is a rot guard: the file is
// only rewritten by TestAliasesVenueOracleMatchesTheFlatVerbs with
// DHO_ALIAS_GOLDEN_UPDATE=1, then this digest is updated.
const aliasGoldenSHA256 = "54fd7116c0d9899f2bb6b26d88d9d6cb2fb772835770c5e8f05da4c371c96b46"

type aliasResult struct {
	Name   string `json:"name"`
	Exit   int    `json:"exit"`
	Stdout string `json:"stdout"`
}

func TestAliasGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(aliasGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != aliasGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", aliasGolden, got, aliasGoldenSHA256)
	}
}

// aliasSetup fills the {app} and {below} names of a setup statement.
func aliasSetup(t *testing.T, statement string) string {
	t.Helper()
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := pgmigrate.LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	var app string
	for _, head := range pgmigrate.Heads(baseline, chain) {
		if head != "0066" {
			app = head
		}
	}
	var below string
	for _, entry := range pgmigrate.WithChain(entries, baseline, chain) {
		if entry.Revision == app {
			below = entry.Down
		}
	}
	return strings.NewReplacer("{app}", app, "{below}", below).Replace(statement)
}

func goAlias(t *testing.T, uri string, s aliasScenario) (int, string) {
	t.Helper()
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, alias := range pgmigrate.Aliases(resolve) {
		if alias.Name == s.args[0] {
			run = alias.Run
		}
	}
	if run == nil {
		t.Fatalf("no %s alias", s.args[0])
	}
	lookup := func(key string) (string, bool) {
		if key == pgmigrate.CutoverEnv && s.cutover {
			return "1", true
		}
		return "", false
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), cli.Env{Args: s.args[1:], Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stdout.String()+stderr.String(), "postgres://") {
		t.Fatal("the output carries the DSN")
	}
	return code, stdout.String()
}

// normalizeStatus sorts the heads of the "Current" line: Alembic's order of two heads
// is not stable across runs, dho prints them sorted.
func normalizeStatus(text string) string {
	lines := strings.Split(text, "\n")
	for index, line := range lines {
		if rest, ok := strings.CutPrefix(line, "Current PostgreSQL revision heads: "); ok {
			parts := strings.Split(rest, ", ")
			sort.Strings(parts)
			lines[index] = "Current PostgreSQL revision heads: " + strings.Join(parts, ", ")
		}
	}
	return strings.Join(lines, "\n")
}

func normalizeAlias(s aliasScenario, text string) string {
	switch s.args[0] {
	case "status":
		return normalizeStatus(text)
	case "current":
		return strings.Join(sortedLines(text), "\n")
	}
	return text
}

func pythonAlias(t *testing.T, uri string, s aliasScenario) (int, string) {
	t.Helper()
	var env []string
	if s.cutover {
		env = append(env, "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1")
	}
	return pythonCLI(t, env, uri, append([]string{"migrate"}, s.args...)...)
}

func TestAliasesMatchTheFrozenPythonOutput(t *testing.T) {
	raw, err := os.ReadFile(aliasGolden)
	if err != nil {
		t.Fatal(err)
	}
	var frozen []aliasResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen) != len(aliasScenarios) {
		t.Fatalf("golden has %d scenarios, the test defines %d", len(frozen), len(aliasScenarios))
	}
	uri, exec := revisionsDatabase(t)
	exec("CREATE TABLE alembic_version_saved AS SELECT * FROM alembic_version")
	pending, unknown := 0, 0
	for index, scenario := range aliasScenarios {
		if scenario.setup != "" {
			exec(aliasSetup(t, scenario.setup))
		}
		code, got := goAlias(t, uri, scenario)
		want := frozen[index]
		if want.Name != scenario.name {
			t.Fatalf("scenario %d is %q, the golden has %q", index, scenario.name, want.Name)
		}
		if code != want.Exit || normalizeAlias(scenario, got) != normalizeAlias(scenario, want.Stdout) {
			t.Errorf("%s: dho exit %d printed %q, Python exit %d printed %q", scenario.name, code, got, want.Exit, want.Stdout)
		}
		if strings.Contains(want.Stdout, "Pending required") {
			pending++
		}
		if want.Exit == 1 && want.Stdout == "" {
			unknown++
		}
		exec("DROP TABLE IF EXISTS alembic_version")
		exec("CREATE TABLE alembic_version AS SELECT * FROM alembic_version_saved")
	}
	if pending < 5 || unknown < 1 {
		t.Errorf("the golden has %d pending states and %d refusals: it measures too little", pending, unknown)
	}
}

// TestAliasesVenueOracleMatchesTheFlatVerbs runs the real `dev-hops migrate heads|
// history|current|status` in every scenario and compares its exit code and text with
// dho's. With DHO_ALIAS_GOLDEN_UPDATE=1 it rewrites the frozen file.
func TestAliasesVenueOracleMatchesTheFlatVerbs(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	uri, exec := revisionsDatabase(t)
	exec("CREATE TABLE alembic_version_saved AS SELECT * FROM alembic_version")
	var frozen []aliasResult
	for _, scenario := range aliasScenarios {
		if scenario.setup != "" {
			exec(aliasSetup(t, scenario.setup))
		}
		wantCode, want := pythonAlias(t, uri, scenario)
		gotCode, got := goAlias(t, uri, scenario)
		if gotCode != wantCode || normalizeAlias(scenario, got) != normalizeAlias(scenario, want) {
			t.Errorf("%s: dho exit %d printed %q, Python exit %d printed %q", scenario.name, gotCode, got, wantCode, want)
		}
		frozen = append(frozen, aliasResult{Name: scenario.name, Exit: wantCode, Stdout: want})
		exec("DROP TABLE IF EXISTS alembic_version")
		exec("CREATE TABLE alembic_version AS SELECT * FROM alembic_version_saved")
	}
	if os.Getenv("DHO_ALIAS_GOLDEN_UPDATE") == "1" {
		body, err := json.MarshalIndent(frozen, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(aliasGolden, append(body, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
	_ = fmt.Sprint
}
