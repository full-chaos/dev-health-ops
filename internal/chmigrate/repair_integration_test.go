//go:build integration

package chmigrate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

type repairDB struct {
	instance *containers.Instance
	httpDSN  string
}

func (db repairDB) do(t *testing.T, statement string) string {
	t.Helper()
	parsed, err := url.Parse(db.httpDSN)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	request, err := http.NewRequest(http.MethodPost, "http://"+parsed.Host+"/?database="+strings.TrimPrefix(parsed.Path, "/"), strings.NewReader(statement))
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth(parsed.User.Username(), password)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("clickhouse %d for %.120s: %s", response.StatusCode, statement, body)
	}
	return string(body)
}

func startRepairDB(t *testing.T) repairDB {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	return repairDB{instance: instance, httpDSN: dsn}
}

const (
	orgOne   = "11111111-0000-4000-8000-000000000001"
	orgTwo   = "22222222-0000-4000-8000-000000000002"
	orgThree = "33333333-0000-4000-8000-000000000003"
)

// seedRepos writes repositories that moved between organizations. The newest
// row (by last_synced) of each id is the active one; the older ones are stale.
func (db repairDB) seedRepos(t *testing.T) {
	t.Helper()
	db.do(t, "TRUNCATE TABLE repos")
	row := func(id, repo, org, lastSynced string) {
		db.do(t, fmt.Sprintf("INSERT INTO repos (id, repo, ref, created_at, settings, tags, last_synced, org_id) VALUES ('%s', '%s', NULL, toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), NULL, NULL, toDateTime64('%s', 3, 'UTC'), '%s')", id, repo, lastSynced, org))
	}
	// One stale row.
	row("a0000000-0000-4000-8000-000000000001", "acme/one", orgOne, "2026-02-01 10:00:00.123")
	row("a0000000-0000-4000-8000-000000000001", "acme/one", orgTwo, "2026-03-01 10:00:00.456")
	// Two stale rows, three organizations.
	row("a0000000-0000-4000-8000-000000000002", "acme/two", orgOne, "2026-02-01 00:00:00")
	row("a0000000-0000-4000-8000-000000000002", "acme/two", orgTwo, "2026-02-15 00:00:00")
	row("a0000000-0000-4000-8000-000000000002", "acme/two", orgThree, "2026-03-15 23:59:59.999")
	// A single organization: never stale.
	row("a0000000-0000-4000-8000-000000000003", "acme/three", orgOne, "2026-03-01 00:00:00")
	// A repository name longer than the column and one that is not ASCII.
	row("a0000000-0000-4000-8000-000000000004", "a-repository-with-a-name-that-is-longer-than-forty-characters/x", orgTwo, "2026-01-01 00:00:00.001")
	row("a0000000-0000-4000-8000-000000000004", "a-repository-with-a-name-that-is-longer-than-forty-characters/x", orgThree, "2026-01-02 00:00:00")
	row("a0000000-0000-4000-8000-000000000005", "café/日本語", orgOne, "2026-01-01 00:00:00")
	row("a0000000-0000-4000-8000-000000000005", "café/日本語", orgTwo, "2026-01-01 00:00:01")
}

// state is the rows left in repos, as the check of what --apply did.
func (db repairDB) state(t *testing.T) string {
	return strings.TrimSpace(db.do(t, "SELECT toString(id), org_id FROM repos ORDER BY id, org_id FORMAT TSV"))
}

type repairScenario struct {
	name  string
	args  []string
	empty bool // no duplicates seeded
	env   []string
}

var repairScenarios = []repairScenario{
	{name: "dry run", args: nil},
	{name: "dry run for the org that owns the newest row of one group", args: []string{"--org", orgTwo}},
	{name: "dry run for the org that owns the newest row of the other group", args: []string{"--org", orgThree}},
	{name: "dry run for an org that owns no newest row", args: []string{"--org", orgOne}},
	{name: "an explicit empty org is no filter", args: []string{"--org", ""}},
	{name: "no duplicates", args: nil, empty: true},
	{name: "apply", args: []string{"--apply"}},
	{name: "apply for one org", args: []string{"--apply", "--org", orgThree}},
	{name: "apply with nothing to delete", args: []string{"--apply"}, empty: true},
}

type repairResult struct {
	Name   string `json:"name"`
	Exit   int    `json:"exit"`
	Stdout string `json:"stdout"`
	State  string `json:"state"`
}

func (db repairDB) reset(t *testing.T, s repairScenario) {
	t.Helper()
	if s.empty {
		db.do(t, "TRUNCATE TABLE repos")
		db.do(t, "INSERT INTO repos (id, repo, ref, created_at, settings, tags, last_synced, org_id) VALUES ('a0000000-0000-4000-8000-000000000009', 'solo/repo', NULL, toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), NULL, NULL, toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), '"+orgOne+"')")
		return
	}
	db.seedRepos(t)
}

func (db repairDB) goRun(t *testing.T, s repairScenario) repairResult {
	t.Helper()
	env := map[string]string{"CLICKHOUSE_URI": db.instance.URI}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var repair func(context.Context, cli.Env) int
	for _, child := range chmigrate.Command().Children {
		if child.Name == "repair" {
			repair = child.Run
		}
	}
	var stdout, stderr bytes.Buffer
	code := repair(context.Background(), cli.Env{Args: s.args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stdout.String()+stderr.String(), "clickhouse://") {
		t.Fatal("the output carries the DSN")
	}
	if code != 0 {
		t.Fatalf("%s: exit %d, stderr %s", s.name, code, stderr.String())
	}
	return repairResult{Name: s.name, Exit: code, Stdout: stdout.String(), State: db.state(t)}
}

func (db repairDB) pythonRun(t *testing.T, s repairScenario) repairResult {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	command := exec.Command(python, append([]string{"-c", program, "migrate", "clickhouse", "repair"}, s.args...)...)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "CLICKHOUSE_URI="+db.httpDSN, "OTEL_ENABLED=false")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("run python: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
	}
	if code != 0 {
		t.Fatalf("%s: python exit %d, stderr %s", s.name, code, stderr.String())
	}
	return repairResult{Name: s.name, Exit: code, Stdout: stdout.String(), State: db.state(t)}
}

const repairGolden = "testdata/repair_golden.json"

// repairGoldenSHA256 pins testdata/repair_golden.json (R24): the report and the
// rows left for every scenario, written by the real `dev-hops migrate clickhouse
// repair`. The producer is deleted with the Python CLI, so this is a rot guard,
// not a freshness check: the file is only rewritten by
// TestRepairVenueOracleMatchesThePythonProducer with DHO_REPAIR_GOLDEN_UPDATE=1,
// then this digest is updated.
const repairGoldenSHA256 = "71881e8263ab0d427ed0b4e98040d079659c9c6a4fef10baf0acf2e045eab051"

func TestRepairGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(repairGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != repairGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", repairGolden, got, repairGoldenSHA256)
	}
}

// TestRepairMatchesTheFrozenPythonOutput runs the verb on a real ClickHouse and
// compares the report and the rows left with what the real Python verb left for
// the same rows (frozen; no Python needed).
func TestRepairMatchesTheFrozenPythonOutput(t *testing.T) {
	raw, err := os.ReadFile(repairGolden)
	if err != nil {
		t.Fatal(err)
	}
	var frozen []repairResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen) != len(repairScenarios) {
		t.Fatalf("golden has %d scenarios, the test defines %d", len(frozen), len(repairScenarios))
	}
	db := startRepairDB(t)
	for index, s := range repairScenarios {
		db.reset(t, s)
		got := db.goRun(t, s)
		want := frozen[index]
		if got.Stdout != want.Stdout || got.State != want.State {
			t.Errorf("%s: dho\n%s\nstate %q\nfrozen Python\n%s\nstate %q", s.name, got.Stdout, got.State, want.Stdout, want.State)
		}
	}
	// A comparison of two empty reports passes for any implementation.
	found := 0
	for _, item := range frozen {
		if strings.Contains(item.Stdout, "Found 5 stale duplicate row(s)") || strings.Contains(item.Stdout, "Deleted 5 stale") {
			found++
		}
	}
	if found < 2 {
		t.Fatalf("the golden records the five seeded stale rows in only %d scenarios: it measures nothing", found)
	}
}

// TestRepairVenueOracleMatchesThePythonProducer runs every scenario through the
// real `dev-hops migrate clickhouse repair` and through dho, and compares the
// report and the rows left. With DHO_REPAIR_GOLDEN_UPDATE=1 it rewrites the frozen file.
func TestRepairVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	db := startRepairDB(t)
	var frozen []repairResult
	for _, s := range repairScenarios {
		db.reset(t, s)
		py := db.pythonRun(t, s)
		db.reset(t, s)
		got := db.goRun(t, s)
		if py.Stdout != got.Stdout || py.State != got.State {
			t.Errorf("%s: python\n%s\nstate %q\ndho\n%s\nstate %q", s.name, py.Stdout, py.State, got.Stdout, got.State)
		}
		frozen = append(frozen, py)
	}
	if os.Getenv("DHO_REPAIR_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(frozen, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(repairGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}
