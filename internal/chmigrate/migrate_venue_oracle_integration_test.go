//go:build integration

package chmigrate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The executed proof of `dho migrate clickhouse upgrade|status` (CHAOS-6899):
// the same scratch ClickHouse states go through the real Python verbs
// (`dev-hops migrate clickhouse upgrade|status [--check]`, whose runner is the
// chain that produced the checked-in baseline) and through dho, and what is
// compared is what an operator or a wait-for-migrations probe reads: the exit
// code, the schema each leaves (every object's CREATE statement, the seed rows
// and the recorded versions, by digest), and the applied / pending versions.
//
// RULED named divergences (not findings), each pinned as the two planes' own
// answers so it cannot change unseen:
//   - the output format: Python prints a text list and a log line, dho prints
//     one JSON document; the applied and pending versions are read out of both;
//   - dho refuses what Python would apply on top: a database below the head
//     (some baseline versions unrecorded), a database holding objects the
//     baseline does not create and no versions (foreign) and an ordering
//     contract other than 2 -- all before writing anything;
//   - `status --check` on a database whose versions are all recorded but
//     lacks a baseline object: dho reports schema_mismatch and exits 1
//     (it reads the schema), Python reports nothing pending and exits 0.

// migrateFact is what one verb run showed.
type migrateFact struct {
	Name string `json:"name"`
	Exit int    `json:"exit"`
	// AppliedN and PendingN count the versions Python's status listed, and
	// AppliedDigests / PendingDigests are the sorted sha256 of every one of
	// their names: the whole set is compared, but a golden that spelled the
	// file names would hold strings a secret scanner reads as keys
	// (065_llm_token_usage_run_id.sql). Go's side is computed the same way from
	// the database's own version rows and dho's status.
	AppliedN       int      `json:"applied"`
	PendingN       int      `json:"pending"`
	AppliedDigests []string `json:"applied_digests,omitempty"`
	PendingDigests []string `json:"pending_digests,omitempty"`
	Total          int      `json:"total,omitempty"`
	// Tables lists the named tables a database still holds after the run.
	Tables []string `json:"tables,omitempty"`
	// Schema is the digest of the database after an upgrade (objects, seed
	// rows, versions), the empty string for a run that is not an upgrade.
	Schema string `json:"schema,omitempty"`
	// Error is the machine-readable error code dho answered with, "" for
	// Python (whose error text is a traceback, not a contract).
	Error string `json:"error,omitempty"`
	// State is dho's status state, "" for Python.
	State string `json:"state,omitempty"`
}

const (
	migrateGolden       = "testdata/migrate_golden.json"
	migrateGoldenSHA256 = "0365fa5b91403320e81e12b073c4c7981ba3d7c7ca9338a754c75d6e0cc422ee"
)

var (
	pythonAppliedLine = regexp.MustCompile(`^\s+\[applied [^\]]*\]\s+(\S+)\s*$`)
	pythonPendingLine = regexp.MustCompile(`^\s+\[pending\]\s+(\S+)\s*$`)
	pythonSummaryLine = regexp.MustCompile(`^(\d+) applied, (\d+) pending, (\d+) total$`)
)

// parsePythonStatus reads the applied and pending versions out of Python's
// `migrate clickhouse status` text and requires its own summary line to agree
// with them.
func parsePythonStatus(t *testing.T, stdout string) (applied, pending []string, total int) {
	t.Helper()
	summary := ""
	for _, line := range strings.Split(stdout, "\n") {
		switch {
		case pythonAppliedLine.MatchString(line):
			applied = append(applied, pythonAppliedLine.FindStringSubmatch(line)[1])
		case pythonPendingLine.MatchString(line):
			pending = append(pending, pythonPendingLine.FindStringSubmatch(line)[1])
		case pythonSummaryLine.MatchString(line):
			summary = line
		}
	}
	match := pythonSummaryLine.FindStringSubmatch(summary)
	if match == nil {
		t.Fatalf("python status printed no summary line:\n%s", stdout)
	}
	a, _ := strconv.Atoi(match[1])
	p, _ := strconv.Atoi(match[2])
	total, _ = strconv.Atoi(match[3])
	if a != len(applied) || p != len(pending) || total != a+p {
		t.Fatalf("python status summary %q disagrees with its own list (%d applied, %d pending):\n%s", summary, len(applied), len(pending), stdout)
	}
	sort.Strings(applied)
	sort.Strings(pending)
	return applied, pending, total
}

// migrateEnv is one database on the shared server, seen by both planes.
type migrateEnv struct {
	instance *containers.Instance
	admin    driver.Conn
	baseline chmigrate.Baseline
	chain    []chmigrate.ChainFile
	frozen   map[string]migrateFact
	live     bool
	recorded []migrateFact
}

func (e *migrateEnv) nativeURI(t *testing.T, database string) string {
	t.Helper()
	parsed, err := url.Parse(e.instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + database
	return parsed.String()
}

// goVerb runs `dho migrate clickhouse <verb>` on database.
func (e *migrateEnv) goVerb(t *testing.T, database, verb, contract string, args ...string) (int, string, string) {
	t.Helper()
	var run func(context.Context, cli.Env) int
	for _, child := range chmigrate.Command().Children {
		if child.Name == verb {
			run = child.Run
		}
	}
	env := map[string]string{chmigrate.ClickHouseURIKey: e.nativeURI(t, database)}
	if contract != "" {
		env[chmigrate.OrderingContractEnv] = contract
	}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stdout.String()+stderr.String(), "clickhouse://") {
		t.Fatal("the dho output carries the DSN")
	}
	return code, stdout.String(), stderr.String()
}

// pythonVerb runs the real `dev-hops migrate clickhouse <verb>` on database.
func (e *migrateEnv) pythonVerb(t *testing.T, database, verb, contract string, args ...string) (int, string, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	command := exec.Command(python, append([]string{"-c", program, "migrate", "clickhouse", verb}, args...)...)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "CLICKHOUSE_URI="+httpDSN(t, context.Background(), e.instance, database), "OTEL_ENABLED=false")
	if contract != "" {
		command.Env = append(command.Env, chmigrate.OrderingContractEnv+"="+contract)
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	code := 0
	if err := command.Run(); err != nil {
		exit, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run python: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
		}
		code = exit.ExitCode()
	}
	return code, stdout.String(), stderr.String()
}

// schemaDigest is the digest of the database as an upgrade leaves it.
func (e *migrateEnv) schemaDigest(t *testing.T, database string) string {
	t.Helper()
	captured := capture(t, context.Background(), openDatabase(t, e.instance.URI, database), database, productionContract)
	raw, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (e *migrateEnv) newDatabase(t *testing.T) string { return scratchDatabase(t, e.admin) }

// pythonFact is the Python plane's fact for the step: executed while
// recording (and comparing live), read from the golden otherwise.
func (e *migrateEnv) pythonFact(t *testing.T, name string, run func() migrateFact) migrateFact {
	t.Helper()
	if e.live {
		fact := run()
		fact.Name = name
		e.recorded = append(e.recorded, fact)
		return fact
	}
	fact, ok := e.frozen[name]
	if !ok {
		t.Fatalf("%s: the golden holds no Python fact for this step; regenerate it with DHO_MIGRATE_GOLDEN_UPDATE=1", name)
	}
	return fact
}

func (e *migrateEnv) pythonStatus(t *testing.T, database string, args ...string) migrateFact {
	t.Helper()
	code, stdout, stderr := e.pythonVerb(t, database, "status", "2", args...)
	if code != 0 && code != 1 {
		t.Fatalf("python status exit %d: %s", code, stderr)
	}
	applied, pending, total := parsePythonStatus(t, stdout)
	return migrateFact{
		Exit: code, AppliedN: len(applied), PendingN: len(pending), Total: total,
		AppliedDigests: nameDigests(applied), PendingDigests: nameDigests(pending),
	}
}

func (e *migrateEnv) goStatus(t *testing.T, database string, args ...string) (migrateFact, chmigrate.Status) {
	t.Helper()
	code, stdout, stderr := e.goVerb(t, database, "status", "2", args...)
	var status chmigrate.Status
	if err := json.Unmarshal([]byte(stdout), &status); err != nil {
		t.Fatalf("dho status printed %q (%v), stderr %q", stdout, err, stderr)
	}
	return migrateFact{Exit: code, State: status.State}, status
}

// requireSameStatus holds dho's status of a database to Python's status of the
// same state: the exit code, the count of applied versions and the pending
// ones. wantExit overrides the exit code dho must answer with where a named
// divergence applies.
func (e *migrateEnv) requireSameStatus(t *testing.T, name, database string, py migrateFact, goFact migrateFact, status chmigrate.Status, wantGoExit int) {
	t.Helper()
	goApplied, goPending := e.goVersionSets(t, database, status)
	if !reflect.DeepEqual(goApplied, py.AppliedDigests) {
		t.Errorf("%s: the applied versions differ (dho's database %d rows, python %d listed)", name, len(goApplied), py.AppliedN)
	}
	if !reflect.DeepEqual(goPending, py.PendingDigests) {
		t.Errorf("%s: the pending versions differ (dho %d, python %d listed)", name, len(goPending), py.PendingN)
	}
	if py.Total != py.AppliedN+py.PendingN {
		t.Fatalf("%s: python's own list does not add up: %+v", name, py)
	}
	if goFact.Exit != wantGoExit {
		t.Errorf("%s: dho exit %d, want %d (python exit %d)", name, goFact.Exit, wantGoExit, py.Exit)
	}
	if status.Applied != py.AppliedN {
		t.Errorf("%s: dho reports %d applied, python %d", name, status.Applied, py.AppliedN)
	}
}

// nameDigests is the sorted sha256 of each name (nil for none).
func nameDigests(names []string) []string {
	var out []string
	for _, name := range names {
		sum := sha256.Sum256([]byte(name))
		out = append(out, hex.EncodeToString(sum[:]))
	}
	sort.Strings(out)
	return out
}

// goVersionSets is dho's side of the applied and pending version sets, as the
// same digests: the applied versions are the database's own rows, the pending
// ones what dho's status names (every known version for a database with none
// recorded, else the missing baseline versions and the chain files after the
// head).
func (e *migrateEnv) goVersionSets(t *testing.T, database string, status chmigrate.Status) (applied, pending []string) {
	t.Helper()
	conn := openDatabase(t, e.instance.URI, database)
	var exists uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.tables WHERE database = ? AND name = ?", database, chmigrate.SchemaMigrationsTable).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	var recorded []string
	if exists == 1 {
		rows, err := conn.Query(context.Background(), "SELECT version FROM `"+database+"`.schema_migrations ORDER BY version")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var version string
			if err := rows.Scan(&version); err != nil {
				t.Fatal(err)
			}
			recorded = append(recorded, version)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	var waiting []string
	switch status.State {
	case "empty", "foreign":
		waiting = append(waiting, e.baseline.Versions...)
		for _, file := range e.chain {
			waiting = append(waiting, file.Version)
		}
	default:
		waiting = append(append(waiting, status.Missing...), status.Pending...)
	}
	return nameDigests(recorded), nameDigests(waiting)
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func newMigrateEnv(t *testing.T, live bool) *migrateEnv {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	env := &migrateEnv{instance: instance, admin: openDatabase(t, instance.URI, ""), baseline: baseline, chain: chain, live: live, frozen: map[string]migrateFact{}}
	if !live {
		raw, err := os.ReadFile(migrateGolden)
		if err != nil {
			t.Fatal(err)
		}
		var facts []migrateFact
		if err := json.Unmarshal(raw, &facts); err != nil {
			t.Fatal(err)
		}
		for _, fact := range facts {
			env.frozen[fact.Name] = fact
		}
	}
	return env
}

func TestMigrateGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(migrateGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != migrateGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", migrateGolden, got, migrateGoldenSHA256)
	}
}

// runMigrateScenarios is the whole proof: live it runs both planes and
// compares them (and records the Python facts), frozen it runs dho against
// the recorded Python facts.
func runMigrateScenarios(t *testing.T, live bool) *migrateEnv {
	t.Helper()
	ctx := context.Background()
	e := newMigrateEnv(t, live)
	totalVersions := len(e.baseline.Versions) + len(e.chain)

	// ---- status of a database no verb has touched ----
	emptyPy, emptyGo := e.newDatabase(t), e.newDatabase(t)
	for _, args := range [][]string{{"--check"}, nil} {
		name := "status empty" + strings.Join(append([]string{""}, args...), " ")
		py := e.pythonFact(t, name, func() migrateFact { return e.pythonStatus(t, emptyPy, args...) })
		goFact, status := e.goStatus(t, emptyGo, args...)
		wantExit := py.Exit // both exit 1 under --check (all pending), 0 without
		e.requireSameStatus(t, name, emptyGo, py, goFact, status, wantExit)
		if status.State != "empty" || py.PendingN != totalVersions || py.Total != totalVersions {
			t.Errorf("%s: dho %s, python %d pending of %d, want empty and all %d pending", name, status.State, py.PendingN, py.Total, totalVersions)
		}
	}

	// ---- status of a foreign database (an unrelated table, no versions) ----
	foreignPy, foreignGo := e.newDatabase(t), e.newDatabase(t)
	for _, database := range []string{foreignPy, foreignGo} {
		if e.live || database == foreignGo {
			if err := openDatabase(t, e.instance.URI, database).Exec(ctx, "CREATE TABLE unrelated_sentinel (x Int8) ENGINE = Memory"); err != nil {
				t.Fatal(err)
			}
		}
	}
	name := "status foreign --check"
	py := e.pythonFact(t, name, func() migrateFact { return e.pythonStatus(t, foreignPy, "--check") })
	goFact, status := e.goStatus(t, foreignGo, "--check")
	e.requireSameStatus(t, name, foreignGo, py, goFact, status, py.Exit)
	if status.State != "foreign" || py.PendingN != totalVersions {
		t.Errorf("%s: dho %s, python %d pending, want foreign and all %d pending", name, status.State, py.PendingN, totalVersions)
	}

	// ---- upgrade a fresh database ----
	pyHead, goHead := e.newDatabase(t), e.newDatabase(t)
	up := e.pythonFact(t, "upgrade fresh", func() migrateFact {
		code, _, stderr := e.pythonVerb(t, pyHead, "upgrade", "2")
		if code != 0 {
			t.Fatalf("python upgrade of a fresh database exit %d: %s", code, stderr)
		}
		return migrateFact{Exit: code, Schema: e.schemaDigest(t, pyHead)}
	})
	code, stdout, stderr := e.goVerb(t, goHead, "upgrade", "2")
	if code != 0 {
		t.Fatalf("dho upgrade of a fresh database exit %d: %s%s", code, stdout, stderr)
	}
	var result chmigrate.Result
	if err := json.Unmarshal([]byte(stdout), &result); err != nil || result.Action != "baseline_applied" {
		t.Fatalf("dho upgrade printed %q (%v), want baseline_applied", stdout, err)
	}
	if got := e.schemaDigest(t, goHead); got != up.Schema || up.Exit != code {
		t.Errorf("upgrade fresh: dho left digest %s exit %d, python %s exit %d: the two runners built different databases", got, code, up.Schema, up.Exit)
	}

	// ---- status at the head, each database built by the other plane's runner too ----
	for _, args := range [][]string{{"--check"}, nil} {
		name := "status head" + strings.Join(append([]string{""}, args...), " ")
		py := e.pythonFact(t, name, func() migrateFact { return e.pythonStatus(t, pyHead, args...) })
		goFact, status := e.goStatus(t, goHead, args...)
		e.requireSameStatus(t, name, goHead, py, goFact, status, py.Exit)
		if status.State != "at_head" || py.Exit != 0 || py.PendingN != 0 || py.AppliedN != totalVersions {
			t.Errorf("%s: dho %s, python exit %d with %d pending, %d applied; want at_head, exit 0, none pending, all %d applied", name, status.State, py.Exit, py.PendingN, py.AppliedN, totalVersions)
		}
	}
	// Python's own reading of the database dho built: nothing pending.
	crossPy := e.pythonFact(t, "status python reads the dho-built database", func() migrateFact { return e.pythonStatus(t, goHead, "--check") })
	if crossPy.Exit != 0 || crossPy.PendingN != 0 {
		t.Errorf("python reads the dho-built database as %+v: its runner would still apply migrations", crossPy)
	}

	// ---- upgrade is a no-op on a database the other runner built ----
	noopGo := e.pythonFact(t, "upgrade python on the dho-built database", func() migrateFact {
		before := e.schemaDigest(t, goHead)
		code, _, stderr := e.pythonVerb(t, goHead, "upgrade", "2")
		if code != 0 {
			t.Fatalf("python upgrade over the dho-built database exit %d: %s", code, stderr)
		}
		return migrateFact{Exit: code, Schema: before + "=" + e.schemaDigest(t, goHead)}
	})
	if halves := strings.Split(noopGo.Schema, "="); noopGo.Exit != 0 || len(halves) != 2 || halves[0] != halves[1] || halves[0] != up.Schema {
		t.Errorf("python's upgrade changed the dho-built database or it differs from python's own: %+v (want %s)", noopGo, up.Schema)
	}
	code, stdout, stderr = e.goVerb(t, goHead, "upgrade", "2")
	var again chmigrate.Result
	if err := json.Unmarshal([]byte(stdout), &again); code != 0 || err != nil || again.Action != "up_to_date" || e.schemaDigest(t, goHead) != up.Schema {
		t.Errorf("dho upgrade at the head: exit %d, %q (%v) %s: want up_to_date and the same database", code, stdout, err, stderr)
	}

	// ---- below the head: one version unrecorded ----
	dropped := e.baseline.Versions[len(e.baseline.Versions)/2]
	for _, database := range []string{pyHead, goHead} {
		if e.live || database == goHead {
			if err := openDatabase(t, e.instance.URI, database).Exec(ctx, "ALTER TABLE schema_migrations DELETE WHERE version = ? SETTINGS mutations_sync = 2", dropped); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, args := range [][]string{{"--check"}, nil} {
		name := "status below head" + strings.Join(append([]string{""}, args...), " ")
		py := e.pythonFact(t, name, func() migrateFact { return e.pythonStatus(t, pyHead, args...) })
		goFact, status := e.goStatus(t, goHead, args...)
		e.requireSameStatus(t, name, goHead, py, goFact, status, py.Exit)
		if status.State != "below_head" || !reflect.DeepEqual(py.PendingDigests, nameDigests([]string{dropped})) || !reflect.DeepEqual(status.Missing, []string{dropped}) {
			t.Errorf("%s: dho %s missing %v, python pending %v, want below_head naming only %s", name, status.State, status.Missing, py.PendingDigests, dropped)
		}
	}
	// Named divergence: dho refuses to upgrade a database below the head and
	// writes nothing; the Python runner applies the missing migration.
	before := e.schemaDigest(t, goHead)
	code, stdout, stderr = e.goVerb(t, goHead, "upgrade", "2")
	if code != cli.ExitFailure || !strings.Contains(stderr, `"code":"below_head"`) || stdout != "" || e.schemaDigest(t, goHead) != before {
		t.Errorf("dho upgrade below the head: exit %d, stdout %q, stderr %q, database changed %v: want a below_head refusal that writes nothing", code, stdout, stderr, e.schemaDigest(t, goHead) != before)
	}
	pyBelow := e.pythonFact(t, "upgrade python below the head", func() migrateFact {
		code, _, stderr := e.pythonVerb(t, pyHead, "upgrade", "2")
		if code != 0 {
			t.Fatalf("python upgrade below the head exit %d: %s", code, stderr)
		}
		return migrateFact{Exit: code, Schema: e.schemaDigest(t, pyHead)}
	})
	if pyBelow.Exit != 0 || pyBelow.Schema != up.Schema {
		t.Errorf("python re-applied the missing migration and left %+v, want exit 0 and the head database %s", pyBelow, up.Schema)
	}
	if err := openDatabase(t, e.instance.URI, goHead).Exec(ctx, "INSERT INTO schema_migrations (version, applied_at) VALUES (?, now64(3))", dropped); err != nil {
		t.Fatal(err)
	}

	// ---- a dropped baseline object with every version recorded ----
	var view chmigrate.Object
	for _, object := range e.baseline.Objects {
		if object.IsView() {
			view = object
			break
		}
	}
	for _, database := range []string{pyHead, goHead} {
		if e.live || database == goHead {
			if err := openDatabase(t, e.instance.URI, database).Exec(ctx, "DROP VIEW "+view.Name); err != nil {
				t.Fatal(err)
			}
		}
	}
	name = "status schema mismatch --check"
	py = e.pythonFact(t, name, func() migrateFact { return e.pythonStatus(t, pyHead, "--check") })
	goFact, status = e.goStatus(t, goHead, "--check")
	// Named divergence: Python reads only the version rows (exit 0, nothing
	// pending); dho reads the schema too and exits 1.
	e.requireSameStatus(t, name, goHead, py, goFact, status, cli.ExitFailure)
	if status.State != "schema_mismatch" || !reflect.DeepEqual(status.MissingObjects, []string{view.Name}) || py.Exit != 0 || py.PendingN != 0 {
		t.Errorf("%s: dho %s missing objects %v, python exit %d pending %v; want schema_mismatch naming %s and python blind to it", name, status.State, status.MissingObjects, py.Exit, py.PendingN, view.Name)
	}

	// ---- foreign: dho refuses, Python applies the chain on top ----
	before = e.schemaDigest2(t, foreignGo)
	code, stdout, stderr = e.goVerb(t, foreignGo, "upgrade", "2")
	if code != cli.ExitFailure || !strings.Contains(stderr, `"code":"foreign_database"`) || stdout != "" || e.schemaDigest2(t, foreignGo) != before {
		t.Errorf("dho upgrade over a foreign database: exit %d, stdout %q, stderr %q: want a foreign_database refusal that writes nothing", code, stdout, stderr)
	}
	pyForeign := e.pythonFact(t, "upgrade python over a foreign database", func() migrateFact {
		code, _, stderr := e.pythonVerb(t, foreignPy, "upgrade", "2")
		if code != 0 {
			t.Fatalf("python upgrade over a foreign database exit %d: %s", code, stderr)
		}
		return migrateFact{Exit: code, Tables: e.tables(t, foreignPy, "unrelated_sentinel")}
	})
	if pyForeign.Exit != 0 || !reflect.DeepEqual(pyForeign.Tables, []string{"unrelated_sentinel"}) {
		t.Errorf("python over a foreign database: %+v, want exit 0 with the unrelated table kept", pyForeign)
	}

	// ---- an ordering contract other than 2: dho refuses before connecting ----
	for _, contract := range []string{"", "1", "3", "x"} {
		code, stdout, stderr := e.goVerb(t, e.newDatabase(t), "upgrade", contract)
		wantCode := `"code":"settings_mismatch"` // unset and 1 are the legacy contract
		if contract == "3" || contract == "x" {
			wantCode = `"code":"configuration_error"`
		}
		if code != cli.ExitFailure || stdout != "" || !strings.Contains(stderr, wantCode) {
			t.Errorf("dho upgrade with OPERATIONAL_ORDERING_CONTRACT=%s: exit %d, stdout %q, stderr %q, want a refusal with %s", contract, code, stdout, stderr, wantCode)
		}
	}
	return e
}

// tables lists which of the named tables exist in database.
func (e *migrateEnv) tables(t *testing.T, database string, names ...string) []string {
	t.Helper()
	var found []string
	for _, name := range names {
		var count uint64
		if err := openDatabase(t, e.instance.URI, database).QueryRow(context.Background(), "SELECT count() FROM system.tables WHERE database = ? AND name = ?", database, name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			found = append(found, name)
		}
	}
	return found
}

// schemaDigest2 is the digest of the objects a database holds, whatever they are
// (a refused upgrade leaves a database that is not a migrated one).
func (e *migrateEnv) schemaDigest2(t *testing.T, database string) string {
	t.Helper()
	rows, err := openDatabase(t, e.instance.URI, database).Query(context.Background(), "SELECT name, engine, create_table_query FROM system.tables WHERE database = ? ORDER BY name", database)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var builder strings.Builder
	for rows.Next() {
		var name, engine, create string
		if err := rows.Scan(&name, &engine, &create); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&builder, "%s|%s|%s\n", name, engine, create)
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(sum[:])
}

// TestMigrateClickHouseVenueOracleMatchesThePythonRunner runs the scenarios
// through the real Python verbs and through dho on a scratch ClickHouse. With
// DHO_MIGRATE_GOLDEN_UPDATE=1 it rewrites the frozen file.
func TestMigrateClickHouseVenueOracleMatchesThePythonRunner(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	e := runMigrateScenarios(t, true)
	if os.Getenv("DHO_MIGRATE_GOLDEN_UPDATE") == "1" && !t.Failed() {
		raw, err := json.MarshalIndent(e.recorded, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(migrateGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}

// TestMigrateClickHouseMatchesTheFrozenPythonOutput runs dho through the same
// scenarios against the facts the real Python verbs produced (frozen; no
// Python needed).
func TestMigrateClickHouseMatchesTheFrozenPythonOutput(t *testing.T) {
	e := runMigrateScenarios(t, false)
	// A comparison against an empty golden passes for any implementation.
	if len(e.frozen) < 12 {
		t.Fatalf("the golden holds %d Python facts: it measures nothing", len(e.frozen))
	}
	if e.frozen["upgrade fresh"].Schema == "" || e.frozen["status head --check"].AppliedN == 0 || len(e.frozen["status head --check"].AppliedDigests) != e.frozen["status head --check"].AppliedN {
		t.Fatal("the golden holds no upgrade digest or no applied versions: it measures nothing")
	}
}
