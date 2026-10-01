//go:build integration

package routing

// The differential oracle of `dho goapi routing disable|status` against the REAL
// Python verbs (`dev-hops go-api routing disable|status`, dev_health_ops.cli.main
// with the real plan_disable/apply_disable and routing_status_rows): the same routing
// state in two databases, the same command line through each producer, and the state
// the verbs exist to reach -- the rows of go_api_routing_state, read back -- compared
// column by column, with the plan the operator reads before typing --apply and the
// status JSON compared on the keys both name.
//
// Named differences (each is asserted, not skipped):
//   - dho requires --recorded-by and --review-evidence on --apply (Python takes the
//     operator from $DEV_HOPS_OPERATOR / $SUDO_USER / $USER); the oracle passes both
//     the same value, and a scenario asserts dho's refusal when they are missing.
//   - the plan lines of dho carry the row's document digest in a last column.
//   - dho writes an audit row per applied change (routingauditschema); Python does not.
//   - a refusal exits 3 in dho (the dispatcher's ExitRefused, main.go's fold note) and 2 in Python;
//     Python's argparse usage errors are exit 2 there too, and dho reports them as refusals.
//   - `disable` keys its UPDATE on the row's own document digest, so dho turns off a row serving a
//     document the catalog does not name (DOCUMENT_DRIFT) and Python leaves it untouched (tightening 2
//     of main.go); the scenario asserts both sides.
//   - `--candidate-build` over an operation whose only live row serves a document the catalog does not name:
//     Python has no catalog row to compare the guard with and moves on; dho SKIPS that operation, names it,
//     applies the others and exits 3 (DeadRowsOnly). With a catalog row AND a drifted row the guard is
//     checked against the catalog row's build alone by both, and dho also turns the drifted row off,
//     unguarded. The scenarios assert both.
//   - `status` when the planes disagree classifies against the DEPLOYED plane's digest (PENDING /
//     MATCH), Python against its own; and dho reports a drifted live row on the catalog row's
//     `unreachable_document_digests`, where Python lists it as its own DOCUMENT_DRIFT entry. A live row
//     of an operation the catalog does not register is listed alike by both (UNREGISTERED).
//     The status states below assert both.
//
// The Python verbs' answers were executed once on routingPythonBuild and are frozen in
// testdata/golden/routing.json (the recipe regenerates them by execution): no Python runs in the test.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	oracleOperator = "venue-oracle"
	oracleEvidence = "venue oracle run"
	oracleBuildA   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	oracleBuildB   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// oracleRow is one seeded row of go_api_routing_state (and the candidate build it
// references).
type oracleRow struct {
	schema, document, operation, build, mode string
	rollout                                  int
}

type oraclePlane struct {
	pool *pgxpool.Pool
	dsn  string
}

func (p oraclePlane) seed(t *testing.T, rows []oracleRow) {
	t.Helper()
	ctx := context.Background()
	if _, err := p.pool.Exec(ctx, `TRUNCATE go_api_routing_state, go_api_candidate_build CASCADE`); err != nil {
		t.Fatal(err)
	}
	// The audit table is Go's own: cleared so a scenario reads only its own rows.
	if _, err := p.pool.Exec(ctx, `TRUNCATE go_api_routing_audits`); err != nil {
		t.Logf("audit table not cleared: %v", err)
	}
	for _, row := range rows {
		if _, err := p.pool.Exec(ctx, `INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
			VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, row.schema, row.document, row.operation, row.build); err != nil {
			t.Fatal(err)
		}
		if _, err := p.pool.Exec(ctx, `INSERT INTO go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by, updated_at)
			VALUES ($1,$2,$3,$4,'go',$5,$6,'seeded','seed','2026-01-01T00:00:00Z')`,
			row.schema, row.document, row.operation, row.build, row.mode, row.rollout); err != nil {
			t.Fatal(err)
		}
	}
}

// state dumps every column the verbs decide, and whether updated_at moved.
func (p oraclePlane) state(t *testing.T) []string {
	t.Helper()
	rows, err := p.pool.Query(context.Background(), `SELECT schema_digest, document_digest, selected_operation, current_candidate_build,
		owner, mode, coalesce(eligible_orgs::text,'<null>'), rollout_percentage, coalesce(review_evidence,'<null>'), coalesce(recorded_by,'<null>'),
		updated_at > '2026-01-01T00:00:01Z'::timestamptz FROM go_api_routing_state ORDER BY 1,2,3`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var schema, document, operation, build, owner, mode, eligible, evidence, by string
		var rollout int
		var moved bool
		if err := rows.Scan(&schema, &document, &operation, &build, &owner, &mode, &eligible, &rollout, &evidence, &by, &moved); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%d|%s|%s|moved=%v", schema, document, operation, build, owner, mode, eligible, rollout, evidence, by, moved))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// routingPythonBuild is the build whose Python `dev-hops go-api routing` verbs answered the scenarios: a build
// that still carried the Python CLI.
const routingPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// routingPythonProgram is the entry point the producer runs: the real dev-hops CLI.
const routingPythonProgram = "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"

// routingPythonSettings are the variables that shape the producer's answers, as constants: the producer's
// environment AND part of the golden's request key (a changed value fails the frozen replay). The database
// address is a per-run value, appended by name.
var routingPythonSettings = map[string]string{"PYTHONHASHSEED": "0", "OTEL_ENABLED": "false", "DEV_HOPS_OPERATOR": oracleOperator, "GO_API_QUERY_API_URL": ""}

func routingPythonEnv(root string) []string {
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "PYTHONPATH=" + filepath.Join(root, "src"), "PYTHONDONTWRITEBYTECODE=1"}
	names := make([]string, 0, len(routingPythonSettings))
	for name := range routingPythonSettings {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		env = append(env, name+"="+routingPythonSettings[name])
	}
	return env
}

func oraclePython(t *testing.T, root string, plane oraclePlane, args ...string) (int, string, string) {
	t.Helper()
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, append([]string{"-c", routingPythonProgram, "go-api", "routing"}, args...)...)
	uri := strings.Replace(plane.dsn, "postgres://", "postgresql://", 1)
	command.Env = append(routingPythonEnv(root), "POSTGRES_URI="+uri, "DATABASE_URI="+uri)
	var stdout, stderr strings.Builder
	command.Stdout, command.Stderr = &stdout, &stderr
	code := 0
	if err := command.Run(); err != nil {
		exit, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run python: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
		}
		code = exit.ExitCode()
	}
	if strings.Contains(stderr.String(), "Traceback") {
		t.Fatalf("the Python verb crashed: %s", stderr.String())
	}
	return code, stdout.String(), stderr.String()
}

func oracleGo(t *testing.T, plane oraclePlane, catalogPath string, args ...string) (int, string, string) {
	t.Helper()
	argv := append(append([]string{}, args...), "-postgres-uri", plane.dsn, "-catalog", catalogPath)
	out, errOut, err := captureVerb(t, argv...)
	return exitCodeFor(err), out, errOut
}

var (
	noRowToken   = regexp.MustCompile(`\(no row\)`)
	dryRunCounts = regexp.MustCompile(`^DRY RUN: (\d+) row\(s\) would change, (\d+) unchanged`)
)

// planLines reduces the plan an operator reads to what both producers print: the
// operation, its current and new mode, the build and whether nothing changes, plus
// the NOTE lines, the DRY RUN and applied summaries. dho's extra last column (the
// document digest) is dropped; the "(N of those have no row)" clause is dropped.
func planLines(text string) []string {
	var out []string
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimRight(raw, " ")
		switch {
		case strings.HasPrefix(line, "schema_digest "):
			out = append(out, line)
		case strings.HasPrefix(line, "OPERATION "):
			out = append(out, "HEADER")
		case strings.Contains(line, " -> ") && !strings.HasPrefix(line, " "):
			fields := strings.Fields(noRowToken.ReplaceAllString(line, "NOROW"))
			// operation from -> to build [document] [ [no change] ]
			if len(fields) < 5 {
				out = append(out, "?? "+line)
				continue
			}
			noChange := strings.HasSuffix(line, "[no change]")
			entry := strings.Join(fields[:5], " ")
			if noChange {
				entry += " [no change]"
			}
			out = append(out, entry)
		case strings.HasPrefix(line, "    NOTE:"):
			out = append(out, line)
		case strings.HasPrefix(line, "DRY RUN:"):
			if m := dryRunCounts.FindStringSubmatch(line); m != nil {
				out = append(out, "DRY RUN "+m[1]+"/"+m[2])
			} else {
				out = append(out, line)
			}
		case strings.HasPrefix(line, "applied:"):
			out = append(out, line)
		}
	}
	return out
}

type oracleScenario struct {
	name string
	rows func(catalog map[string]string, ops []string) []oracleRow
	// args is the logical command line ("--flag value", as Python spells it); dho takes the
	// same words. apply adds --apply and the provenance flags each producer needs.
	args  []string
	apply bool
	// difference, when set, names a difference: the scenario's rows and plan are asserted by this
	// function on both producers' results instead of being compared.
	difference func(t *testing.T, py, goRun oracleRun)
}

// oracleRun is what one producer did in a scenario.
type oracleRun struct {
	code  int
	state []string
	plan  []string
	err   string
}

func oracleBaseRows(schema string, catalog map[string]string, ops []string) []oracleRow {
	return []oracleRow{
		{schema, catalog[ops[0]], ops[0], oracleBuildA, "primary", 100},
		{schema, catalog[ops[1]], ops[1], oracleBuildA, "canary", 25},
		{schema, catalog[ops[2]], ops[2], oracleBuildA, "python", 0},
		{schema, catalog[ops[3]], ops[3], oracleBuildB, "shadow", 0},
	}
}

// recordedRun is one scenario as the Python producer left it: the exit code, the rows of go_api_routing_state
// it read back and the plan lines the operator reads. (Its stderr is not stored: it can name the database.)
type recordedRun struct {
	Name  string   `json:"name"`
	Code  int      `json:"code"`
	State []string `json:"state"`
	Plan  []string `json:"plan"`
}

// recordedStatus is one `status --json` run of the Python producer: its exit code and its document with the
// per-run text normalized (updated_at removed, go_plane_error reduced to whether one is set).
type recordedStatus struct {
	Code int    `json:"code"`
	Out  string `json:"out"`
}

type routingRecorded struct {
	Scenarios []recordedRun    `json:"scenarios"`
	Status    []recordedStatus `json:"status"`
}

func TestGoAPIRoutingMatchesFrozenPython(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/routing.json",
		PythonBuild: routingPythonBuild,
		SHA256:      "de17712318d152134c7d9b9f66b7dc125a768471d5ffcc14df4ed8bc1d81e8bc",
		Recipe: "git worktree add --detach $DIR " + routingPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/goapicli/routing/ -test '^TestGoAPIRoutingMatchesFrozenPython$' -python-root $DIR",
	})
	pyRoot := golden.PythonRoot(t, repoRoot)
	catalogPath := filepath.Join(repoRoot, goapiproof.DefaultCatalogPath)
	catalog, err := goapiproof.LoadOperationCatalog(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	var ops []string
	for name := range catalog {
		ops = append(ops, name)
	}
	sort.Strings(ops)
	if len(ops) < 6 {
		t.Fatalf("the catalog registers %d operations; the oracle needs at least 6", len(ops))
	}
	ops = ops[:6]
	live := localSchemaDigest()
	stale := "sha256:" + strings.Repeat("7", 64)

	goPool, goDSN := startVerbPostgres(t)
	goPlane := oraclePlane{goPool, goDSN}

	scenarios := []oracleScenario{
		{name: "dry run, every registered operation", args: []string{"disable", "--mode", "disabled"}},
		{name: "dry run, named operations, python mode", args: []string{"disable", "--mode", "python", "--operations", ops[0] + "," + ops[2]}},
		{name: "apply disabled, every registered operation", args: []string{"disable", "--mode", "disabled"}, apply: true},
		{name: "apply python, two operations", args: []string{"disable", "--mode", "python", "--operations", ops[0] + "," + ops[1]}, apply: true},
		{name: "apply shadow over a primary row", args: []string{"disable", "--mode", "shadow", "--operations", ops[0]}, apply: true},
		{name: "apply, an operation with no row", args: []string{"disable", "--mode", "disabled", "--operations", ops[4]}, apply: true},
		{name: "apply, a row already in the mode", args: []string{"disable", "--mode", "python", "--operations", ops[2]}, apply: true},
		{name: "guard matching the row's build", args: []string{"disable", "--mode", "disabled", "--operations", ops[0], "--candidate-build", oracleBuildA}, apply: true},
		{name: "guard naming another build", args: []string{"disable", "--mode", "disabled", "--operations", ops[0], "--candidate-build", oracleBuildB}, apply: true},
		{name: "guard on a dry run", args: []string{"disable", "--mode", "disabled", "--operations", ops[3], "--candidate-build", oracleBuildA}},
		{name: "unknown operation", args: []string{"disable", "--mode", "disabled", "--operations", "noSuchOperation"}, apply: true},
		{name: "empty operations", args: []string{"disable", "--mode", "disabled", "--operations", " , "}, apply: true},
		{name: "mode canary refused", args: []string{"disable", "--mode", "canary", "--operations", ops[0]}, apply: true},
		{name: "mode missing", args: []string{"disable", "--operations", ops[0]}},
		{name: "operations in a different order and duplicated", args: []string{"disable", "--mode", "disabled", "--operations", ops[1] + "," + ops[0] + "," + ops[1]}, apply: true},
		{
			name: "a row at a stale schema digest only",
			rows: func(c map[string]string, o []string) []oracleRow {
				return []oracleRow{{stale, c[o[0]], o[0], oracleBuildA, "primary", 100}, {live, c[o[1]], o[1], oracleBuildA, "canary", 10}}
			},
			args: []string{"disable", "--mode", "disabled", "--operations", ops[0] + "," + ops[1]}, apply: true,
		},
		{
			name: "a row serving a document the catalog does not name",
			rows: func(c map[string]string, o []string) []oracleRow {
				return []oracleRow{{live, "sha256:" + strings.Repeat("9", 64), o[0], oracleBuildA, "primary", 100}, {live, c[o[1]], o[1], oracleBuildA, "canary", 10}}
			},
			args: []string{"disable", "--mode", "disabled", "--operations", ops[0] + "," + ops[1]}, apply: true,
			difference: func(t *testing.T, py, goRun oracleRun) {
				t.Helper()
				drifted := func(run oracleRun) string {
					for _, line := range run.state {
						if strings.Contains(line, "|"+ops[0]+"|") {
							return line
						}
					}
					return ""
				}
				if line := drifted(py); !strings.Contains(line, "|primary|") || !strings.HasSuffix(line, "moved=false") {
					t.Errorf("python must leave the drifted row untouched: %s", line)
				}
				if line := drifted(goRun); !strings.Contains(line, "|disabled|") || !strings.HasSuffix(line, "moved=true") {
					t.Errorf("dho must turn the drifted row off (keyed on the row's own document digest): %s", line)
				}
				// the other operation (a normal live row) is compared as everywhere else
				other := func(run oracleRun) string {
					for _, line := range run.state {
						if strings.Contains(line, "|"+ops[1]+"|") {
							return line
						}
					}
					return ""
				}
				if other(py) != other(goRun) || !strings.HasSuffix(other(py), "moved=true") {
					t.Errorf("the ordinary row differs: python %s go %s", other(py), other(goRun))
				}
				if py.code != 0 || goRun.code != 0 {
					t.Errorf("both apply: python %d go %d", py.code, goRun.code)
				}
			},
		},
	}

	scenarios = append(scenarios, oracleScenario{
		name: "a guard over a row serving a document the catalog does not name",
		rows: func(c map[string]string, o []string) []oracleRow {
			return []oracleRow{{live, "sha256:" + strings.Repeat("9", 64), o[0], oracleBuildA, "primary", 100}, {live, c[o[1]], o[1], oracleBuildA, "canary", 10}}
		},
		args: []string{"disable", "--mode", "disabled", "--operations", ops[0] + "," + ops[1], "--candidate-build", oracleBuildA}, apply: true,
		difference: func(t *testing.T, py, goRun oracleRun) {
			t.Helper()
			rowOf := func(run oracleRun, operation string) string {
				for _, line := range run.state {
					if strings.Contains(line, "|"+operation+"|") {
						return line
					}
				}
				return ""
			}
			// both apply the ordinary row alike
			if rowOf(py, ops[1]) != rowOf(goRun, ops[1]) || !strings.HasSuffix(rowOf(py, ops[1]), "moved=true") {
				t.Errorf("the ordinary row differs: python %s go %s", rowOf(py, ops[1]), rowOf(goRun, ops[1]))
			}
			// neither touches the drifted row (dho skips it because the guard has no catalog row to compare with)
			for name, run := range map[string]oracleRun{"python": py, "dho": goRun} {
				if line := rowOf(run, ops[0]); !strings.Contains(line, "|primary|") || !strings.HasSuffix(line, "moved=false") {
					t.Errorf("%s changed the drifted row under a guard: %s", name, line)
				}
			}
			if py.code != 0 || goRun.code != 3 {
				t.Errorf("exit: python %d (want 0) dho %d (want 3: skipped, not disabled)", py.code, goRun.code)
			}
		},
	})

	scenarios = append(scenarios, oracleScenario{
		name: "a guard over an operation with a catalog row and a drifted row",
		rows: func(c map[string]string, o []string) []oracleRow {
			return []oracleRow{
				{live, c[o[0]], o[0], oracleBuildA, "primary", 100},
				{live, "sha256:" + strings.Repeat("9", 64), o[0], oracleBuildB, "canary", 100},
			}
		},
		args: []string{"disable", "--mode", "disabled", "--operations", ops[0], "--candidate-build", oracleBuildA}, apply: true,
		difference: func(t *testing.T, py, goRun oracleRun) {
			t.Helper()
			catalogRow := func(run oracleRun) string {
				for _, line := range run.state {
					if strings.Contains(line, "|"+catalog[ops[0]]+"|") {
						return line
					}
				}
				return ""
			}
			driftRow := func(run oracleRun) string {
				for _, line := range run.state {
					if strings.Contains(line, "|sha256:"+strings.Repeat("9", 64)+"|") {
						return line
					}
				}
				return ""
			}
			// the guard is checked against the catalog row's build alone, by both: it matches, both disable it
			if catalogRow(py) != catalogRow(goRun) || !strings.HasSuffix(catalogRow(py), "moved=true") || !strings.Contains(catalogRow(py), "|disabled|") {
				t.Errorf("the catalog row differs: python %s go %s", catalogRow(py), catalogRow(goRun))
			}
			// the drifted row (another build, so never guarded): python leaves it, dho turns it off unguarded
			if line := driftRow(py); !strings.Contains(line, "|canary|") || !strings.HasSuffix(line, "moved=false") {
				t.Errorf("python must leave the drifted row untouched: %s", line)
			}
			if line := driftRow(goRun); !strings.Contains(line, "|disabled|") || !strings.HasSuffix(line, "moved=true") {
				t.Errorf("dho must turn the drifted row off without guarding it against the named build: %s", line)
			}
			if py.code != 0 || goRun.code != 0 {
				t.Errorf("exit: python %d dho %d, both want 0", py.code, goRun.code)
			}
		},
	})

	rowsFor := func(sc oracleScenario) []oracleRow {
		if sc.rows != nil {
			return sc.rows(catalog, ops)
		}
		return oracleBaseRows(live, catalog, ops)
	}
	runArgs := func(sc oracleScenario) (pyArgs, goArgs []string) {
		pyArgs = append([]string{}, sc.args...)
		goArgs = append([]string{}, sc.args...)
		if sc.apply {
			pyArgs = append(pyArgs, "--apply", "--review-evidence", oracleEvidence)
			goArgs = append(goArgs, "--apply", "--recorded-by", oracleOperator, "--review-evidence", oracleEvidence)
		}
		return pyArgs, goArgs
	}

	// The key: the scenarios (names, command lines, the rows each one seeds) and the digests they act on.
	type keyed struct {
		Name  string      `json:"name"`
		Args  []string    `json:"args"`
		Apply bool        `json:"apply"`
		Rows  [][5]string `json:"rows"`
	}
	keys := make([]keyed, len(scenarios))
	for index, sc := range scenarios {
		key := keyed{Name: sc.name, Args: sc.args, Apply: sc.apply}
		for _, row := range rowsFor(sc) {
			key.Rows = append(key.Rows, [5]string{row.schema, row.document, row.operation, row.build, fmt.Sprintf("%s/%d", row.mode, row.rollout)})
		}
		keys[index] = key
	}
	input, err := json.Marshal(map[string]any{"scenarios": keys, "live": live, "catalogOperations": ops})
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("go-api routing scenarios", routingPythonProgram, input, routingPythonSettings)
	answers := golden.Produce(t, pyRoot, []venueoracle.Request{request}, func(root string, _ []venueoracle.Request) []venueoracle.Response {
		pyPool, pyDSN := startVerbPostgres(t)
		py := oraclePlane{pyPool, pyDSN}
		var recorded routingRecorded
		for _, sc := range scenarios {
			py.seed(t, rowsFor(sc))
			pyArgs, _ := runArgs(sc)
			code, out, _ := oraclePython(t, root, py, pyArgs...)
			recorded.Scenarios = append(recorded.Scenarios, recordedRun{Name: sc.name, Code: code, State: py.state(t), Plan: planLines(out)})
		}
		recorded.Status = routingStatusPython(t, root, py, catalog, ops, live, stale)
		body, err := json.Marshal(recorded)
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: string(body)}}
	})
	golden.Consumed(t, answers...)
	var frozen routingRecorded
	if err := json.Unmarshal([]byte(answers[0].Body), &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen.Scenarios) != len(scenarios) || len(frozen.Status) != 5 {
		t.Fatalf("the golden holds %d scenarios and %d status runs, the test runs %d and 5", len(frozen.Scenarios), len(frozen.Status), len(scenarios))
	}

	mismatches, applied := 0, 0
	for index, sc := range scenarios {
		goPlane.seed(t, rowsFor(sc))
		_, goArgs := runArgs(sc)
		goCode, goOut, goErr := oracleGo(t, goPlane, catalogPath, goArgs...)
		want := frozen.Scenarios[index]
		if want.Name != sc.name {
			t.Fatalf("scenario %d is %q, the golden has %q", index, sc.name, want.Name)
		}
		pyRun := oracleRun{want.Code, want.State, want.Plan, ""}
		goRun := oracleRun{goCode, goPlane.state(t), planLines(goOut), goErr}
		if sc.difference != nil {
			sc.difference(t, pyRun, goRun)
			if t.Failed() {
				mismatches++
			}
			continue
		}
		differences := []string{}
		// A refusal is exit 2 in Python and exit 3 in dho (named difference): nothing else may differ.
		if pyRun.code != goRun.code && !(pyRun.code == 2 && goRun.code == 3) {
			differences = append(differences, fmt.Sprintf("exit python %d go %d", pyRun.code, goRun.code))
		}
		if pyRun.code == 2 && goRun.code != 3 && goRun.code != 2 {
			differences = append(differences, fmt.Sprintf("python refused (2), dho exit %d", goRun.code))
		}
		if !reflect.DeepEqual(pyRun.state, goRun.state) {
			differences = append(differences, fmt.Sprintf("rows differ:\n python %v\n go     %v", pyRun.state, goRun.state))
		}
		if !reflect.DeepEqual(pyRun.plan, goRun.plan) {
			differences = append(differences, fmt.Sprintf("plan differs:\n python %v\n go     %v", pyRun.plan, goRun.plan))
		}
		if len(differences) > 0 {
			mismatches++
			t.Errorf("%s: %s\n go stderr: %.300s", sc.name, strings.Join(differences, "; "), goErr)
		}
		if sc.apply && pyRun.code == 0 {
			for _, line := range pyRun.state {
				if strings.HasSuffix(line, "moved=true") {
					applied++
				}
			}
		}
	}
	if applied < 4 {
		t.Fatalf("only %d row(s) were rewritten across the scenarios: the comparison measures too little", applied)
	}
	t.Logf("%d scenarios, %d mismatches, %d rewritten rows", len(scenarios), mismatches, applied)
	oracleStatus(t, goPlane, catalogPath, catalog, ops, live, stale, frozen.Status)
	golden.SkipDiff(t)
	golden.Finish(t)
}

// censusLines is the `rows by schema_digest:` block of a text `status`: the heading and the digest lines up to
// the first blank line, which both producers print alike.
func censusLines(text string) string {
	lines := strings.Split(text, "\n")
	var out []string
	in := false
	for _, line := range lines {
		if strings.HasPrefix(line, "rows by schema_digest:") {
			in = true
		}
		if in {
			if strings.TrimSpace(line) == "" {
				break
			}
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

// normalizeStatus reduces a Python `status --json` document to what the comparison reads: the per-run volatile
// fields go (updated_at moves with the clock) and go_plane_error is reduced to whether one is set (its text
// can name the address of a fake server).
func normalizeStatus(raw string) string {
	var doc any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return raw
	}
	var walk func(any) any
	walk = func(value any) any {
		switch typed := value.(type) {
		case map[string]any:
			out := map[string]any{}
			for key, child := range typed {
				switch {
				case key == "updated_at":
				case key == "go_plane_error" && child != nil:
					out[key] = "<set>"
				default:
					out[key] = walk(child)
				}
			}
			return out
		case []any:
			out := make([]any, len(typed))
			for index, child := range typed {
				out[index] = walk(child)
			}
			return out
		}
		return value
	}
	normalized, err := json.Marshal(walk(doc))
	if err != nil {
		return raw
	}
	return string(normalized)
}

// routingStatusPython runs the four `status --json` states of the producer: the same seeds and query-api
// fakes oracleStatus compares dho against.
func routingStatusPython(t *testing.T, root string, py oraclePlane, catalog map[string]string, ops []string, live, stale string) []recordedStatus {
	t.Helper()
	record := func(code int, out string) recordedStatus {
		return recordedStatus{Code: code, Out: normalizeStatus(out)}
	}
	rows := []oracleRow{
		{live, catalog[ops[0]], ops[0], oracleBuildA, "primary", 100},
		{live, catalog[ops[1]], ops[1], oracleBuildA, "canary", 25},
		{stale, catalog[ops[2]], ops[2], oracleBuildA, "primary", 100},
		{live, catalog[ops[3]], ops[3], oracleBuildB, "shadow", 0},
		{live, catalog[ops[4]], ops[4], oracleBuildB, "disabled", 0},
	}
	py.seed(t, rows)
	server := startQueryAPI(t, live, catalog)
	code, out, _ := oraclePython(t, root, py, "status", "--json", "--query-api-url", server.URL)
	var recorded []recordedStatus
	recorded = append(recorded, record(code, out))
	// The text form of the same planes-agree state: the census lines an operator reads (live / STALE markers).
	textCode, textOut, _ := oraclePython(t, root, py, "status", "--query-api-url", server.URL)
	recorded = append(recorded, recordedStatus{Code: textCode, Out: censusLines(textOut)})
	server.Close()
	code, out, _ = oraclePython(t, root, py, "status", "--json")
	recorded = append(recorded, record(code, out))

	drift := "sha256:" + strings.Repeat("9", 64)
	py.seed(t, []oracleRow{
		{live, catalog[ops[0]], ops[0], oracleBuildA, "primary", 100},
		{live, drift, ops[3], oracleBuildB, "primary", 100},
		{live, "sha256:" + strings.Repeat("8", 64), "notInTheCatalog", oracleBuildB, "canary", 5},
	})
	server = startQueryAPI(t, live, catalog)
	code, out, _ = oraclePython(t, root, py, "status", "--json", "--query-api-url", server.URL)
	recorded = append(recorded, record(code, out))
	server.Close()
	other := startQueryAPI(t, stale, catalog)
	defer other.Close()
	code, out, _ = oraclePython(t, root, py, "status", "--json", "--query-api-url", other.URL)
	recorded = append(recorded, record(code, out))
	return recorded
}

// oracleStatus compares `status --json` of both producers over a state that holds every
// classification both name (a live primary row, a canary row, a shadow and a disabled row, a row at a
// stale schema digest, an operation with no row), then asserts the named differences on a state with a drifted and an
// unregistered live row and with the planes disagreeing.
func oracleStatus(t *testing.T, goPlane oraclePlane, catalogPath string, catalog map[string]string, ops []string, live, stale string, pyStatus []recordedStatus) {
	t.Helper()
	rows := []oracleRow{
		{live, catalog[ops[0]], ops[0], oracleBuildA, "primary", 100},
		{live, catalog[ops[1]], ops[1], oracleBuildA, "canary", 25},
		{stale, catalog[ops[2]], ops[2], oracleBuildA, "primary", 100},
		{live, catalog[ops[3]], ops[3], oracleBuildB, "shadow", 0},
		{live, catalog[ops[4]], ops[4], oracleBuildB, "disabled", 0},
	}
	goPlane.seed(t, rows)
	server := startQueryAPI(t, live, catalog)
	goCode, goOut, goErr := oracleGo(t, goPlane, catalogPath, "status", "-json", "-registry-url", server.URL+"/registry")
	compareStatus(t, "planes agree", false, pyStatus[0].Code, pyStatus[0].Out, "", goCode, goOut, goErr)
	textCode, textOut, textErr := oracleGo(t, goPlane, catalogPath, "status", "-registry-url", server.URL+"/registry")
	if textCode != pyStatus[1].Code || censusLines(textOut) != pyStatus[1].Out || !strings.Contains(pyStatus[1].Out, "<- live") || !strings.Contains(pyStatus[1].Out, "<- STALE") {
		t.Errorf("the text census of the planes-agree state differs (python exit %d, go exit %d):\n python: %q\n go: %q\n%.200s", pyStatus[1].Code, textCode, pyStatus[1].Out, censusLines(textOut), textErr)
	}
	server.Close()
	// No query-api at all: both report it and exit 0.
	goCode, goOut, goErr = oracleGo(t, goPlane, catalogPath, "status", "-json")
	compareStatus(t, "no query-api", true, pyStatus[2].Code, pyStatus[2].Out, "", goCode, goOut, goErr)

	// Named differences. A live row serving a document the catalog does not name, a live row of an
	// operation the catalog does not register, and a deployed plane on another schema digest.
	drift := "sha256:" + strings.Repeat("9", 64)
	rows = []oracleRow{
		{live, catalog[ops[0]], ops[0], oracleBuildA, "primary", 100},
		{live, drift, ops[3], oracleBuildB, "primary", 100},
		{live, "sha256:" + strings.Repeat("8", 64), "notInTheCatalog", oracleBuildB, "canary", 5},
	}
	goPlane.seed(t, rows)
	server = startQueryAPI(t, live, catalog)
	defer server.Close()
	pyCode, pyOut := pyStatus[3].Code, pyStatus[3].Out
	goCode, goOut, goErr = oracleGo(t, goPlane, catalogPath, "status", "-json", "-registry-url", server.URL+"/registry")
	if pyCode != 0 || goCode != 0 {
		t.Fatalf("status must never refuse: python %d go %d\n%.300s", pyCode, goCode, goErr)
	}
	var pyDoc, goDoc struct {
		Operations []map[string]any `json:"operations"`
		Rows       map[string]int   `json:"rows_by_schema_digest"`
	}
	if err := json.Unmarshal([]byte(pyOut), &pyDoc); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(goOut), &goDoc); err != nil {
		t.Fatal(err)
	}
	find := func(list []map[string]any, operation, document string) map[string]any {
		for _, row := range list {
			if row["operation"] == operation && (document == "" || row["document_digest"] == document) {
				return row
			}
		}
		return nil
	}
	if row := find(pyDoc.Operations, ops[3], drift); row == nil || row["digest_state"] != "DOCUMENT_DRIFT" {
		t.Errorf("python must list the drifted row as DOCUMENT_DRIFT: %v", row)
	}
	if row := find(pyDoc.Operations, "notInTheCatalog", ""); row == nil || row["digest_state"] != "UNREGISTERED" {
		t.Errorf("python must list the unregistered live row as UNREGISTERED: %v", row)
	}
	// dho: the catalog row of the drifted operation names the drifted document; no separate entry.
	if find(goDoc.Operations, ops[3], drift) != nil {
		t.Errorf("dho lists the drifted row as an entry of its own; the oracle's named difference changed")
	}
	// A live row of an operation the catalog does not register is listed by both, alike.
	pyRow, goRow := find(pyDoc.Operations, "notInTheCatalog", ""), find(goDoc.Operations, "notInTheCatalog", "")
	if goRow == nil {
		t.Errorf("dho does not list the live row of an unregistered operation")
	} else {
		for _, key := range statusRowKeys {
			if !reflect.DeepEqual(pyRow[key], goRow[key]) {
				t.Errorf("the unregistered row's %s: python %v go %v", key, pyRow[key], goRow[key])
			}
		}
	}
	row := find(goDoc.Operations, ops[3], catalog[ops[3]])
	unreachable, _ := row["unreachable_document_digests"].([]any)
	if row == nil || len(unreachable) != 1 || unreachable[0] != drift {
		t.Errorf("dho must name the drifted document on the catalog row's unreachable_document_digests: %v", row)
	}
	if !reflect.DeepEqual(pyDoc.Rows, goDoc.Rows) {
		t.Errorf("rows_by_schema_digest: python %v go %v", pyDoc.Rows, goDoc.Rows)
	}
	// The planes disagree: dho classifies against the deployed plane's digest.
	other := startQueryAPI(t, stale, catalog)
	defer other.Close()
	pyCode, pyOut = pyStatus[4].Code, pyStatus[4].Out
	goCode, goOut, _ = oracleGo(t, goPlane, catalogPath, "status", "-json", "-registry-url", other.URL+"/registry")
	var pyTop, goTop map[string]any
	if pyCode != 0 || goCode != 0 || json.Unmarshal([]byte(pyOut), &pyTop) != nil || json.Unmarshal([]byte(goOut), &goTop) != nil {
		t.Fatalf("status with disagreeing planes: python %d go %d", pyCode, goCode)
	}
	for _, key := range []string{"python_plane_schema_digest", "go_plane_schema_digest", "planes_agree", "rows_by_schema_digest"} {
		if !reflect.DeepEqual(pyTop[key], goTop[key]) {
			t.Errorf("planes disagree: %s differs: python %v go %v", key, pyTop[key], goTop[key])
		}
	}
	if pyTop["planes_agree"] != false {
		t.Errorf("the planes were made to disagree: %v", pyTop["planes_agree"])
	}
	goOps, _ := goTop["operations"].([]any)
	pending := 0
	for _, item := range goOps {
		if entry, _ := item.(map[string]any); entry["digest_state"] == "PENDING" {
			pending++
		}
	}
	if pending == 0 {
		t.Errorf("dho must report the rows written at this checkout's digest as PENDING while the deployed plane reads another")
	}
}

// statusKeys are the keys `status --json` names in both producers; updated_at moves with the clock.
var (
	statusTopKeys = []string{"python_plane_schema_digest", "python_plane_digest_error", "go_plane_schema_digest", "planes_agree",
		"registry_db_error", "catalog_loaded", "rows_by_schema_digest"}
	statusRowKeys = []string{"operation", "document_digest", "digest_state", "mode", "current_candidate_build", "rollout_percentage",
		"owner", "stale_digests", "proven", "reachable"}
)

// dispatchable is a live row whose mode sends traffic to Go: the rows whose `reachable` is the deployed
// plane's to confirm.
func dispatchable(row map[string]any) bool {
	return row["digest_state"] == "MATCH" && (row["mode"] == "canary" || row["mode"] == "primary")
}

// compareStatus compares one pair of status runs. reachableUnknown names the difference of a run with no
// query-api: dho reports `reachable` as null (unknown: the deployed plane was never asked), Python as true.
func compareStatus(t *testing.T, name string, reachableUnknown bool, pyCode int, pyOut, pyErr string, goCode int, goOut, goErr string) {
	t.Helper()
	if pyCode != 0 || goCode != 0 {
		t.Errorf("%s: status exit python %d go %d (it never refuses)\n python: %.300s\n go: %.300s", name, pyCode, goCode, pyErr, goErr)
		return
	}
	var py, goDoc map[string]any
	if err := json.Unmarshal([]byte(pyOut), &py); err != nil {
		t.Errorf("%s: the Python status is not JSON: %v (%.200s)", name, err, pyOut)
		return
	}
	if err := json.Unmarshal([]byte(goOut), &goDoc); err != nil {
		t.Errorf("%s: the Go status is not JSON: %v (%.200s)", name, err, goOut)
		return
	}
	for _, key := range statusTopKeys {
		if !reflect.DeepEqual(py[key], goDoc[key]) {
			t.Errorf("%s: %s differs: python %v go %v", name, key, py[key], goDoc[key])
		}
	}
	// go_plane_error: Python words its own text; compare only whether one is set.
	if (py["go_plane_error"] == nil) != (goDoc["go_plane_error"] == nil) {
		t.Errorf("%s: go_plane_error: python %v go %v", name, py["go_plane_error"], goDoc["go_plane_error"])
	}
	pyOps, _ := py["operations"].([]any)
	goOps, _ := goDoc["operations"].([]any)
	project := func(items []any) []string {
		var out []string
		for _, item := range items {
			row, _ := item.(map[string]any)
			var cells []string
			for _, key := range statusRowKeys {
				if key == "reachable" && reachableUnknown && dispatchable(row) {
					continue
				}
				cell, _ := json.Marshal(row[key])
				cells = append(cells, key+"="+string(cell))
			}
			out = append(out, strings.Join(cells, " "))
		}
		sort.Strings(out)
		return out
	}
	if reachableUnknown {
		for _, item := range goOps {
			if row, _ := item.(map[string]any); dispatchable(row) && row["reachable"] != nil {
				t.Errorf("%s: dho reports reachable %v for %v; with no query-api it must be unknown (null)", name, row["reachable"], row["operation"])
			}
		}
		for _, item := range pyOps {
			if row, _ := item.(map[string]any); dispatchable(row) && row["reachable"] != true {
				t.Errorf("%s: python reachable %v for a MATCH row: the named difference changed", name, row["reachable"])
			}
		}
	}
	if a, b := project(pyOps), project(goOps); !reflect.DeepEqual(a, b) {
		inA, inB := map[string]bool{}, map[string]bool{}
		for _, line := range a {
			inA[line] = true
		}
		for _, line := range b {
			inB[line] = true
		}
		var onlyPython, onlyGo []string
		for _, line := range a {
			if !inB[line] {
				onlyPython = append(onlyPython, line)
			}
		}
		for _, line := range b {
			if !inA[line] {
				onlyGo = append(onlyGo, line)
			}
		}
		t.Errorf("%s: the operations differ (%d python, %d go):\n only python:\n  %s\n only go:\n  %s", name, len(a), len(b),
			strings.Join(onlyPython, "\n  "), strings.Join(onlyGo, "\n  "))
	}
	if len(pyOps) < 4 {
		t.Errorf("%s: the status listed %d operations: the comparison measures too little", name, len(pyOps))
	}
}
