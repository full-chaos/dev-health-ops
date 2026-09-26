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
// It needs the full project Python environment (live oracle): the venue-oracles job
// runs it on main and on dispatch.

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

func oraclePython(t *testing.T, plane oraclePlane, args ...string) (int, string, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	command := exec.Command(python, append([]string{"-c", program, "go-api", "routing"}, args...)...)
	uri := strings.Replace(plane.dsn, "postgres://", "postgresql://", 1)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "POSTGRES_URI="+uri, "DATABASE_URI="+uri,
		"OTEL_ENABLED=false", "DEV_HOPS_OPERATOR="+oracleOperator, "GO_API_QUERY_API_URL=")
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

func TestGoAPIRoutingVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(root, goapiproof.DefaultCatalogPath)
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

	pyPool, pyDSN := startVerbPostgres(t)
	goPool, goDSN := startVerbPostgres(t)
	py, goPlane := oraclePlane{pyPool, pyDSN}, oraclePlane{goPool, goDSN}

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

	mismatches, applied := 0, 0
	for _, sc := range scenarios {
		rowsFor := func(plane string) []oracleRow {
			if sc.rows != nil {
				return sc.rows(catalog, ops)
			}
			return oracleBaseRows(live, catalog, ops)
		}
		py.seed(t, rowsFor("python"))
		goPlane.seed(t, rowsFor("go"))
		if before, after := py.state(t), goPlane.state(t); !reflect.DeepEqual(before, after) {
			t.Fatalf("%s: the two planes were not seeded alike:\n%v\n%v", sc.name, before, after)
		}
		pyArgs := append([]string{}, sc.args...)
		goArgs := append([]string{}, sc.args...)
		if sc.apply {
			pyArgs = append(pyArgs, "--apply", "--review-evidence", oracleEvidence)
			goArgs = append(goArgs, "--apply", "--recorded-by", oracleOperator, "--review-evidence", oracleEvidence)
		}
		pyCode, pyOut, pyErr := oraclePython(t, py, pyArgs...)
		goCode, goOut, goErr := oracleGo(t, goPlane, catalogPath, goArgs...)
		pyRun := oracleRun{pyCode, py.state(t), planLines(pyOut), pyErr}
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
			t.Errorf("%s: %s\n python stderr: %.300s\n go stderr: %.300s", sc.name, strings.Join(differences, "; "), pyErr, goErr)
		}
		pyState := pyRun.state
		if sc.apply && pyCode == 0 {
			for _, line := range pyState {
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
	oracleStatus(t, py, goPlane, catalogPath, catalog, ops, live, stale)
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}

// oracleStatus compares `status --json` of both producers over a state that holds every
// classification both name (a live primary row, a canary row, a shadow and a disabled row, a row at a
// stale schema digest, an operation with no row), then asserts the named differences on a state with a drifted and an
// unregistered live row and with the planes disagreeing.
func oracleStatus(t *testing.T, py, goPlane oraclePlane, catalogPath string, catalog map[string]string, ops []string, live, stale string) {
	t.Helper()
	rows := []oracleRow{
		{live, catalog[ops[0]], ops[0], oracleBuildA, "primary", 100},
		{live, catalog[ops[1]], ops[1], oracleBuildA, "canary", 25},
		{stale, catalog[ops[2]], ops[2], oracleBuildA, "primary", 100},
		{live, catalog[ops[3]], ops[3], oracleBuildB, "shadow", 0},
		{live, catalog[ops[4]], ops[4], oracleBuildB, "disabled", 0},
	}
	py.seed(t, rows)
	goPlane.seed(t, rows)
	server := startQueryAPI(t, live, catalog)
	pyCode, pyOut, pyErr := oraclePython(t, py, "status", "--json", "--query-api-url", server.URL)
	goCode, goOut, goErr := oracleGo(t, goPlane, catalogPath, "status", "-json", "-registry-url", server.URL+"/registry")
	compareStatus(t, "planes agree", false, pyCode, pyOut, pyErr, goCode, goOut, goErr)
	server.Close()
	// No query-api at all: both report it and exit 0.
	pyCode, pyOut, pyErr = oraclePython(t, py, "status", "--json")
	goCode, goOut, goErr = oracleGo(t, goPlane, catalogPath, "status", "-json")
	compareStatus(t, "no query-api", true, pyCode, pyOut, pyErr, goCode, goOut, goErr)

	// Named differences. A live row serving a document the catalog does not name, a live row of an
	// operation the catalog does not register, and a deployed plane on another schema digest.
	drift := "sha256:" + strings.Repeat("9", 64)
	rows = []oracleRow{
		{live, catalog[ops[0]], ops[0], oracleBuildA, "primary", 100},
		{live, drift, ops[3], oracleBuildB, "primary", 100},
		{live, "sha256:" + strings.Repeat("8", 64), "notInTheCatalog", oracleBuildB, "canary", 5},
	}
	py.seed(t, rows)
	goPlane.seed(t, rows)
	server = startQueryAPI(t, live, catalog)
	defer server.Close()
	pyCode, pyOut, pyErr = oraclePython(t, py, "status", "--json", "--query-api-url", server.URL)
	goCode, goOut, goErr = oracleGo(t, goPlane, catalogPath, "status", "-json", "-registry-url", server.URL+"/registry")
	if pyCode != 0 || goCode != 0 {
		t.Fatalf("status must never refuse: python %d go %d\n%.300s\n%.300s", pyCode, goCode, pyErr, goErr)
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
	pyCode, pyOut, _ = oraclePython(t, py, "status", "--json", "--query-api-url", other.URL)
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
