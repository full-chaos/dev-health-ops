//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonDowngrade runs the real `dev-hops migrate postgres downgrade <target>` (Alembic's real
// downgrade, `command.downgrade`) against uri.
func pythonDowngrade(t *testing.T, uri, target string) (code int, output string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	command := exec.Command(python, "-c", program, "migrate", "postgres", "downgrade", target)
	pyURI := strings.Replace(uri, "postgres://", "postgresql://", 1)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "POSTGRES_URI="+pyURI, "DATABASE_URI="+pyURI, "OTEL_ENABLED=false")
	command.Env = removeEnv(command.Env, "MIGRATION_DATABASE_URI")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	exitCode := 0
	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode = exitErr.ExitCode()
	} else if err != nil {
		t.Fatalf("running the python producer: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
	}
	return exitCode, stdout.String() + stderr.String()
}

// alembicVersions reads the recorded revision(s), so the test can prove Python's downgrade actually
// moved them and Go's refusal left them untouched.
func alembicVersions(t *testing.T, uri string) []string {
	t.Helper()
	conn := connect(t, uri)
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), "SELECT version_num FROM alembic_version ORDER BY version_num")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			t.Fatal(err)
		}
		out = append(out, version)
	}
	return out
}

// goDowngrade runs `dho migrate postgres downgrade <args...>` against uri.
func goDowngrade(t *testing.T, uri string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(resolve).Children {
		if child.Name == "downgrade" {
			run = child.Run
		}
	}
	var out, errs bytes.Buffer
	got := run(context.Background(), cli.Env{Args: args, Lookup: func(string) (string, bool) { return "", false }, Stdout: &out, Stderr: &errs})
	if strings.Contains(out.String()+errs.String(), "postgres://") {
		t.Fatal("the output carries the DSN")
	}
	return got, out.String(), errs.String()
}

// downInstance is one PostgreSQL server with scratch databases.
type downInstance struct {
	instance *containers.Instance
	admin    *pgx.Conn
}

func newDownInstance(t *testing.T) downInstance {
	t.Helper()
	instance, err := containers.StartPostgres(context.Background())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	return downInstance{instance: instance, admin: connect(t, instance.URI)}
}

// at is a scratch database at chain revision k of the Go chain (k = 0 is the baseline,
// len(chain) the head): the baseline plus the first k chain files, built by dho's own
// upgrade (which TestBaselineVenueOracleIsTheExecutedPythonUpgrade holds equal to the
// Python upgrade at every chain revision).
func (d downInstance) at(t *testing.T, k int) string {
	t.Helper()
	uri := databaseURI(t, d.instance.URI, scratchDatabase(t, d.admin))
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(context.Background(), connect(t, uri), baseline, chain[:k]); err != nil {
		t.Fatalf("upgrade to chain position %d: %v", k, err)
	}
	return uri
}

// schemaShape is the public schema as the catalogs describe it -- columns (type,
// nullability, default), constraints, indexes, sequences and the recorded revisions --
// the state a downgrade must reach. Not a mock: read from the database.
func schemaShape(t *testing.T, uri string) string {
	t.Helper()
	conn := connect(t, uri)
	queries := []string{
		"SELECT 'col ' || c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod) || ' notnull=' || a.attnotnull || ' default=' || coalesce(pg_get_expr(d.adbin, d.adrelid), '') FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND a.attnum > 0 AND NOT a.attisdropped",
		"SELECT 'con ' || c.relname || ' ' || con.conname || ' ' || pg_get_constraintdef(con.oid) FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public'",
		"SELECT 'idx ' || indexdef FROM pg_indexes WHERE schemaname = 'public'",
		"SELECT 'rel ' || c.relkind::text || ' ' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public'",
		"SELECT 'ver ' || version_num FROM alembic_version",
	}
	var lines []string
	for _, query := range queries {
		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			t.Fatalf("read the schema shape: %v", err)
		}
		got, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, got...)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// Each down file, run by dho on a real PostgreSQL, leaves exactly the schema the
// previous revision's upgrade builds -- asserted at every step of 0145 -> 0138, and the
// up/down/up round trip returns to the head. A down file that left an object behind, or
// dropped one too many, fails on the step that did it.
func TestDowngradeReachesThePreviousRevisionsSchema(t *testing.T) {
	d := newDownInstance(t)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	want := make([]string, len(chain)+1)
	for k := range want {
		want[k] = schemaShape(t, d.at(t, k))
		if k > 0 && want[k] == want[k-1] {
			t.Fatalf("chain revision %s changes nothing recorded in the shape: the comparison measures nothing", chain[k-1].Revision)
		}
	}
	head := d.at(t, len(chain))
	for k := len(chain) - 1; k >= 0; k-- {
		code, stdout, stderr := goDowngrade(t, head, previousName(chain, k))
		if code != cli.ExitOK {
			t.Fatalf("downgrade %s -> %s: exit %d stderr %s", chain[k].Revision, previousName(chain, k), code, stderr)
		}
		var result pgmigrate.DowngradeResult
		if err := json.Unmarshal([]byte(stdout), &result); err != nil || !reflect.DeepEqual(result.Reverted, []string{chain[k].Revision}) {
			t.Fatalf("downgrade one step from %s: result %q (%v)", chain[k].Revision, stdout, err)
		}
		if got := schemaShape(t, head); got != want[k] {
			t.Fatalf("after reverting %s the schema is not the one the upgrade to %s builds:\n%s", chain[k].Revision, previousName(chain, k), shapeDiff(want[k], got))
		}
	}
	if got := alembicVersions(t, head); !reflect.DeepEqual(got, []string{"0066", "0138"}) {
		t.Fatalf("after reverting the whole chain alembic_version holds %v, want [0066 0138]", got)
	}
	// up again: the round trip.
	baseline, _ := pgmigrate.LoadBaseline()
	if _, err := pgmigrate.Upgrade(context.Background(), connect(t, head), baseline, chain); err != nil {
		t.Fatal(err)
	}
	if got := schemaShape(t, head); got != want[len(chain)] {
		t.Fatalf("up, down, up did not return to the head:\n%s", shapeDiff(want[len(chain)], got))
	}
}

func previousName(chain []pgmigrate.ChainFile, k int) string {
	if k == 0 {
		return "0138"
	}
	return chain[k-1].Revision
}

func shapeDiff(want, got string) string {
	inWant := map[string]bool{}
	for _, line := range strings.Split(want, "\n") {
		inWant[line] = true
	}
	inGot := map[string]bool{}
	var out []string
	for _, line := range strings.Split(got, "\n") {
		inGot[line] = true
		if !inWant[line] {
			out = append(out, "  extra:   "+line)
		}
	}
	for _, line := range strings.Split(want, "\n") {
		if !inGot[line] {
			out = append(out, "  missing: "+line)
		}
	}
	return strings.Join(out, "\n")
}

// A database at the production shape (0066 and the application head both recorded):
// an explicit target reverts only the application branch and leaves 0066; the refusal
// cells leave the database byte-for-byte as it was. Enumerated by cell.
func TestDowngradeRefusalsLeaveTheDatabaseAlone(t *testing.T) {
	d := newDownInstance(t)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	head := d.at(t, len(chain))
	shape := schemaShape(t, head)
	cells := []struct {
		args []string
		code int
		name string
	}{
		{[]string{"-1"}, cli.ExitFailure, "below_baseline_floor"},
		{[]string{"-2"}, cli.ExitFailure, "below_baseline_floor"},
		{[]string{"base"}, cli.ExitRefused, "unsupported_target"},
		{[]string{"0066"}, cli.ExitRefused, "below_baseline_floor"},
		{[]string{"0137"}, cli.ExitRefused, "below_baseline_floor"},
		{[]string{"0999"}, cli.ExitRefused, "unknown_revision"},
		{[]string{"014"}, cli.ExitRefused, "unknown_revision"},
	}
	for _, cell := range cells {
		code, stdout, stderr := goDowngrade(t, head, cell.args...)
		if code != cell.code || stdout != "" || !strings.Contains(stderr, `"code":"`+cell.name+`"`) {
			t.Errorf("downgrade %v: exit %d stdout %q stderr %q, want exit %d and %s", cell.args, code, stdout, stderr, cell.code, cell.name)
		}
		if got := schemaShape(t, head); got != shape {
			t.Fatalf("downgrade %v changed the database:\n%s", cell.args, shapeDiff(shape, got))
		}
	}
	// at the target already: a no-op success.
	top := chain[len(chain)-1].Revision
	code, stdout, stderr := goDowngrade(t, head, top)
	if code != cli.ExitOK || !strings.Contains(stdout, `"action":"at_target"`) || schemaShape(t, head) != shape {
		t.Errorf("downgrade %s (the current revision): exit %d stdout %q stderr %q", top, code, stdout, stderr)
	}
	// above current: Go refuses, database untouched.
	low := d.at(t, 2)
	lowShape := schemaShape(t, low)
	if code, _, stderr := goDowngrade(t, low, top); code != cli.ExitFailure || !strings.Contains(stderr, "target_not_below_current") || schemaShape(t, low) != lowShape {
		t.Errorf("downgrade %s from chain position 2: exit %d stderr %s", top, code, stderr)
	}
	// An explicit target leaves 0066 alone on a two-head database.
	if code, _, stderr := goDowngrade(t, head, "0144"); code != cli.ExitOK {
		t.Fatalf("downgrade 0144: exit %d %s", code, stderr)
	}
	if got := alembicVersions(t, head); !reflect.DeepEqual(got, []string{"0066", "0144"}) {
		t.Fatalf("downgrade 0144 left alembic_version %v, want 0066 and 0144", got)
	}
	// the state refusals on a database that is not a chain database.
	empty := databaseURI(t, d.instance.URI, scratchDatabase(t, d.admin))
	if code, _, stderr := goDowngrade(t, empty, "0139"); code != cli.ExitFailure || !strings.Contains(stderr, "no_revision_recorded") {
		t.Errorf("empty database: exit %d %s", code, stderr)
	}
	ahead := d.at(t, len(chain))
	if _, err := connect(t, ahead).Exec(context.Background(), "UPDATE alembic_version SET version_num = '9999' WHERE version_num <> '0066'"); err != nil {
		t.Fatal(err)
	}
	aheadShape := schemaShape(t, ahead)
	if code, _, stderr := goDowngrade(t, ahead, "0139"); code != cli.ExitFailure || !strings.Contains(stderr, "ahead_of_build") || schemaShape(t, ahead) != aheadShape {
		t.Errorf("ahead-of-build database: exit %d %s", code, stderr)
	}
}

// Alembic runs the whole downgrade walk in one transaction (env.py wraps it in
// context.begin_transaction()), so a step that fails rolls back every step before it;
// dho does the same. A valid row (purpose 'setup') makes 0141's down violate the
// restored check constraint after 0145, 0144, 0143 and 0142 were reverted: nothing may
// change, and the error says so.
func TestDowngradeFailureMidWalkRollsBackTheWholeWalk(t *testing.T) {
	d := newDownInstance(t)
	chain, _ := pgmigrate.LoadChain()
	uri := d.at(t, len(chain))
	insertSetupRevocation(t, uri)
	before := schemaShape(t, uri)
	code, _, stderr := goDowngrade(t, uri, "0140")
	if code != cli.ExitFailure || !strings.Contains(stderr, "downgrade_failed") || !strings.Contains(stderr, "down 0141") || !strings.Contains(stderr, "rolled back") {
		t.Fatalf("exit %d stderr %s, want downgrade_failed naming 0141 and the rollback", code, stderr)
	}
	if got := schemaShape(t, uri); got != before {
		t.Fatalf("the failed walk changed the database:\n%s", shapeDiff(before, got))
	}
	if got := alembicVersions(t, uri); !reflect.DeepEqual(got, []string{"0066", "0145"}) {
		t.Fatalf("alembic_version %v after the failed walk, want 0066 and 0145", got)
	}
}

// insertSetupRevocation adds a row only 0141's reverse cannot keep.
func insertSetupRevocation(t *testing.T, uri string) {
	t.Helper()
	conn := connect(t, uri)
	rows, err := conn.Query(context.Background(), "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = 'provider_oauth_revocations' ORDER BY ordinal_position")
	if err != nil {
		t.Fatal(err)
	}
	type column struct {
		name, kind, nullable string
		def                  *string
	}
	var columns []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.name, &c.kind, &c.nullable, &c.def); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, c)
	}
	rows.Close()
	var names, values []string
	for _, c := range columns {
		if c.nullable == "YES" || c.def != nil {
			if c.name != "purpose" {
				continue
			}
		}
		names = append(names, c.name)
		switch {
		case c.name == "purpose":
			values = append(values, "'setup'")
		case c.kind == "uuid":
			values = append(values, "gen_random_uuid()")
		case strings.HasPrefix(c.kind, "timestamp"):
			values = append(values, "now()")
		case c.kind == "integer" || c.kind == "bigint":
			values = append(values, "1")
		case c.kind == "boolean":
			values = append(values, "false")
		case c.kind == "jsonb" || c.kind == "json":
			values = append(values, "'{}'")
		default:
			values = append(values, "'x'")
		}
	}
	if _, err := conn.Exec(context.Background(), "INSERT INTO provider_oauth_revocations ("+strings.Join(names, ", ")+") VALUES ("+strings.Join(values, ", ")+")"); err != nil {
		t.Fatalf("insert a setup revocation: %v", err)
	}
}

// TestDowngradeVenueOracleMatchesPythonDowngrade is the executed differential oracle:
// the same databases (built by dho's upgrade, at the production shape) are downgraded
// by the real Python verb (`command.downgrade`, real Alembic) and by dho, and the
// resulting schema shape and alembic_version must be identical for every target dho
// accepts; for the targets dho refuses, Python must succeed (the named divergence) and
// dho must have changed nothing.
func TestDowngradeVenueOracleMatchesPythonDowngrade(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	d := newDownInstance(t)
	chain, _ := pgmigrate.LoadChain()
	for _, target := range []string{"0144", "0141", "0139", "0138", "0145"} {
		pythonURI, goURI := d.at(t, len(chain)), d.at(t, len(chain))
		if code, output := pythonDowngrade(t, pythonURI, target); code != 0 {
			t.Fatalf("the real Python downgrade %s failed (exit %d): %s", target, code, output)
		}
		if code, _, stderr := goDowngrade(t, goURI, target); code != cli.ExitOK {
			t.Fatalf("dho downgrade %s: exit %d %s", target, code, stderr)
		}
		if got, want := schemaShape(t, goURI), schemaShape(t, pythonURI); got != want {
			t.Errorf("downgrade %s: dho and Python disagree:\n%s", target, shapeDiff(want, got))
		}
		if target != "0145" && equalSorted(alembicVersions(t, pythonURI), []string{"0066", "0145"}) {
			t.Errorf("downgrade %s: Python did not move alembic_version", target)
		}
	}
	// The named divergences: Python reverts 0066 for a relative step and succeeds; dho
	// refuses and changes nothing.
	for _, target := range []string{"-1", "-2"} {
		pythonURI, goURI := d.at(t, len(chain)), d.at(t, len(chain))
		pyCode, pyOutput := pythonDowngrade(t, pythonURI, target)
		before := schemaShape(t, goURI)
		goCode, _, _ := goDowngrade(t, goURI, target)
		if pyCode != 0 || goCode != cli.ExitFailure || schemaShape(t, goURI) != before {
			t.Errorf("downgrade %s: python exit %d (%s), dho exit %d: want python 0 and dho a refusal that changed nothing", target, pyCode, pyOutput, goCode)
		}
	}
	// A failing walk: both roll the whole walk back and leave the same database.
	pyFail, goFail := d.at(t, len(chain)), d.at(t, len(chain))
	insertSetupRevocation(t, pyFail)
	insertSetupRevocation(t, goFail)
	pyBefore := schemaShape(t, pyFail)
	pyCode, _ := pythonDowngrade(t, pyFail, "0140")
	goCode, _, _ := goDowngrade(t, goFail, "0140")
	if pyCode == 0 || goCode != cli.ExitFailure {
		t.Errorf("failing walk: python exit %d, dho exit %d, want both to fail", pyCode, goCode)
	}
	if schemaShape(t, pyFail) != pyBefore || schemaShape(t, goFail) != pyBefore {
		t.Errorf("failing walk: Python or dho left the database changed:\n%s", shapeDiff(pyBefore, schemaShape(t, goFail)))
	}
	// -1 on a single-head database (no 0066): the same result from both.
	pythonURI, goURI := d.at(t, len(chain)), d.at(t, len(chain))
	for _, uri := range []string{pythonURI, goURI} {
		if _, err := connect(t, uri).Exec(context.Background(), "DELETE FROM alembic_version WHERE version_num = '0066'"); err != nil {
			t.Fatal(err)
		}
	}
	if code, output := pythonDowngrade(t, pythonURI, "-1"); code != 0 {
		t.Fatalf("python -1 on a single head: exit %d %s", code, output)
	}
	if code, _, stderr := goDowngrade(t, goURI, "-1"); code != cli.ExitOK {
		t.Fatalf("dho -1 on a single head: exit %d %s", code, stderr)
	}
	if got, want := schemaShape(t, goURI), schemaShape(t, pythonURI); got != want {
		t.Errorf("-1 on a single head: dho and Python disagree:\n%s", shapeDiff(want, got))
	}
	venueoracle.WriteProof(t)
}

func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
