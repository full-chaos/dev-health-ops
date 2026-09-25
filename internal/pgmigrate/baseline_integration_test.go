//go:build integration

package pgmigrate_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// updateEnv, set to 1, rewrites baseline/head.json from the executed
// upgrade instead of comparing against it. CI never sets it, so there the
// capture is a drift check.
const updateEnv = "DHO_PG_BASELINE_UPDATE"

// upgradeProgram runs the Python upgrade the migrate Job runs
// (`dev-hops migrate postgres`: _run_upgrade) against one database, once per
// revision named after the URI ("head" for the whole chain, "0138" to stop at a
// revision), with no River step (MIGRATION_DATABASE_URI is unset).
const upgradeProgram = `
import argparse, sys
from dev_health_ops.db import normalize_async_postgres_uri
from dev_health_ops.migrate import _run_upgrade
uri = normalize_async_postgres_uri(sys.argv[1])
for revision in sys.argv[2:]:
    code = _run_upgrade(argparse.Namespace(db=uri, revision=revision))
    if code:
        sys.exit(code)
`

// pythonUpgrade builds a database with the real Python upgrade, revision by revision.
func pythonUpgrade(t *testing.T, python, root, uri string, revisions ...string) {
	t.Helper()
	command := exec.Command(python, append([]string{"-c", upgradeProgram, uri}, revisions...)...)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("the Python upgrade to %v failed: %v", revisions, pyoracle.RunError(python, err, output))
	}
}

// productionSettings are the settings the head is captured with: the ones
// production runs (read from prod, 2026-09-24).
var productionSettings = pgmigrate.Settings{Cutover: true, RiverSchema: "river"}

// TestBaselineIsTheExecutedPythonUpgrade is the baseline's provenance and the
// differential oracle of the whole migrator:
//
//  1. capture: run the REAL Python upgrade, with production's settings, to the
//     baseline revision (0138 and the River cutover 0066: prod's state) on a fresh
//     database and dump it (pg_dump, from the server's own image). The checked-in
//     baseline must equal that capture (drift check, by execution).
//  2. a baseline that fails part-way leaves the database empty.
//  3. oracle: `pgmigrate.Upgrade` on a fresh database (the baseline, then every
//     chain revision) must build the same database as the real Python upgrade to
//     the head, byte for byte in canonical form.
//  4. oracle: `pgmigrate.Upgrade` on a database the real Python upgrade left at the
//     baseline revision and at every chain revision below the head must reach the
//     same head database (CHAOS-6801: the migrate hook on a live database).
//  5. a second Upgrade applies nothing; a database without the cutover head is
//     refused as below the head, naming the cutover; a database missing the
//     application head is refused naming it; a public schema without
//     alembic_version is refused as foreign.
func TestBaselineIsTheExecutedPythonUpgrade(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})
	if instance.Container == nil {
		t.Fatal("this test dumps databases with the server's own pg_dump and needs a container instance, not a remote DSN")
	}
	admin := connect(t, instance.URI)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)

	t.Setenv(pgmigrate.CutoverEnv, "1")
	t.Setenv(pgmigrate.RiverSchemaEnv, productionSettings.RiverSchema)
	t.Setenv("MIGRATION_DATABASE_URI", "")
	os.Unsetenv("MIGRATION_DATABASE_URI")
	if got := pgmigrate.ReadSettings(os.LookupEnv); got != productionSettings {
		t.Fatalf("the capture environment reads as %+v, want production's %+v", got, productionSettings)
	}

	// pythonAt builds a database with the real Python upgrade to a revision (and the
	// cutover) and captures it.
	// It also returns when the upgrade started: the rows it seeds carry timestamps
	// from then, and a database the migrator upgrades later is captured over the whole
	// span.
	pythonAt := func(revisions ...string) (string, pgmigrate.Baseline, time.Time) {
		t.Helper()
		database := scratchDatabase(t, admin)
		started := time.Now()
		pythonUpgrade(t, python, root, databaseURI(t, instance.URI, database), revisions...)
		captured := capture(t, ctx, instance, database, window{started, time.Now()})
		captured.Cutover, captured.RiverSchema = productionSettings.Cutover, productionSettings.RiverSchema
		return database, captured, started
	}

	// 1. the baseline is the Python upgrade to its own revision.
	baselineDB, captured, baselineStarted := pythonAt("0138", "0066")
	if !reflect.DeepEqual(captured.Heads, []string{"0066", "0138"}) {
		t.Fatalf("the Python upgrade to 0138 with production's settings recorded heads %v, want [0066 0138]", captured.Heads)
	}
	checkedIn := captured
	if os.Getenv(updateEnv) == "1" {
		writeBaseline(t, captured)
	} else if checkedIn, err = pgmigrate.LoadBaseline(); err != nil {
		t.Fatal(err)
	}
	if diff := compare(captured, checkedIn); diff != "" {
		t.Fatalf("baseline/head.json is not what the Python upgrade to 0138 builds today (%s); regenerate it with "+
			"%s=1 go test -tags=integration -run TestBaselineIsTheExecutedPythonUpgrade ./internal/pgmigrate",
			diff, updateEnv)
	}
	wantHeads := pgmigrate.Heads(checkedIn, chain)

	// The whole Python chain: the database every dho path must reach.
	_, head, _ := pythonAt("head")
	if !reflect.DeepEqual(head.Heads, wantHeads) {
		t.Fatalf("the Python upgrade to the head recorded %v, the baseline and its chain give %v: a revision has no chain file", head.Heads, wantHeads)
	}
	headCanonical := roundTrip(t, ctx, instance, admin, head)

	// 2. a baseline that fails part-way leaves nothing: schema and data apply in
	// one transaction.
	brokenDB := scratchDatabase(t, admin)
	brokenConn := connect(t, databaseURI(t, instance.URI, brokenDB))
	broken := checkedIn
	broken.Data += "\nSELECT 1/0;\n"
	if _, err := pgmigrate.Upgrade(ctx, brokenConn, broken, chain); err == nil {
		t.Fatal("a baseline that fails in its data applied without error")
	}
	var relations int
	if err := brokenConn.QueryRow(ctx, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public'").Scan(&relations); err != nil {
		t.Fatal(err)
	}
	if relations != 0 {
		t.Fatalf("a failed baseline left %d relations in public", relations)
	}

	// 3. a fresh database: the baseline, then every chain revision.
	goDB := scratchDatabase(t, admin)
	goConn := connect(t, databaseURI(t, instance.URI, goDB))
	goStarted := time.Now()
	result, err := pgmigrate.Upgrade(ctx, goConn, checkedIn, chain)
	if err != nil {
		t.Fatalf("upgrade a fresh database: %v", err)
	}
	goWindow := window{goStarted, time.Now()}
	if result.Action != "baseline_applied" || len(result.Applied) != len(chain) {
		t.Fatalf("fresh upgrade = %+v, want baseline_applied with all %d chain revisions applied", result, len(chain))
	}
	// The oracle compares CANONICAL dumps. pg_dump output does not survive a
	// restore byte for byte: Postgres re-parses some expressions it deparsed
	// (a varchar IN list becomes per-element casts, nested ANDs flatten), so
	// the dump of any restored database differs from its source in those
	// lines while meaning the same. Passing both databases through the same
	// one dump-and-restore puts them in one form, and there they must be
	// byte-identical.
	goCapture := capture(t, ctx, instance, goDB, goWindow)
	goCapture.Cutover, goCapture.RiverSchema = productionSettings.Cutover, productionSettings.RiverSchema
	// The raw schema dumps may differ ONLY by Postgres's re-parse forms.
	diff, differing := reparseOnlyDiff(head.Schema, goCapture.Schema)
	if diff != "" {
		t.Fatalf("the raw schema dumps differ beyond the known re-parse forms: %s", diff)
	}
	t.Logf("raw schema dumps: %d line(s) differ, every one a known re-parse form", differing)
	goCanonical := roundTrip(t, ctx, instance, admin, goCapture)
	if diff := compare(goCanonical, headCanonical); diff != "" {
		t.Fatalf("dho migrate postgres built a different database than the Python upgrade: %s", diff)
	}

	// 4. a live database the Python upgrade left at the baseline revision, and at every
	// chain revision below the head, reaches the same head (the hook's job).
	fromRevisions := []string{"0138"}
	for _, file := range chain[:len(chain)-1] {
		fromRevisions = append(fromRevisions, file.Revision)
	}
	for _, revision := range fromRevisions {
		var database string
		started := baselineStarted
		if revision == "0138" {
			database = baselineDB
		} else {
			database, _, started = pythonAt(revision, "0066")
		}
		conn := connect(t, databaseURI(t, instance.URI, database))
		upgraded, err := pgmigrate.Upgrade(ctx, conn, checkedIn, chain)
		if err != nil {
			t.Fatalf("upgrade a database the Python upgrade left at %s: %v", revision, err)
		}
		if upgraded.Action != "chain_applied" || len(upgraded.Applied) == 0 {
			t.Fatalf("upgrade from %s = %+v, want chain_applied", revision, upgraded)
		}
		got := capture(t, ctx, instance, database, window{started, time.Now()})
		got.Cutover, got.RiverSchema = productionSettings.Cutover, productionSettings.RiverSchema
		if !reflect.DeepEqual(got.Heads, wantHeads) {
			t.Fatalf("from %s alembic_version = %v, want %v", revision, got.Heads, wantHeads)
		}
		if diff := compare(roundTrip(t, ctx, instance, admin, got), headCanonical); diff != "" {
			t.Fatalf("dho upgraded a database at %s to a different database than the Python upgrade: %s", revision, diff)
		}
		again, err := pgmigrate.Upgrade(ctx, conn, checkedIn, chain)
		if err != nil || again.Action != "up_to_date" || len(again.Applied) != 0 {
			t.Fatalf("re-run from %s = %+v, %v; want up_to_date with nothing applied", revision, again, err)
		}
	}

	// 5. refusals.
	again, err := pgmigrate.Upgrade(ctx, goConn, checkedIn, chain)
	if err != nil || again.Action != "up_to_date" || len(again.Applied) != 0 {
		t.Fatalf("re-run = %+v, %v; want up_to_date with nothing applied", again, err)
	}
	application := ""
	for _, h := range checkedIn.Heads {
		if h != "0066" {
			application = h
		}
	}
	for _, missing := range []string{"0066", chain[len(chain)-1].Revision} {
		wantMissing := missing
		if missing != "0066" {
			// The database recorded only the cutover head: the application head the
			// chain continues is what it lacks.
			wantMissing = application
		}
		if _, err := goConn.Exec(ctx, "DELETE FROM alembic_version WHERE version_num = $1", missing); err != nil {
			t.Fatal(err)
		}
		_, err = pgmigrate.Upgrade(ctx, goConn, checkedIn, chain)
		var below pgmigrate.BelowHeadError
		if !errors.As(err, &below) || !reflect.DeepEqual(below.Missing, []string{wantMissing}) {
			t.Fatalf("upgrade with %s unrecorded = %v, want below the head naming %s", missing, err, wantMissing)
		}
		if missing == "0066" && !strings.Contains(err.Error(), "River cutover (revision 0066) is expected") {
			t.Fatalf("the refusal does not name the missing cutover: %v", err)
		}
		if _, err := goConn.Exec(ctx, "INSERT INTO alembic_version (version_num) VALUES ($1)", missing); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := goConn.Exec(ctx, "DROP TABLE alembic_version"); err != nil {
		t.Fatal(err)
	}
	_, err = pgmigrate.Upgrade(ctx, goConn, checkedIn, chain)
	var foreign pgmigrate.ForeignDatabaseError
	if !errors.As(err, &foreign) {
		t.Fatalf("upgrade over tables without alembic_version = %v, want a foreign-database refusal", err)
	}
}

// window is the time a migration ran. A seed row stamped inside it was
// stamped by the migration itself (now(), datetime.now()), so the captured
// literal is replaced by now(): dho then stamps the rows at its own run, as
// the Python upgrade does, and two captures compare equal. A zero window
// replaces nothing.
type window struct{ from, to time.Time }

var timestampLiteral = regexp.MustCompile(`'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)(\+00)?'`)

func (w window) replaceWithNow(data string) string {
	if w.from.IsZero() {
		return data
	}
	from, to := w.from.Add(-2*time.Second).UTC(), w.to.Add(2*time.Second).UTC()
	return timestampLiteral.ReplaceAllStringFunc(data, func(literal string) string {
		match := timestampLiteral.FindStringSubmatch(literal)
		stamp, err := time.Parse("2006-01-02 15:04:05.999999", match[1])
		if err != nil || stamp.Before(from) || stamp.After(to) {
			return literal
		}
		return "now()"
	})
}

// randomIDColumns are the seeded columns the Python upgrade fills with a
// fresh uuid4 on every run (measured by diffing two runs' data dumps; nothing
// else differs but the migration-time stamps). The baseline keeps the ids it
// captured, so dho seeds fixed ids. Comparisons replace each distinct value in
// these columns by a token in order of first appearance, which keeps
// role_permissions.permission_id pointing at the same permissions row while
// ignoring the value itself. Any other difference still fails.
var randomIDColumns = map[string]map[string]bool{
	"feature_flags":    {"id": true},
	"permissions":      {"id": true},
	"role_permissions": {"id": true, "permission_id": true},
	"tier_limits":      {"id": true},
}

var insertLine = regexp.MustCompile(`^INSERT INTO public\.(\w+) \((.*?)\) VALUES \((.*)\);$`)

var uuidLiteral = regexp.MustCompile(`^'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'$`)

func canonicalIDs(data string) string {
	tokens := map[string]string{}
	lines := strings.Split(data, "\n")
	for index, line := range lines {
		match := insertLine.FindStringSubmatch(line)
		if match == nil || randomIDColumns[match[1]] == nil {
			continue
		}
		columns := strings.Split(match[2], ", ")
		values := splitValues(match[3])
		if len(columns) != len(values) {
			continue
		}
		for position, column := range columns {
			if !randomIDColumns[match[1]][column] || !uuidLiteral.MatchString(values[position]) {
				continue
			}
			token, ok := tokens[values[position]]
			if !ok {
				token = fmt.Sprintf("'<random-id-%d>'", len(tokens)+1)
				tokens[values[position]] = token
			}
			values[position] = token
		}
		lines[index] = "INSERT INTO public." + match[1] + " (" + match[2] + ") VALUES (" + strings.Join(values, ", ") + ");"
	}
	return strings.Join(lines, "\n")
}

// splitValues splits an INSERT's VALUES list on top-level commas, outside
// quoted strings and parentheses.
func splitValues(list string) []string {
	var values []string
	var current strings.Builder
	quoted, depth := false, 0
	for i := 0; i < len(list); i++ {
		c := list[i]
		switch {
		case c == '\'' && quoted && i+1 < len(list) && list[i+1] == '\'':
			current.WriteString("''")
			i++
			continue
		case c == '\'':
			quoted = !quoted
		case !quoted && c == '(':
			depth++
		case !quoted && c == ')':
			depth--
		case !quoted && depth == 0 && c == ',':
			values = append(values, strings.TrimSpace(current.String()))
			current.Reset()
			continue
		}
		current.WriteByte(c)
	}
	return append(values, strings.TrimSpace(current.String()))
}

// roundTrip restores a dump into a fresh database and dumps that again.
func roundTrip(t *testing.T, ctx context.Context, instance *containers.Instance, admin *pgx.Conn, dump pgmigrate.Baseline) pgmigrate.Baseline {
	t.Helper()
	database := scratchDatabase(t, admin)
	conn := connect(t, databaseURI(t, instance.URI, database))
	started := time.Now()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{dump.Schema, dump.Data} {
		if _, err := tx.Exec(ctx, part); err != nil {
			t.Fatalf("restore a dump for the canonical form: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	restored := capture(t, ctx, instance, database, window{started, time.Now()})
	restored.Cutover, restored.RiverSchema = dump.Cutover, dump.RiverSchema
	return restored
}

func connect(t *testing.T, uri string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func scratchDatabase(t *testing.T, admin *pgx.Conn) string {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "dho_pg_baseline_" + hex.EncodeToString(suffix)
	if _, err := admin.Exec(context.Background(), "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop %s: %v", name, err)
		}
	})
	return name
}

func databaseURI(t *testing.T, uri, database string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + database
	return parsed.String()
}

// capture dumps one database with the container's own pg_dump: the schema,
// then the data as column INSERTs, plus the recorded alembic heads.
func capture(t *testing.T, ctx context.Context, instance *containers.Instance, database string, migrated window) pgmigrate.Baseline {
	t.Helper()
	schema := pgDump(t, ctx, instance, database, "--schema-only")
	data := sortAlembicVersionRows(migrated.replaceWithNow(pgDump(t, ctx, instance, database, "--data-only", "--column-inserts")))
	conn := connect(t, databaseURI(t, instance.URI, database))
	rows, err := conn.Query(ctx, "SELECT version_num FROM alembic_version ORDER BY version_num")
	if err != nil {
		t.Fatal(err)
	}
	heads, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if len(heads) == 0 || !strings.Contains(schema, "CREATE TABLE") {
		t.Fatalf("database %s holds %d heads after the upgrade and its dump has no table", database, len(heads))
	}
	return pgmigrate.Baseline{Heads: heads, Schema: schema, Data: data}
}

// sortAlembicVersionRows puts the alembic_version INSERTs of a data dump in revision
// order. pg_dump writes rows in physical order, and an UPDATE moves a row to the end
// of the heap, so a database whose application head was moved by a chain revision
// dumps its two rows in the other order than one Alembic wrote directly: the same
// database, a different line order.
func sortAlembicVersionRows(data string) string {
	const prefix = "INSERT INTO public.alembic_version "
	lines := strings.Split(data, "\n")
	var rows []int
	for index, line := range lines {
		if strings.HasPrefix(line, prefix) {
			rows = append(rows, index)
		}
	}
	values := make([]string, len(rows))
	for position, index := range rows {
		values[position] = lines[index]
	}
	sort.Strings(values)
	for position, index := range rows {
		lines[index] = values[position]
	}
	return strings.Join(lines, "\n")
}

// pgDump runs the container's own pg_dump. Its stderr (warnings) goes to a
// separate file in the container, so only the SQL on stdout is captured; any
// warning is logged.
func pgDump(t *testing.T, ctx context.Context, instance *containers.Instance, database string, args ...string) string {
	t.Helper()
	dump := "pg_dump --username=worker_test --dbname=" + database + " --no-owner " + strings.Join(args, " ")
	run := func(script string) string {
		code, reader, err := instance.Container.Exec(ctx, []string{"sh", "-c", script}, tcexec.Multiplexed())
		if err != nil {
			t.Fatalf("%s: %v", script, err)
		}
		output, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if code != 0 {
			t.Fatalf("%s exited %d: %s", script, code, output)
		}
		return string(output)
	}
	output := run(dump + " 2>/tmp/pg_dump.err")
	if warnings := strings.TrimSpace(run("cat /tmp/pg_dump.err")); warnings != "" {
		t.Logf("pg_dump %v warned: %s", args, warnings)
	}
	return pgmigrate.SanitizeDump(output)
}

func compare(got, want pgmigrate.Baseline) string {
	if got.Cutover != want.Cutover || got.RiverSchema != want.RiverSchema {
		return fmt.Sprintf("settings cutover=%v river_schema=%q vs cutover=%v river_schema=%q", got.Cutover, got.RiverSchema, want.Cutover, want.RiverSchema)
	}
	if !reflect.DeepEqual(got.Heads, want.Heads) {
		return fmt.Sprintf("heads %v vs %v", got.Heads, want.Heads)
	}
	if got.Schema != want.Schema {
		return "schema dumps differ at: " + firstDifferentLine(got.Schema, want.Schema)
	}
	if gotData, wantData := canonicalIDs(got.Data), canonicalIDs(want.Data); gotData != wantData {
		return "data dumps differ at: " + firstDifferentLine(gotData, wantData)
	}
	return ""
}

func firstDifferentLine(a, b string) string {
	x, y := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := 0; i < len(x) || i < len(y); i++ {
		var p, q string
		if i < len(x) {
			p = x[i]
		}
		if i < len(y) {
			q = y[i]
		}
		if p != q {
			return fmt.Sprintf("line %d:\n  got  %q\n  want %q", i+1, p, q)
		}
	}
	return "none"
}

func writeBaseline(t *testing.T, baseline pgmigrate.Baseline) {
	t.Helper()
	data, err := json.MarshalIndent(baseline, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("baseline", "head.json")
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s: heads %v, schema %d bytes, data %d bytes", path, baseline.Heads, len(baseline.Schema), len(baseline.Data))
}
