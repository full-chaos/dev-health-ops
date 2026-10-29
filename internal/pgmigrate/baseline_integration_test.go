//go:build integration

package pgmigrate_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
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
)

// updateEnv, set to 1, rewrites baseline/head.json from the database the archived
// chain builds (TestBaselineEqualsTheArchivedChain) instead of comparing against it.
// CI never sets it, so there the capture is a drift check.
const updateEnv = "DHO_PG_BASELINE_UPDATE"

// productionSettings are the settings the head is captured with: the ones
// production runs (read from prod, 2026-09-24).
var productionSettings = pgmigrate.Settings{Cutover: true, RiverSchema: "river"}

// window is the time a migration ran. A seed row stamped inside it was
// stamped by the migration itself (now(), datetime.now()), so the captured
// literal is replaced by now(): dho then stamps the rows at its own run, as
// the Python upgrade does, and two captures compare equal. A zero window
// replaces nothing.
type window struct{ from, to time.Time }

var timestampLiteral = regexp.MustCompile(`'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)(\+00)?'`)

func (w window) replaceWithNow(data string) string { return w.replaceWith(data, "now()") }

// replaceWith replaces each timestamp literal inside the window by replacement.
func (w window) replaceWith(data, replacement string) string {
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
		return replacement
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
	return mapRandomIDs(data, func(n int) string { return fmt.Sprintf("'<random-id-%d>'", n) })
}

// mapRandomIDs replaces each distinct value of a randomIDColumns column by token(n), n the order of first
// appearance, so a value used twice (role_permissions.permission_id) stays one value.
func mapRandomIDs(data string, token func(n int) string) string {
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
			mapped, ok := tokens[values[position]]
			if !ok {
				mapped = token(len(tokens) + 1)
				tokens[values[position]] = mapped
			}
			values[position] = mapped
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
