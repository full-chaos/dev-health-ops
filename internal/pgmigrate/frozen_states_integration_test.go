//go:build integration

package pgmigrate_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// CHAOS-7797: the databases the REAL Python Alembic upgrade builds are INPUT and TRUTH of the baseline, hook and
// preflight oracles. They are executed once (golden recording, on statesPythonBuild) and frozen as the captures
// the oracles already compare (pgmigrate.Baseline: heads, pg_dump schema, pg_dump data), one per state, plus the
// row count of every table. A frozen run restores a capture into a scratch database and asserts what it restored
// (heads, table set, row counts) BEFORE anything else runs on it: an empty or truncated dump fails there.
//
// A state is named by its revisions ("0138+0066", "head"); the era states by their own names.

// statesUpgradeProgram is the Python upgrade the migrate Job runs (_run_upgrade), once per revision named in argv.
// The database address goes by environment (DATABASE_URI), never argv.
const statesUpgradeProgram = `
import argparse, os, sys
from dev_health_ops.db import normalize_async_postgres_uri
from dev_health_ops.migrate import _run_upgrade
uri = normalize_async_postgres_uri(os.environ["DATABASE_URI"])
for revision in sys.argv[1:]:
    code = _run_upgrade(argparse.Namespace(db=uri, revision=revision))
    if code:
        sys.exit(code)
`

// statesEraProgram builds a database as an older release left it: Alembic upgraded to argv[1] on the
// application branch, plus the cutover branch (0066).
const statesEraProgram = `
import os, sys
from alembic import command
from dev_health_ops.db import normalize_async_postgres_uri
from dev_health_ops.migrate import _make_alembic_config

cfg = _make_alembic_config(normalize_async_postgres_uri(os.environ["DATABASE_URI"]))
command.upgrade(cfg, sys.argv[1])
command.upgrade(cfg, "0066")
`

// statesPythonSettings are the variables that shape the producer's answers, as constants: the producer's
// environment AND part of the golden's request key (a changed value fails the frozen replay).
var statesPythonSettings = map[string]string{
	"PYTHONHASHSEED":                        "0",
	"OTEL_ENABLED":                          "false",
	"DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER": "1",
	"RIVER_DATABASE_SCHEMA":                 "river",
}

// stateSpec names one state to build. Kind "upgrade" runs statesUpgradeProgram over Revisions; "era" runs
// statesEraProgram to Revisions[0]; "era-head" is the era state of Revisions[0] upgraded to the head by the real
// Alembic (a fork of it, so the era's own seed rows are the same rows).
type stateSpec struct {
	Kind      string   `json:"kind"`
	Revisions []string `json:"revisions"`
}

func (s stateSpec) name() string {
	switch s.Kind {
	case "era":
		return "era:" + s.Revisions[0]
	case "era-head":
		return "era-head:" + s.Revisions[0]
	}
	return strings.Join(s.Revisions, "+")
}

func upgradeSpec(revisions ...string) stateSpec {
	return stateSpec{Kind: "upgrade", Revisions: revisions}
}

// frozenState is what one execution left: the capture the oracles compare and what the database held.
type frozenState struct {
	Name    string             `json:"name"`
	Capture pgmigrate.Baseline `json:"capture"`
	Tables  map[string]int     `json:"tables"`
}

// frozenStates is the golden's body: the image the states were built on, and the states.
type frozenStates struct {
	PostgresImage string        `json:"postgres_image"`
	PostgresMajor int           `json:"postgres_major"`
	States        []frozenState `json:"states"`
}

// tableCounts is the row count of every base table of the public schema.
func tableCounts(t *testing.T, ctx context.Context, conn *pgx.Conn) map[string]int {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name")
	if err != nil {
		t.Fatal(err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, name := range names {
		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM public."+pgx.Identifier{name}.Sanitize()).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[name] = count
	}
	return counts
}

func serverMajor(t *testing.T, ctx context.Context, conn *pgx.Conn) int {
	t.Helper()
	var version int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version / 10000
}

// pythonState runs the Python program of a spec against a scratch database and returns the database. extra
// carries the database address by name; the child runs in the closed environment of the producer.
func runStatesProgram(t *testing.T, ctx context.Context, producer *venueoracle.Producer, uri, program string, args ...string) {
	t.Helper()
	command, err := producer.Command(ctx, statesPythonSettings, []string{"DATABASE_URI=" + uri}, append([]string{"-c", program}, args...)...)
	if err != nil {
		t.Fatal(err)
	}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("the Python program for %v failed: %v", args, pyoracle.RunError(command.Path, err, output))
	}
}

// produceStates builds every state with the REAL Python, on one scratch Postgres, and returns what it left.
func produceStates(t *testing.T, producer *venueoracle.Producer, specs []stateSpec) frozenStates {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	if instance.Container == nil {
		t.Fatal("the states are dumped with the server's own pg_dump and need a container instance, not a remote DSN")
	}
	admin := connect(t, instance.URI)
	out := frozenStates{PostgresImage: containers.PostgresImage, PostgresMajor: serverMajor(t, ctx, admin)}
	eras := map[string]string{}
	eraWindows := map[string]window{}
	for _, spec := range specs {
		database := scratchDatabase(t, admin)
		uri := databaseURI(t, instance.URI, database)
		var captured pgmigrate.Baseline
		switch spec.Kind {
		case "upgrade":
			started := time.Now()
			runStatesProgram(t, ctx, producer, uri, statesUpgradeProgram, spec.Revisions...)
			captured = stabilized(capture(t, ctx, instance, database, window{started, time.Now()}))
		case "era":
			started := time.Now()
			runStatesProgram(t, ctx, producer, uri, statesEraProgram, spec.Revisions[0])
			eraWindows[spec.Revisions[0]] = window{started, time.Now()}
			// the era's own seed rows keep a stamp that is the same in every recording (a literal, not now()): both the
			// era and its upgrade carry them, the hook oracle compares them as literals, and a hook that rewrote one
			// would show. The recording's own timestamps would differ between two runs.
			captured = stabilized(eraStamped(capture(t, ctx, instance, database, window{}), eraWindows[spec.Revisions[0]]))
			eras[spec.Revisions[0]] = database
		case "era-head":
			eraDatabase, ok := eras[spec.Revisions[0]]
			if !ok {
				t.Fatalf("era-head %s: its era state is not built before it", spec.Revisions[0])
			}
			if _, err := admin.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '"+eraDatabase+"' AND pid <> pg_backend_pid()"); err != nil {
				t.Fatal(err)
			}
			fork := scratchDatabaseName(t)
			if _, err := admin.Exec(ctx, "CREATE DATABASE "+fork+" TEMPLATE "+eraDatabase); err != nil {
				t.Fatalf("fork the era database: %v", err)
			}
			t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+fork+" WITH (FORCE)") })
			database = fork
			uri = databaseURI(t, instance.URI, fork)
			// the era's seed rows are older than the window by construction (they were stamped before the fork)
			time.Sleep(3 * time.Second)
			started := time.Now()
			runStatesProgram(t, ctx, producer, uri, statesUpgradeProgram, "head")
			raw := eraStamped(capture(t, ctx, instance, database, window{}), eraWindows[spec.Revisions[0]])
			raw.Data = window{started, time.Now()}.replaceWithNow(raw.Data)
			captured = stabilized(raw)
		default:
			t.Fatalf("unknown state kind %q", spec.Kind)
		}
		captured.Cutover, captured.RiverSchema = productionSettings.Cutover, productionSettings.RiverSchema
		out.States = append(out.States, frozenState{Name: spec.name(), Capture: captured, Tables: tableCounts(t, ctx, connect(t, uri))})
	}
	return out
}

// openStatesGolden is the one entry of a frozen test: it opens the golden at path, records the states of specs
// (only while recording: the closure is the only place Python starts) and returns what it holds.
func openStatesGolden(t *testing.T, path, sha256, test string, specs []stateSpec) (*venueoracle.Golden, frozenStates) {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        path,
		PythonBuild: statesPythonBuild,
		SHA256:      sha256,
		Recipe: "git worktree add --detach $DIR " + statesPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/pgmigrate/ -test '^" + test + "$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)
	input, err := json.Marshal(map[string]any{"specs": specs, "image": containers.PostgresImage})
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("pgmigrate states", statesUpgradeProgram+"\n#era\n"+statesEraProgram, input, statesPythonSettings)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		body, err := json.Marshal(produceStates(t, producer, specs))
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: venueoracle.PackBody(body)}}
	})
	golden.Consumed(t, answers...)
	var frozen frozenStates
	decoder := json.NewDecoder(strings.NewReader(venueoracle.UnpackBody(t, answers[0].Body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&frozen); err != nil {
		t.Fatalf("decode the frozen states: %v", err)
	}
	return golden, frozen
}

// state is the frozen state of name; a missing name fails (the specs and the golden's key must agree).
func (f frozenStates) state(t *testing.T, name string) frozenState {
	t.Helper()
	for _, s := range f.States {
		if s.Name == name {
			return s
		}
	}
	names := make([]string, 0, len(f.States))
	for _, s := range f.States {
		names = append(names, s.Name)
	}
	t.Fatalf("the golden holds no state %q (holds %v): re-record it", name, names)
	return frozenState{}
}

// restored is a frozen state restored into a scratch database.
type restored struct {
	database string
	started  time.Time // before the restore transaction began: the seed rows' now() is inside a window that starts here
}

// restoreState restores a frozen capture into a scratch database and asserts what it restored BEFORE it is used:
// the Postgres image and major version the states were built on, the recorded alembic heads, the table set and
// the row count of every table. An empty, truncated or altered dump fails here, not as a vacuous pass later.
func (f frozenStates) restoreState(t *testing.T, ctx context.Context, instance *containers.Instance, admin *pgx.Conn, name string) restored {
	t.Helper()
	if f.PostgresImage != containers.PostgresImage {
		t.Fatalf("the states were built on %s, this run uses %s: re-record the goldens on the new image", f.PostgresImage, containers.PostgresImage)
	}
	if major := serverMajor(t, ctx, admin); major != f.PostgresMajor {
		t.Fatalf("the states were built on Postgres %d, this server is Postgres %d: re-record the goldens", f.PostgresMajor, major)
	}
	s := f.state(t, name)
	database := scratchDatabase(t, admin)
	conn := connect(t, databaseURI(t, instance.URI, database))
	started := time.Now()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{s.Capture.Schema, s.Capture.Data} {
		if _, err := tx.Exec(ctx, part); err != nil {
			t.Fatalf("restore the frozen state %q: %v", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, "SELECT version_num FROM public.alembic_version ORDER BY version_num")
	if err != nil {
		t.Fatal(err)
	}
	heads, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Capture.Heads) == 0 || strings.Join(heads, ",") != strings.Join(s.Capture.Heads, ",") {
		t.Fatalf("the restored state %q records alembic heads %v, the golden recorded %v", name, heads, s.Capture.Heads)
	}
	counts := tableCounts(t, ctx, conn)
	if len(s.Tables) == 0 {
		t.Fatalf("the frozen state %q records no table: an empty state proves nothing", name)
	}
	if diff := countsDiff(s.Tables, counts); diff != "" {
		t.Fatalf("the restored state %q differs from what the Python upgrade left: %s", name, diff)
	}
	// no connection stays open on the restored database: a test may clone it (CREATE DATABASE ... TEMPLATE)
	if err := conn.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return restored{database: database, started: started}
}

func countsDiff(want, got map[string]int) string {
	var problems []string
	for name, count := range want {
		have, ok := got[name]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("table %s is missing", name))
		case have != count:
			problems = append(problems, fmt.Sprintf("table %s holds %d rows, recorded %d", name, have, count))
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			problems = append(problems, fmt.Sprintf("table %s was not recorded", name))
		}
	}
	sort.Strings(problems)
	return strings.Join(problems, "; ")
}

// eraStampLiteral is the stamp the era's seed rows carry in the golden, in place of the minute the recording ran.
const eraStampLiteral = "'2000-01-01 00:00:00'"

// eraStamped replaces the era build's own timestamps (inside its window) by eraStampLiteral.
func eraStamped(c pgmigrate.Baseline, eraWindow window) pgmigrate.Baseline {
	c.Data = eraWindow.replaceWith(c.Data, eraStampLiteral)
	return c
}

// stabilized replaces every random id the Python upgrade minted (the randomIDColumns) by a fixed uuid, so two
// recordings are byte-identical; the same value stays the same value, so a foreign key still points at its row.
func stabilized(c pgmigrate.Baseline) pgmigrate.Baseline {
	c.Data = mapRandomIDs(c.Data, func(n int) string {
		return "'" + venueoracle.StableUUID(fmt.Sprintf("pgmigrate-random-id-%d", n)) + "'"
	})
	return c
}
