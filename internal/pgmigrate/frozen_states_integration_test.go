//go:build integration

package pgmigrate_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// CHAOS-7797: the databases the REAL Python Alembic upgrade builds are INPUT and TRUTH of the baseline, hook and
// preflight oracles. They are executed once (golden recording, on statesPythonBuild) and frozen as the captures
// the oracles already compare (pgmigrate.Baseline: heads, pg_dump schema, pg_dump data), one per state, plus the
// row count of every table. A frozen run restores a capture into a scratch database and asserts what it restored
// (heads, table set, row counts) BEFORE anything else runs on it: an empty or truncated dump fails there.
//
// A state is named by its revisions ("0138+0066", "head"); the era states by their own names.

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

// openStatesGolden is the one entry of a frozen test: it opens the golden at path and returns the states of specs
// it holds.
func openStatesGolden(t *testing.T, path, sha256, test string, specs []stateSpec) (*venueoracle.Golden, frozenStates) {
	t.Helper()
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        path,
		PythonBuild: statesPythonBuild,
		SHA256:      sha256,
	})
	input, err := json.Marshal(map[string]any{"specs": specs, "image": containers.PostgresImage})
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("pgmigrate states", input)
	answers := golden.Produce(t, []venueoracle.Request{request})
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
