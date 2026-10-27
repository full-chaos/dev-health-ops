//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func buildIndexVerb(t *testing.T, uri string, args ...string) (int, map[string]any, string) {
	t.Helper()
	return buildIndexVerbFor(t, uri, "failure-streak", args...)
}

func buildIndexVerbFor(t *testing.T, uri, index string, args ...string) (int, map[string]any, string) {
	t.Helper()
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(resolve).Children {
		if child.Name == "build-index" {
			run = child.Run
		}
	}
	var out, errs bytes.Buffer
	code := run(context.Background(), cli.Env{
		Args:   append(append([]string{}, args...), index),
		Lookup: func(string) (string, bool) { return "", false },
		Stdout: &out, Stderr: &errs,
	})
	var result map[string]any
	if code == cli.ExitOK {
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			t.Fatalf("result is not JSON: %v: %s", err, out.String())
		}
	}
	return code, result, errs.String()
}

func streakIndexDefinition(t *testing.T, conn *pgx.Conn) (definition string, valid bool) {
	t.Helper()
	return indexDefinition(t, conn, pgmigrate.FailureStreakIndexName)
}

func indexDefinition(t *testing.T, conn *pgx.Conn, name string) (definition string, valid bool) {
	t.Helper()
	err := conn.QueryRow(context.Background(), `
SELECT pg_get_indexdef(i.indexrelid), i.indisvalid AND i.indisready
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname = $1 AND c.relnamespace = 'public'::regnamespace`, name).Scan(&definition, &valid)
	if err != nil {
		t.Fatalf("read the index: %v", err)
	}
	return definition, valid
}

func chainPosition(t *testing.T, revision string) (position, length int) {
	t.Helper()
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	for i, file := range chain {
		if file.Revision == revision {
			return i, len(chain)
		}
	}
	t.Fatalf("the chain holds no revision %s", revision)
	return 0, 0
}

// The verb builds the index before revision 0159, finds it valid on a second run, repairs an INVALID leftover, and
// revision 0159 then does nothing: the database holds the definition a plain walk to the head builds.
func TestBuildIndexBeforeRevision0159(t *testing.T) {
	position, length := chainPosition(t, "0159")
	d := newDownInstance(t)
	headConn := connect(t, d.at(t, length))
	wantDefinition, valid := streakIndexDefinition(t, headConn)
	if !valid || !strings.Contains(wantDefinition, "(org_id, integration_id, source_id, dataset_key, updated_at DESC, id DESC)") {
		t.Fatalf("the walk's index is %q valid=%v", wantDefinition, valid)
	}

	uri := d.at(t, position)
	conn := connect(t, uri)
	var present bool
	if err := conn.QueryRow(context.Background(), "SELECT to_regclass('public.ix_sync_run_units_failure_streak') IS NOT NULL").Scan(&present); err != nil || present {
		t.Fatalf("the index exists before revision 0159 (err %v)", err)
	}
	for _, step := range []struct {
		name   string
		damage string
		action string
	}{
		{"first build", "", "built"},
		{"second run", "", "present"},
		{"invalid leftover", "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'public.ix_sync_run_units_failure_streak'::regclass", "rebuilt"},
	} {
		if step.damage != "" {
			if _, err := conn.Exec(context.Background(), step.damage); err != nil {
				t.Fatal(err)
			}
		}
		code, result, stderr := buildIndexVerb(t, uri)
		if code != cli.ExitOK || result["action"] != step.action || result["valid"] != true {
			t.Fatalf("%s: exit %d result %v stderr %s, want action %s", step.name, code, result, stderr, step.action)
		}
		if got, valid := streakIndexDefinition(t, conn); !valid || got != wantDefinition {
			t.Fatalf("%s: index %q valid=%v, want %q", step.name, got, valid, wantDefinition)
		}
	}

	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	applied, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain)
	if err != nil {
		t.Fatalf("upgrade to the head: %v", err)
	}
	if len(applied.Applied) == 0 || applied.Applied[0] != "0159" {
		t.Fatalf("applied %v, want it to start at 0159", applied.Applied)
	}
	if got, valid := streakIndexDefinition(t, conn); !valid || got != wantDefinition {
		t.Fatalf("after revision 0159: index %q valid=%v, want %q", got, valid, wantDefinition)
	}
}

// A database without sync_run_units is refused: the Job runs against an existing database, and a "success" there would
// let the release go on without the index.
func TestBuildIndexFailsWithoutTheTable(t *testing.T) {
	d := newDownInstance(t)
	uri := databaseURI(t, d.instance.URI, scratchDatabase(t, d.admin))
	code, _, stderr := buildIndexVerb(t, uri)
	if code == cli.ExitOK || !strings.Contains(stderr, "sync_run_units") {
		t.Fatalf("exit %d stderr %s, want a failure naming sync_run_units", code, stderr)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func indexRowCount(t *testing.T, conn *pgx.Conn) int {
	t.Helper()
	return indexRowCountFor(t, conn, pgmigrate.FailureStreakIndexName)
}

func indexRowCountFor(t *testing.T, conn *pgx.Conn, name string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM pg_class WHERE relname = $1 AND relnamespace = 'public'::regnamespace", name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// holdWriter opens a transaction that holds a ROW EXCLUSIVE lock on sync_run_units, as an in-flight writer does: it blocks
// both CREATE INDEX CONCURRENTLY and DROP INDEX CONCURRENTLY until released.
func holdWriter(t *testing.T, uri string) (release func()) {
	t.Helper()
	conn := connect(t, uri)
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), "LOCK TABLE public.sync_run_units IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), "SELECT 'sentinel_query_text_xyz'"); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() { once.Do(func() { _ = tx.Rollback(context.Background()) }) }
	t.Cleanup(release)
	return release
}

// When the build fails for good and the blocker clears before the cleanup, the INVALID index the failed build left is dropped.
func TestBuildIndexFinalFailureDropsTheInvalidIndex(t *testing.T) {
	position, _ := chainPosition(t, "0159")
	d := newDownInstance(t)
	uri := d.at(t, position)
	release := holdWriter(t, uri)

	conn := connect(t, uri)
	logs := &syncBuffer{}
	go func() {
		for !strings.Contains(logs.String(), "attempt failed") {
			time.Sleep(10 * time.Millisecond)
		}
		release()
	}()
	_, err := pgmigrate.BuildFailureStreakIndex(context.Background(), conn,
		pgmigrate.IndexBuildOptions{LockTimeout: 2 * time.Second, Attempts: 1, Deadline: 4 * time.Second, CleanupBudget: 30 * time.Second}, slog.New(slog.NewJSONHandler(logs, nil)))
	if err == nil {
		t.Fatal("the build succeeded under a held writer lock")
	}
	if n := indexRowCount(t, conn); n != 0 {
		t.Fatalf("%d index rows left after the final failure; log %s", n, logs.String())
	}
}

// When the blocker outlives the cleanup too, the INVALID index stays, the verb exits non-zero and logs the distinct line;
// the next run drops it and builds a valid one.
func TestBuildIndexFinalFailureUnderHeldLockLeavesIndexForNextRun(t *testing.T) {
	position, _ := chainPosition(t, "0159")
	d := newDownInstance(t)
	uri := d.at(t, position)
	release := holdWriter(t, uri)

	conn := connect(t, uri)
	logs := &syncBuffer{}
	_, err := pgmigrate.BuildFailureStreakIndex(context.Background(), conn,
		pgmigrate.IndexBuildOptions{LockTimeout: time.Second, Attempts: 1, Deadline: 3 * time.Second, CleanupBudget: 3 * time.Second}, slog.New(slog.NewJSONHandler(logs, nil)))
	if err == nil {
		t.Fatal("the build succeeded under a held writer lock")
	}
	stderr := logs.String()
	if !strings.Contains(stderr, "build-index: invalid index left; the next run drops it") {
		t.Fatalf("stderr lacks the leftover line: %s", stderr)
	}
	if _, valid := streakIndexDefinition(t, conn); valid {
		t.Fatal("the leftover index is valid")
	}
	release()
	code, result, stderr := buildIndexVerb(t, uri)
	if code != cli.ExitOK || result["action"] != "rebuilt" || result["valid"] != true {
		t.Fatalf("next run: exit %d result %v stderr %s", code, result, stderr)
	}
}

// A VALID index is never dropped by the cleanup that follows a final failed build.
func TestDropInvalidFailureStreakIndexKeepsAValidIndex(t *testing.T) {
	_, length := chainPosition(t, "0159")
	d := newDownInstance(t)
	conn := connect(t, d.at(t, length))
	if _, valid := streakIndexDefinition(t, conn); !valid {
		t.Fatal("the head database has no valid streak index")
	}
	pgmigrate.DropInvalidUnitsIndex(context.Background(), conn, "failure-streak",
		pgmigrate.IndexBuildOptions{LockTimeout: time.Second}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if _, valid := streakIndexDefinition(t, conn); !valid {
		t.Fatal("the valid index did not survive the cleanup")
	}
}

// Mirrors of the failure-streak cleanup tests for the open-units index (revision 0161), on the migrated schema.
// When the build fails for good and the blocker clears before the cleanup, the INVALID index the failed build left is dropped.
func TestBuildOpenUnitsIndexFinalFailureDropsTheInvalidIndex(t *testing.T) {
	position, _ := chainPosition(t, "0161")
	d := newDownInstance(t)
	uri := d.at(t, position)
	release := holdWriter(t, uri)

	conn := connect(t, uri)
	logs := &syncBuffer{}
	go func() {
		for !strings.Contains(logs.String(), "attempt failed") {
			time.Sleep(10 * time.Millisecond)
		}
		release()
	}()
	_, err := pgmigrate.BuildOpenUnitsIndex(context.Background(), conn,
		pgmigrate.IndexBuildOptions{LockTimeout: 2 * time.Second, Attempts: 1, Deadline: 4 * time.Second, CleanupBudget: 30 * time.Second}, slog.New(slog.NewJSONHandler(logs, nil)))
	if err == nil {
		t.Fatal("the build succeeded under a held writer lock")
	}
	if n := indexRowCountFor(t, conn, pgmigrate.OpenUnitsIndexName); n != 0 {
		t.Fatalf("%d index rows left after the final failure; log %s", n, logs.String())
	}
}

// When the blocker outlives the cleanup too, the INVALID index stays, the verb exits non-zero and logs the distinct line;
// the next run drops it and builds a valid one.
func TestBuildOpenUnitsIndexFinalFailureUnderHeldLockLeavesIndexForNextRun(t *testing.T) {
	position, _ := chainPosition(t, "0161")
	d := newDownInstance(t)
	uri := d.at(t, position)
	release := holdWriter(t, uri)

	conn := connect(t, uri)
	logs := &syncBuffer{}
	_, err := pgmigrate.BuildOpenUnitsIndex(context.Background(), conn,
		pgmigrate.IndexBuildOptions{LockTimeout: time.Second, Attempts: 1, Deadline: 3 * time.Second, CleanupBudget: 3 * time.Second}, slog.New(slog.NewJSONHandler(logs, nil)))
	if err == nil {
		t.Fatal("the build succeeded under a held writer lock")
	}
	stderr := logs.String()
	if !strings.Contains(stderr, "build-index: invalid index left; the next run drops it") {
		t.Fatalf("stderr lacks the leftover line: %s", stderr)
	}
	if _, valid := indexDefinition(t, conn, pgmigrate.OpenUnitsIndexName); valid {
		t.Fatal("the leftover index is valid")
	}
	release()
	code, result, stderr := buildIndexVerbFor(t, uri, "open-units")
	if code != cli.ExitOK || result["action"] != "rebuilt" || result["valid"] != true {
		t.Fatalf("next run: exit %d result %v stderr %s", code, result, stderr)
	}
}

// A VALID index is never dropped by the cleanup that follows a final failed build.
func TestDropInvalidOpenUnitsIndexKeepsAValidIndex(t *testing.T) {
	_, length := chainPosition(t, "0161")
	d := newDownInstance(t)
	conn := connect(t, d.at(t, length))
	if _, valid := indexDefinition(t, conn, pgmigrate.OpenUnitsIndexName); !valid {
		t.Fatal("the head database has no valid open-units index")
	}
	pgmigrate.DropInvalidUnitsIndex(context.Background(), conn, "open-units",
		pgmigrate.IndexBuildOptions{LockTimeout: time.Second}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if _, valid := indexDefinition(t, conn, pgmigrate.OpenUnitsIndexName); !valid {
		t.Fatal("the valid index did not survive the cleanup")
	}
}

// The open-units verb builds the index before revision 0161, finds it valid on a second run, repairs an INVALID leftover,
// and revision 0161 then does nothing for it: the database holds the definition a plain walk to the head builds.
func TestBuildOpenUnitsIndexBeforeRevision0161(t *testing.T) {
	position, length := chainPosition(t, "0161")
	d := newDownInstance(t)
	headConn := connect(t, d.at(t, length))
	definitionOf := func(conn *pgx.Conn) (definition string, valid bool) {
		err := conn.QueryRow(context.Background(), `
SELECT pg_get_indexdef(i.indexrelid), i.indisvalid AND i.indisready
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname = $1 AND c.relnamespace = 'public'::regnamespace`, pgmigrate.OpenUnitsIndexName).Scan(&definition, &valid)
		if err != nil {
			t.Fatalf("read the index: %v", err)
		}
		return definition, valid
	}
	wantDefinition, valid := definitionOf(headConn)
	if !valid || !strings.Contains(wantDefinition, "(sync_run_id) WHERE") {
		t.Fatalf("the walk's index is %q valid=%v", wantDefinition, valid)
	}
	uri := d.at(t, position)
	conn := connect(t, uri)
	var present bool
	if err := conn.QueryRow(context.Background(), "SELECT to_regclass('public.ix_sync_run_units_open_run') IS NOT NULL").Scan(&present); err != nil || present {
		t.Fatalf("the index exists before revision 0161 (err %v)", err)
	}
	for _, step := range []struct{ name, damage, action string }{
		{"first build", "", "built"},
		{"second run", "", "present"},
		{"invalid leftover", "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'public.ix_sync_run_units_open_run'::regclass", "rebuilt"},
	} {
		if step.damage != "" {
			if _, err := conn.Exec(context.Background(), step.damage); err != nil {
				t.Fatal(err)
			}
		}
		code, result, stderr := buildIndexVerbFor(t, uri, "open-units")
		if code != cli.ExitOK || result["action"] != step.action || result["valid"] != true {
			t.Fatalf("%s: exit %d result %v stderr %s, want action %s", step.name, code, result, stderr, step.action)
		}
		if got, valid := definitionOf(conn); !valid || got != wantDefinition {
			t.Fatalf("%s: index %q valid=%v, want %q", step.name, got, valid, wantDefinition)
		}
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	applied, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain)
	if err != nil {
		t.Fatalf("upgrade to the head: %v", err)
	}
	if len(applied.Applied) == 0 || applied.Applied[0] != "0161" {
		t.Fatalf("applied %v, want it to start at 0161", applied.Applied)
	}
	if got, valid := definitionOf(conn); !valid || got != wantDefinition {
		t.Fatalf("after revision 0161: index %q valid=%v, want %q", got, valid, wantDefinition)
	}
}

// The two indexes have their own build lock: a held streak lock does not stop an open-units build, and a held open-units lock
// refuses an open-units run over an index that is not valid.
func TestBuildOpenUnitsIndexHasItsOwnBuildLock(t *testing.T) {
	position, _ := chainPosition(t, "0161")
	d := newDownInstance(t)
	uri := d.at(t, position)
	holder := connect(t, uri)
	hold := func(key int64) {
		t.Helper()
		var got bool
		if err := holder.QueryRow(context.Background(), "SELECT pg_try_advisory_lock($1)", key).Scan(&got); err != nil || !got {
			t.Fatalf("take lock %#x: %v %v", key, got, err)
		}
	}
	hold(pgmigrate.BuildIndexLockKey)
	conn := connect(t, uri)
	if _, err := pgmigrate.BuildOpenUnitsIndex(context.Background(), conn,
		pgmigrate.IndexBuildOptions{LockTimeout: time.Second, Attempts: 1, Deadline: 60 * time.Second}, slog.New(slog.NewJSONHandler(io.Discard, nil))); err != nil {
		t.Fatalf("open-units build under a held streak lock: %v", err)
	}
	if _, err := holder.Exec(context.Background(), "SELECT pg_advisory_unlock_all()"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(context.Background(), "DROP INDEX public."+pgmigrate.OpenUnitsIndexName); err != nil {
		t.Fatal(err)
	}
	hold(pgmigrate.OpenUnitsBuildIndexLockKey)
	_, err := pgmigrate.BuildOpenUnitsIndex(context.Background(), conn,
		pgmigrate.IndexBuildOptions{LockTimeout: time.Second, Attempts: 1, Deadline: 60 * time.Second}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if !errors.Is(err, pgmigrate.ErrBuildLockHeld) {
		t.Fatalf("err = %v, want ErrBuildLockHeld", err)
	}
}

// From the database at revision 0159 the chain applies 0160 and then 0161 in order, and 0161 leaves its column and its index.
func TestTheChainAppliesRevisions0160And0161InOrderFrom0159(t *testing.T) {
	position, _ := chainPosition(t, "0160")
	d := newDownInstance(t)
	conn := connect(t, d.at(t, position))
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	applied, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain)
	if err != nil {
		t.Fatalf("upgrade from 0159: %v", err)
	}
	if len(applied.Applied) < 2 || applied.Applied[0] != "0160" || applied.Applied[1] != "0161" {
		t.Fatalf("applied %v, want it to start 0160, 0161", applied.Applied)
	}
	var column, index bool
	if err := conn.QueryRow(context.Background(), `SELECT
EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_run_post_dispatches' AND column_name = 'recorded_at'),
to_regclass('public.`+pgmigrate.OpenUnitsIndexName+`') IS NOT NULL`).Scan(&column, &index); err != nil || !column || !index {
		t.Fatalf("after 0161: recorded_at=%v open-units index=%v err=%v", column, index, err)
	}
}
