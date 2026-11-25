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

// headPosition is the chain position of the head: the baseline plus every chain revision.
func headPosition(t *testing.T) int {
	t.Helper()
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	return len(chain)
}

// archivedHead is a scratch database the archived chain built to 0162: the index definitions as production built them
// (CREATE INDEX from the revision's text). A database the baseline built holds the same index in pg_dump's re-parsed
// form, which reads differently and means the same.
func (d downInstance) archivedHead(t *testing.T) string {
	t.Helper()
	uri := databaseURI(t, d.instance.URI, scratchDatabase(t, d.admin))
	archivedUpgradeTo(t, context.Background(), connect(t, uri), "0162")
	return uri
}

// withoutIndex is a scratch database at the head with the index of revision 0159 (failure streak), 0161 (open units) or
// 0167 (finalize outbox) dropped: the state before that revision, which the build-index verb exists for. 0159 and 0161
// are in the baseline; 0167 is a chain revision whose index the head holds.
func (d downInstance) withoutIndex(t *testing.T, revision string) string {
	t.Helper()
	index := map[string]string{"0159": pgmigrate.FailureStreakIndexName, "0161": pgmigrate.OpenUnitsIndexName, "0167": pgmigrate.FinalizeOutboxIndexName}[revision]
	if index == "" {
		t.Fatalf("no build-index revision %s", revision)
	}
	uri := d.at(t, headPosition(t))
	if _, err := connect(t, uri).Exec(context.Background(), "DROP INDEX public."+index); err != nil {
		t.Fatal(err)
	}
	return uri
}

// The verb builds the index before revision 0159, finds it valid on a second run, repairs an INVALID leftover, and
// revision 0159 then does nothing: the database holds the definition a plain walk to the head builds.
func TestBuildIndexBeforeRevision0159(t *testing.T) {
	d := newDownInstance(t)
	headConn := connect(t, d.archivedHead(t))
	wantDefinition, valid := streakIndexDefinition(t, headConn)
	if !valid || !strings.Contains(wantDefinition, "(org_id, integration_id, source_id, dataset_key, updated_at DESC, id DESC)") {
		t.Fatalf("the walk's index is %q valid=%v", wantDefinition, valid)
	}

	uri := d.withoutIndex(t, "0159")
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
	if applied.Action != "up_to_date" || len(applied.Applied) != 0 {
		t.Fatalf("upgrade after the build = %+v, want up_to_date: the index is in the baseline", applied)
	}
	if got, valid := streakIndexDefinition(t, conn); !valid || got != wantDefinition {
		t.Fatalf("after the upgrade: index %q valid=%v, want %q", got, valid, wantDefinition)
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
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0159")
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
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0159")
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
	length := headPosition(t)
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
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0161")
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
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0161")
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
	length := headPosition(t)
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
	d := newDownInstance(t)
	headConn := connect(t, d.archivedHead(t))
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
	uri := d.withoutIndex(t, "0161")
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
	if applied.Action != "up_to_date" || len(applied.Applied) != 0 {
		t.Fatalf("upgrade after the build = %+v, want up_to_date: the index is in the baseline", applied)
	}
	if got, valid := definitionOf(conn); !valid || got != wantDefinition {
		t.Fatalf("after the upgrade: index %q valid=%v, want %q", got, valid, wantDefinition)
	}
}

// The two indexes have their own build lock: a held streak lock does not stop an open-units build, and a held open-units lock
// refuses an open-units run over an index that is not valid.
func TestBuildOpenUnitsIndexHasItsOwnBuildLock(t *testing.T) {
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0161")
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

// The finalize-outbox verb builds the index CONCURRENTLY on a database one revision below 0167, finds it valid on a
// second run, repairs an INVALID leftover, and revision 0167 then does nothing for it: the database holds the
// definition a plain walk to the head builds. While the build runs the outbox keeps taking writes (CONCURRENTLY holds
// SHARE UPDATE EXCLUSIVE, which no INSERT conflicts with).
func TestBuildFinalizeOutboxIndexBeforeRevision0167(t *testing.T) {
	d := newDownInstance(t)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	position := -1
	for index, file := range chain {
		if file.Revision == "0167" {
			position = index
		}
	}
	if position < 0 {
		t.Fatal("the chain holds no revision 0167")
	}
	headConn := connect(t, d.at(t, len(chain)))
	wantDefinition, valid := indexDefinition(t, headConn, pgmigrate.FinalizeOutboxIndexName)
	if !valid || !strings.Contains(wantDefinition, "(((args #>> '{domain,id}'::text[]))) WHERE ((job_kind)::text = 'metrics.daily_finalize'::text)") {
		t.Fatalf("the walk's index is %q valid=%v", wantDefinition, valid)
	}
	uri := d.at(t, position)
	conn := connect(t, uri)
	var present bool
	if err := conn.QueryRow(context.Background(), "SELECT to_regclass('public."+pgmigrate.FinalizeOutboxIndexName+"') IS NOT NULL").Scan(&present); err != nil || present {
		t.Fatalf("the index exists below revision 0167 (err %v)", err)
	}
	for _, step := range []struct{ name, damage, action string }{
		{"first build", "", "built"},
		{"second run", "", "present"},
		{"invalid leftover", "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'public." + pgmigrate.FinalizeOutboxIndexName + "'::regclass", "rebuilt"},
	} {
		if step.damage != "" {
			if _, err := conn.Exec(context.Background(), step.damage); err != nil {
				t.Fatal(err)
			}
		}
		code, result, stderr := buildIndexVerbFor(t, uri, "finalize-outbox")
		if code != cli.ExitOK || result["action"] != step.action || result["valid"] != true {
			t.Fatalf("%s: exit %d result %v stderr %s, want action %s", step.name, code, result, stderr, step.action)
		}
		if got, valid := indexDefinition(t, conn, pgmigrate.FinalizeOutboxIndexName); !valid || got != wantDefinition {
			t.Fatalf("%s: index %q valid=%v, want %q", step.name, got, valid, wantDefinition)
		}
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	applied, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain)
	if err != nil {
		t.Fatalf("upgrade to the head: %v", err)
	}
	if got, valid := indexDefinition(t, conn, pgmigrate.FinalizeOutboxIndexName); !valid || got != wantDefinition {
		t.Fatalf("after the upgrade (%+v): index %q valid=%v, want %q", applied, got, valid, wantDefinition)
	}
}

// The finalize-outbox index has its own build lock: a held open-units lock does not stop a finalize-outbox build.
func TestBuildFinalizeOutboxIndexHasItsOwnBuildLock(t *testing.T) {
	d := newDownInstance(t)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	uri := d.at(t, len(chain)-1)
	holder := connect(t, uri)
	const openUnitsLockKey = int64(0x64686f5f6979)
	var locked bool
	if err := holder.QueryRow(context.Background(), "SELECT pg_try_advisory_lock($1)", openUnitsLockKey).Scan(&locked); err != nil || !locked {
		t.Fatalf("hold the open-units build lock: %v %v", locked, err)
	}
	if code, result, stderr := buildIndexVerbFor(t, uri, "finalize-outbox"); code != cli.ExitOK || result["action"] != "built" {
		t.Fatalf("finalize-outbox under a held open-units lock: exit %d result %v stderr %s, want built", code, result, stderr)
	}
}

// Revision 0167 on a database one revision below it: a VALID prebuilt index (the hook's) is kept, the same object; an
// INVALID one (a failed CONCURRENTLY build the hook did not clean up) is replaced, so the walk never records 0167 over an
// index the planner cannot use.
func TestRevision0167KeepsAValidIndexAndReplacesAnInvalidOne(t *testing.T) {
	d := newDownInstance(t)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	position := -1
	for index, file := range chain {
		if file.Revision == "0167" {
			position = index
		}
	}
	if position < 0 {
		t.Fatal("the chain holds no revision 0167")
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	wantDefinition, valid := indexDefinition(t, connect(t, d.at(t, len(chain))), pgmigrate.FinalizeOutboxIndexName)
	if !valid {
		t.Fatalf("the walk's index %q is not valid", wantDefinition)
	}
	indexOID := func(t *testing.T, conn *pgx.Conn) uint32 {
		t.Helper()
		var oid uint32
		if err := conn.QueryRow(context.Background(), "SELECT 'public."+pgmigrate.FinalizeOutboxIndexName+"'::regclass::oid").Scan(&oid); err != nil {
			t.Fatal(err)
		}
		return oid
	}
	for _, step := range []struct {
		name, damage string
		keepsObject  bool
	}{
		{"valid prebuilt index", "", true},
		{"invalid leftover", "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'public." + pgmigrate.FinalizeOutboxIndexName + "'::regclass", false},
	} {
		t.Run(step.name, func(t *testing.T) {
			conn := connect(t, d.at(t, position))
			if _, err := conn.Exec(context.Background(), "CREATE INDEX "+pgmigrate.FinalizeOutboxIndexBody); err != nil {
				t.Fatal(err)
			}
			if step.damage != "" {
				if _, err := conn.Exec(context.Background(), step.damage); err != nil {
					t.Fatal(err)
				}
				if _, valid := indexDefinition(t, conn, pgmigrate.FinalizeOutboxIndexName); valid {
					t.Fatal("the planted index is still valid: the cell would prove nothing")
				}
			}
			before := indexOID(t, conn)
			applied, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain)
			if err != nil {
				t.Fatalf("upgrade to the head: %v", err)
			}
			got, valid := indexDefinition(t, conn, pgmigrate.FinalizeOutboxIndexName)
			if !valid || got != wantDefinition {
				t.Fatalf("after the upgrade (%+v): index %q valid=%v, want the walk's %q valid", applied, got, valid, wantDefinition)
			}
			if after := indexOID(t, conn); (after == before) != step.keepsObject {
				t.Fatalf("index oid %d -> %d, want kept=%v", before, after, step.keepsObject)
			}
		})
	}
}

// finalizeOutboxBuildLockKey is the build-index lock key of the finalize-outbox verb ("dho_iz"). The 0167 cell below
// first proves the builder uses it (a held key refuses the builder), so revision 0167 and the builder cannot drift apart.
const finalizeOutboxBuildLockKey = int64(0x64686f5f697a)

// revision0167Database is a scratch database one revision below 0167, with the chain and baseline to walk it up.
func revision0167Database(t *testing.T, d downInstance) (string, pgmigrate.Baseline, []pgmigrate.ChainFile) {
	t.Helper()
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	position := -1
	for index, file := range chain {
		if file.Revision == "0167" {
			position = index
		}
	}
	if position < 0 {
		t.Fatal("the chain holds no revision 0167")
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	return d.at(t, position), baseline, chain
}

// Revision 0167 refuses to run while the finalize-outbox builder holds its lock: it never drops an index a running
// CONCURRENTLY build is about to make valid. Released, the walk applies 0167.
func TestRevision0167RefusesWhileTheFinalizeOutboxBuilderRuns(t *testing.T) {
	d := newDownInstance(t)
	uri, baseline, chain := revision0167Database(t, d)
	holder := connect(t, uri)
	var locked bool
	if err := holder.QueryRow(context.Background(), "SELECT pg_try_advisory_lock($1)", finalizeOutboxBuildLockKey).Scan(&locked); err != nil || !locked {
		t.Fatalf("hold the finalize-outbox build lock: %v %v", locked, err)
	}
	conn := connect(t, uri)
	if _, err := pgmigrate.BuildFinalizeOutboxIndex(context.Background(), conn, pgmigrate.IndexBuildOptions{LockTimeout: time.Second, Attempts: 1}, slog.New(slog.NewJSONHandler(io.Discard, nil))); !errors.Is(err, pgmigrate.ErrBuildLockHeld) {
		t.Fatalf("builder under the held key: err %v, want ErrBuildLockHeld (the cell's key is not the builder's)", err)
	}
	if _, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain); err == nil || !strings.Contains(err.Error(), "build-index finalize-outbox is running") {
		t.Fatalf("upgrade under the held build lock: err %v, want the running-builder refusal", err)
	}
	if _, err := holder.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", finalizeOutboxBuildLockKey); err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain); err != nil {
		t.Fatalf("upgrade after the build lock was released: %v", err)
	}
	if _, valid := indexDefinition(t, conn, pgmigrate.FinalizeOutboxIndexName); !valid {
		t.Fatal("the index is not valid after the upgrade")
	}
}

// With a valid prebuilt index, revision 0167 takes no lock on worker_job_outbox: an in-flight outbox writer (ROW
// EXCLUSIVE) does not hold up the walk. The walk's session runs under a lock_timeout, so a SHARE lock request fails it.
func TestRevision0167OverAValidIndexDoesNotWaitForOutboxWriters(t *testing.T) {
	d := newDownInstance(t)
	uri, baseline, chain := revision0167Database(t, d)
	conn := connect(t, uri)
	if _, err := conn.Exec(context.Background(), "CREATE INDEX "+pgmigrate.FinalizeOutboxIndexBody); err != nil {
		t.Fatal(err)
	}
	writer := connect(t, uri)
	tx, err := writer.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), "LOCK TABLE public.worker_job_outbox IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(context.Background(), "SET lock_timeout = '2s'"); err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain); err != nil {
		t.Fatalf("upgrade over a valid index with an outbox writer open: %v (0167 asked for a lock on the outbox)", err)
	}
}
