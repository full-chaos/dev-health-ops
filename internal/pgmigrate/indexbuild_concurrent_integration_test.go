//go:build integration

package pgmigrate_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

const buildLockKey = pgmigrate.BuildIndexLockKey

var lockHeldRefusal = pgmigrate.ErrBuildLockHeld.Error()

// sqlRecorder is a pgx tracer that keeps every statement a connection sends.
type sqlRecorder struct {
	mu   sync.Mutex
	sent []string
}

func (r *sqlRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, data.SQL)
	return ctx
}

func (r *sqlRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

var ddlStatement = regexp.MustCompile(`(?i)^\s*(CREATE|DROP|REINDEX|ALTER|LOCK)\b`)

// ddl is every statement sent that changes or locks the schema.
func (r *sqlRecorder) ddl() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, sql := range r.sent {
		if ddlStatement.MatchString(sql) {
			out = append(out, strings.TrimSpace(sql))
		}
	}
	return out
}

func tracedConn(t *testing.T, uri string) (*pgx.Conn, *sqlRecorder) {
	t.Helper()
	config, err := pgx.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &sqlRecorder{}
	config.Tracer = recorder
	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn, recorder
}

func backendPID(t *testing.T, conn *pgx.Conn) int32 {
	t.Helper()
	var pid int32
	if err := conn.QueryRow(context.Background(), "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return pid
}

// holdBuildLock takes the build lock on its own session, as a running build-index run holds it.
func holdBuildLock(t *testing.T, uri string) *pgx.Conn {
	t.Helper()
	conn := connect(t, uri)
	if _, err := conn.Exec(context.Background(), "SELECT pg_advisory_lock($1)", buildLockKey); err != nil {
		t.Fatal(err)
	}
	return conn
}

type indexState struct {
	oid   uint32
	valid bool
}

func streakIndexState(t *testing.T, conn *pgx.Conn) indexState {
	t.Helper()
	var s indexState
	err := conn.QueryRow(context.Background(), `
SELECT i.indexrelid::oid, i.indisvalid AND i.indisready
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname = $1 AND c.relnamespace = 'public'::regnamespace`, pgmigrate.FailureStreakIndexName).Scan(&s.oid, &s.valid)
	if err != nil {
		t.Fatalf("read the index: %v", err)
	}
	return s
}

func waitUntil(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", within, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitsForALock(t *testing.T, observer *pgx.Conn, pid int32) func() bool {
	return func() bool {
		var waiting bool
		if err := observer.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)", pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		return waiting
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

type buildOutcome struct {
	result pgmigrate.IndexBuildResult
	err    error
}

const plantValidSQL = "UPDATE pg_index SET indisvalid = true WHERE indexrelid = 'public.ix_sync_run_units_failure_streak'::regclass"

// A fake second builder finishes the build while this run's table-lock probe waits: it holds SHARE UPDATE EXCLUSIVE (the
// lock a concurrent build holds) and, inside that lock, turns the planted INVALID index VALID (the reverse of the plant),
// then commits. When the probe gets the lock the index is VALID: the run must keep that same index, not drop and rebuild it.
// (A real REINDEX INDEX CONCURRENTLY queued ahead of the probe ended in 40P01 on PG 18, so the fake keeps the order fixed.)
func TestBuildIndexSkipsAnIndexAnotherBuilderMadeValidWhileTheProbeWaited(t *testing.T) {
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0159")
	plantInvalidIndex(t, uri)
	observer := connect(t, uri)
	before := streakIndexState(t, observer)
	ctx := context.Background()

	builder, err := connect(t, uri).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = builder.Rollback(context.Background()) }()
	if _, err := builder.Exec(ctx, "LOCK TABLE public.sync_run_units IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}

	run, recorder := tracedConn(t, uri)
	runPID := backendPID(t, run)
	runDone := make(chan buildOutcome, 1)
	go func() {
		result, err := pgmigrate.BuildFailureStreakIndex(ctx, run,
			pgmigrate.IndexBuildOptions{LockTimeout: 30 * time.Second, Attempts: 1, Deadline: 90 * time.Second}, discardLogger())
		runDone <- buildOutcome{result, err}
	}()
	waitUntil(t, 20*time.Second, "the run's probe to queue behind the second builder", waitsForALock(t, observer, runPID))

	if _, err := builder.Exec(ctx, plantValidSQL); err != nil {
		t.Fatal(err)
	}
	if err := builder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	outcome := <-runDone
	if outcome.err != nil || outcome.result.Action != "present" || !outcome.result.Valid {
		t.Fatalf("result %+v err %v, want action present (the index another builder made valid)", outcome.result, outcome.err)
	}
	for _, sql := range recorder.ddl() {
		if !strings.HasPrefix(sql, "LOCK TABLE") {
			t.Fatalf("the run sent %q over an index another builder made valid", sql)
		}
	}
	if after := streakIndexState(t, observer); after != (indexState{oid: before.oid, valid: true}) {
		t.Fatalf("index %+v, want the same relation %d, valid", after, before.oid)
	}
}

// While another build-index run holds the build lock and the index is INVALID, a run refuses at once with the named
// refusal and sends no DDL: the INVALID index stays the same relation. The verb reports it as build_index_lock_held.
func TestBuildIndexRefusesWhileAnotherRunHoldsTheBuildLock(t *testing.T) {
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0159")
	plantInvalidIndex(t, uri)
	observer := connect(t, uri)
	before := streakIndexState(t, observer)
	holdBuildLock(t, uri)

	run, recorder := tracedConn(t, uri)
	logs := &syncBuffer{}
	started := time.Now()
	result, err := pgmigrate.BuildFailureStreakIndex(context.Background(), run,
		pgmigrate.IndexBuildOptions{LockTimeout: 2 * time.Second, Attempts: 3, RetryDelay: time.Second, Deadline: 30 * time.Second},
		slog.New(slog.NewJSONHandler(logs, nil)))
	if !errors.Is(err, pgmigrate.ErrBuildLockHeld) {
		t.Fatalf("result %+v err %v, want the refusal %q", result, err, lockHeldRefusal)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("the refusal took %s; the loser must not wait", elapsed)
	}
	if result.Attempts != 0 || result.Valid {
		t.Fatalf("result %+v, want no attempt", result)
	}
	if ddl := recorder.ddl(); len(ddl) != 0 {
		t.Fatalf("the refused run sent DDL: %q", ddl)
	}
	for _, line := range logLines(t, logs.String()) {
		if msg, _ := line["msg"].(string); strings.Contains(msg, "attempt failed") || strings.Contains(msg, "drop") {
			t.Fatalf("the refused run logged %q", msg)
		}
	}
	if after := streakIndexState(t, observer); after != before {
		t.Fatalf("index %+v after the refusal, want it untouched %+v", after, before)
	}

	code, _, stderr := buildIndexVerb(t, uri, "--lock-timeout=2s", "--attempts=1")
	if code == cli.ExitOK || !strings.Contains(stderr, `"code":"build_index_lock_held"`) {
		t.Fatalf("verb: exit %d stderr %s, want build_index_lock_held", code, stderr)
	}
	if after := streakIndexState(t, observer); after != before {
		t.Fatalf("index %+v after the verb's refusal, want it untouched %+v", after, before)
	}
}

// While another run holds the build lock and the index is already VALID, a run reports present with exit 0 and sends no DDL.
func TestBuildIndexLoserReportsPresentWhenTheIndexIsValid(t *testing.T) {
	length := headPosition(t)
	d := newDownInstance(t)
	uri := d.at(t, length)
	observer := connect(t, uri)
	before := streakIndexState(t, observer)
	if !before.valid {
		t.Fatal("the head database has no valid streak index")
	}
	holdBuildLock(t, uri)

	run, recorder := tracedConn(t, uri)
	result, err := pgmigrate.BuildFailureStreakIndex(context.Background(), run,
		pgmigrate.IndexBuildOptions{LockTimeout: 2 * time.Second, Attempts: 1, Deadline: 30 * time.Second}, discardLogger())
	if err != nil || result.Action != "present" || !result.Valid || result.Attempts != 0 {
		t.Fatalf("result %+v err %v, want present", result, err)
	}
	if ddl := recorder.ddl(); len(ddl) != 0 {
		t.Fatalf("the run sent DDL: %q", ddl)
	}
	code, out, stderr := buildIndexVerb(t, uri)
	if code != cli.ExitOK || out["action"] != "present" || out["valid"] != true {
		t.Fatalf("verb: exit %d result %v stderr %s", code, out, stderr)
	}
	if after := streakIndexState(t, observer); after != before {
		t.Fatalf("index %+v, want it untouched %+v", after, before)
	}
}

// The 24b shape: a second run starts while the first rebuilds an INVALID leftover (held up by a writer). The second run
// refuses at once with no DDL; it never probes the table or drops the first run's in-progress index, so the first run
// ends VALID on its one attempt.
func TestBuildIndexSecondRunDuringABuildRefusesAndLeavesTheBuildAlone(t *testing.T) {
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0159")
	plantInvalidIndex(t, uri)
	observer := connect(t, uri)
	release := holdWriter(t, uri)
	ctx := context.Background()

	first := connect(t, uri)
	firstPID := backendPID(t, first)
	firstDone := make(chan buildOutcome, 1)
	go func() {
		result, err := pgmigrate.BuildFailureStreakIndex(ctx, first,
			pgmigrate.IndexBuildOptions{LockTimeout: 2 * time.Second, Attempts: 1, Deadline: 90 * time.Second}, discardLogger())
		firstDone <- buildOutcome{result, err}
	}()
	waitUntil(t, 30*time.Second, "the first run's concurrent drop or build to start", func() bool {
		var running bool
		if err := observer.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active'
AND (query LIKE 'DROP INDEX CONCURRENTLY%' OR query LIKE 'CREATE INDEX CONCURRENTLY%'))`, firstPID).Scan(&running); err != nil {
			t.Fatal(err)
		}
		return running
	})

	second, recorder := tracedConn(t, uri)
	started := time.Now()
	result, err := pgmigrate.BuildFailureStreakIndex(ctx, second,
		pgmigrate.IndexBuildOptions{LockTimeout: 2 * time.Second, Attempts: 2, RetryDelay: time.Second, Deadline: 20 * time.Second, CleanupBudget: 10 * time.Second},
		discardLogger())
	elapsed := time.Since(started)
	release()
	firstOutcome := <-firstDone

	if !errors.Is(err, pgmigrate.ErrBuildLockHeld) {
		t.Fatalf("second run: result %+v err %v, want the refusal %q", result, err, lockHeldRefusal)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("the second run took %s; it must not wait", elapsed)
	}
	if ddl := recorder.ddl(); len(ddl) != 0 {
		t.Fatalf("the second run sent DDL: %q", ddl)
	}
	if firstOutcome.err != nil || firstOutcome.result.Action != "rebuilt" || firstOutcome.result.Attempts != 1 || !firstOutcome.result.Valid {
		t.Fatalf("first run: result %+v err %v, want rebuilt on its one attempt", firstOutcome.result, firstOutcome.err)
	}
	if n := indexRowCount(t, observer); n != 1 {
		t.Fatalf("%d indexes named %s, want 1", n, pgmigrate.FailureStreakIndexName)
	}
	if s := streakIndexState(t, observer); !s.valid {
		t.Fatal("the index is not valid")
	}
}
