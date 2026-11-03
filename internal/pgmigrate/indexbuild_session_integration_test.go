//go:build integration

package pgmigrate_test

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// plantSearchPath puts public ahead of pg_catalog, so the test's public functions shadow the catalog functions of the same
// signature for that session only.
func plantSearchPath(t *testing.T, uri string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", "public,pg_catalog")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// plantBackendPID makes pg_backend_pid() on a planted session return the real pid plus public.plant_backend.delta, as a
// transaction-mode pooler that moves the session to another backend would.
func plantBackendPID(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	for _, sql := range []string{
		"CREATE TABLE public.plant_backend (delta integer NOT NULL)",
		"INSERT INTO public.plant_backend VALUES (0)",
		"CREATE FUNCTION public.pg_backend_pid() RETURNS integer LANGUAGE sql AS 'SELECT pg_catalog.pg_backend_pid() + (SELECT delta FROM public.plant_backend)'",
	} {
		if _, err := conn.Exec(context.Background(), sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
}

func setPlantedDelta(t *testing.T, conn *pgx.Conn, delta int) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), "UPDATE public.plant_backend SET delta = $1", delta); err != nil {
		t.Fatal(err)
	}
}

// holdTableLock holds SHARE UPDATE EXCLUSIVE on sync_run_units, so a run stops at its table-lock probe until commit.
func holdTableLock(t *testing.T, uri string) pgx.Tx {
	t.Helper()
	tx, err := connect(t, uri).Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), "LOCK TABLE public.sync_run_units IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	return tx
}

func probeWaits(t *testing.T, observer *pgx.Conn) func() bool {
	return func() bool {
		var waiting bool
		if err := observer.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pg_locks
WHERE locktype = 'relation' AND relation = 'public.sync_run_units'::regclass AND mode = 'ShareUpdateExclusiveLock' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		return waiting
	}
}

// The session moves to another backend between the build lock and the first DDL (planted: pg_backend_pid() changes while
// the run waits at its table-lock probe). The run refuses with no DDL, no retry and no cleanup: the INVALID index stays the
// same relation. The verb reports it as build_index_session_changed.
func TestBuildIndexRefusesWhenTheBackendChangesUnderTheBuildLock(t *testing.T) {
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0159")
	plantInvalidIndex(t, uri)
	observer := connect(t, uri)
	before := streakIndexState(t, observer)
	plantBackendPID(t, observer)
	planted := plantSearchPath(t, uri)
	ctx := context.Background()

	gate := holdTableLock(t, uri)
	run, recorder := tracedConn(t, planted)
	logs := &syncBuffer{}
	runDone := make(chan buildOutcome, 1)
	go func() {
		result, err := pgmigrate.BuildFailureStreakIndex(ctx, run,
			pgmigrate.IndexBuildOptions{LockTimeout: 30 * time.Second, Attempts: 3, RetryDelay: time.Second, Deadline: 60 * time.Second},
			slog.New(slog.NewJSONHandler(logs, nil)))
		runDone <- buildOutcome{result, err}
	}()
	waitUntil(t, 20*time.Second, "the run's table-lock probe to wait", probeWaits(t, observer))
	setPlantedDelta(t, observer, 1)
	if err := gate.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	outcome := <-runDone
	if !errors.Is(outcome.err, pgmigrate.ErrBackendChanged) {
		t.Fatalf("result %+v err %v, want ErrBackendChanged", outcome.result, outcome.err)
	}
	if outcome.result.Attempts != 1 || outcome.result.Valid {
		t.Fatalf("result %+v, want one attempt and no retry", outcome.result)
	}
	for _, sql := range recorder.ddl() {
		if !strings.HasPrefix(sql, "LOCK TABLE") {
			t.Fatalf("the run sent %q after the backend changed", sql)
		}
	}
	releaseRefused := false
	for _, line := range logLines(t, logs.String()) {
		if msg, _ := line["msg"].(string); strings.Contains(msg, "dropped") {
			t.Fatalf("the run logged %q after the backend changed", msg)
		}
		if line["reason"] == "backend_changed" {
			releaseRefused = true
		}
	}
	if !releaseRefused {
		t.Fatalf("the release did not report the changed backend:\n%s", logs.String())
	}
	if after := streakIndexState(t, observer); after != before {
		t.Fatalf("index %+v after the refusal, want it untouched %+v", after, before)
	}

	// The refused run cannot unlock from a changed backend; its session end releases the lock.
	if err := run.Close(ctx); err != nil {
		t.Fatal(err)
	}
	setPlantedDelta(t, observer, 0)
	gate = holdTableLock(t, uri)
	verbDone := make(chan string, 1)
	go func() {
		code, _, stderr := buildIndexVerb(t, planted, "--lock-timeout=30s", "--attempts=1", "--deadline=60s")
		if code == cli.ExitOK {
			stderr = "exit 0: " + stderr
		}
		verbDone <- stderr
	}()
	waitUntil(t, 20*time.Second, "the verb's table-lock probe to wait", probeWaits(t, observer))
	setPlantedDelta(t, observer, 1)
	if err := gate.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if stderr := <-verbDone; strings.HasPrefix(stderr, "exit 0") || !strings.Contains(stderr, `"code":"build_index_session_changed"`) {
		t.Fatalf("verb: %s, want build_index_session_changed", stderr)
	}
	if after := streakIndexState(t, observer); after != before {
		t.Fatalf("index %+v after the verb's refusal, want it untouched %+v", after, before)
	}
}

// pg_advisory_unlock returns false at the release (planted: it unlocks, then answers false). The run reports it with its
// own reason, as an error and as a Warn line, never silently.
func TestBuildIndexReportsAnUnlockThatReturnsFalse(t *testing.T) {
	length := headPosition(t)
	d := newDownInstance(t)
	uri := d.at(t, length)
	observer := connect(t, uri)
	if _, err := observer.Exec(context.Background(),
		"CREATE FUNCTION public.pg_advisory_unlock(key bigint) RETURNS boolean LANGUAGE plpgsql AS 'BEGIN PERFORM pg_catalog.pg_advisory_unlock(key); RETURN false; END'"); err != nil {
		t.Fatal(err)
	}
	run, _ := tracedConn(t, plantSearchPath(t, uri))
	logs := &syncBuffer{}
	result, err := pgmigrate.BuildFailureStreakIndex(context.Background(), run,
		pgmigrate.IndexBuildOptions{LockTimeout: 2 * time.Second, Attempts: 1, Deadline: 30 * time.Second},
		slog.New(slog.NewJSONHandler(logs, nil)))
	if !errors.Is(err, pgmigrate.ErrBuildLockLost) {
		t.Fatalf("result %+v err %v, want ErrBuildLockLost", result, err)
	}
	warned := false
	for _, line := range logLines(t, logs.String()) {
		if line["level"] == "WARN" && line["reason"] == "unlock_returned_false" {
			warned = true
		}
	}
	if !warned {
		t.Fatalf("no Warn line with reason unlock_returned_false in:\n%s", logs.String())
	}
	var free bool
	if err := observer.QueryRow(context.Background(), "SELECT pg_try_advisory_lock($1)", buildLockKey).Scan(&free); err != nil || !free {
		t.Fatalf("the build lock is still held after the run (err %v)", err)
	}
}
