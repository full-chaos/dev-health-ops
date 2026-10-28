//go:build integration

package pgmigrate_test

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

const plantInvalidSQL = "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'public.ix_sync_run_units_failure_streak'::regclass"

// plantInvalidIndex builds the index with the verb and marks it INVALID, as a failed CREATE INDEX CONCURRENTLY leaves it.
func plantInvalidIndex(t *testing.T, uri string) {
	t.Helper()
	if code, _, stderr := buildIndexVerb(t, uri); code != cli.ExitOK {
		t.Fatalf("plant build: exit %d %s", code, stderr)
	}
	conn := connect(t, uri)
	if _, err := conn.Exec(context.Background(), plantInvalidSQL); err != nil {
		t.Fatal(err)
	}
}

func logLines(t *testing.T, text string) []map[string]any {
	t.Helper()
	var out []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			continue
		}
		out = append(out, m)
	}
	return out
}

func linesWith(lines []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, l := range lines {
		if l["msg"] == msg {
			out = append(out, l)
		}
	}
	return out
}

// An INVALID leftover plus a writer transaction older than lock_timeout that ends inside the build window: the verb waits
// it out and ends with a VALID index (action rebuilt). Before the change every attempt failed 55P03.
func TestBuildIndexOutlastsALongTransactionOverAnInvalidLeftover(t *testing.T) {
	position, _ := chainPosition(t, "0159")
	d := newDownInstance(t)
	uri := d.at(t, position)
	plantInvalidIndex(t, uri)
	release := holdWriter(t, uri)
	timer := time.AfterFunc(12*time.Second, release)
	defer timer.Stop()

	code, result, stderr := buildIndexVerb(t, uri, "--lock-timeout=2s", "--attempts=3", "--retry-delay=1s", "--deadline=90s")
	if code != cli.ExitOK || result["action"] != "rebuilt" || result["valid"] != true {
		t.Fatalf("exit %d result %v stderr %s", code, result, stderr)
	}
	conn := connect(t, uri)
	if _, valid := streakIndexDefinition(t, conn); !valid {
		t.Fatal("the index is not valid")
	}
}

// A holder that outlives the window: the build ends inside the window plus the cleanup budget with the leftover key and
// the blocker row, and the writers behind the build never wait on it.
func TestBuildIndexHolderLongerThanTheWindowLeavesTheLeftoverAndBlocksNoWriter(t *testing.T) {
	position, _ := chainPosition(t, "0159")
	d := newDownInstance(t)
	uri := d.at(t, position)
	plantInvalidIndex(t, uri)
	release := holdWriter(t, uri)

	writer := connect(t, uri)
	writerCtx, cancelWriters := context.WithCancel(context.Background())
	defer cancelWriters()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var writes, writesInsideTheBuild int
	var writeErr error
	var building atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			startedInside := building.Load()
			_, err := writer.Exec(writerCtx, "UPDATE public.sync_run_units SET updated_at = updated_at WHERE false")
			finishedInside := building.Load()
			mu.Lock()
			writes++
			if startedInside && finishedInside {
				writesInsideTheBuild++
			}
			if err != nil && writeErr == nil {
				writeErr = err
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
		}
	}()

	logs := &syncBuffer{}
	conn := connect(t, uri)
	// A failure-only guard (window 12 s + cleanup 3 s + 60 s): a build that does not end with its window never returns.
	// On the guard the build is cancelled first, then the writers are stopped, cancelled and joined, then the test fails.
	building.Store(true)
	_, err := runBuildGuarded(t, 75*time.Second, "the build must end with its window plus the cleanup budget",
		func() {
			building.Store(false)
			close(stop)
			cancelWriters()
			wg.Wait()
		},
		func(ctx context.Context) (pgmigrate.IndexBuildResult, error) {
			return pgmigrate.BuildFailureStreakIndex(ctx, conn, pgmigrate.IndexBuildOptions{
				LockTimeout: 2 * time.Second, Attempts: 2, RetryDelay: 0, Deadline: 12 * time.Second, CleanupBudget: 3 * time.Second,
				SampleEvery: 200 * time.Millisecond, HeartbeatEvery: time.Second,
			}, slog.New(slog.NewJSONHandler(logs, nil)))
		})
	building.Store(false)
	close(stop)
	wg.Wait()
	if err == nil {
		t.Fatal("the build succeeded under a holder that never ended")
	}
	mu.Lock()
	defer mu.Unlock()
	// "Blocks no writer" as an operation count, not a latency: writes that started AND finished while the build was
	// running. A writer held behind the build lands only after it, so it never counts here.
	if writeErr != nil || writesInsideTheBuild < 50 {
		t.Fatalf("writers: %d writes, %d of them finished while the build ran (want >= 50), error %v", writes, writesInsideTheBuild, writeErr)
	}
	lines := logLines(t, logs.String())
	for _, msg := range []string{"build-index: drop of the invalid index failed", "build-index: invalid index left; the next run drops it"} {
		if len(linesWith(lines, msg)) == 0 {
			t.Fatalf("log lacks %q: %s", msg, logs.String())
		}
	}
	if len(linesWith(lines, "build-index: dropped the invalid index after the final failure")) != 0 {
		t.Fatal("the log claims a drop")
	}
	if blockers := linesWith(lines, "build-index: blocker"); len(blockers) == 0 || blockers[0]["state"] != "idle in transaction" {
		t.Fatalf("no blocker row for the holder: %s", logs.String())
	}
	assertBlockerLinesAreAllowlisted(t, lines, logs.String())
	if n := indexRowCount(t, connect(t, uri)); n != 1 {
		t.Fatalf("%d index rows, want exactly the INVALID leftover", n)
	}

	release()
	code, result, stderr := buildIndexVerb(t, uri)
	if code != cli.ExitOK || result["action"] != "rebuilt" || result["valid"] != true {
		t.Fatalf("rerun: exit %d result %v stderr %s", code, result, stderr)
	}
}

// Blocker and heartbeat lines carry the allowlisted fields only, never query text or a value.
func assertBlockerLinesAreAllowlisted(t *testing.T, lines []map[string]any, raw string) {
	t.Helper()
	allowed := map[string]bool{"time": true, "level": true, "msg": true, "stage": true, "attempt": true, "elapsed_s": true,
		"pid": true, "application_name": true, "usename": true, "state": true, "xact_age_s": true, "wait_event_type": true, "hidden": true}
	seen := 0
	for _, l := range lines {
		msg, _ := l["msg"].(string)
		if msg != "build-index: blocker" && msg != "build-index: waiting" {
			continue
		}
		seen++
		for k := range l {
			if !allowed[k] {
				t.Fatalf("%s line carries field %q: %v", msg, k, l)
			}
		}
	}
	if seen == 0 {
		t.Fatal("no blocker or heartbeat line to check")
	}
	if strings.Contains(raw, "sentinel_query_text_xyz") {
		t.Fatalf("a log line carries query text: %s", raw)
	}
}

// While a build waits for an older transaction, the heartbeat names the blocker with the allowlisted fields only.
func TestBuildIndexHeartbeatLogsTheBlockerWithAllowlistedFieldsOnly(t *testing.T) {
	position, _ := chainPosition(t, "0159")
	d := newDownInstance(t)
	uri := d.at(t, position)
	release := holdWriter(t, uri)
	timer := time.AfterFunc(6*time.Second, release)
	defer timer.Stop()

	logs := &syncBuffer{}
	conn := connect(t, uri)
	result, err := pgmigrate.BuildFailureStreakIndex(context.Background(), conn, pgmigrate.IndexBuildOptions{
		LockTimeout: 2 * time.Second, Attempts: 1, Deadline: 60 * time.Second,
		SampleEvery: 200 * time.Millisecond, HeartbeatEvery: time.Second,
	}, slog.New(slog.NewJSONHandler(logs, nil)))
	if err != nil || !result.Valid {
		t.Fatalf("result %+v err %v log %s", result, err, logs.String())
	}
	lines := logLines(t, logs.String())
	if len(linesWith(lines, "build-index: waiting")) == 0 {
		t.Fatalf("no heartbeat line: %s", logs.String())
	}
	assertBlockerLinesAreAllowlisted(t, lines, logs.String())
}

// A holder of SHARE UPDATE EXCLUSIVE (a long VACUUM, DDL) ends the attempt at lock_timeout, not at the window: the table-lock
// wait stays short.
func TestBuildIndexTableLockWaitStaysUnderLockTimeout(t *testing.T) {
	position, _ := chainPosition(t, "0159")
	d := newDownInstance(t)
	uri := d.at(t, position)
	holder := connect(t, uri)
	tx, err := holder.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), "LOCK TABLE public.sync_run_units IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	conn := connect(t, uri)
	// The attempt ends with the 1 s lock_timeout; one that waited for the 5 min window never returns inside the 60 s
	// failure-only guard. On the guard the build is cancelled first and the holder transaction is rolled back.
	_, err = runBuildGuarded(t, 60*time.Second, "the attempt must end at lock_timeout",
		func() { _ = tx.Rollback(context.Background()) },
		func(ctx context.Context) (pgmigrate.IndexBuildResult, error) {
			return pgmigrate.BuildFailureStreakIndex(ctx, conn, pgmigrate.IndexBuildOptions{
				LockTimeout: time.Second, Attempts: 1, Deadline: 5 * time.Minute, SampleEvery: 100 * time.Millisecond,
			}, slog.New(slog.NewJSONHandler(logs, nil)))
		})
	if err == nil {
		t.Fatal("the attempt must fail at lock_timeout, not succeed")
	}
	lines := logLines(t, logs.String())
	failed := linesWith(lines, "build-index: attempt failed")
	if len(failed) != 1 || failed[0]["stage"] != "probe" || failed[0]["sqlstate"] != "55P03" {
		t.Fatalf("attempt failure lines %v; log %s", failed, logs.String())
	}
	if blockers := linesWith(lines, "build-index: blocker"); len(blockers) == 0 {
		t.Fatalf("no blocker row: %s", logs.String())
	}
}

// The start line and the line after each failed attempt report the index state.
func TestBuildIndexLogsTheExistingIndexState(t *testing.T) {
	position, _ := chainPosition(t, "0159")
	for name, tc := range map[string]struct {
		prepare func(t *testing.T, uri string)
		want    string
	}{
		"none":    {func(t *testing.T, uri string) {}, "none"},
		"valid":   {func(t *testing.T, uri string) { buildIndexVerb(t, uri) }, "valid"},
		"invalid": {plantInvalidIndex, "invalid"},
	} {
		t.Run(name, func(t *testing.T) {
			d := newDownInstance(t)
			uri := d.at(t, position)
			tc.prepare(t, uri)
			logs := &syncBuffer{}
			conn := connect(t, uri)
			if _, err := pgmigrate.BuildFailureStreakIndex(context.Background(), conn, pgmigrate.IndexBuildOptions{
				LockTimeout: 2 * time.Second, Attempts: 1, Deadline: time.Minute,
			}, slog.New(slog.NewJSONHandler(logs, nil))); err != nil {
				t.Fatal(err)
			}
			lines := linesWith(logLines(t, logs.String()), "build-index: existing index")
			if len(lines) == 0 || lines[0]["when"] != "start" || lines[0]["state"] != tc.want {
				t.Fatalf("start line %v, want state %s; log %s", lines, tc.want, logs.String())
			}
			wantValid := tc.want != "none" && tc.want != "invalid"
			if lines[0]["indisvalid"] != wantValid {
				t.Fatalf("indisvalid %v, want %v", lines[0]["indisvalid"], wantValid)
			}
		})
	}
}

// Any older snapshot in the database makes CREATE INDEX CONCURRENTLY wait (virtualxid), not only a writer on the table:
// (E3) a repeatable-read snapshot that touched ANOTHER table, (E2) one long read-committed statement with no table lock.
// With an INVALID leftover planted, the build waits for the holder, ends "rebuilt" and VALID, and the heartbeat names the
// holder's pid.
func TestBuildIndexOutlastsSnapshotHoldersOnOtherTablesAndLongStatements(t *testing.T) {
	position, _ := chainPosition(t, "0159")
	for name, start := range map[string]func(t *testing.T, uri string) (pid uint32){
		"E3 repeatable-read snapshot on another table": func(t *testing.T, uri string) uint32 {
			conn := connect(t, uri)
			tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
			if err != nil {
				t.Fatal(err)
			}
			var n int
			if err := tx.QueryRow(context.Background(), "SELECT count(*) FROM public.alembic_version").Scan(&n); err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			release := func() { once.Do(func() { _ = tx.Rollback(context.Background()) }) }
			t.Cleanup(release)
			time.AfterFunc(10*time.Second, release)
			return conn.PgConn().PID()
		},
		"E2 one long read-committed statement": func(t *testing.T, uri string) uint32 {
			conn := connect(t, uri)
			pid := conn.PgConn().PID()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = conn.Exec(ctx, "SELECT pg_sleep(10)")
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
			return pid
		},
	} {
		t.Run(name, func(t *testing.T) {
			d := newDownInstance(t)
			uri := d.at(t, position)
			plantInvalidIndex(t, uri)
			holderPID := start(t, uri)
			time.Sleep(500 * time.Millisecond)

			logs := &syncBuffer{}
			conn := connect(t, uri)
			result, err := pgmigrate.BuildFailureStreakIndex(context.Background(), conn, pgmigrate.IndexBuildOptions{
				LockTimeout: 2 * time.Second, Attempts: 3, RetryDelay: time.Second, Deadline: 90 * time.Second,
				SampleEvery: 200 * time.Millisecond, HeartbeatEvery: time.Second,
			}, slog.New(slog.NewJSONHandler(logs, nil)))
			if err != nil || !result.Valid || result.Action != "rebuilt" {
				t.Fatalf("result %+v err %v log %s", result, err, logs.String())
			}
			named := false
			for _, l := range linesWith(logLines(t, logs.String()), "build-index: waiting") {
				if pid, _ := l["pid"].(float64); uint32(pid) == holderPID {
					named = true
				}
			}
			if !named {
				t.Fatalf("no heartbeat line names the holder pid %d: %s", holderPID, logs.String())
			}
			assertBlockerLinesAreAllowlisted(t, logLines(t, logs.String()), logs.String())
		})
	}
}
