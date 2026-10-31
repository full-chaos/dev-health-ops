package authschema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// syncBuffer is a log sink a migration goroutine and the test can share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lockWarnings returns the "lock wait timed out" lines logged so far.
func (b *syncBuffer) lockWarnings(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line is not JSON: %q", raw)
		}
		if line["msg"] == "auth migration lock wait timed out" {
			lines = append(lines, line)
		}
	}
	return lines
}

func newCapturedLogger() (*slog.Logger, *syncBuffer) {
	sink := &syncBuffer{}
	return slog.New(slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug})), sink
}

// lockScriptConn is a querier whose transactions run a script: the migration
// SQL fails with failures[i] on attempt i, and succeeds once the script runs
// out. It records every statement and on which object it was sent.
type lockScriptConn struct {
	failOn   string // the statement the script fails; empty means the migration SQL
	failures []error
	begins   int
	commits  int
	sent     []string // "conn: <sql>" or "tx<n>: <sql> <args>"
}

func (c *lockScriptConn) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	c.sent = append(c.sent, "conn: "+sql)
	return pgconn.CommandTag{}, nil
}

func (c *lockScriptConn) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errStopped
}

func (c *lockScriptConn) QueryRow(context.Context, string, ...any) pgx.Row { return valueRow{} }

func (c *lockScriptConn) Begin(context.Context) (pgx.Tx, error) {
	c.begins++
	return &lockScriptTx{conn: c, attempt: c.begins}, nil
}

type lockScriptTx struct {
	pgx.Tx
	conn    *lockScriptConn
	attempt int
}

const scriptedMigrationSQL = "ALTER TABLE credential_registry ADD CONSTRAINT scripted CHECK (true)"

func (tx *lockScriptTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	entry := "tx: " + sql
	for _, arg := range args {
		if s, ok := arg.(string); ok {
			entry += " " + s
		}
	}
	tx.conn.sent = append(tx.conn.sent, entry)
	failOn := tx.conn.failOn
	if failOn == "" {
		failOn = scriptedMigrationSQL
	}
	if strings.Contains(sql, failOn) && tx.attempt <= len(tx.conn.failures) {
		return pgconn.CommandTag{}, tx.conn.failures[tx.attempt-1]
	}
	return pgconn.CommandTag{}, nil
}

func (tx *lockScriptTx) Commit(context.Context) error {
	tx.conn.commits++
	return nil
}

func (tx *lockScriptTx) Rollback(context.Context) error { return nil }

func lockTimeoutError() error {
	return &pgconn.PgError{Code: lockNotAvailable, Message: "canceling statement due to lock timeout"}
}

var scriptedMigration = Migration{Version: 37, Name: "scripted", SQL: scriptedMigrationSQL}

func TestTheLockTimeoutIsSetOnTheMigrationTransactionBeforeItsSQL(t *testing.T) {
	conn := &lockScriptConn{}
	if err := applyOne(context.Background(), conn, Options{Schema: "auth"}, scriptedMigration); err != nil {
		t.Fatalf("applyOne: %v", err)
	}
	lockAt, sqlAt := -1, -1
	for i, sent := range conn.sent {
		if strings.Contains(sent, "lock_timeout") {
			if !strings.HasPrefix(sent, "tx: ") || !strings.Contains(sent, "true)") {
				t.Fatalf("lock_timeout must be transaction-local (set_config(..., true) on the transaction): %q", sent)
			}
			if !strings.HasSuffix(sent, " 5000ms") {
				t.Fatalf("lock_timeout = %q, want the default 5000ms", sent)
			}
			lockAt = i
		}
		if strings.HasSuffix(sent, scriptedMigrationSQL) {
			sqlAt = i
		}
	}
	if lockAt < 0 || sqlAt < 0 || lockAt > sqlAt {
		t.Fatalf("want lock_timeout set before the migration SQL, got lock at %d, SQL at %d: %q", lockAt, sqlAt, conn.sent)
	}
	if conn.begins != 1 || conn.commits != 1 {
		t.Fatalf("begins=%d commits=%d, want one attempt that commits", conn.begins, conn.commits)
	}
}

func TestAMigrationWhoseLockWaitTimesOutOnEveryAttemptFailsLoudly(t *testing.T) {
	logger, sink := newCapturedLogger()
	conn := &lockScriptConn{failures: []error{lockTimeoutError(), lockTimeoutError(), lockTimeoutError()}}
	options := Options{Schema: "auth", Logger: logger, LockTimeout: 20 * time.Millisecond, LockAttempts: 3}

	started := time.Now()
	err := applyOne(context.Background(), conn, options, scriptedMigration)
	if !errors.Is(err, ErrMigrationLockTimeout) || !errors.Is(err, ErrMigrationFailed) {
		t.Fatalf("err = %v, want ErrMigrationLockTimeout and ErrMigrationFailed", err)
	}
	// One pause of the lock timeout between attempts lets the queued readers
	// through before the next attempt queues again.
	//abstime:allow lower bound: the two pauses can only lengthen the run, so load never fails this; it fails only a run with no pause between attempts
	if elapsed := time.Since(started); elapsed < 2*options.LockTimeout {
		t.Fatalf("3 attempts took %s, want at least the 2 pauses between them (%s)", elapsed, 2*options.LockTimeout)
	}
	if conn.begins != 3 || conn.commits != 0 {
		t.Fatalf("begins=%d commits=%d, want 3 attempts and no commit", conn.begins, conn.commits)
	}
	warnings := sink.lockWarnings(t)
	if len(warnings) != 3 {
		t.Fatalf("want one warning per timed-out attempt (3), got %d: %v", len(warnings), warnings)
	}
	for i, want := range []string{"retrying", "retrying", "giving_up"} {
		w := warnings[i]
		if w["level"] != "WARN" || w["outcome"] != want || w["attempt"] != float64(i+1) ||
			w["attempts"] != float64(3) || w["version"] != float64(37) || w["lock_timeout"] != "20ms" {
			t.Fatalf("warning %d = %v, want WARN attempt %d of 3, outcome %s, version 37, lock_timeout 20ms", i, w, i+1, want)
		}
	}
}

func TestAMigrationRetriedAfterALockTimeoutCommitsOnce(t *testing.T) {
	logger, sink := newCapturedLogger()
	conn := &lockScriptConn{failures: []error{lockTimeoutError()}}
	options := Options{Schema: "auth", Logger: logger, LockTimeout: time.Millisecond, LockAttempts: 3}

	if err := applyOne(context.Background(), conn, options, scriptedMigration); err != nil {
		t.Fatalf("applyOne: %v", err)
	}
	if conn.begins != 2 || conn.commits != 1 {
		t.Fatalf("begins=%d commits=%d, want a second attempt that commits", conn.begins, conn.commits)
	}
	if warnings := sink.lockWarnings(t); len(warnings) != 1 || warnings[0]["outcome"] != "retrying" {
		t.Fatalf("want one retrying warning, got %v", warnings)
	}
}

func TestALockTimeoutOnTheVersionRowIsRetriedToo(t *testing.T) {
	conn := &lockScriptConn{failOn: "INSERT INTO", failures: []error{lockTimeoutError()}}
	options := Options{Schema: "auth", LockTimeout: time.Millisecond, LockAttempts: 3}

	if err := applyOne(context.Background(), conn, options, scriptedMigration); err != nil {
		t.Fatalf("applyOne: %v", err)
	}
	if conn.begins != 2 || conn.commits != 1 {
		t.Fatalf("begins=%d commits=%d, want a second attempt that commits", conn.begins, conn.commits)
	}
}

func TestAMigrationFailureThatIsNotALockTimeoutIsNotRetried(t *testing.T) {
	logger, sink := newCapturedLogger()
	conn := &lockScriptConn{failures: []error{
		&pgconn.PgError{Code: "42P01", Message: "relation does not exist"},
		lockTimeoutError(),
	}}
	options := Options{Schema: "auth", Logger: logger, LockTimeout: time.Millisecond, LockAttempts: 3}

	err := applyOne(context.Background(), conn, options, scriptedMigration)
	if !errors.Is(err, ErrMigrationFailed) || errors.Is(err, ErrMigrationLockTimeout) {
		t.Fatalf("err = %v, want ErrMigrationFailed and not ErrMigrationLockTimeout", err)
	}
	if !strings.Contains(err.Error(), "42P01") {
		t.Fatalf("err = %v, want the SQLSTATE of the real failure", err)
	}
	if conn.begins != 1 {
		t.Fatalf("begins = %d, want 1: only a lock timeout is retried", conn.begins)
	}
	if warnings := sink.lockWarnings(t); len(warnings) != 0 {
		t.Fatalf("want no lock warning, got %v", warnings)
	}
}

func TestACancelledRunStopsBetweenLockAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &lockScriptConn{failures: []error{lockTimeoutError(), lockTimeoutError()}}
	options := Options{Schema: "auth", LockTimeout: time.Hour, LockAttempts: 2}

	done := make(chan error, 1)
	go func() { done <- applyOne(ctx, conn, options, scriptedMigration) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrMigrationFailed) {
			t.Fatalf("err = %v, want ErrMigrationFailed wrapping context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled run kept waiting out its pause")
	}
	if conn.begins != 1 {
		t.Fatalf("begins = %d, want 1", conn.begins)
	}
}

func TestTheLockPolicyDefaultsAndBounds(t *testing.T) {
	if timeout, attempts := (Options{}).lockPolicy(); timeout != 5*time.Second || attempts != 3 {
		t.Fatalf("default lock policy = %s x %d, want 5s x 3", timeout, attempts)
	}
	if timeout, _ := (Options{LockTimeout: time.Microsecond}).lockPolicy(); timeout < time.Millisecond {
		t.Fatalf("lock policy = %s; a timeout under 1ms renders as 0ms, which turns lock_timeout off", timeout)
	}
	for name, options := range map[string]Options{
		"negative timeout":  {LockTimeout: -time.Second},
		"sub-ms timeout":    {LockTimeout: time.Microsecond},
		"negative attempts": {LockAttempts: -1},
	} {
		options.Schema, options.RuntimeRole = "auth", "app_runtime"
		if err := options.Validate("migrator"); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("%s: Validate = %v, want ErrInvalidOptions", name, err)
		}
	}
	if err := (Options{Schema: "auth", RuntimeRole: "app_runtime", LockTimeout: time.Millisecond, LockAttempts: 1}).Validate("migrator"); err != nil {
		t.Fatalf("Validate(1ms x 1) = %v, want nil", err)
	}
}
