//go:build integration

package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// recordFailedAttempt's two branches for a row that is already locked.
// login.py checks the lock first, so they run only when a concurrent
// request locked the row in between; the scenario cannot reach them, so
// they are pinned here against record_failed_attempt's body: a held lock
// moves only updated_at, an expired one restarts the count.
func TestRecordFailedAttemptOnALockedRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `CREATE TABLE login_attempts (id uuid PRIMARY KEY, email text NOT NULL UNIQUE,
	attempt_count integer NOT NULL, first_attempt_at timestamptz, locked_until timestamptz, created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	first := now.Add(-10 * time.Minute)
	h := handlers{Deps: Deps{Now: func() time.Time { return now }, NewUUID: uuid.New}}
	for _, tc := range []struct {
		name        string
		lockedUntil time.Time
		wantCount   int
		wantFirst   time.Time
		wantLocked  bool
	}{
		{"held", now.Add(5 * time.Minute), 5, first, true},
		{"expired", now.Add(-time.Second), 1, now, false},
	} {
		email := tc.name + "@example.com"
		if _, err := pool.Exec(ctx, `INSERT INTO login_attempts VALUES ($1, $2, 5, $3, $4, $3, $3)`,
			uuid.New(), email, first, tc.lockedUntil); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.recordFailedAttempt(ctx, tx, email); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var count int
		var gotFirst, updated time.Time
		var locked *time.Time
		if err := pool.QueryRow(ctx, `SELECT attempt_count, first_attempt_at, locked_until, updated_at FROM login_attempts
WHERE email = $1`, email).Scan(&count, &gotFirst, &locked, &updated); err != nil {
			t.Fatal(err)
		}
		if count != tc.wantCount || !gotFirst.Equal(tc.wantFirst) || (locked != nil) != tc.wantLocked || !updated.Equal(now) {
			t.Errorf("%s: count %d first %v locked %v updated %v", tc.name, count, gotFirst, locked, updated)
		}
		if tc.wantLocked && !locked.Equal(tc.lockedUntil) {
			t.Errorf("%s: the held lock moved to %v", tc.name, locked)
		}
	}
}

func startLockoutPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `CREATE TABLE login_attempts (id uuid PRIMARY KEY, email text NOT NULL,
	attempt_count integer NOT NULL, first_attempt_at timestamptz, locked_until timestamptz, created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL, CONSTRAINT uq_login_attempts_email UNIQUE (email))`); err != nil {
		t.Fatal(err)
	}
	return pool
}

// failConcurrently runs k recordFailedAttempt calls for one address, each in
// its own transaction, released together by a barrier. It returns every error.
func failConcurrently(ctx context.Context, pool *pgxpool.Pool, h handlers, email string, k int) []error {
	start := make(chan struct{})
	errs := make([]error, k)
	var wg sync.WaitGroup
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx, err := pool.Begin(ctx)
			if err != nil {
				errs[i] = err
				return
			}
			<-start
			if err := h.recordFailedAttempt(ctx, tx, email); err != nil {
				_ = tx.Rollback(ctx)
				errs[i] = err
				return
			}
			errs[i] = tx.Commit(ctx)
		}(i)
	}
	close(start)
	wg.Wait()
	return errs
}

// Concurrent FIRST failures for a new address: the rows INSERT must not race
// on UNIQUE(email). The failures serialise on the row, so the fifth locks and
// the rest only move updated_at: count 5, locked, first_attempt_at = now.
func TestRecordFailedAttemptConcurrentFirstFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool := startLockoutPool(t, ctx)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	h := handlers{Deps: Deps{Now: func() time.Time { return now }, NewUUID: uuid.New}}
	const email = "first-race@example.com"
	for i, err := range failConcurrently(ctx, pool, h, email, 8) {
		if err != nil {
			t.Errorf("failure %d: %v", i, err)
		}
	}
	var rows, count int
	var first, locked *time.Time
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM login_attempts WHERE email = $1`, email).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT attempt_count, first_attempt_at, locked_until FROM login_attempts WHERE email = $1`,
		email).Scan(&count, &first, &locked); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || count != lockoutFailureThreshold {
		t.Errorf("rows %d count %d, want 1 row with count %d", rows, count, lockoutFailureThreshold)
	}
	if first == nil || !first.Equal(now) {
		t.Errorf("first_attempt_at %v, want %v", first, now)
	}
	if locked == nil || !locked.Equal(now.Add(lockoutDuration)) {
		t.Errorf("locked_until %v, want %v", locked, now.Add(lockoutDuration))
	}
}

// Concurrent failures on an existing row must not lose an update: 4 failures
// on a row at count 0 end at count 4 and no lock.
func TestRecordFailedAttemptConcurrentNoLostUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool := startLockoutPool(t, ctx)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	h := handlers{Deps: Deps{Now: func() time.Time { return now }, NewUUID: uuid.New}}
	const email = "existing-race@example.com"
	const k = lockoutFailureThreshold - 1
	if _, err := pool.Exec(ctx, `INSERT INTO login_attempts VALUES ($1, $2, 0, NULL, NULL, $3, $3)`,
		uuid.New(), email, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	for i, err := range failConcurrently(ctx, pool, h, email, k) {
		if err != nil {
			t.Errorf("failure %d: %v", i, err)
		}
	}
	var count int
	var locked *time.Time
	if err := pool.QueryRow(ctx, `SELECT attempt_count, locked_until FROM login_attempts WHERE email = $1`,
		email).Scan(&count, &locked); err != nil {
		t.Fatal(err)
	}
	if count != k || locked != nil {
		t.Errorf("count %d locked %v, want count %d and no lock", count, locked, k)
	}
}

// afterFirstStatement runs hook once, right after the first statement of any
// kind (Exec, Query or QueryRow) returns.
type afterFirstStatement struct {
	pgx.Tx
	hook  func() error
	fired bool
	err   error
}

func (c *afterFirstStatement) after() {
	if c.fired {
		return
	}
	c.fired = true
	c.err = c.hook()
}

func (c *afterFirstStatement) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := c.Tx.Exec(ctx, sql, args...)
	if err == nil {
		c.after()
	}
	return tag, err
}

func (c *afterFirstStatement) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := c.Tx.Query(ctx, sql, args...)
	if err == nil {
		c.after()
	}
	return rows, err
}

func (c *afterFirstStatement) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	row := c.Tx.QueryRow(ctx, sql, args...)
	c.after()
	return row
}

// A successful login (clearAttempts) may run while a failure is in flight.
// The failure must not error. Two outcomes are valid: the clear committed
// before the failure took its row lock (a fresh row, count 1), or it waited
// on that lock and ran after the failure (no row).
func TestRecordFailedAttemptSurvivesAConcurrentClear(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool := startLockoutPool(t, ctx)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	h := handlers{Deps: Deps{Now: func() time.Time { return now }, NewUUID: uuid.New}}
	const email = "clear-race@example.com"
	if _, err := pool.Exec(ctx, `INSERT INTO login_attempts VALUES ($1, $2, 2, $3, NULL, $3, $3)`,
		uuid.New(), email, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pidCh := make(chan int, 1)
	deleteDone := make(chan error, 1)
	hook := func() error {
		go func() {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				pidCh <- 0
				deleteDone <- err
				return
			}
			defer conn.Release()
			var pid int
			if err := conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				pidCh <- 0
				deleteDone <- err
				return
			}
			pidCh <- pid
			_, err = conn.Exec(ctx, `DELETE FROM login_attempts WHERE email = $1`, email)
			deleteDone <- err
		}()
		pid := <-pidCh
		if pid == 0 {
			return fmt.Errorf("delete connection: %w", <-deleteDone)
		}
		for i := 0; i < 200; i++ {
			select {
			case err := <-deleteDone:
				deleteDone <- err
				return nil
			default:
			}
			var waiting bool
			if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock')`,
				pid).Scan(&waiting); err != nil {
				return err
			}
			if waiting {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
		return errors.New("the DELETE neither committed nor waited on a lock")
	}
	wrapped := &afterFirstStatement{Tx: tx, hook: hook}
	if err := h.recordFailedAttempt(ctx, wrapped, email); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("recordFailedAttempt: %v", err)
	}
	if wrapped.err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("hook: %v", wrapped.err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("delete: %v", err)
	}
	var rows, count int
	var first, locked *time.Time
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM login_attempts WHERE email = $1`, email).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	switch rows {
	case 0:
		t.Log("the clear waited on the row lock and ran after the failure")
	case 1:
		if err := pool.QueryRow(ctx, `SELECT attempt_count, first_attempt_at, locked_until FROM login_attempts WHERE email = $1`,
			email).Scan(&count, &first, &locked); err != nil {
			t.Fatal(err)
		}
		if count != 1 || first == nil || !first.Equal(now) || locked != nil {
			t.Errorf("count %d first %v locked %v, want count 1, first %v, no lock", count, first, locked, now)
		}
		t.Log("the clear committed first; the failure made a fresh row")
	default:
		t.Errorf("%d rows for one address", rows)
	}
}
