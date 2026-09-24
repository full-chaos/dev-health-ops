//go:build integration

package session

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
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
