//go:build integration

package admin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

const insertMemberSQL = `INSERT INTO memberships (id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', NULL, now(), now(), now())`

// waitBlockedBy waits until a backend is blocked by holder's pid, bounded by
// ctx: a wait on the lock graph, never on the wall clock.
func waitBlockedBy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, holder uint32, done <-chan struct{}) {
	t.Helper()
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE $1::int = ANY(pg_blocking_pids(pid))`, int32(holder)).
			Scan(&waiting); err != nil {
			t.Fatalf("pg_blocking_pids: %v", err)
		}
		if waiting > 0 {
			return
		}
		select {
		case <-done:
			t.Fatal("the statement ended without waiting for the held row")
		case <-ctx.Done():
			t.Fatal("the statement never waited for the held row")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func count(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The guarded delete of an ownerless organization against a membership that
// lands at the same time: no membership is ever lost to the cascade.
func TestDeleteOrganizationWithoutMembersAgainstAConcurrentMember(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
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
	pgschema.Apply(ctx, t, pool)
	store := pgStore{Pool: pool, Now: time.Now}
	seed := func(t *testing.T) (org, user uuid.UUID) {
		org, user = uuid.New(), uuid.New()
		pgseed.Org(ctx, t, pool, org.String(), "community")
		pgseed.User(ctx, t, pool, user.String())
		return org, user
	}

	// I-1: the member insert holds the org row (its foreign key check) when
	// the guarded delete runs. The delete waits, then sees the member and
	// keeps the org: the membership and the org both stay.
	t.Run("member insert first: the delete waits and keeps the org", func(t *testing.T) {
		cellCtx, cellCancel := context.WithTimeout(ctx, time.Minute)
		defer cellCancel()
		org, user := seed(t)
		member, err := pool.Begin(cellCtx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = member.Rollback(context.Background()) }()
		if _, err := member.Exec(cellCtx, insertMemberSQL, uuid.New(), org, user); err != nil {
			t.Fatal(err)
		}
		var holder uint32
		if err := member.QueryRow(cellCtx, `SELECT pg_backend_pid()`).Scan(&holder); err != nil {
			t.Fatal(err)
		}
		type result struct {
			deleted bool
			err     error
		}
		got := make(chan result, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			deleted, err := store.deleteOrganizationWithoutMembers(cellCtx, org)
			got <- result{deleted, err}
		}()
		waitBlockedBy(t, cellCtx, pool, holder, done)
		if err := member.Commit(cellCtx); err != nil {
			t.Fatal(err)
		}
		r := <-got
		if r.err != nil || r.deleted {
			t.Fatalf("guarded delete = %v, %v; want kept (false), no error", r.deleted, r.err)
		}
		if n := count(t, cellCtx, pool, `SELECT count(*) FROM organizations WHERE id = $1`, org); n != 1 {
			t.Fatalf("org rows = %d, want 1", n)
		}
		if n := count(t, cellCtx, pool, `SELECT count(*) FROM memberships WHERE org_id = $1`, org); n != 1 {
			t.Fatalf("membership rows = %d, want 1 (the concurrent member is kept)", n)
		}
	})

	// I-2: the guard holds the org row (its first statement) when a member
	// insert arrives. The insert waits; the guard finds no member, deletes and
	// commits; the insert then fails its foreign key. Nothing is half-written.
	t.Run("guard first: the member insert waits and fails its foreign key", func(t *testing.T) {
		cellCtx, cellCancel := context.WithTimeout(ctx, time.Minute)
		defer cellCancel()
		org, user := seed(t)
		guard, err := pool.Begin(cellCtx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = guard.Rollback(context.Background()) }()
		var one int
		if err := guard.QueryRow(cellCtx, `SELECT 1 FROM organizations WHERE id = $1 FOR UPDATE`, org).Scan(&one); err != nil {
			t.Fatal(err)
		}
		var holder uint32
		if err := guard.QueryRow(cellCtx, `SELECT pg_backend_pid()`).Scan(&holder); err != nil {
			t.Fatal(err)
		}
		got := make(chan error, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err := pool.Exec(cellCtx, insertMemberSQL, uuid.New(), org, user)
			got <- err
		}()
		waitBlockedBy(t, cellCtx, pool, holder, done)
		var exists bool
		if err := guard.QueryRow(cellCtx, `SELECT EXISTS (SELECT 1 FROM memberships WHERE org_id = $1)`, org).Scan(&exists); err != nil || exists {
			t.Fatalf("member check = %v, %v; want none", exists, err)
		}
		if _, err := guard.Exec(cellCtx, `DELETE FROM organizations WHERE id = $1`, org); err != nil {
			t.Fatal(err)
		}
		if err := guard.Commit(cellCtx); err != nil {
			t.Fatal(err)
		}
		var pgErr *pgconn.PgError
		if err := <-got; !errors.As(err, &pgErr) || pgErr.Code != "23503" {
			t.Fatalf("the waiting member insert = %v, want the 23503 foreign key failure", err)
		}
		if n := count(t, cellCtx, pool, `SELECT count(*) FROM memberships WHERE org_id = $1`, org); n != 0 {
			t.Fatalf("membership rows = %d, want 0", n)
		}
	})
}
