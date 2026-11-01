//go:build integration

package acrmint

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// migrateSchema builds the REAL schema, the pgmigrate baseline and chain, so
// impersonation_sessions, users and organizations have exactly the columns a
// deployment has (the hand-DDL guard in internal/pgmigrate refuses a
// hand-written copy of a production table).
func migrateSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	conn, err := pgx.Connect(ctx, pool.Config().ConnString())
	if err != nil {
		t.Fatalf("connect for the schema: %v", err)
	}
	defer conn.Close(context.Background())
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("apply the schema: %v", err)
	}
}

// TestPGImpersonationReadsTheActorSideOfALiveSession: on a live database with
// the migrated schema, an admin with an active session is an impersonating
// actor; its target is not; an admin whose only session has ended, or has
// expired, is not; a principal with no actor row is not, also when a session
// with a NULL admin (a deleted admin) targets it. A read on a missing table is
// an error, never false.
func TestPGImpersonationReadsTheActorSideOfALiveSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	reader := PGImpersonation{Pool: pool, Sessions: noDelegation{}, Now: func() time.Time { return now }}
	if _, err := reader.ImpersonatingActor(ctx, uuid.New(), uuid.New()); err == nil {
		t.Fatal("a read with no impersonation_sessions table answered without an error")
	}
	migrateSchema(t, ctx, pool)
	org := uuid.New()
	active, target, ended, expired, stranger := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name) VALUES ($1, 'o', 'o')`, org); err != nil {
		t.Fatal(err)
	}
	for i, id := range []uuid.UUID{active, target, ended, expired, stranger} {
		if _, err := pool.Exec(ctx, `INSERT INTO users (id, email) VALUES ($1, $2)`, id, "u"+string(rune('a'+i))+"@example.test"); err != nil {
			t.Fatal(err)
		}
	}
	rows := []struct {
		admin            uuid.UUID
		expires, endedAt any
	}{
		{active, now.Add(time.Hour), nil},
		{ended, now.Add(time.Hour), now.Add(-time.Minute)},
		{expired, now.Add(-time.Second), nil},
	}
	for _, row := range rows {
		if _, err := pool.Exec(ctx, `INSERT INTO impersonation_sessions (id, admin_user_id, target_user_id, target_org_id, target_role, expires_at, ended_at)
VALUES ($1, $2, $3, $4, 'member', $5, $6)`, uuid.New(), row.admin, target, org, row.expires, row.endedAt); err != nil {
			t.Fatal(err)
		}
	}
	// A session whose admin was deleted keeps a NULL admin_user_id: it is
	// nobody's actor side.
	if _, err := pool.Exec(ctx, `INSERT INTO impersonation_sessions (id, admin_user_id, target_user_id, target_org_id, target_role, expires_at)
VALUES ($1, NULL, $2, $3, 'member', $4)`, uuid.New(), stranger, org, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		principal uuid.UUID
		want      bool
	}{
		"actor of an active session": {active, true},
		"target of active sessions":  {target, false},
		"actor of an ended session":  {ended, false},
		"actor of an expired one":    {expired, false},
		"no row as actor":            {stranger, false},
	} {
		got, err := reader.ImpersonatingActor(ctx, tc.principal, uuid.New())
		if err != nil || got != tc.want {
			t.Errorf("%s: %v, %v; want %v", name, got, err, tc.want)
		}
	}
}
