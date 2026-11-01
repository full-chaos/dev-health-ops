//go:build integration

package acrmint

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// impersonationDDL is the slice of the schema the reader reads, with the
// column types of the baseline (internal/pgmigrate/baseline/head.json).
var impersonationDDL = []string{
	`CREATE TABLE public.organizations (id uuid PRIMARY KEY, name text NOT NULL)`,
	`CREATE TABLE public.users (id uuid PRIMARY KEY, email text NOT NULL UNIQUE)`,
	`CREATE TABLE public.impersonation_sessions (
		id uuid PRIMARY KEY, admin_user_id uuid NOT NULL REFERENCES public.users(id),
		target_user_id uuid NOT NULL REFERENCES public.users(id),
		target_org_id uuid NOT NULL REFERENCES public.organizations(id),
		target_role varchar(50) NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
		expires_at timestamptz NOT NULL, ended_at timestamptz)`,
}

// TestPGImpersonationReadsTheActorSideOfALiveSession: on a live database, an
// admin with an active session is an impersonating actor; its target is not;
// an admin whose only session has ended, or has expired, is not; a principal
// with no row is not. A read on a missing table is an error, never false.
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
	reader := PGImpersonation{Pool: pool, Now: func() time.Time { return now }}
	if _, err := reader.ImpersonatingActor(ctx, uuid.New()); err == nil {
		t.Fatal("a read with no impersonation_sessions table answered without an error")
	}
	for _, statement := range impersonationDDL {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	org := uuid.New()
	active, target, ended, expired, stranger := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, name) VALUES ($1, 'o')`, org); err != nil {
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
	for name, tc := range map[string]struct {
		principal uuid.UUID
		want      bool
	}{
		"actor of an active session": {active, true},
		"target of active sessions":  {target, false},
		"actor of an ended session":  {ended, false},
		"actor of an expired one":    {expired, false},
		"no row":                     {stranger, false},
	} {
		got, err := reader.ImpersonatingActor(ctx, tc.principal)
		if err != nil || got != tc.want {
			t.Errorf("%s: %v, %v; want %v", name, got, err, tc.want)
		}
	}
}
