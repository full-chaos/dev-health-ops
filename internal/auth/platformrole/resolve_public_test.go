package platformrole

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type captureRow struct{}

func (captureRow) Scan(...any) error { return nil }

type captureQueryer struct{ sqls []string }

func (c *captureQueryer) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	c.sqls = append(c.sqls, sql)
	return captureRow{}
}

// ResolveOn is a decision input: the holder is a public.users row that is
// active, never an auth.principals row.
func TestResolveOnReadsPublicUsersNotAuthPrincipals(t *testing.T) {
	s := &Store{q: `"auth"`, logger: slog.Default()}
	q := &captureQueryer{}
	if _, err := s.ResolveOn(context.Background(), q, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if len(q.sqls) != 1 {
		t.Fatalf("%d statements, want one", len(q.sqls))
	}
	sql := q.sqls[0]
	if !strings.Contains(sql, "public.users") || !strings.Contains(sql, "is_active") || !strings.Contains(sql, `"auth".platform_role_assignments`) {
		t.Errorf("ResolveOn read %q, want auth assignments joined to public.users is_active", sql)
	}
	if strings.Contains(sql, ".principals") || strings.Contains(sql, "disabled_at") || strings.Contains(sql, "kind") {
		t.Errorf("ResolveOn read %q, want no auth principals read", sql)
	}
}

// A write's guard is auth-native: its actor check is a precondition of auth's own verb.
func TestWriteGuardResolveStaysOnAuthPrincipals(t *testing.T) {
	s := &Store{q: `"auth"`, logger: slog.Default()}
	q := &captureQueryer{}
	if _, err := s.resolveOn(context.Background(), q, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if len(q.sqls) != 1 || !strings.Contains(q.sqls[0], `"auth".principals`) {
		t.Errorf("resolveOn read %v, want the auth principals join", q.sqls)
	}
}
