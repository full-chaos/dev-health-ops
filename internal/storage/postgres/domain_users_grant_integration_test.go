//go:build integration

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgconn"
)

// A database migrated while the domain role held table-wide SELECT on public.users is narrowed by the next migrate run
// to the reader columns, and another role's column ACL on the table is left as it was.
func TestDomainRoleUsersReadIsNarrowedToItsReaderColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)

	reader := strings.Replace(roles.domain, "grant_domain_runtime", "grant_users_reader", 1)
	if reader == roles.domain {
		t.Fatalf("domain role %q has no grant_domain_runtime prefix", roles.domain)
	}
	t.Cleanup(func() { containers.DropRole(admin, reader, t.Logf) })
	for _, statement := range []string{
		"CREATE ROLE " + reader + " NOLOGIN",
		"GRANT SELECT (id, token_version) ON TABLE public.users TO " + reader,
		"GRANT SELECT ON TABLE public.users TO " + roles.domain,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema:     grantSchema,
		DomainRole: roles.domain,
		QueueRole:  roles.queue,
	}); err != nil {
		t.Fatal(err)
	}

	held := func(role, column string) bool {
		t.Helper()
		var ok bool
		if err := admin.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.users', $2, 'SELECT')`, role, column).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	var tableWide bool
	if err := admin.QueryRow(ctx, `SELECT has_table_privilege($1, 'public.users', 'SELECT')`, roles.domain).Scan(&tableWide); err != nil {
		t.Fatal(err)
	}
	if tableWide {
		t.Fatal("the domain role still holds table-wide SELECT on public.users after migrate")
	}
	for column, want := range map[string]bool{
		"id": true, "email": true, "full_name": true, "password_hash": false, "token_version": false, "username": false,
	} {
		if got := held(roles.domain, column); got != want {
			t.Errorf("domain role SELECT on users.%s = %t, want %t", column, got, want)
		}
	}
	for column, want := range map[string]bool{"id": true, "token_version": true, "email": false, "password_hash": false} {
		if got := held(reader, column); got != want {
			t.Errorf("other role SELECT on users.%s = %t, want %t (migrate must not change its column ACL)", column, got, want)
		}
	}

	domain := connectAs(t, ctx, uri, roles.domain, grantDomainPass)
	if err := CheckDomainAuthorization(ctx, domain, roles.domain, grantSchema); err != nil {
		t.Fatalf("authorization rejected the narrowed users grant: %v", err)
	}
	if _, err := domain.Exec(ctx, "SELECT COUNT(*) FROM public.users"); err != nil {
		t.Fatalf("the heartbeat count is denied to the domain role: %v", err)
	}
	_, err := domain.Exec(ctx, "SELECT password_hash FROM public.users")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("domain role read of users.password_hash: err = %v, want SQLSTATE 42501", err)
	}
}
