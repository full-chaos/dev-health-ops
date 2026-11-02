// Package superuserwriter gives an integration test a database role that can
// write everything in the public schema except public.users.is_superuser.
package superuserwriter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const password = "superuserwriter-test-only-not-a-credential"

// Pool returns a pool connected as a role that holds every privilege on the
// public tables except a write of public.users.is_superuser: users keeps
// SELECT, DELETE and column-scoped INSERT and UPDATE of every other column. A
// statement that names the column fails 42501. admin is a superuser pool of
// the database at uri.
func Pool(ctx context.Context, t testing.TB, admin *pgxpool.Pool, uri string) *pgxpool.Pool {
	t.Helper()
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	role := "nosu_" + hex.EncodeToString(suffix[:])
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	rows, err := admin.Query(ctx, `SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name <> 'is_superuser' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatal(err)
	}
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, pgx.Identifier{column}.Sanitize())
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	list := strings.Join(columns, ", ")
	for _, statement := range []string{
		fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %q`, role),
		fmt.Sprintf(`GRANT ALL ON ALL TABLES IN SCHEMA public TO %q`, role),
		fmt.Sprintf(`REVOKE ALL ON public.users FROM %q`, role),
		fmt.Sprintf(`GRANT SELECT, DELETE ON public.users TO %q`, role),
		fmt.Sprintf(`GRANT INSERT (%s), UPDATE (%s) ON public.users TO %q`, list, list, role),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
