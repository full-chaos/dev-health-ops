//go:build integration

package rivermigrate_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// The api role's refresh_tokens grants follow go-api's AUTH_PROXY_SESSIONS
// through the real command, over the real ops schema: on, INSERT and every
// UPDATE but revoked_at are revoked (auth-service owns sessions; go-api only
// revokes); off again, a migrate run grants them back (the rollback).
func TestMigrateRiverRefreshTokensGrantsFollowAuthProxySessions(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)

	domain, queue, coordinator, api := randomName(t, "rt_domain_"), randomName(t, "rt_queue_"), randomName(t, "rt_coord_"), randomName(t, "rt_api_")
	for _, role := range []string{domain, queue, coordinator, api} {
		if _, err := admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'"); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, role := range []string{domain, queue, coordinator, api} {
			_, _ = admin.Exec(context.Background(), "DROP OWNED BY "+role)
			_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
		}
	})
	migrate := func(sessions string) (int, string) {
		settings := map[string]string{
			"MIGRATION_DATABASE_URI":          instance.URI,
			"RIVER_DOMAIN_DATABASE_ROLE":      domain,
			"RIVER_QUEUE_DATABASE_ROLE":       queue,
			"RIVER_COORDINATOR_DATABASE_ROLE": coordinator,
			"API_DATABASE_ROLE":               api,
		}
		if sessions != "" {
			settings["AUTH_PROXY_SESSIONS"] = sessions
		}
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
		return rivermigrate.Execute(ctx, "dho", nil, lookup, &stdout, &stderr), stderr.String()
	}
	held := func(privilege string, column string) bool {
		t.Helper()
		var ok bool
		query, args := "SELECT has_table_privilege($1, 'public.refresh_tokens', $2)", []any{api, privilege}
		if column != "" {
			query, args = "SELECT has_column_privilege($1, 'public.refresh_tokens', $2, $3)", []any{api, column, privilege}
		}
		if err := admin.QueryRow(ctx, query, args...).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	type want struct{ insert, update, updateRevokedAt, updateUserID, delete, selectTable bool }
	check := func(label string, w want) {
		t.Helper()
		got := want{held("INSERT", ""), held("UPDATE", ""), held("UPDATE", "revoked_at"), held("UPDATE", "user_id"),
			held("DELETE", ""), held("SELECT", "")}
		if got != w {
			t.Fatalf("%s: api role holds %+v on public.refresh_tokens, want %+v", label, got, w)
		}
	}
	goAPIOwns := want{insert: true, update: true, updateRevokedAt: true, updateUserID: true, delete: true, selectTable: true}
	authOwns := want{insert: false, update: false, updateRevokedAt: true, updateUserID: false, delete: true, selectTable: true}

	if code, stderr := migrate(""); code != cli.ExitOK {
		t.Fatalf("flag unset: exit %d\n%s", code, stderr)
	}
	check("flag unset", goAPIOwns)
	if code, stderr := migrate("true"); code != cli.ExitOK {
		t.Fatalf("flag on: exit %d\n%s", code, stderr)
	}
	check("flag on", authOwns)
	if code, stderr := migrate("yes"); code != 1 || !strings.Contains(stderr, "AUTH_PROXY_SESSIONS") {
		t.Fatalf("an unparsable flag: exit %d, stderr:\n%s", code, stderr)
	}
	check("after a refused run", authOwns)
	if code, stderr := migrate("false"); code != cli.ExitOK {
		t.Fatalf("flag off again: exit %d\n%s", code, stderr)
	}
	check("flag off again (rollback)", goAPIOwns)
}
