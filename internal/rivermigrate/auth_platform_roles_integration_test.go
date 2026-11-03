//go:build integration

package rivermigrate_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// usersInsertWhileOwned and usersUpdateWhileOwned are the go-api writers'
// columns (TestGoAPIUsersWritesStayWithinTheOwnedGrant holds them equal to the
// posture): one statement over each list is every go-api users write at once.
const (
	usersInsertWhileOwned = `INSERT INTO users (id, email, username, password_hash, full_name, avatar_url, auth_provider,
	auth_provider_id, sso_provider_id, is_active, is_verified, token_version, last_login_at, created_at, updated_at,
	saml_nameid_format, saml_sp_name_qualifier)
VALUES ($1, $2, NULL, NULL, 'n', NULL, 'local', NULL, NULL, true, false, 0, NULL, now(), now(), NULL, NULL)`
	usersUpdateWhileOwned = `UPDATE users SET email = email, username = username, password_hash = password_hash,
	full_name = full_name, avatar_url = avatar_url, is_active = is_active, is_verified = is_verified,
	token_version = token_version + 1, last_login_at = now(), updated_at = now(), saml_nameid_format = saml_nameid_format,
	saml_sp_name_qualifier = saml_sp_name_qualifier WHERE id = $1`
)

// The api role's users grants follow AUTH_PLATFORM_ROLES_OWNED through the
// real command, over the real ops schema (CHAOS-10993). On: no role writes
// public.users.is_superuser (42501), every go-api users write still runs, and
// readiness holds the exact owned manifest and names an excess or a missing
// grant. A bad value changes nothing. Off again, a migrate run grants the
// table-wide INSERT and UPDATE back (the rollback) and the superuser write
// works again.
func TestMigrateRiverUsersGrantsFollowPlatformRolesOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
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

	domain, queue, coordinator, api := randomName(t, "pr_domain_"), randomName(t, "pr_queue_"), randomName(t, "pr_coord_"), randomName(t, "pr_api_")
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
	migrate := func(owned string) (int, string) {
		settings := map[string]string{
			"MIGRATION_DATABASE_URI":          instance.URI,
			"RIVER_DOMAIN_DATABASE_ROLE":      domain,
			"RIVER_QUEUE_DATABASE_ROLE":       queue,
			"RIVER_COORDINATOR_DATABASE_ROLE": coordinator,
			"API_DATABASE_ROLE":               api,
			"AUTH_PROXY_SESSIONS":             "true",
		}
		if owned != "" {
			settings["AUTH_PLATFORM_ROLES_OWNED"] = owned
		}
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
		return rivermigrate.Execute(ctx, "dho", nil, lookup, &stdout, &stderr), stderr.String()
	}
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = api, "x"
	config.MaxConns = 1
	apiPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(apiPool.Close)

	held := func(role, privilege, column string) bool {
		t.Helper()
		var ok bool
		query, args := "SELECT has_table_privilege($1, 'public.users', $2)", []any{role, privilege}
		if column != "" {
			query, args = "SELECT has_column_privilege($1, 'public.users', $2, $3)", []any{role, column, privilege}
		}
		if err := admin.QueryRow(ctx, query, args...).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	type want struct{ insert, update, delete, selectTable, insertSuperuser, updateSuperuser, insertEmail, updateEmail, updateID bool }
	check := func(label string, w want) {
		t.Helper()
		got := want{held(api, "INSERT", ""), held(api, "UPDATE", ""), held(api, "DELETE", ""), held(api, "SELECT", ""),
			held(api, "INSERT", "is_superuser"), held(api, "UPDATE", "is_superuser"), held(api, "INSERT", "email"),
			held(api, "UPDATE", "email"), held(api, "UPDATE", "id")}
		if got != w {
			t.Fatalf("%s: api role holds %+v on public.users, want %+v", label, got, w)
		}
	}
	goAPIWritesAll := want{insert: true, update: true, delete: true, selectTable: true, insertSuperuser: true, updateSuperuser: true,
		insertEmail: true, updateEmail: true, updateID: true}
	ownedByAuth := want{delete: true, selectTable: true, insertEmail: true, updateEmail: true}
	refused := func(label, sql string, args ...any) {
		t.Helper()
		_, err := apiPool.Exec(ctx, sql, args...)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("%s: err = %v, want SQLSTATE 42501", label, err)
		}
	}
	runs := func(label, sql string, args ...any) {
		t.Helper()
		if _, err := apiPool.Exec(ctx, sql, args...); err != nil {
			t.Errorf("%s: %v", label, err)
		}
	}
	exact := func(label string, owned, wantOK bool) {
		t.Helper()
		err := postgresstore.CheckAPIAuthorizationFor(ctx, apiPool, api, "river", true, owned)
		if (err == nil) != wantOK {
			t.Errorf("%s: CheckAPIAuthorizationFor(owned=%v) = %v, want ok=%v", label, owned, err, wantOK)
		}
	}
	gapsName := func(label, column, privilege string, excess bool) {
		t.Helper()
		gaps, err := postgresstore.DiagnoseRolePosture(ctx, apiPool, api, postgresstore.APIPostureFor(true, true))
		if err != nil {
			t.Fatalf("%s: diagnose: %v", label, err)
		}
		for _, gap := range gaps {
			list := gap.Missing
			if excess {
				list = gap.Excess
			}
			if gap.TableName == "users" && gap.ColumnName == column && strings.Contains(strings.Join(list, ","), privilege) {
				return
			}
		}
		t.Errorf("%s: no gap names users %q %s (excess=%v): %v", label, column, privilege, excess, gaps)
	}
	user := "00000000-0000-4000-8000-0000000a1093"

	if code, stderr := migrate(""); code != cli.ExitOK {
		t.Fatalf("flag unset: exit %d\n%s", code, stderr)
	}
	check("flag unset", goAPIWritesAll)
	exact("flag unset", false, true)
	runs("flag unset: insert", usersInsertWhileOwned, user, "pr-10993@example.test")
	runs("flag unset: superuser update (rollback control)", `UPDATE users SET is_superuser = is_superuser WHERE id = $1`, user)

	if code, stderr := migrate("true"); code != cli.ExitOK {
		t.Fatalf("flag on: exit %d\n%s", code, stderr)
	}
	check("flag on", ownedByAuth)
	exact("flag on", true, true)
	exact("flag on, read as off", false, false)
	refused("flag on: insert naming is_superuser",
		`INSERT INTO users (id, email, is_superuser) VALUES (gen_random_uuid(), 'pr-10993-b@example.test', false)`)
	refused("flag on: superuser update", `UPDATE users SET is_superuser = true WHERE id = $1`, user)
	refused("flag on: superuser tuple update", `UPDATE users SET (email, is_superuser) = (email, false) WHERE id = $1`, user)
	runs("flag on: every go-api insert column", usersInsertWhileOwned, "00000000-0000-4000-8000-0000000a1094", "pr-10993-c@example.test")
	runs("flag on: every go-api update column", usersUpdateWhileOwned, user)
	runs("flag on: row lock", `SELECT id FROM users WHERE id = $1 FOR UPDATE`, user)
	runs("flag on: delete", `DELETE FROM users WHERE id = $1`, "00000000-0000-4000-8000-0000000a1094")
	for _, role := range []string{domain, queue, coordinator} {
		for _, privilege := range []string{"INSERT", "UPDATE"} {
			if held(role, privilege, "is_superuser") {
				t.Errorf("flag on: %s holds %s (is_superuser) on public.users", role, privilege)
			}
		}
	}

	if _, err := admin.Exec(ctx, "GRANT INSERT ON TABLE public.users TO "+api); err != nil {
		t.Fatal(err)
	}
	exact("excess table-wide INSERT", true, false)
	gapsName("excess table-wide INSERT", "", "INSERT", true)
	if code, stderr := migrate("true"); code != cli.ExitOK {
		t.Fatalf("flag on, re-run: exit %d\n%s", code, stderr)
	}
	check("flag on, re-run", ownedByAuth)
	if _, err := admin.Exec(ctx, "GRANT INSERT (is_superuser) ON TABLE public.users TO "+api); err != nil {
		t.Fatal(err)
	}
	exact("excess INSERT (is_superuser)", true, false)
	if _, err := admin.Exec(ctx, "REVOKE INSERT (is_superuser), UPDATE (updated_at) ON TABLE public.users FROM "+api); err != nil {
		t.Fatal(err)
	}
	exact("missing UPDATE (updated_at)", true, false)
	gapsName("missing UPDATE (updated_at)", "updated_at", "UPDATE", false)
	if code, stderr := migrate("true"); code != cli.ExitOK {
		t.Fatalf("flag on, repair: exit %d\n%s", code, stderr)
	}
	exact("flag on, repaired", true, true)

	if code, stderr := migrate("yes"); code != 1 || !strings.Contains(stderr, "AUTH_PLATFORM_ROLES_OWNED") {
		t.Fatalf("an unparsable flag: exit %d, stderr:\n%s", code, stderr)
	}
	check("after a refused run", ownedByAuth)

	if code, stderr := migrate("false"); code != cli.ExitOK {
		t.Fatalf("flag off again: exit %d\n%s", code, stderr)
	}
	check("flag off again (rollback)", goAPIWritesAll)
	exact("flag off again (rollback)", false, true)
	runs("rollback: superuser update", `UPDATE users SET is_superuser = is_superuser WHERE id = $1`, user)
}
