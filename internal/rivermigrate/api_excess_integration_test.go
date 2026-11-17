//go:build integration

package rivermigrate_test

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// The River hook fails when, after its apply, the api role holds a privilege
// beyond its posture (DiagnoseRolePosture's excess), and names each one with
// how it reaches the role and its grantor. A grant by another grantor survives
// the hook's REVOKE, so only this read sees it. Two role shapes: P is prod (the
// hook logs in as the superuser that owns every object, users not
// owner-moved; the auth migrator holds users privileges WITH GRANT OPTION from
// it), T is a NOSUPERUSER hook login with users owner-moved to the auth
// migrator. Every cell also runs go-api's readiness query as an api login on
// the same state: a hook exit 1 must never meet a passing readiness.
func TestRiverHookFailsOnAPIRoleExcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgresIsolated(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	const (
		api, domain, queue, coordinator = "devhealth_api", "devhealth_domain", "devhealth_queue", "devhealth_coordinator"
		migrator, authMigrator, other   = "cell_river_migrator", "devhealth_auth_migrator", "cell_other_grantor"
		group                           = "cell_api_group"
		undeclared                      = "sync_dispatch_transport_routes"
	)
	exec := func(conn *pgx.Conn, statements ...string) {
		t.Helper()
		for _, statement := range statements {
			if _, err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
	}
	uriFor := func(role, database string) string {
		t.Helper()
		parsed, err := url.Parse(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		if role != "" {
			parsed.User = url.UserPassword(role, "x")
		}
		parsed.Path = "/" + database
		return parsed.String()
	}
	connect := func(role, database string) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, uriFor(role, database))
		if err != nil {
			t.Fatalf("connect as %q to %s: %v", role, database, err)
		}
		return conn
	}
	adminExec := func(statements ...string) {
		t.Helper()
		for _, statement := range statements {
			if _, err := admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
	}
	for _, role := range []string{api, domain, queue, coordinator, migrator, authMigrator, other} {
		adminExec("CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'")
	}
	adminExec("CREATE ROLE " + group + " NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")

	migrate := func(hookLogin, database string) (int, string) {
		settings := map[string]string{
			"MIGRATION_DATABASE_URI":          uriFor(hookLogin, database),
			"RIVER_DOMAIN_DATABASE_ROLE":      domain,
			"RIVER_QUEUE_DATABASE_ROLE":       queue,
			"RIVER_COORDINATOR_DATABASE_ROLE": coordinator,
			"API_DATABASE_ROLE":               api,
			"AUTH_PROXY_SESSIONS":             "true",
			"AUTH_PLATFORM_ROLES_OWNED":       "true",
		}
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
		code := rivermigrate.Execute(ctx, "dho", []string{"--apply-and-check"}, lookup, &stdout, &stderr)
		return code, stdout.String() + stderr.String()
	}

	type shape struct {
		name string
		// hook is the hook's login; "" = the container superuser, which owns
		// every object pgschema builds (shape P).
		hook string
		base string
		// owner runs the cells' owner-side statements (the owner of every
		// public object but users in T; of every object in P).
		owner string
	}
	shapes := []shape{
		{name: "P", hook: "", base: "cell_base_p", owner: ""},
		{name: "T", hook: migrator, base: "cell_base_t", owner: migrator},
	}

	adminExec("CREATE DATABASE cell_base_p")
	pgschema.ApplyURI(ctx, t, uriFor("", "cell_base_p"))
	func() {
		conn := connect("", "cell_base_p")
		defer conn.Close(context.Background())
		exec(conn,
			"GRANT SELECT, INSERT, DELETE, REFERENCES ON public.users TO "+authMigrator+" WITH GRANT OPTION",
			"GRANT INSERT (saml_nameid_format, saml_sp_name_qualifier), UPDATE (saml_nameid_format, saml_sp_name_qualifier) ON public.users TO "+authMigrator+" WITH GRANT OPTION",
			"GRANT SELECT, INSERT ON public.saml_assertion_replays TO "+other+" WITH GRANT OPTION",
		)
	}()

	adminExec("CREATE DATABASE cell_base_t OWNER " + migrator)
	pgschema.ApplyURI(ctx, t, uriFor("", "cell_base_t"))
	func() {
		conn := connect("", "cell_base_t")
		defer conn.Close(context.Background())
		exec(conn, `DO $$ DECLARE r record; BEGIN
			FOR r IN SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm', 'S') AND NOT (c.relkind = 'S' AND EXISTS (
					SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype IN ('a', 'i')))
			LOOP
				IF r.relname = 'users' THEN
					EXECUTE format('ALTER TABLE public.%I OWNER TO `+authMigrator+`', r.relname);
				ELSIF r.relkind = 'S' THEN
					EXECUTE format('ALTER SEQUENCE public.%I OWNER TO `+migrator+`', r.relname);
				ELSE
					EXECUTE format('ALTER TABLE public.%I OWNER TO `+migrator+`', r.relname);
				END IF;
			END LOOP;
			FOR r IN SELECT p.oid::regprocedure AS sig FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public'
			LOOP
				EXECUTE format('ALTER FUNCTION %s OWNER TO `+migrator+`', r.sig);
			END LOOP;
		END $$`)
	}()
	func() {
		conn := connect(authMigrator, "cell_base_t")
		defer conn.Close(context.Background())
		exec(conn, "GRANT ALL ON public.users TO "+migrator+" WITH GRANT OPTION")
	}()
	func() {
		conn := connect(migrator, "cell_base_t")
		defer conn.Close(context.Background())
		exec(conn, "GRANT SELECT, INSERT ON public.saml_assertion_replays TO "+other+" WITH GRANT OPTION")
	}()

	for _, s := range shapes {
		if code, out := migrate(s.hook, s.base); code != cli.ExitOK {
			t.Fatalf("shape %s baseline: dho migrate river --apply-and-check: exit %d\n%s", s.name, code, out)
		}
	}

	const excessLine = "excess privileges for the api role"
	type readiness int
	const (
		readinessPasses readiness = iota
		readinessRefuses
		readinessLogged
	)
	type cell struct {
		name      string
		setup     func(s shape, database string)
		wantCode  int
		readiness readiness
		// names must all appear in the hook's output when it fails on excess.
		names   func(s shape) []string
		after   func(s shape, database string)
		cleanup func()
	}
	held := func(database, query string, args ...any) bool {
		t.Helper()
		conn := connect("", database)
		defer conn.Close(context.Background())
		var ok bool
		if err := conn.QueryRow(ctx, query, args...).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	cells := []cell{
		{
			name: "a another grantor's grant outside the posture",
			setup: func(s shape, database string) {
				conn := connect(authMigrator, database)
				defer conn.Close(context.Background())
				exec(conn, "GRANT INSERT (saml_nameid_format) ON public.users TO "+api)
				otherConn := connect(other, database)
				defer otherConn.Close(context.Background())
				exec(otherConn, "GRANT SELECT ON public.saml_assertion_replays TO "+api)
			},
			wantCode:  1,
			readiness: readinessRefuses,
			names: func(shape) []string {
				return []string{"saml_nameid_format", "saml_assertion_replays", authMigrator, other}
			},
			after: func(s shape, database string) {
				if !held(database, "SELECT has_column_privilege($1, 'public.users', 'saml_nameid_format', 'INSERT')", api) {
					t.Errorf("shape %s cell a: the other grantor's INSERT (saml_nameid_format) is gone; want it still held (the hook cannot revoke it)", s.name)
				}
			},
		},
		{
			name: "a2 an allowed privilege WITH GRANT OPTION",
			setup: func(s shape, database string) {
				conn := connect(authMigrator, database)
				defer conn.Close(context.Background())
				exec(conn, "GRANT SELECT ON public.users TO "+api+" WITH GRANT OPTION")
			},
			wantCode:  1,
			readiness: readinessRefuses,
			names:     func(shape) []string { return []string{"users", "SELECT WITH GRANT OPTION", authMigrator} },
		},
		{
			name: "a3 a default ACL naming the api role",
			setup: func(s shape, database string) {
				conn := connect(authMigrator, database)
				defer conn.Close(context.Background())
				exec(conn, "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "+api)
			},
			wantCode:  0,
			readiness: readinessPasses,
		},
		{
			name: "a4 a privilege outside the posture through PUBLIC",
			setup: func(s shape, database string) {
				conn := connect(s.owner, database)
				defer conn.Close(context.Background())
				exec(conn, "GRANT SELECT ON public."+undeclared+" TO PUBLIC")
			},
			wantCode:  1,
			readiness: readinessRefuses,
			names:     func(shape) []string { return []string{undeclared, "PUBLIC"} },
		},
		{
			name: "a4m a privilege outside the posture through role membership",
			setup: func(s shape, database string) {
				conn := connect(s.owner, database)
				defer conn.Close(context.Background())
				exec(conn, "GRANT SELECT ON public."+undeclared+" TO "+group)
				adminExec("GRANT " + group + " TO " + api)
			},
			wantCode:  1,
			readiness: readinessRefuses,
			names:     func(shape) []string { return []string{undeclared, group} },
			cleanup:   func() { adminExec("REVOKE " + group + " FROM " + api) },
		},
		{
			name: "b hook-made grants only (the step-1 SAML grants, then this posture)",
			setup: func(s shape, database string) {
				conn := connect(s.hook, database)
				defer conn.Close(context.Background())
				exec(conn,
					"GRANT SELECT, INSERT ON public.saml_assertion_replays TO "+api,
					"GRANT INSERT (saml_nameid_format, saml_sp_name_qualifier), UPDATE (saml_nameid_format, saml_sp_name_qualifier) ON public.users TO "+api)
			},
			wantCode:  0,
			readiness: readinessPasses,
			after: func(s shape, database string) {
				if held(database, `SELECT has_table_privilege($1, 'public.saml_assertion_replays', 'SELECT')
					OR has_table_privilege($1, 'public.saml_assertion_replays', 'INSERT')
					OR has_column_privilege($1, 'public.users', 'saml_nameid_format', 'INSERT')
					OR has_column_privilege($1, 'public.users', 'saml_nameid_format', 'UPDATE')
					OR has_column_privilege($1, 'public.users', 'saml_sp_name_qualifier', 'INSERT')
					OR has_column_privilege($1, 'public.users', 'saml_sp_name_qualifier', 'UPDATE')`, api) {
					t.Errorf("shape %s cell b: the api role still holds a hook-made retired SAML grant", s.name)
				}
			},
		},
		{
			name: "b2 another grantor of an allowed privilege",
			setup: func(s shape, database string) {
				conn := connect(authMigrator, database)
				defer conn.Close(context.Background())
				exec(conn, "GRANT SELECT ON public.users TO "+api)
			},
			wantCode:  0,
			readiness: readinessPasses,
		},
		{
			name: "c a River role's table is missing",
			setup: func(s shape, database string) {
				conn := connect(s.owner, database)
				defer conn.Close(context.Background())
				exec(conn, "DROP TABLE public.worker_job_delivery_abandonments CASCADE")
			},
			wantCode:  1,
			readiness: readinessLogged,
		},
		{
			name: "c2 an api table is missing",
			setup: func(s shape, database string) {
				conn := connect(s.owner, database)
				defer conn.Close(context.Background())
				exec(conn, "DROP TABLE public.internal_service_credential_audits CASCADE")
			},
			wantCode:  0,
			readiness: readinessLogged,
		},
	}

	serial := 0
	for _, s := range shapes {
		for _, c := range cells {
			serial++
			database := "cell_" + strings.ToLower(s.name) + "_" + string(rune('a'+serial))
			createOwner := ""
			if s.hook != "" {
				createOwner = " OWNER " + s.hook
			}
			// A template must have no session; a just-closed pool's backend can
			// still be exiting.
			for attempt := 0; ; attempt++ {
				_, err := admin.Exec(ctx, "CREATE DATABASE "+database+" TEMPLATE "+s.base+createOwner)
				if err == nil {
					break
				}
				if attempt == 20 {
					t.Fatalf("create %s from %s: %v", database, s.base, err)
				}
				time.Sleep(500 * time.Millisecond)
			}
			label := "shape " + s.name + " " + c.name
			c.setup(s, database)
			code, out := migrate(s.hook, database)

			apiConfig, err := pgxpool.ParseConfig(uriFor(api, database))
			if err != nil {
				t.Fatal(err)
			}
			apiConfig.MaxConns = 1
			apiPool, err := pgxpool.NewWithConfig(ctx, apiConfig)
			if err != nil {
				t.Fatal(err)
			}
			readinessErr := postgresstore.CheckRolePosture(ctx, apiPool, api, "river", postgresstore.APIPostureFor(true, true))
			apiPool.Close()
			t.Logf("%s: hook exit %d | readiness %v", label, code, readinessErr)

			if code != c.wantCode {
				t.Errorf("%s: hook exit %d, want %d\n%s", label, code, c.wantCode, out)
			}
			switch c.readiness {
			case readinessPasses:
				if readinessErr != nil {
					t.Errorf("%s: go-api readiness refused (%v), want it to pass", label, readinessErr)
				}
			case readinessRefuses:
				if !errors.Is(readinessErr, postgresstore.ErrPostureRefused) {
					t.Errorf("%s: go-api readiness = %v, want a posture refusal", label, readinessErr)
				}
			}
			if c.readiness != readinessLogged && code == 1 && readinessErr == nil {
				t.Errorf("%s: PARITY: the hook failed while go-api readiness passes (a false hook fail)", label)
			}
			if c.wantCode == 1 && c.names != nil {
				if !strings.Contains(out, excessLine) {
					t.Errorf("%s: the hook output lacks %q\n%s", label, excessLine, out)
				}
				for _, name := range c.names(s) {
					if !strings.Contains(out, name) {
						t.Errorf("%s: the hook output does not name %q\n%s", label, name, out)
					}
				}
			}
			if c.name[0] == 'c' && c.wantCode == 1 && !strings.Contains(out, "found missing privileges") {
				t.Errorf("%s: want today's missing-privileges failure text\n%s", label, out)
			}
			if c.wantCode == 0 && strings.Contains(out, excessLine) {
				t.Errorf("%s: the hook passed but printed %q\n%s", label, excessLine, out)
			}
			if c.after != nil {
				c.after(s, database)
			}
			if c.cleanup != nil {
				c.cleanup()
			}
		}
	}
}
