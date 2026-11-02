//go:build integration

package ownermove

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/aclguard"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// moveEnv is the prod shape before the move: a SUPERUSER role (old) owns the database, schema river and every object
// of public and river, and granted the runtime roles their privileges; a plain login (mig, the auth migrator) holds
// grant-option rows on public tables and granted a runtime role (rt2) from them; an extension (citext) owns objects in
// public, operators and an operator class among them; old holds default privileges in river.
type moveEnv struct {
	instance                 *containers.Instance
	uri                      string
	admin                    *pgx.Conn
	old, next, mig, rt1, rt2 string
	nextURI, oldURI, migURI  string
	database                 string
}

const testPassword = "owner-move-test-only"

func roleURI(t *testing.T, uri, role string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, testPassword)
	return parsed.String()
}

func dial(t *testing.T, ctx context.Context, uri string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func exec(t *testing.T, ctx context.Context, conn *pgx.Conn, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func newMoveEnv(t *testing.T, ctx context.Context) *moveEnv {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	env := &moveEnv{instance: instance, uri: instance.URI}
	env.admin = dial(t, ctx, instance.URI)
	for _, r := range []struct {
		prefix string
		into   *string
	}{{"omv_old", &env.old}, {"omv_next", &env.next}, {"omv_mig", &env.mig}, {"omv_rt1", &env.rt1}, {"omv_rt2", &env.rt2}} {
		if *r.into, err = containers.RoleName(r.prefix, instance); err != nil {
			t.Fatal(err)
		}
	}
	if err := env.admin.QueryRow(ctx, `SELECT current_database()`).Scan(&env.database); err != nil {
		t.Fatal(err)
	}
	exec(t, ctx, env.admin,
		`CREATE ROLE `+env.old+` LOGIN SUPERUSER PASSWORD '`+testPassword+`'`,
		`CREATE ROLE `+env.next+` LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '`+testPassword+`'`,
		`CREATE ROLE `+env.mig+` LOGIN NOSUPERUSER PASSWORD '`+testPassword+`'`,
		`CREATE ROLE `+env.rt1+` LOGIN NOSUPERUSER PASSWORD '`+testPassword+`'`,
		`CREATE ROLE `+env.rt2+` LOGIN NOSUPERUSER PASSWORD '`+testPassword+`'`,
		`ALTER DATABASE `+quote(env.database)+` OWNER TO `+env.old,
	)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := pgx.Connect(closeCtx, instance.URI)
		if err != nil {
			return
		}
		defer conn.Close(closeCtx)
		_, _ = conn.Exec(closeCtx, `ALTER DATABASE `+quote(env.database)+` OWNER TO CURRENT_USER`)
		for _, role := range []string{env.old, env.next, env.mig, env.rt1, env.rt2} {
			_, _ = conn.Exec(closeCtx, `DROP OWNED BY `+role+` CASCADE`)
			_, _ = conn.Exec(closeCtx, `DROP ROLE IF EXISTS `+role)
		}
	})
	env.oldURI, env.nextURI, env.migURI = roleURI(t, instance.URI, env.old), roleURI(t, instance.URI, env.next), roleURI(t, instance.URI, env.mig)
	old := dial(t, ctx, env.oldURI)
	exec(t, ctx, old,
		`CREATE SCHEMA river`,
		`CREATE EXTENSION citext SCHEMA public`,
		`CREATE TABLE public.users (id serial PRIMARY KEY, email text, name text, secret text)`,
		`CREATE INDEX users_email_idx ON public.users (email)`,
		`CREATE TABLE public.orgs (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label citext)`,
		`CREATE SEQUENCE public.standalone_seq`,
		`CREATE VIEW public.users_view AS SELECT id, email FROM public.users`,
		`CREATE FUNCTION public.definer_fn(text) RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS 'SELECT true'`,
		`CREATE FUNCTION public.plain_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'`,
		`CREATE TYPE river.job_state AS ENUM ('available', 'running')`,
		`CREATE TABLE river.jobs (id bigserial PRIMARY KEY, state river.job_state NOT NULL DEFAULT 'available')`,
		`CREATE FUNCTION river.notify_fn() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'`,
		`GRANT CONNECT ON DATABASE `+quote(env.database)+` TO `+env.rt1+`, `+env.mig,
		`REVOKE TEMPORARY ON DATABASE `+quote(env.database)+` FROM PUBLIC`,
		`GRANT USAGE ON SCHEMA public TO `+env.rt1+`, `+env.mig+`, `+env.rt2,
		`GRANT SELECT, INSERT ON public.users TO `+env.rt1,
		`GRANT UPDATE (name) ON public.users TO `+env.rt1,
		`GRANT USAGE ON SEQUENCE public.users_id_seq TO `+env.rt1,
		`GRANT SELECT ON public.users_view TO `+env.rt1,
		`REVOKE ALL ON FUNCTION public.definer_fn(text) FROM PUBLIC`,
		`GRANT EXECUTE ON FUNCTION public.definer_fn(text) TO `+env.rt1,
		`GRANT SELECT (id, email) ON public.users TO `+env.mig+` WITH GRANT OPTION`,
		`GRANT REFERENCES ON public.users TO `+env.mig,
		`GRANT DELETE ON public.orgs TO `+env.mig+` WITH GRANT OPTION`,
		`GRANT USAGE ON SCHEMA river TO `+env.rt1,
		`GRANT SELECT, UPDATE ON river.jobs TO `+env.rt1,
		`GRANT USAGE ON TYPE river.job_state TO `+env.rt1,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA river GRANT SELECT ON TABLES TO `+env.rt1,
	)
	mig := dial(t, ctx, env.migURI)
	exec(t, ctx, mig,
		`GRANT SELECT (id, email) ON public.users TO `+env.rt2,
		`GRANT DELETE ON public.orgs TO `+env.rt2,
	)
	return env
}

// aclSnapshotSQL is every ACL entry and owner of the database, the ops schemas and their objects, as text, in a
// stable order. ACL text carries grantor and grantee names.
const aclSnapshotSQL = `
SELECT line FROM (
  SELECT 'db ' || pg_get_userbyid(datdba) || ' ' || coalesce(datacl::text, '-') AS line FROM pg_database WHERE datname = current_database()
  UNION ALL SELECT 'nsp ' || nspname || ' ' || pg_get_userbyid(nspowner) || ' ' || coalesce(nspacl::text, '-') FROM pg_namespace WHERE nspname IN ('public', 'river')
  UNION ALL SELECT 'rel ' || n.nspname || '.' || c.relname || ' ' || pg_get_userbyid(c.relowner) || ' ' || coalesce(c.relacl::text, '-')
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname IN ('public', 'river')
  UNION ALL SELECT 'col ' || n.nspname || '.' || c.relname || '.' || a.attname || ' ' || a.attacl::text
    FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN ('public', 'river') AND a.attacl IS NOT NULL
  UNION ALL SELECT 'fn ' || p.oid::regprocedure::text || ' ' || pg_get_userbyid(p.proowner) || ' ' || coalesce(p.proacl::text, '-')
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname IN ('public', 'river')
  UNION ALL SELECT 'type ' || n.nspname || '.' || t.typname || ' ' || pg_get_userbyid(t.typowner) || ' ' || coalesce(t.typacl::text, '-')
    FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname IN ('public', 'river')
  UNION ALL SELECT 'defacl ' || pg_get_userbyid(defaclrole) || ' ' || defaclobjtype::text || ' ' || defaclacl::text FROM pg_default_acl
) s ORDER BY 1`

func snapshot(t *testing.T, ctx context.Context, conn *pgx.Conn) []string {
	t.Helper()
	rows, err := conn.Query(ctx, aclSnapshotSQL)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// privilegeRowsSQL is every stored ACL entry of the ops schemas, the schemas and the database as
// (object, grantor, grantee, privilege, grantable).
const privilegeRowsSQL = `
SELECT object, pg_get_userbyid(grantor), CASE WHEN grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(grantee) END, priv, grantable FROM (
  SELECT 'db' AS object, a.grantor, a.grantee, a.privilege_type AS priv, a.is_grantable AS grantable
    FROM pg_database d, aclexplode(d.datacl) a WHERE d.datname = current_database()
  UNION ALL SELECT 'nsp ' || n.nspname, a.grantor, a.grantee, a.privilege_type, a.is_grantable FROM pg_namespace n, aclexplode(n.nspacl) a
    WHERE n.nspname IN ('public', 'river')
  UNION ALL SELECT 'rel ' || n.nspname || '.' || c.relname, a.grantor, a.grantee, a.privilege_type, a.is_grantable
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, aclexplode(c.relacl) a WHERE n.nspname IN ('public', 'river')
  UNION ALL SELECT 'col ' || n.nspname || '.' || c.relname || '.' || att.attname, a.grantor, a.grantee, a.privilege_type, a.is_grantable
    FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace, aclexplode(att.attacl) a
    WHERE n.nspname IN ('public', 'river')
  UNION ALL SELECT 'fn ' || p.oid::regprocedure::text, a.grantor, a.grantee, a.privilege_type, a.is_grantable
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace, aclexplode(p.proacl) a WHERE n.nspname IN ('public', 'river')
  UNION ALL SELECT 'type ' || n.nspname || '.' || t.typname, a.grantor, a.grantee, a.privilege_type, a.is_grantable
    FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace, aclexplode(t.typacl) a WHERE n.nspname IN ('public', 'river')
) s`

type privilegeRow struct {
	object, grantor, grantee, privilege string
	grantable                           bool
}

func privilegeRows(t *testing.T, ctx context.Context, conn *pgx.Conn) []privilegeRow {
	t.Helper()
	rows, err := conn.Query(ctx, privilegeRowsSQL)
	if err != nil {
		t.Fatal(err)
	}
	var out []privilegeRow
	for rows.Next() {
		var r privilegeRow
		if err := rows.Scan(&r.object, &r.grantor, &r.grantee, &r.privilege, &r.grantable); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	return out
}

// grantSets maps each grantee (old and new named alike) to its set of "object privilege grantable" with the grantor
// role mapped through rename, for an equality check that ignores only the owner swap.
func grantSets(rows []privilegeRow, rename map[string]string) map[string]map[string]bool {
	sets := map[string]map[string]bool{}
	for _, r := range rows {
		grantee, grantor := r.grantee, r.grantor
		if v, ok := rename[grantee]; ok {
			grantee = v
		}
		if v, ok := rename[grantor]; ok {
			grantor = v
		}
		if sets[grantee] == nil {
			sets[grantee] = map[string]bool{}
		}
		key := r.object + " " + r.privilege + " by " + grantor
		if r.grantable {
			key += " WITH GRANT OPTION"
		}
		sets[grantee][key] = true
	}
	return sets
}

func (e *moveEnv) move(t *testing.T, ctx context.Context, mode Mode, from, to string, restore bool) Report {
	t.Helper()
	var report Report
	for _, step := range []Options{
		{Schemas: []string{"river"}, From: from, To: to, Mode: mode, Restore: restore},
		{Schemas: []string{"public"}, DatabaseOwner: true, From: from, To: to, Mode: mode, Restore: restore},
	} {
		r, err := Move(ctx, e.admin, step)
		if err != nil {
			t.Fatalf("move %v %s -> %s (%s): %v", step.Schemas, from, to, mode, err)
		}
		report = r
	}
	return report
}

func ownedByRole(t *testing.T, ctx context.Context, conn *pgx.Conn, role string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(ctx, ownedLeftSQL, []string{"public", "river"}, role, true).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestMoveRewritesGrantorsAndKeepsEveryPrivilegeSet(t *testing.T) {
	ctx := context.Background()
	e := newMoveEnv(t, ctx)
	before := privilegeRows(t, ctx, e.admin)
	oldGranted := 0
	for _, r := range before {
		if r.grantor == e.old {
			oldGranted++
		}
	}
	if oldGranted == 0 {
		t.Fatal("the fixture holds no ACL entry granted by the old owner")
	}

	river, err := Move(ctx, e.admin, Options{Schemas: []string{"river"}, From: e.old, To: e.next, Mode: Apply})
	if err != nil {
		t.Fatalf("move river: %v", err)
	}
	public, err := Move(ctx, e.admin, Options{Schemas: []string{"public"}, DatabaseOwner: true, From: e.old, To: e.next, Mode: Apply})
	if err != nil {
		t.Fatalf("move public and the database: %v", err)
	}
	for _, r := range []Report{river, public} {
		if !r.Applied || r.OldGrantorRowsLeft != 0 {
			t.Fatalf("report %s", r.Line())
		}
	}
	if public.ExtensionSkipped == 0 {
		t.Fatalf("the extension's objects were not skipped: %s", public.Line())
	}
	if public.Moved["database"] != 1 || public.Moved["schema"] != 0 || river.Moved["schema"] != 1 {
		t.Fatalf("schema/database counts: %s | %s", river.Line(), public.Line())
	}
	if len(river.DefaultACLs) != 1 || river.DefaultACLs[0].Schema != "river" || river.DefaultACLs[0].ObjectType != "r" {
		t.Fatalf("the old owner's default privilege row is not listed: %s", river.Line())
	}

	after := privilegeRows(t, ctx, e.admin)
	if got, want := grantSets(after, nil), grantSets(before, map[string]string{e.old: e.next}); !reflect.DeepEqual(got, want) {
		t.Fatalf("per-grantee privilege sets changed beyond the grantor old -> new:\n got %v\nwant %v", got, want)
	}
	for _, r := range after {
		if r.grantor == e.old || r.grantee == e.old {
			if !strings.HasPrefix(r.object, "fn public.") && !strings.HasPrefix(r.object, "type public.") {
				t.Errorf("an ACL entry still names the old owner: %+v", r)
			}
		}
	}
	migOptions := 0
	for _, r := range after {
		if r.grantee == e.mig && r.grantable {
			migOptions++
			if r.grantor != e.next {
				t.Errorf("the migrator's grant-option row %+v has grantor %s, want %s", r, r.grantor, e.next)
			}
		}
		if r.grantee == e.rt2 && !strings.HasPrefix(r.object, "nsp ") && r.grantor != e.mig {
			t.Errorf("a row the migrator granted changed its grantor: %+v", r)
		}
	}
	if migOptions != 3 {
		t.Fatalf("the migrator holds %d grant-option rows after the move, want 3", migOptions)
	}

	// The auth migrator still grants from its options: auth-grants as the migrator works after the move.
	mig := dial(t, ctx, e.migURI)
	exec(t, ctx, mig,
		`REVOKE SELECT (id, email) ON public.users FROM `+e.rt2,
		`GRANT SELECT (id, email) ON public.users TO `+e.rt2,
		`REVOKE DELETE ON public.orgs FROM `+e.rt2,
		`GRANT DELETE ON public.orgs TO `+e.rt2,
	)

	if n := ownedByRole(t, ctx, e.admin, e.old); n != 0 {
		t.Fatalf("the old owner still owns %d non-extension objects", n)
	}
	var datdba, river2, public2 string
	if err := e.admin.QueryRow(ctx, `SELECT pg_get_userbyid(datdba), (SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'river'),
		(SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'public') FROM pg_database WHERE datname = current_database()`).Scan(&datdba, &river2, &public2); err != nil {
		t.Fatal(err)
	}
	if datdba != e.next || river2 != e.next || public2 != "pg_database_owner" {
		t.Fatalf("owners: database %s, river %s, public %s", datdba, river2, public2)
	}
	var extOwner string
	if err := e.admin.QueryRow(ctx, `SELECT pg_get_userbyid(typowner) FROM pg_type WHERE typname = 'citext'`).Scan(&extOwner); err != nil {
		t.Fatal(err)
	}
	if extOwner != e.old {
		t.Fatalf("the extension's type moved to %s; extension members stay with the old owner", extOwner)
	}

	// The new owner runs DDL and grants in both schemas and at the database level as a NOSUPERUSER login.
	next := dial(t, ctx, e.nextURI)
	exec(t, ctx, next,
		`CREATE TABLE public.after_move (id int)`,
		`CREATE INDEX IF NOT EXISTS after_move_idx ON public.after_move (id)`,
		`CREATE SCHEMA IF NOT EXISTS river`,
		`CREATE TABLE river.after_move (id int)`,
		`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM `+e.rt1,
		`GRANT SELECT, INSERT ON public.users TO `+e.rt1,
		`REVOKE CREATE ON SCHEMA public FROM `+e.rt1,
		`DO $$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database()); END $$`,
		`DO $$ BEGIN EXECUTE format('GRANT CONNECT ON DATABASE %I TO `+e.rt2+`', current_database()); END $$`,
	)
	if err := VerifyPin(ctx, next, e.next, "river"); err != nil {
		t.Fatalf("the pin of the new owner after the move: %v", err)
	}
}

func TestPlanReadsOnlyAndPredictsTheApply(t *testing.T) {
	ctx := context.Background()
	e := newMoveEnv(t, ctx)
	before := snapshot(t, ctx, e.admin)

	// A plan neither waits for a table lock nor for the migrators' locks: it runs no ALTER.
	holder := dial(t, ctx, e.uri)
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `LOCK TABLE public.users IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1), pg_advisory_xact_lock($2)`, RiverMigrationLockKey, aclguard.WriterLockKey); err != nil {
		t.Fatal(err)
	}
	planCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var plans []Report
	for _, options := range []Options{
		{Schemas: []string{"river"}, From: e.old, To: e.next, Mode: Plan, LockTimeout: 200 * time.Millisecond},
		{Schemas: []string{"public"}, DatabaseOwner: true, From: e.old, To: e.next, Mode: Plan, LockTimeout: 200 * time.Millisecond},
	} {
		report, err := Move(planCtx, e.admin, options)
		if err != nil {
			t.Fatalf("plan %v: %v", options.Schemas, err)
		}
		if report.Applied || report.OldGrantorRowsLeft != -1 || !strings.Contains(report.Line(), "mode=plan applied=false") || !strings.Contains(report.Line(), "old_grantor_rows_left=n/a") {
			t.Fatalf("plan report: %s", report.Line())
		}
		plans = append(plans, report)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if after := snapshot(t, ctx, e.admin); !reflect.DeepEqual(before, after) {
		t.Fatal("the plan changed the catalog")
	}
	if plans[1].GrantorRows == 0 || plans[0].GrantorRows == 0 {
		t.Fatalf("the plan counts no grantor rows: %s | %s", plans[0].Line(), plans[1].Line())
	}

	for i, options := range []Options{
		{Schemas: []string{"river"}, From: e.old, To: e.next, Mode: Apply},
		{Schemas: []string{"public"}, DatabaseOwner: true, From: e.old, To: e.next, Mode: Apply},
	} {
		applied, err := Move(ctx, e.admin, options)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(applied.Moved, plans[i].Moved) || !reflect.DeepEqual(applied.Followed, plans[i].Followed) ||
			applied.ExtensionSkipped != plans[i].ExtensionSkipped || applied.GrantorRows != plans[i].GrantorRows {
			t.Fatalf("the apply differs from its plan:\nplan  %s\napply %s", plans[i].Line(), applied.Line())
		}
	}
}

func TestDryRunChangesNothing(t *testing.T) {
	ctx := context.Background()
	e := newMoveEnv(t, ctx)
	before := snapshot(t, ctx, e.admin)
	report := e.move(t, ctx, DryRun, e.old, e.next, false)
	if report.Applied || report.OldGrantorRowsLeft != 0 || !strings.Contains(report.Line(), "mode=dry-run applied=false") {
		t.Fatalf("dry run report: %s", report.Line())
	}
	if after := snapshot(t, ctx, e.admin); !reflect.DeepEqual(before, after) {
		t.Fatal("the dry run changed the catalog")
	}
}

func TestALockWaitPastTheTimeoutRollsBack(t *testing.T) {
	ctx := context.Background()
	e := newMoveEnv(t, ctx)
	before := snapshot(t, ctx, e.admin)
	holders := map[string]string{
		"a reader of a public table": `LOCK TABLE public.users IN ACCESS SHARE MODE`,
		"the River migration lock":   `SELECT pg_advisory_xact_lock(30330037)`,
		"the postgres walk lock":     `SELECT pg_advisory_xact_lock(110399707902055)`,
		"the privilege writer lock":  `SELECT pg_advisory_xact_lock(71757299384321)`,
	}
	for name, hold := range holders {
		t.Run(name, func(t *testing.T) {
			holder := dial(t, ctx, e.uri)
			tx, err := holder.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if _, err := tx.Exec(ctx, hold); err != nil {
				t.Fatal(err)
			}
			moveCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			_, err = Move(moveCtx, e.admin, Options{Schemas: []string{"public"}, DatabaseOwner: true, From: e.old, To: e.next, Mode: Apply, LockTimeout: 300 * time.Millisecond})
			if !errors.Is(err, ErrLockTimeout) {
				t.Fatalf("Move = %v, want ErrLockTimeout", err)
			}
		})
	}
	if PostgresWalkLockKey != 110399707902055 || aclguard.WriterLockKey != 71757299384321 {
		t.Fatal("the literal keys of this test are stale")
	}
	if after := snapshot(t, ctx, e.admin); !reflect.DeepEqual(before, after) {
		t.Fatal("a move that hit the lock timeout changed the catalog")
	}
}

func TestRestoreBringsBackEveryACLByteForByteAndAMoveWorksAgain(t *testing.T) {
	ctx := context.Background()
	e := newMoveEnv(t, ctx)
	before := snapshot(t, ctx, e.admin)
	e.move(t, ctx, Apply, e.old, e.next, false)
	if moved := snapshot(t, ctx, e.admin); reflect.DeepEqual(before, moved) {
		t.Fatal("the forward move changed nothing")
	}
	if _, err := Move(ctx, e.admin, Options{Schemas: []string{"public"}, DatabaseOwner: true, From: e.next, To: e.old, Mode: Apply}); err == nil {
		t.Fatal("a move to a superuser without -restore succeeded")
	}
	e.move(t, ctx, Apply, e.next, e.old, true)
	if after := snapshot(t, ctx, e.admin); !reflect.DeepEqual(before, after) {
		t.Fatalf("the ACL snapshot after the restore differs:\nbefore %v\nafter  %v", before, after)
	}
	report := e.move(t, ctx, Apply, e.old, e.next, false)
	if !report.Applied {
		t.Fatalf("the second forward move: %s", report.Line())
	}
	if n := ownedByRole(t, ctx, e.admin, e.old); n != 0 {
		t.Fatalf("after the second move the old owner still owns %d objects", n)
	}
}

func TestMoveRefusals(t *testing.T) {
	ctx := context.Background()
	e := newMoveEnv(t, ctx)
	before := snapshot(t, ctx, e.admin)
	next := dial(t, ctx, e.nextURI)
	if _, err := Move(ctx, next, Options{Schemas: []string{"river"}, From: e.old, To: e.next, Mode: Apply}); err == nil || !strings.Contains(err.Error(), "superuser") {
		t.Fatalf("a NOSUPERUSER login ran the move: %v", err)
	}
	for name, options := range map[string]Options{
		"public without -database-owner": {Schemas: []string{"public"}, From: e.old, To: e.next, Mode: Apply},
		"a superuser new owner":          {Schemas: []string{"river"}, From: e.next, To: e.old, Mode: Apply},
		"a missing schema":               {Schemas: []string{"nosuch"}, From: e.old, To: e.next, Mode: Apply},
		"a missing role":                 {Schemas: []string{"river"}, From: e.old, To: "omv_nobody", Mode: Apply},
		"a schema of another owner":      {Schemas: []string{"river"}, From: e.rt1, To: e.next, Mode: Apply},
	} {
		if _, err := Move(ctx, e.admin, options); !errors.Is(err, ErrOwnerMove) {
			t.Errorf("%s: Move = %v, want ErrOwnerMove", name, err)
		}
	}
	old := dial(t, ctx, e.oldURI)
	exec(t, ctx, old, `CREATE OPERATOR river.### (LEFTARG = int, RIGHTARG = int, FUNCTION = int4pl)`)
	_, err := Move(ctx, e.admin, Options{Schemas: []string{"river"}, From: e.old, To: e.next, Mode: Apply})
	if !errors.Is(err, ErrOwnerMove) || !strings.Contains(err.Error(), "operator") {
		t.Fatalf("an operator owned by the old owner outside an extension: %v", err)
	}
	exec(t, ctx, old, `DROP OPERATOR river.### (int, int)`)
	if after := snapshot(t, ctx, e.admin); !reflect.DeepEqual(before, after) {
		t.Fatal("a refused move changed the catalog")
	}
}

func TestPin(t *testing.T) {
	ctx := context.Background()
	e := newMoveEnv(t, ctx)
	old := dial(t, ctx, e.oldURI)
	next := dial(t, ctx, e.nextURI)
	if err := VerifyPin(ctx, old, e.old, "river"); !errors.Is(err, ErrOwnerPin) || !strings.Contains(err.Error(), "SUPERUSER") {
		t.Fatalf("a superuser pinned and connected as itself: %v", err)
	}
	if err := VerifyPin(ctx, next, e.next, "river"); !errors.Is(err, ErrOwnerPin) {
		t.Fatalf("the pin before the move: %v", err)
	}
	e.move(t, ctx, Apply, e.old, e.next, false)
	if err := VerifyPin(ctx, next, e.next, "river"); err != nil {
		t.Fatalf("the pin after the move: %v", err)
	}
	if err := VerifyPin(ctx, e.admin, e.next, "river"); !errors.Is(err, ErrOwnerPin) || !strings.Contains(err.Error(), "migration login is") {
		t.Fatalf("a superuser DSN under the pin: %v", err)
	}
	if err := VerifyPin(ctx, next, "omv_nobody", "river"); !errors.Is(err, ErrOwnerPin) {
		t.Fatalf("a missing pinned role: %v", err)
	}
	for name, ddl := range map[string][]string{
		"a river table of the old owner":    {`CREATE TABLE river.stray (id int)`, `DROP TABLE river.stray`},
		"a public table of the old owner":   {`CREATE TABLE public.stray (id int)`, `DROP TABLE public.stray`},
		"a river function of the old owner": {`CREATE FUNCTION river.stray() RETURNS int LANGUAGE sql AS 'SELECT 1'`, `DROP FUNCTION river.stray()`},
		"schema river of the old owner":     {`ALTER SCHEMA river OWNER TO ` + e.old, `ALTER SCHEMA river OWNER TO ` + e.next},
		"the database of the old owner":     {`ALTER DATABASE ` + quote(e.database) + ` OWNER TO ` + e.old, `ALTER DATABASE ` + quote(e.database) + ` OWNER TO ` + e.next},
	} {
		t.Run(name, func(t *testing.T) {
			exec(t, ctx, old, ddl[0])
			err := VerifyPin(ctx, next, e.next, "river")
			exec(t, ctx, old, ddl[1])
			if !errors.Is(err, ErrOwnerPin) {
				t.Fatalf("VerifyPin = %v, want ErrOwnerPin", err)
			}
		})
	}
	exec(t, ctx, e.admin, `ALTER ROLE `+e.next+` CREATEDB`)
	err := VerifyPin(ctx, next, e.next, "river")
	exec(t, ctx, e.admin, `ALTER ROLE `+e.next+` NOCREATEDB`)
	if !errors.Is(err, ErrOwnerPin) || !strings.Contains(err.Error(), "CREATEDB") {
		t.Fatalf("a CREATEDB pinned role: %v", err)
	}
	if err := VerifyPin(ctx, next, e.next, "river"); err != nil {
		t.Fatalf("the pin after the probes: %v", err)
	}
}
