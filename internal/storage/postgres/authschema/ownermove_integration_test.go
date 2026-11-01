//go:build integration

package authschema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// ownerMoveEnv is the prod shape before the move: a SUPERUSER role that owns the auth schema, every auth object and
// the ops tables (the role `devhealth`), a NOSUPERUSER migrator with no CREATE on the database, the runtime role
// holding its public grants from the superuser, and the lineage applied up to the version before the last.
type ownerMoveEnv struct {
	instance                                       *containers.Instance
	admin, old, migrator, runtime                  *pgxpool.Pool
	oldRole, migratorRole, runtimeRole, writerRole string
	schema                                         string
	lastVersion                                    int
}

// everyKindDDL creates one object of each kind the move handles, inside the auth schema, owned by the creator.
var everyKindDDL = []string{
	`CREATE VIEW auth.zz_view AS SELECT 1 AS one`,
	`CREATE MATERIALIZED VIEW auth.zz_matview AS SELECT 1 AS one`,
	`CREATE SEQUENCE auth.zz_seq`,
	`CREATE TABLE auth.zz_serial (id serial PRIMARY KEY, a int, b int)`,
	`CREATE STATISTICS auth.zz_stat (dependencies) ON a, b FROM auth.zz_serial`,
	`CREATE FUNCTION auth.zz_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'`,
	`CREATE PROCEDURE auth.zz_proc() LANGUAGE sql AS 'SELECT 1'`,
	`CREATE FUNCTION auth.zz_sfunc(int, int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 + $2'`,
	`CREATE AGGREGATE auth.zz_agg(int) (SFUNC = auth.zz_sfunc, STYPE = int)`,
	`CREATE TYPE auth.zz_enum AS ENUM ('a')`,
	`CREATE DOMAIN auth.zz_domain AS int`,
	`CREATE TYPE auth.zz_composite AS (a int)`,
	`CREATE COLLATION auth.zz_coll (provider = libc, locale = 'C')`,
	`CREATE TEXT SEARCH DICTIONARY auth.zz_tsdict (TEMPLATE = simple)`,
	`CREATE TEXT SEARCH CONFIGURATION auth.zz_tsconf (COPY = pg_catalog.english)`,
}

// ownersByRoleSQL counts, by owner role, every object in the schema of every kind an owner can differ on.
const ownersByRoleSQL = `
SELECT r.rolname::text, count(*) FROM (
  SELECT c.relowner AS o FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1
  UNION ALL SELECT p.proowner FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1
  UNION ALL SELECT t.typowner FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = $1
     AND t.typcategory <> 'A' AND (t.typtype IN ('d','e','r','m') OR (t.typtype = 'c' AND NOT EXISTS (SELECT 1 FROM pg_class k WHERE k.reltype = t.oid)))
  UNION ALL SELECT x.collowner FROM pg_collation x JOIN pg_namespace n ON n.oid = x.collnamespace WHERE n.nspname = $1
  UNION ALL SELECT x.stxowner FROM pg_statistic_ext x JOIN pg_namespace n ON n.oid = x.stxnamespace WHERE n.nspname = $1
  UNION ALL SELECT x.dictowner FROM pg_ts_dict x JOIN pg_namespace n ON n.oid = x.dictnamespace WHERE n.nspname = $1
  UNION ALL SELECT x.cfgowner FROM pg_ts_config x JOIN pg_namespace n ON n.oid = x.cfgnamespace WHERE n.nspname = $1
  UNION ALL SELECT x.oprowner FROM pg_operator x JOIN pg_namespace n ON n.oid = x.oprnamespace WHERE n.nspname = $1
  UNION ALL SELECT n.nspowner FROM pg_namespace n WHERE n.nspname = $1
) objs JOIN pg_roles r ON r.oid = objs.o GROUP BY 1 ORDER BY 1`

// aclSnapshotSQL lists every ACL entry of the auth and public schemas (relations, columns, functions, types and the
// schemas themselves) with its grantor, using the default ACL where none is stored.
const aclSnapshotSQL = `
SELECT kind, object, grantor, grantee, priv, grantable FROM (
  SELECT 'relation' AS kind, n.nspname || '.' || c.relname AS object, gr.rolname::text AS grantor,
         coalesce(ge.rolname::text, 'PUBLIC') AS grantee, a.privilege_type AS priv, a.is_grantable AS grantable
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault((CASE c.relkind WHEN 'S' THEN 's' ELSE 'r' END)::"char", c.relowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname IN ('auth','public') AND c.relkind IN ('r','p','v','m','f','S')
  UNION ALL
  SELECT 'column', n.nspname || '.' || c.relname || '.' || att.attname, gr.rolname::text,
         coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(att.attacl) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname IN ('auth','public') AND NOT att.attisdropped
  UNION ALL
  SELECT 'schema', n.nspname, gr.rolname::text, coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_namespace n CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname IN ('auth','public')
  UNION ALL
  SELECT 'function', n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', gr.rolname::text,
         coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
  CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname = 'auth'
  UNION ALL
  SELECT 'type', n.nspname || '.' || t.typname, gr.rolname::text, coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  CROSS JOIN LATERAL aclexplode(coalesce(t.typacl, acldefault('T', t.typowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname = 'auth' AND t.typcategory <> 'A'
) s ORDER BY 1, 2, 3, 4, 5, 6`

func newOwnerMoveEnv(t *testing.T, ctx context.Context, kinds bool) *ownerMoveEnv {
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
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)

	env := &ownerMoveEnv{instance: instance, admin: admin, schema: "auth"}
	for _, r := range []struct {
		prefix string
		into   *string
	}{{"om_old", &env.oldRole}, {"om_migrator", &env.migratorRole}, {"om_runtime", &env.runtimeRole}, {"om_writer", &env.writerRole}} {
		if *r.into, err = containers.RoleName(r.prefix, instance); err != nil {
			t.Fatalf("derive role %s: %v", r.prefix, err)
		}
	}
	database, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatalf("read database name: %v", err)
	}
	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN SUPERUSER PASSWORD '%s'", Quote(mustValidatedIdentifier(env.oldRole)), fixturePassword),
		fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '%s'", Quote(mustValidatedIdentifier(env.migratorRole)), fixturePassword),
		fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '%s'", Quote(mustValidatedIdentifier(env.runtimeRole)), fixturePassword),
		fmt.Sprintf("CREATE ROLE %s NOLOGIN", Quote(mustValidatedIdentifier(env.writerRole))),
		fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s, %s", Quote(mustValidatedIdentifier(database)), Quote(mustValidatedIdentifier(env.migratorRole)), Quote(mustValidatedIdentifier(env.runtimeRole))),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		for _, role := range []string{env.runtimeRole, env.migratorRole, env.writerRole, env.oldRole} {
			containers.DropRole(admin, role, t.Logf)
		}
	})

	conn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatalf("connect for the ops schema: %v", err)
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("apply the ops schema: %v", err)
	}
	owned := []string{LegacyRefreshTokenGrant().Table, "email_verification_tokens", "password_reset_tokens", publicAuditTable, "memberships"}
	for table := range publicReadOracle {
		owned = append(owned, table)
	}
	for _, grant := range InviteWriteGrants() {
		owned = append(owned, grant.Table)
	}
	for _, table := range owned {
		if _, err := conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE public.%s OWNER TO %s`, Quote(mustValidatedIdentifier(table)), Quote(mustValidatedIdentifier(env.oldRole)))); err != nil {
			t.Fatalf("hand public.%s to the superuser owner: %v", table, err)
		}
	}
	_ = conn.Close(ctx)

	env.old = env.pool(t, ctx, env.oldRole)
	env.migrator = env.pool(t, ctx, env.migratorRole)
	env.runtime = env.pool(t, ctx, env.runtimeRole)

	options := env.options(false)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	env.lastVersion = migrations[len(migrations)-1].Version
	oldConn, err := env.old.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer oldConn.Release()
	if err := ensureSchemaAndVersionTable(ctx, oldConn.Conn(), options); err != nil {
		t.Fatalf("ensure schema as the superuser owner: %v", err)
	}
	for _, m := range migrations[:len(migrations)-1] {
		if err := applyOne(ctx, oldConn.Conn(), options, m); err != nil {
			t.Fatalf("apply %04d_%s as the superuser owner: %v", m.Version, m.Name, err)
		}
	}
	if kinds {
		for _, statement := range everyKindDDL {
			if _, err := oldConn.Exec(ctx, statement); err != nil {
				t.Fatalf("seed %q: %v", statement, err)
			}
		}
		if _, err := oldConn.Exec(ctx, `CREATE FUNCTION auth.zz_definer() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS 'SELECT 1'`); err != nil {
			t.Fatalf("seed the definer function: %v", err)
		}
		if _, err := oldConn.Exec(ctx, fmt.Sprintf(`ALTER FUNCTION auth.zz_definer() OWNER TO %s`, Quote(mustValidatedIdentifier(env.writerRole)))); err != nil {
			t.Fatalf("hand the definer function to the writer: %v", err)
		}
	}
	if err := ApplyRuntimeGrants(ctx, oldConn.Conn(), env.options(true)); err != nil {
		t.Fatalf("runtime grants as the superuser owner: %v", err)
	}
	return env
}

func (e *ownerMoveEnv) pool(t *testing.T, ctx context.Context, role string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, fixtureDSN(t, e.instance.URI, role))
	if err != nil {
		t.Fatalf("pool for %s: %v", role, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func (e *ownerMoveEnv) options(usersOwned bool) Options {
	return Options{Schema: e.schema, RuntimeRole: e.runtimeRole, UsersOwned: usersOwned}
}

func (e *ownerMoveEnv) moveOptions(apply bool) OwnerMoveOptions {
	return OwnerMoveOptions{Schema: e.schema, From: e.oldRole, To: e.migratorRole, RuntimeRole: e.runtimeRole, Apply: apply}
}

func (e *ownerMoveEnv) reverseOptions(apply bool) OwnerMoveOptions {
	return OwnerMoveOptions{Schema: e.schema, From: e.migratorRole, To: e.oldRole, RuntimeRole: e.runtimeRole, Apply: apply, Restore: true}
}

func (e *ownerMoveEnv) owners(t *testing.T, ctx context.Context) map[string]int {
	t.Helper()
	rows, err := e.admin.Query(ctx, ownersByRoleSQL, e.schema)
	if err != nil {
		t.Fatalf("read owners: %v", err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var role string
		var n int
		if err := rows.Scan(&role, &n); err != nil {
			t.Fatal(err)
		}
		counts[role] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return counts
}

func (e *ownerMoveEnv) aclSnapshot(t *testing.T, ctx context.Context) []string {
	t.Helper()
	rows, err := e.admin.Query(ctx, aclSnapshotSQL)
	if err != nil {
		t.Fatalf("read ACLs: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var kind, object, grantor, grantee, priv string
		var grantable bool
		if err := rows.Scan(&kind, &object, &grantor, &grantee, &priv, &grantable); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s|%s|%s>%s|%s|%t", kind, object, grantor, grantee, priv, grantable))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// runtimeReadsUsers is the read go-api's login flow needs from the runtime role: a manifest column of public.users.
func (e *ownerMoveEnv) runtimeReadsUsers(ctx context.Context) error {
	var n int
	return e.runtime.QueryRow(ctx, `SELECT count(*) FROM (SELECT email FROM public.users LIMIT 1) s`).Scan(&n)
}

func (e *ownerMoveEnv) move(t *testing.T, ctx context.Context, o OwnerMoveOptions) OwnerMoveReport {
	t.Helper()
	report, err := OwnerMove(ctx, e.admin, o)
	if err != nil {
		t.Fatalf("OwnerMove %s -> %s (apply=%t): %v", o.From, o.To, o.Apply, err)
	}
	return report
}

func requireOnlyOwner(t *testing.T, owners map[string]int, allowed ...string) {
	t.Helper()
	for role, n := range owners {
		if !slices.Contains(allowed, role) {
			t.Errorf("%d object(s) are owned by %s; only %v may own any", n, role, allowed)
		}
	}
}

func diffLines(a, b []string) (onlyA, onlyB []string) {
	inA, inB := map[string]bool{}, map[string]bool{}
	for _, s := range a {
		inA[s] = true
	}
	for _, s := range b {
		inB[s] = true
	}
	for _, s := range a {
		if !inB[s] {
			onlyA = append(onlyA, s)
		}
	}
	for _, s := range b {
		if !inA[s] {
			onlyB = append(onlyB, s)
		}
	}
	return onlyA, onlyB
}

// R2 (upgrade): a superuser-owned database at the version before the last moves to the migrator, the last migration
// then applies AS the migrator, and the posture verify and the owner pin pass.
func TestOwnerMoveUpgradeFromSuperuserOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, true)

	before := env.owners(t, ctx)
	if before[env.oldRole] == 0 {
		t.Fatalf("the fixture is not superuser-owned: %v", before)
	}
	report := env.move(t, ctx, env.moveOptions(true))
	if !report.Applied {
		t.Error("the report says nothing was applied")
	}
	after := env.owners(t, ctx)
	requireOnlyOwner(t, after, env.migratorRole, env.writerRole)
	if after[env.writerRole] != 1 {
		t.Errorf("the writer-owned definer function moved or vanished: owners = %v", after)
	}
	for kind, want := range map[string]int{"table": 1, "view": 1, "materialized view": 1, "sequence": 1, "function": 1, "procedure": 1, "aggregate": 1, "type": 1, "domain": 1, "collation": 1, "statistics": 1, "text search dictionary": 1, "text search configuration": 1} {
		if report.Moved[kind] < want {
			t.Errorf("report.Moved[%q] = %d, want at least %d (%v)", kind, report.Moved[kind], want, report.Moved)
		}
	}
	var superuser bool
	if err := env.admin.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = $1`, env.migratorRole).Scan(&superuser); err != nil || superuser {
		t.Fatalf("the new owner is a superuser (%v)", err)
	}

	options := env.options(true)
	result, err := Apply(ctx, env.migrator, options)
	if err != nil {
		t.Fatalf("Apply as the migrator after the move: %v", err)
	}
	if result.CurrentVersion != env.lastVersion || len(result.AppliedVersions) != 1 {
		t.Errorf("Apply result = %+v, want exactly version %d applied", result, env.lastVersion)
	}
	current, head, err := Check(ctx, env.migrator, env.schema)
	if err != nil || current != head {
		t.Errorf("Check = %d/%d, %v", current, head, err)
	}
	if err := env.runtimeReadsUsers(ctx); err != nil {
		t.Errorf("the runtime role cannot read public.users after the move and the grants pass: %v", err)
	}
	if _, err := Grants(ctx, env.migrator, options); err != nil {
		t.Errorf("the grants pass as the migrator: %v", err)
	}
}

// A dry run reports and changes nothing: no owner, no ACL entry.
func TestOwnerMoveDryRunChangesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, true)
	ownersBefore, aclBefore := env.owners(t, ctx), env.aclSnapshot(t, ctx)
	report := env.move(t, ctx, env.moveOptions(false))
	if report.Applied {
		t.Error("a dry run says it applied")
	}
	if len(report.Moved) == 0 {
		t.Error("a dry run reports nothing that would move")
	}
	if fmt.Sprint(env.owners(t, ctx)) != fmt.Sprint(ownersBefore) {
		t.Error("a dry run changed owners")
	}
	if onlyA, onlyB := diffLines(aclBefore, env.aclSnapshot(t, ctx)); len(onlyA)+len(onlyB) > 0 {
		t.Errorf("a dry run changed ACL entries: lost %v, gained %v", onlyA, onlyB)
	}
}

// R3: a second run moves nothing and rewrites no ACL entry.
func TestOwnerMoveIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, true)
	env.move(t, ctx, env.moveOptions(true))
	aclAfterFirst := env.aclSnapshot(t, ctx)
	second := env.move(t, ctx, env.moveOptions(true))
	for kind, n := range second.Moved {
		if n != 0 {
			t.Errorf("a second run moved %d %s object(s)", n, kind)
		}
	}
	if second.GrantRowsMoved != 0 {
		t.Errorf("a second run moved %d grant rows", second.GrantRowsMoved)
	}
	if onlyA, onlyB := diffLines(aclAfterFirst, env.aclSnapshot(t, ctx)); len(onlyA)+len(onlyB) > 0 {
		t.Errorf("a second run changed ACL entries: lost %v, gained %v", onlyA, onlyB)
	}
}

// C2: the reverse run restores the owners AND every ACL entry (grantor, grantee, privilege, option) of the auth and
// public schemas, compared in full before the move and after the reverse.
func TestOwnerMoveReverseRestoresOwnersAndEveryACLRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, true)
	ownersBefore, aclBefore := env.owners(t, ctx), env.aclSnapshot(t, ctx)
	env.move(t, ctx, env.moveOptions(true))
	if onlyA, onlyB := diffLines(aclBefore, env.aclSnapshot(t, ctx)); len(onlyA)+len(onlyB) == 0 {
		t.Fatal("the forward move changed no ACL entry; the reverse comparison proves nothing")
	}
	env.move(t, ctx, env.reverseOptions(true))
	if fmt.Sprint(env.owners(t, ctx)) != fmt.Sprint(ownersBefore) {
		t.Errorf("owners after the reverse = %v, before = %v", env.owners(t, ctx), ownersBefore)
	}
	if onlyA, onlyB := diffLines(aclBefore, env.aclSnapshot(t, ctx)); len(onlyA)+len(onlyB) > 0 {
		t.Errorf("the reverse run did not restore the ACL: lost %v, gained %v", onlyA, onlyB)
	}
}

// R4: after the move every public privilege of the runtime role has the migrator as grantor and none has the old
// owner; the migrator holds, with grant option, exactly PublicPrivilegeSet (both ways).
func TestOwnerMoveGrantorsAndTheMigratorsOwnPrivilegeSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))

	grantors := map[string]int{}
	for _, line := range env.aclSnapshot(t, ctx) {
		parts := strings.Split(line, "|")
		if !strings.HasPrefix(parts[1], "public.") {
			continue
		}
		pair := strings.SplitN(parts[2], ">", 2)
		if pair[1] == env.runtimeRole {
			grantors[pair[0]]++
		}
	}
	if grantors[env.migratorRole] == 0 {
		t.Errorf("no public privilege of the runtime role has the migrator as grantor: %v", grantors)
	}
	if n := grantors[env.oldRole]; n != 0 {
		t.Errorf("%d public privilege rows of the runtime role still have the old owner as grantor", n)
	}

	have := map[string]bool{}
	rows, err := env.admin.Query(ctx, `
		SELECT c.relname::text, a.privilege_type, '' AS col, a.is_grantable
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL aclexplode(c.relacl) a JOIN pg_roles g ON g.oid = a.grantee
		WHERE n.nspname = 'public' AND g.rolname = $1
		UNION ALL
		SELECT c.relname::text, a.privilege_type, att.attname::text, a.is_grantable
		FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL aclexplode(att.attacl) a JOIN pg_roles g ON g.oid = a.grantee
		WHERE n.nspname = 'public' AND g.rolname = $1 AND NOT att.attisdropped`, env.migratorRole)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var table, priv, col string
		var grantable bool
		if err := rows.Scan(&table, &priv, &col, &grantable); err != nil {
			t.Fatal(err)
		}
		if !grantable && priv != PublicAnchorPrivilege {
			t.Errorf("the migrator holds %s %s %s without grant option", table, priv, col)
		}
		have[table+"|"+priv+"|"+col] = true
	}
	want := map[string]bool{}
	for _, table := range PublicAnchorTables() {
		var exists bool
		if err := env.admin.QueryRow(ctx, `SELECT to_regclass('public.' || quote_ident($1)) IS NOT NULL`, table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			want[table+"|"+PublicAnchorPrivilege+"|"] = true
		}
	}
	for key := range keysOfPrivilegeSet(PublicPrivilegeSet()) {
		parts := strings.Split(key, "|")
		var exists bool
		if parts[2] == "" {
			err = env.admin.QueryRow(ctx, `SELECT to_regclass('public.' || quote_ident($1)) IS NOT NULL`, parts[0]).Scan(&exists)
		} else {
			err = env.admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2)`, parts[0], parts[2]).Scan(&exists)
		}
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			want[key] = true
		}
	}
	if missing := sortedMissing(have, want); len(missing) > 0 {
		t.Errorf("the migrator lacks %d privileges of the set: %v", len(missing), missing)
	}
	if extra := sortedMissing(want, have); len(extra) > 0 {
		t.Errorf("the migrator holds %d privileges outside the set: %v", len(extra), extra)
	}
}

// C1: a session that holds only the runtime role reads a manifest column while the verb is held before its commit
// and again right after it; there is no moment without the grant.
func TestOwnerMoveLeavesNoGapForARuntimeReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	if err := env.runtimeReadsUsers(ctx); err != nil {
		t.Fatalf("the runtime role cannot read before the move: %v", err)
	}
	var mu sync.Mutex
	var seen []string
	check := func(point string) func(context.Context) {
		return func(hookCtx context.Context) {
			err := env.runtimeReadsUsers(hookCtx)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Errorf("the runtime reader failed %s: %v", point, err)
			}
			seen = append(seen, point)
		}
	}
	o := env.moveOptions(true)
	o.hookBeforeCommit, o.hookAfterCommit = check("before the commit"), check("after the commit")
	env.move(t, ctx, o)
	if len(seen) != 2 {
		t.Fatalf("the hooks ran at %v, want both points", seen)
	}
}

// Refusals: nothing changes.
func TestOwnerMoveRefusals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	ownersBefore, aclBefore := env.owners(t, ctx), env.aclSnapshot(t, ctx)
	unchanged := func(t *testing.T) {
		t.Helper()
		if fmt.Sprint(env.owners(t, ctx)) != fmt.Sprint(ownersBefore) {
			t.Error("a refused run changed owners")
		}
		if onlyA, onlyB := diffLines(aclBefore, env.aclSnapshot(t, ctx)); len(onlyA)+len(onlyB) > 0 {
			t.Errorf("a refused run changed ACL entries: lost %v, gained %v", onlyA, onlyB)
		}
	}
	refused := func(t *testing.T, pool *pgxpool.Pool, o OwnerMoveOptions) {
		t.Helper()
		_, err := OwnerMove(ctx, pool, o)
		if !errors.Is(err, ErrOwnerMove) {
			t.Fatalf("error = %v, want ErrOwnerMove", err)
		}
	}
	t.Run("the caller is not a superuser", func(t *testing.T) {
		refused(t, env.migrator, env.moveOptions(true))
		unchanged(t)
	})
	t.Run("the new owner is a superuser", func(t *testing.T) {
		if _, err := env.admin.Exec(ctx, fmt.Sprintf(`ALTER ROLE %s SUPERUSER`, Quote(mustValidatedIdentifier(env.migratorRole)))); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := env.admin.Exec(ctx, fmt.Sprintf(`ALTER ROLE %s NOSUPERUSER`, Quote(mustValidatedIdentifier(env.migratorRole)))); err != nil {
				t.Fatal(err)
			}
		}()
		refused(t, env.admin, env.moveOptions(true))
		unchanged(t)
	})
	t.Run("the new owner can create roles", func(t *testing.T) {
		if _, err := env.admin.Exec(ctx, fmt.Sprintf(`ALTER ROLE %s CREATEROLE`, Quote(mustValidatedIdentifier(env.migratorRole)))); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := env.admin.Exec(ctx, fmt.Sprintf(`ALTER ROLE %s NOCREATEROLE`, Quote(mustValidatedIdentifier(env.migratorRole)))); err != nil {
				t.Fatal(err)
			}
		}()
		refused(t, env.admin, env.moveOptions(true))
		unchanged(t)
	})
	t.Run("the new owner cannot log in", func(t *testing.T) {
		o := env.moveOptions(true)
		o.To = env.writerRole
		refused(t, env.admin, o)
		unchanged(t)
	})
	t.Run("the old owner does not own the schema", func(t *testing.T) {
		o := env.moveOptions(true)
		o.From = env.writerRole
		refused(t, env.admin, o)
		unchanged(t)
	})
	t.Run("an object kind the move cannot carry", func(t *testing.T) {
		for _, statement := range []string{
			`CREATE FUNCTION auth.zz_opfn(int, int) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT $1 = $2'`,
			`CREATE OPERATOR auth.=== (LEFTARG = int, RIGHTARG = int, FUNCTION = auth.zz_opfn)`,
		} {
			if _, err := env.old.Exec(ctx, statement); err != nil {
				t.Fatalf("seed %q: %v", statement, err)
			}
		}
		ownersBefore, aclBefore = env.owners(t, ctx), env.aclSnapshot(t, ctx)
		_, err := OwnerMove(ctx, env.admin, env.moveOptions(true))
		if !errors.Is(err, ErrOwnerMove) || !strings.Contains(fmt.Sprint(err), "operator") {
			t.Fatalf("error = %v, want ErrOwnerMove naming the operator kind", err)
		}
		unchanged(t)
	})
}

// R6: the owner pin names the object kind and the remedy; unset it checks nothing; a pinned owner that is a
// superuser or can create roles is refused; a restore that brings the old owner back fails the pin.
func TestSchemaOwnerPin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, true)
	// The definer function of the writer is not under test here; hand it to the owner so the pin has one rule.
	if _, err := env.admin.Exec(ctx, fmt.Sprintf(`ALTER FUNCTION auth.zz_definer() OWNER TO %s`, Quote(mustValidatedIdentifier(env.oldRole)))); err != nil {
		t.Fatal(err)
	}
	env.move(t, ctx, env.moveOptions(true))
	pinned := env.options(true)
	pinned.SchemaOwnerRole = env.migratorRole
	if _, err := Apply(ctx, env.migrator, pinned); err != nil {
		t.Fatalf("Apply under the pin after the move: %v", err)
	}
	if _, err := Grants(ctx, env.migrator, pinned); err != nil {
		t.Fatalf("Grants under the pin after the move: %v", err)
	}

	leftovers := map[string]string{
		"table":                  `ALTER TABLE auth.zz_serial OWNER TO %s`,
		"view":                   `ALTER VIEW auth.zz_view OWNER TO %s`,
		"materialized view":      `ALTER MATERIALIZED VIEW auth.zz_matview OWNER TO %s`,
		"sequence":               `ALTER SEQUENCE auth.zz_seq OWNER TO %s`,
		"function":               `ALTER FUNCTION auth.zz_fn() OWNER TO %s`,
		"procedure":              `ALTER PROCEDURE auth.zz_proc() OWNER TO %s`,
		"aggregate":              `ALTER AGGREGATE auth.zz_agg(int) OWNER TO %s`,
		"type":                   `ALTER TYPE auth.zz_enum OWNER TO %s`,
		"domain":                 `ALTER DOMAIN auth.zz_domain OWNER TO %s`,
		"collation":              `ALTER COLLATION auth.zz_coll OWNER TO %s`,
		"statistics":             `ALTER STATISTICS auth.zz_stat OWNER TO %s`,
		"text search dictionary": `ALTER TEXT SEARCH DICTIONARY auth.zz_tsdict OWNER TO %s`,
		"schema":                 `ALTER SCHEMA auth OWNER TO %s`,
	}
	kinds := make([]string, 0, len(leftovers))
	for kind := range leftovers {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		t.Run("a leftover "+kind, func(t *testing.T) {
			statement := fmt.Sprintf(leftovers[kind], Quote(mustValidatedIdentifier(env.oldRole)))
			if _, err := env.admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
			defer func() {
				back := fmt.Sprintf(leftovers[kind], Quote(mustValidatedIdentifier(env.migratorRole)))
				if _, err := env.admin.Exec(ctx, back); err != nil {
					t.Fatalf("restore %s: %v", back, err)
				}
			}()
			_, err := Grants(ctx, env.admin, pinned)
			if !errors.Is(err, ErrSchemaOwnerPin) {
				t.Fatalf("error = %v, want ErrSchemaOwnerPin", err)
			}
			if !strings.Contains(err.Error(), kind) || !strings.Contains(err.Error(), "owner-move") {
				t.Errorf("the refusal %q must name the kind %q and the remedy owner-move", err, kind)
			}
			unpinned := pinned
			unpinned.SchemaOwnerRole = ""
			if _, err := Grants(ctx, env.admin, unpinned); err != nil && errors.Is(err, ErrSchemaOwnerPin) {
				t.Errorf("an unset pin still checks owners: %v", err)
			}
		})
	}
	for _, attribute := range []string{"SUPERUSER", "CREATEROLE", "REPLICATION", "BYPASSRLS"} {
		t.Run("a pinned owner with "+attribute, func(t *testing.T) {
			role := Quote(mustValidatedIdentifier(env.migratorRole))
			if _, err := env.admin.Exec(ctx, "ALTER ROLE "+role+" "+attribute); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := env.admin.Exec(ctx, "ALTER ROLE "+role+" NO"+attribute); err != nil {
					t.Fatal(err)
				}
			}()
			_, err := Grants(ctx, env.admin, pinned)
			if !errors.Is(err, ErrSchemaOwnerPin) {
				t.Fatalf("error = %v, want ErrSchemaOwnerPin", err)
			}
		})
	}
}

// C3: after the move, the grants pass on the OLD login leaves the migrator-granted rows; the reverse run is what
// returns them. This states why the rollback is not optional.
func TestRollbackToTheOldLoginNeedsTheReverseMove(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	if _, err := Apply(ctx, env.migrator, env.options(true)); err != nil {
		t.Fatalf("Apply as the migrator: %v", err)
	}
	var held bool
	probe := `SELECT has_column_privilege($1, 'public.users', 'email', 'INSERT')`
	if err := env.admin.QueryRow(ctx, probe, env.runtimeRole).Scan(&held); err != nil || !held {
		t.Fatalf("the runtime role lacks INSERT (email) on public.users before the narrowing (%v)", err)
	}
	// users-owned off narrows the runtime role; on the old login the REVOKE reaches only rows the old login granted.
	narrow := env.options(false)
	_, _ = Grants(ctx, env.old, narrow)
	if err := env.admin.QueryRow(ctx, probe, env.runtimeRole).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if !held {
		t.Error("the narrowing on the old login removed a migrator-granted row; the cell cannot show why the reverse move is needed")
	}
	env.move(t, ctx, env.reverseOptions(true))
	for _, line := range env.aclSnapshot(t, ctx) {
		parts := strings.Split(line, "|")
		if strings.HasPrefix(parts[1], "public.") && strings.Contains(parts[2], env.migratorRole+">") {
			t.Errorf("after the reverse move a public ACL row still has the migrator as grantor: %s", line)
		}
	}
}

// R1/R7: a fresh install. The schema is created for the migrator by a privileged step; the migrator has no CREATE on
// the database; Apply reaches head under the pin.
func TestApplyAsTheOwnerOfAPrecreatedSchemaWithoutDatabaseCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	migratorRole, err := containers.RoleName("om_fresh_migrator", instance)
	if err != nil {
		t.Fatal(err)
	}
	runtimeRole, err := containers.RoleName("om_fresh_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	database, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '%s'", Quote(mustValidatedIdentifier(migratorRole)), fixturePassword),
		fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '%s'", Quote(mustValidatedIdentifier(runtimeRole)), fixturePassword),
		fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s, %s", Quote(mustValidatedIdentifier(database)), Quote(mustValidatedIdentifier(migratorRole)), Quote(mustValidatedIdentifier(runtimeRole))),
		fmt.Sprintf("REVOKE CREATE ON DATABASE %s FROM PUBLIC", Quote(mustValidatedIdentifier(database))),
		fmt.Sprintf("CREATE SCHEMA auth AUTHORIZATION %s", Quote(mustValidatedIdentifier(migratorRole))),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		containers.DropRole(admin, runtimeRole, t.Logf)
		containers.DropRole(admin, migratorRole, t.Logf)
	})
	var canCreate bool
	if err := admin.QueryRow(ctx, `SELECT has_database_privilege($1, current_database(), 'CREATE')`, migratorRole).Scan(&canCreate); err != nil || canCreate {
		t.Fatalf("the migrator can create schemas in the database (%v); the cell proves nothing", err)
	}
	pool, err := pgxpool.New(ctx, fixtureDSN(t, instance.URI, migratorRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	result, err := Apply(ctx, pool, Options{Schema: "auth", RuntimeRole: runtimeRole, SchemaOwnerRole: migratorRole})
	if err != nil {
		t.Fatalf("Apply as the owner of a precreated schema: %v", err)
	}
	current, head, err := Check(ctx, pool, "auth")
	if err != nil || current != head || result.CurrentVersion != head {
		t.Errorf("Check = %d/%d (%v), Apply = %+v", current, head, err, result)
	}
}

// R5: the migrator, as the new table owner, still passes the platform owner guard of migration 0038; the runtime
// role does not.
func TestMigratorKeepsThePlatformOwnerRowRightAfterTheMove(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	if _, err := Apply(ctx, env.migrator, env.options(true)); err != nil {
		t.Fatalf("Apply as the migrator: %v", err)
	}
	const holder = "00000000-0000-4000-8000-000000103521"
	if _, err := env.migrator.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'owner holder')`, holder); err != nil {
		t.Fatalf("seed a principal as the migrator: %v", err)
	}
	if _, err := env.migrator.Exec(ctx, `INSERT INTO auth.platform_role_assignments (principal_id, role_key) VALUES ($1, 'platform_owner')`, holder); err != nil {
		t.Errorf("a platform_owner row written by the new owner is refused: %v", err)
	}
	if _, err := env.runtime.Exec(ctx, `INSERT INTO auth.platform_role_assignments (principal_id, role_key) VALUES ($1, 'platform_owner')`, holder); err == nil {
		t.Error("the runtime role wrote a platform_owner row")
	}
}

// Q5: a widened manifest that the owner has not yet passed to the migrator fails the grants pass with the privilege
// named and no partial grant; `GrantsOnly` as the superuser is the one step that fixes it.
func TestWidenedManifestFailsClosedUntilGrantsOnlyRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	options := env.options(true)
	if _, err := Apply(ctx, env.migrator, options); err != nil {
		t.Fatalf("Apply as the migrator: %v", err)
	}
	if _, err := env.old.Exec(ctx, fmt.Sprintf(`REVOKE UPDATE (last_login_at) ON TABLE public.users FROM %s CASCADE`, Quote(mustValidatedIdentifier(env.migratorRole)))); err != nil {
		t.Fatalf("simulate a widened manifest: %v", err)
	}
	aclBefore := env.aclSnapshot(t, ctx)
	_, err := Grants(ctx, env.migrator, options)
	if err == nil || !strings.Contains(err.Error(), "UPDATE") || !strings.Contains(err.Error(), "last_login_at") {
		t.Fatalf("the grants pass error = %v, want one naming UPDATE (last_login_at)", err)
	}
	if onlyA, onlyB := diffLines(aclBefore, env.aclSnapshot(t, ctx)); len(onlyA)+len(onlyB) > 0 {
		t.Errorf("a failed grants pass left a partial grant: lost %v, gained %v", onlyA, onlyB)
	}
	o := env.moveOptions(true)
	o.GrantsOnly = true
	env.move(t, ctx, o)
	if _, err := Grants(ctx, env.migrator, options); err != nil {
		t.Errorf("the grants pass after GrantsOnly: %v", err)
	}
	gave := false
	for _, line := range env.aclSnapshot(t, ctx) {
		parts := strings.Split(line, "|")
		if parts[1] == "public.users.last_login_at" && strings.HasSuffix(parts[2], ">"+env.migratorRole) && parts[3] == "UPDATE" && parts[4] == "true" {
			gave = true
		}
	}
	if !gave {
		t.Error("GrantsOnly did not give the migrator UPDATE (last_login_at) WITH GRANT OPTION")
	}
}

// The pin has one exception: the append function of the External Push lifecycle, owned by the configured writer role.
// Any other object the writer owns, and the same function without the writer configured, fail the pin.
func TestSchemaOwnerPinExceptsOnlyThePushLifecycleFunctionOfTheWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	if _, err := Apply(ctx, env.migrator, env.options(true)); err != nil {
		t.Fatalf("Apply as the migrator: %v", err)
	}
	const function = "auth." + PushLifecycleFunction + PushLifecycleFunctionSignature
	if _, err := env.admin.Exec(ctx, "ALTER FUNCTION "+function+" OWNER TO "+Quote(mustValidatedIdentifier(env.writerRole))); err != nil {
		t.Fatalf("hand the append function to the writer: %v", err)
	}
	conn, err := env.admin.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	pinned := env.options(true)
	pinned.SchemaOwnerRole = env.migratorRole

	pinned.PushLifecycleWriterRole = env.writerRole
	if err := verifySchemaOwnerPin(ctx, conn.Conn(), pinned); err != nil {
		t.Errorf("the append function owned by the configured writer fails the pin: %v", err)
	}
	other := pinned
	other.PushLifecycleWriterRole = ""
	if err := verifySchemaOwnerPin(ctx, conn.Conn(), other); !errors.Is(err, ErrSchemaOwnerPin) || !strings.Contains(err.Error(), "function") {
		t.Errorf("without a configured writer the append function passes the pin: %v", err)
	}
	if _, err := env.admin.Exec(ctx, `CREATE FUNCTION auth.zz_other() RETURNS int LANGUAGE sql AS 'SELECT 1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := env.admin.Exec(ctx, fmt.Sprintf(`ALTER FUNCTION auth.zz_other() OWNER TO %s`, Quote(mustValidatedIdentifier(env.writerRole)))); err != nil {
		t.Fatal(err)
	}
	if err := verifySchemaOwnerPin(ctx, conn.Conn(), pinned); !errors.Is(err, ErrSchemaOwnerPin) {
		t.Errorf("another function owned by the writer passes the pin: %v", err)
	}
}

// A restore from a backup taken before the move brings the old owner back: the pin fails and names the remedy, and
// the owner move run again makes the pin pass.
func TestPinFailsAfterARestoreBringsTheOldOwnerBackAndOwnerMoveFixesIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	pinned := env.options(true)
	pinned.SchemaOwnerRole = env.migratorRole
	if _, err := Apply(ctx, env.migrator, pinned); err != nil {
		t.Fatalf("Apply under the pin after the move: %v", err)
	}
	env.move(t, ctx, env.reverseOptions(true))
	_, err := Grants(ctx, env.admin, pinned)
	if !errors.Is(err, ErrSchemaOwnerPin) || !strings.Contains(err.Error(), "owner-move") {
		t.Fatalf("after the old owner came back the pin gave %v, want ErrSchemaOwnerPin naming owner-move", err)
	}
	env.move(t, ctx, env.moveOptions(true))
	if _, err := Grants(ctx, env.migrator, pinned); err != nil {
		t.Errorf("the grants pass under the pin after the owner move was run again: %v", err)
	}
}
