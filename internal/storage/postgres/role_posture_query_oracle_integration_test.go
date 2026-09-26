//go:build integration

package postgres

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
)

// CHAOS-6937 differential oracle. The rewritten rolePostureQuery reads
// pg_class.relacl / pg_attribute.attacl / pg_proc.proacl through aclexplode,
// scoped to the two governed schemas, instead of calling has_*_privilege on
// every catalog row and reading information_schema.column_privileges. The two
// are only interchangeable if they AGREE, so this runs the frozen pre-rewrite
// query (legacyRolePostureQuery) and the production one on the same live
// server, as the role under test, through the production argument
// construction, across a grant matrix that touches every predicate. Both
// answers are also checked against the answer the scenario is built to give,
// so a scenario that no longer bites (both sides say yes to a violation)
// fails instead of passing vacuously.

type oracleScenario struct {
	name string
	// apply and revert are run as the superuser. Placeholders: {role} the
	// role being checked, {req} a required table, {reqNoIns}/{reqNoUpd}/
	// {reqNoDel} a required table for which that privilege is NOT allowed,
	// {reqIns}/{reqUpd}/{reqDel} one for which it IS, {colTable}/{colName}
	// the first column-scoped pair, {seq} a required sequence.
	apply, revert []string
	// domainWant is the answer the DOMAIN role's check must give (true =
	// posture holds).
	domainWant bool
	// needs names a placeholder the posture must be able to fill; the
	// scenario is skipped for a role whose posture cannot.
	needs []string
}

func oracleScenarios() []oracleScenario {
	return []oracleScenario{
		{name: "baseline", domainWant: true},

		// ---- table-level, required relations ----
		{name: "required SELECT revoked", needs: []string{"req"},
			apply:  []string{"REVOKE SELECT ON public.{req} FROM {role}"},
			revert: []string{"GRANT SELECT ON public.{req} TO {role}"}},
		{name: "required SELECT with grant option", needs: []string{"req"},
			apply:  []string{"GRANT SELECT ON public.{req} TO {role} WITH GRANT OPTION"},
			revert: []string{"REVOKE GRANT OPTION FOR SELECT ON public.{req} FROM {role}"}},
		{name: "unallowed INSERT", needs: []string{"reqNoIns"},
			apply:  []string{"GRANT INSERT ON public.{reqNoIns} TO {role}"},
			revert: []string{"REVOKE INSERT ON public.{reqNoIns} FROM {role}"}},
		{name: "unallowed INSERT column-level", needs: []string{"reqNoIns"},
			apply:  []string{"GRANT INSERT (id) ON public.{reqNoIns} TO {role}"},
			revert: []string{"REVOKE INSERT (id) ON public.{reqNoIns} FROM {role}"}},
		{name: "allowed INSERT revoked", needs: []string{"reqIns"},
			apply:  []string{"REVOKE INSERT ON public.{reqIns} FROM {role}"},
			revert: []string{"GRANT INSERT ON public.{reqIns} TO {role}"}},
		{name: "allowed INSERT with grant option", needs: []string{"reqIns"},
			apply:  []string{"GRANT INSERT ON public.{reqIns} TO {role} WITH GRANT OPTION"},
			revert: []string{"REVOKE GRANT OPTION FOR INSERT ON public.{reqIns} FROM {role}"}},
		{name: "allowed INSERT column grant option", needs: []string{"reqIns"},
			apply:  []string{"GRANT INSERT (id) ON public.{reqIns} TO {role} WITH GRANT OPTION"},
			revert: []string{"REVOKE INSERT (id) ON public.{reqIns} FROM {role}"}},
		{name: "unallowed UPDATE", needs: []string{"reqNoUpd"},
			apply:  []string{"GRANT UPDATE ON public.{reqNoUpd} TO {role}"},
			revert: []string{"REVOKE UPDATE ON public.{reqNoUpd} FROM {role}"}},
		{name: "unallowed UPDATE column-level", needs: []string{"reqNoUpd"},
			apply:  []string{"GRANT UPDATE (id) ON public.{reqNoUpd} TO {role}"},
			revert: []string{"REVOKE UPDATE (id) ON public.{reqNoUpd} FROM {role}"}},
		{name: "allowed UPDATE revoked", needs: []string{"reqUpd"},
			apply:  []string{"REVOKE UPDATE ON public.{reqUpd} FROM {role}"},
			revert: []string{"GRANT UPDATE ON public.{reqUpd} TO {role}"}},
		{name: "allowed UPDATE column grant option", needs: []string{"reqUpd"},
			apply:  []string{"GRANT UPDATE (id) ON public.{reqUpd} TO {role} WITH GRANT OPTION"},
			revert: []string{"REVOKE UPDATE (id) ON public.{reqUpd} FROM {role}"}},
		{name: "unallowed DELETE", needs: []string{"reqNoDel"},
			apply:  []string{"GRANT DELETE ON public.{reqNoDel} TO {role}"},
			revert: []string{"REVOKE DELETE ON public.{reqNoDel} FROM {role}"}},
		{name: "allowed DELETE revoked", needs: []string{"reqDel"},
			apply:  []string{"REVOKE DELETE ON public.{reqDel} FROM {role}"},
			revert: []string{"GRANT DELETE ON public.{reqDel} TO {role}"}},
		{name: "allowed DELETE with grant option", needs: []string{"reqDel"},
			apply:  []string{"GRANT DELETE ON public.{reqDel} TO {role} WITH GRANT OPTION"},
			revert: []string{"REVOKE GRANT OPTION FOR DELETE ON public.{reqDel} FROM {role}"}},
		{name: "required TRUNCATE", needs: []string{"req"},
			apply:  []string{"GRANT TRUNCATE ON public.{req} TO {role}"},
			revert: []string{"REVOKE TRUNCATE ON public.{req} FROM {role}"}},
		{name: "required REFERENCES", needs: []string{"req"},
			apply:  []string{"GRANT REFERENCES ON public.{req} TO {role}"},
			revert: []string{"REVOKE REFERENCES ON public.{req} FROM {role}"}},
		{name: "required REFERENCES column-level", needs: []string{"req"},
			apply:  []string{"GRANT REFERENCES (id) ON public.{req} TO {role}"},
			revert: []string{"REVOKE REFERENCES (id) ON public.{req} FROM {role}"}},
		{name: "required TRIGGER", needs: []string{"req"},
			apply:  []string{"GRANT TRIGGER ON public.{req} TO {role}"},
			revert: []string{"REVOKE TRIGGER ON public.{req} FROM {role}"}},
		{name: "required MAINTAIN", needs: []string{"req"},
			apply:  []string{"GRANT MAINTAIN ON public.{req} TO {role}"},
			revert: []string{"REVOKE MAINTAIN ON public.{req} FROM {role}"}},
		{name: "required SELECT column grant option", needs: []string{"req"},
			apply:  []string{"GRANT SELECT (id) ON public.{req} TO {role} WITH GRANT OPTION"},
			revert: []string{"REVOKE SELECT (id) ON public.{req} FROM {role}"}},
		{name: "required SELECT via PUBLIC only", needs: []string{"reqNoIns"},
			apply:  []string{"GRANT INSERT ON public.{reqNoIns} TO PUBLIC"},
			revert: []string{"REVOKE INSERT ON public.{reqNoIns} FROM PUBLIC"}},
		{name: "required column via PUBLIC", needs: []string{"reqNoUpd"},
			apply:  []string{"GRANT UPDATE (id) ON public.{reqNoUpd} TO PUBLIC"},
			revert: []string{"REVOKE UPDATE (id) ON public.{reqNoUpd} FROM PUBLIC"}},

		// ---- other public relations ----
		{name: "other relation SELECT", apply: []string{"GRANT SELECT ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE SELECT ON public.oracle_extra FROM {role}"}},
		{name: "other relation INSERT", apply: []string{"GRANT INSERT ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE INSERT ON public.oracle_extra FROM {role}"}},
		{name: "other relation DELETE", apply: []string{"GRANT DELETE ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE DELETE ON public.oracle_extra FROM {role}"}},
		{name: "other relation TRUNCATE", apply: []string{"GRANT TRUNCATE ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE TRUNCATE ON public.oracle_extra FROM {role}"}},
		{name: "other relation column SELECT", apply: []string{"GRANT SELECT (id) ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE SELECT (id) ON public.oracle_extra FROM {role}"}},
		{name: "other relation column REFERENCES", apply: []string{"GRANT REFERENCES (id) ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE REFERENCES (id) ON public.oracle_extra FROM {role}"}},
		{name: "other relation MAINTAIN", apply: []string{"GRANT MAINTAIN ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE MAINTAIN ON public.oracle_extra FROM {role}"}},
		{name: "other relation UPDATE", apply: []string{"GRANT UPDATE ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE UPDATE ON public.oracle_extra FROM {role}"}},
		{name: "other relation REFERENCES", apply: []string{"GRANT REFERENCES ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE REFERENCES ON public.oracle_extra FROM {role}"}},
		{name: "other relation TRIGGER", apply: []string{"GRANT TRIGGER ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE TRIGGER ON public.oracle_extra FROM {role}"}},
		{name: "other relation column INSERT", apply: []string{"GRANT INSERT (id) ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE INSERT (id) ON public.oracle_extra FROM {role}"}},
		{name: "other relation column UPDATE", apply: []string{"GRANT UPDATE (id) ON public.oracle_extra TO {role}"},
			revert: []string{"REVOKE UPDATE (id) ON public.oracle_extra FROM {role}"}},
		{name: "other relation via PUBLIC", apply: []string{"GRANT SELECT ON public.oracle_extra TO PUBLIC"},
			revert: []string{"REVOKE SELECT ON public.oracle_extra FROM PUBLIC"}},
		{name: "other relation column via PUBLIC", apply: []string{"GRANT SELECT (id) ON public.oracle_extra TO PUBLIC"},
			revert: []string{"REVOKE SELECT (id) ON public.oracle_extra FROM PUBLIC"}},
		{name: "other view SELECT", apply: []string{"GRANT SELECT ON public.oracle_extra_view TO {role}"},
			revert: []string{"REVOKE SELECT ON public.oracle_extra_view FROM {role}"}},
		{name: "other materialized view SELECT", apply: []string{"GRANT SELECT ON public.oracle_extra_matview TO {role}"},
			revert: []string{"REVOKE SELECT ON public.oracle_extra_matview FROM {role}"}},
		{name: "other partitioned table SELECT", apply: []string{"GRANT SELECT ON public.oracle_extra_part TO {role}"},
			revert: []string{"REVOKE SELECT ON public.oracle_extra_part FROM {role}"}},

		// ---- sequences ----
		{name: "required sequence USAGE revoked", needs: []string{"seq"},
			apply:  []string{"REVOKE USAGE ON SEQUENCE public.{seq} FROM {role}"},
			revert: []string{"GRANT USAGE ON SEQUENCE public.{seq} TO {role}"}},
		{name: "required sequence USAGE grant option", needs: []string{"seq"},
			apply:  []string{"GRANT USAGE ON SEQUENCE public.{seq} TO {role} WITH GRANT OPTION"},
			revert: []string{"REVOKE GRANT OPTION FOR USAGE ON SEQUENCE public.{seq} FROM {role}"}},
		{name: "required sequence SELECT", needs: []string{"seq"},
			apply:  []string{"GRANT SELECT ON SEQUENCE public.{seq} TO {role}"},
			revert: []string{"REVOKE SELECT ON SEQUENCE public.{seq} FROM {role}"}},
		{name: "required sequence UPDATE", needs: []string{"seq"},
			apply:  []string{"GRANT UPDATE ON SEQUENCE public.{seq} TO {role}"},
			revert: []string{"REVOKE UPDATE ON SEQUENCE public.{seq} FROM {role}"}},
		{name: "other sequence USAGE", apply: []string{"GRANT USAGE ON SEQUENCE public.oracle_extra_seq TO {role}"},
			revert: []string{"REVOKE USAGE ON SEQUENCE public.oracle_extra_seq FROM {role}"}},
		{name: "other sequence SELECT via PUBLIC", apply: []string{"GRANT SELECT ON SEQUENCE public.oracle_extra_seq TO PUBLIC"},
			revert: []string{"REVOKE SELECT ON SEQUENCE public.oracle_extra_seq FROM PUBLIC"}},

		// Before any ALTER DEFAULT PRIVILEGES below: a function created now has a
		// NULL proacl, i.e. the built-in default (PUBLIC may EXECUTE).
		{name: "river function with no ACL (default PUBLIC EXECUTE)",
			apply:  []string{"CREATE FUNCTION river.oracle_river_noacl_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'"},
			revert: []string{"DROP FUNCTION river.oracle_river_noacl_fn()"}},
		{name: "public function with no ACL (default PUBLIC EXECUTE)",
			apply:  []string{"CREATE FUNCTION public.oracle_public_noacl_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'"},
			revert: []string{"DROP FUNCTION public.oracle_public_noacl_fn()"}},
		// ---- River schema ----
		{name: "river schema USAGE", apply: []string{"GRANT USAGE ON SCHEMA river TO {role}"},
			revert: []string{"REVOKE USAGE ON SCHEMA river FROM {role}"}},
		{name: "river schema CREATE", apply: []string{"GRANT CREATE ON SCHEMA river TO {role}"},
			revert: []string{"REVOKE CREATE ON SCHEMA river FROM {role}"}},
		{name: "river table SELECT", apply: []string{"GRANT SELECT ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE SELECT ON river.oracle_river_table FROM {role}"}},
		{name: "river table INSERT", apply: []string{"GRANT INSERT ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE INSERT ON river.oracle_river_table FROM {role}"}},
		{name: "river table UPDATE", apply: []string{"GRANT UPDATE ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE UPDATE ON river.oracle_river_table FROM {role}"}},
		{name: "river table DELETE", apply: []string{"GRANT DELETE ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE DELETE ON river.oracle_river_table FROM {role}"}},
		{name: "river table TRUNCATE", apply: []string{"GRANT TRUNCATE ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE TRUNCATE ON river.oracle_river_table FROM {role}"}},
		{name: "river table REFERENCES", apply: []string{"GRANT REFERENCES ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE REFERENCES ON river.oracle_river_table FROM {role}"}},
		{name: "river table TRIGGER", apply: []string{"GRANT TRIGGER ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE TRIGGER ON river.oracle_river_table FROM {role}"}},
		{name: "river table MAINTAIN", apply: []string{"GRANT MAINTAIN ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE MAINTAIN ON river.oracle_river_table FROM {role}"}},
		{name: "river table column SELECT", apply: []string{"GRANT SELECT (id) ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE SELECT (id) ON river.oracle_river_table FROM {role}"}},
		{name: "river table column INSERT", apply: []string{"GRANT INSERT (id) ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE INSERT (id) ON river.oracle_river_table FROM {role}"}},
		{name: "river table column REFERENCES", apply: []string{"GRANT REFERENCES (id) ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE REFERENCES (id) ON river.oracle_river_table FROM {role}"}},
		{name: "river table column UPDATE", apply: []string{"GRANT UPDATE (id) ON river.oracle_river_table TO {role}"},
			revert: []string{"REVOKE UPDATE (id) ON river.oracle_river_table FROM {role}"}},
		{name: "river table via PUBLIC", apply: []string{"GRANT SELECT ON river.oracle_river_table TO PUBLIC"},
			revert: []string{"REVOKE SELECT ON river.oracle_river_table FROM PUBLIC"}},
		{name: "river sequence SELECT", apply: []string{"GRANT SELECT ON SEQUENCE river.oracle_river_seq TO {role}"},
			revert: []string{"REVOKE SELECT ON SEQUENCE river.oracle_river_seq FROM {role}"}},
		{name: "river sequence UPDATE", apply: []string{"GRANT UPDATE ON SEQUENCE river.oracle_river_seq TO {role}"},
			revert: []string{"REVOKE UPDATE ON SEQUENCE river.oracle_river_seq FROM {role}"}},
		{name: "other sequence UPDATE", apply: []string{"GRANT UPDATE ON SEQUENCE public.oracle_extra_seq TO {role}"},
			revert: []string{"REVOKE UPDATE ON SEQUENCE public.oracle_extra_seq FROM {role}"}},
		{name: "river sequence USAGE", apply: []string{"GRANT USAGE ON SEQUENCE river.oracle_river_seq TO {role}"},
			revert: []string{"REVOKE USAGE ON SEQUENCE river.oracle_river_seq FROM {role}"}},
		{name: "river function EXECUTE", apply: []string{"GRANT EXECUTE ON FUNCTION river.oracle_river_fn() TO {role}"},
			revert: []string{"REVOKE EXECUTE ON FUNCTION river.oracle_river_fn() FROM {role}"}},
		{name: "river function EXECUTE via PUBLIC", apply: []string{"GRANT EXECUTE ON FUNCTION river.oracle_river_fn() TO PUBLIC"},
			revert: []string{"REVOKE EXECUTE ON FUNCTION river.oracle_river_fn() FROM PUBLIC"}},
		{name: "river function default PUBLIC EXECUTE", apply: []string{"ALTER DEFAULT PRIVILEGES IN SCHEMA river GRANT EXECUTE ON FUNCTIONS TO PUBLIC", "CREATE FUNCTION river.oracle_river_default_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'"},
			revert: []string{"DROP FUNCTION river.oracle_river_default_fn()", "ALTER DEFAULT PRIVILEGES IN SCHEMA river REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC"}},

		{name: "owns a sequence", apply: []string{"ALTER SEQUENCE public.oracle_extra_seq OWNER TO {role}"},
			revert: []string{"ALTER SEQUENCE public.oracle_extra_seq OWNER TO CURRENT_USER"}},

		// ---- public functions ----
		{name: "public function EXECUTE", apply: []string{"GRANT EXECUTE ON FUNCTION public.oracle_public_fn() TO {role}"},
			revert: []string{"REVOKE EXECUTE ON FUNCTION public.oracle_public_fn() FROM {role}"}},
		{name: "public function EXECUTE via PUBLIC", apply: []string{"GRANT EXECUTE ON FUNCTION public.oracle_public_fn() TO PUBLIC"},
			revert: []string{"REVOKE EXECUTE ON FUNCTION public.oracle_public_fn() FROM PUBLIC"}},

		// ---- schema / database ----
		{name: "public schema CREATE", apply: []string{"GRANT CREATE ON SCHEMA public TO {role}"},
			revert: []string{"REVOKE CREATE ON SCHEMA public FROM {role}"}},
		{name: "public schema USAGE revoked", apply: []string{"REVOKE USAGE ON SCHEMA public FROM {role}", "REVOKE USAGE ON SCHEMA public FROM PUBLIC"},
			revert: []string{"GRANT USAGE ON SCHEMA public TO {role}", "GRANT USAGE ON SCHEMA public TO PUBLIC"}},
		{name: "database TEMPORARY", apply: []string{"GRANT TEMPORARY ON DATABASE {db} TO {role}"},
			revert: []string{"REVOKE TEMPORARY ON DATABASE {db} FROM {role}"}},
		{name: "database CREATE", apply: []string{"GRANT CREATE ON DATABASE {db} TO {role}"},
			revert: []string{"REVOKE CREATE ON DATABASE {db} FROM {role}"}},

		// ---- ownership and membership ----
		{name: "owns a table", apply: []string{"ALTER TABLE public.oracle_extra OWNER TO {role}"},
			revert: []string{"ALTER TABLE public.oracle_extra OWNER TO CURRENT_USER"}},
		{name: "owns a function", apply: []string{"ALTER FUNCTION public.oracle_public_fn() OWNER TO {role}"},
			revert: []string{"ALTER FUNCTION public.oracle_public_fn() OWNER TO CURRENT_USER"}},
		{name: "member of another role", apply: []string{"GRANT {other} TO {role}"},
			revert: []string{"REVOKE {other} FROM {role}"}},

		// ---- column-scoped relation ----
		{name: "column-scoped table-wide SELECT", needs: []string{"colTable"},
			apply:  []string{"GRANT SELECT ON public.{colTable} TO {role}"},
			revert: []string{"REVOKE SELECT ON public.{colTable} FROM {role}"}},
		{name: "column-scoped table-wide INSERT", needs: []string{"colTable"},
			apply:  []string{"GRANT INSERT ON public.{colTable} TO {role}"},
			revert: []string{"REVOKE INSERT ON public.{colTable} FROM {role}"}},
		{name: "column-scoped declared column grantable", needs: []string{"colTable"},
			apply:  []string{"GRANT SELECT ({colName}) ON public.{colTable} TO {role} WITH GRANT OPTION"},
			revert: []string{"REVOKE SELECT ({colName}) ON public.{colTable} FROM {role}", "GRANT SELECT ({colName}) ON public.{colTable} TO {role}"}},
		{name: "column-scoped declared column revoked", needs: []string{"colTable"},
			apply:  []string{"REVOKE SELECT ({colName}) ON public.{colTable} FROM {role}"},
			revert: []string{"GRANT SELECT ({colName}) ON public.{colTable} TO {role}"}},
		{name: "column-scoped undeclared column SELECT", needs: []string{"colTable", "otherCol"},
			apply:  []string{"GRANT SELECT ({otherCol}) ON public.{colTable} TO {role}"},
			revert: []string{"REVOKE SELECT ({otherCol}) ON public.{colTable} FROM {role}"}},
		{name: "column-scoped undeclared column UPDATE", needs: []string{"colTable", "otherCol"},
			apply:  []string{"GRANT UPDATE ({otherCol}) ON public.{colTable} TO {role}"},
			revert: []string{"REVOKE UPDATE ({otherCol}) ON public.{colTable} FROM {role}"}},
		{name: "column-scoped undeclared column REFERENCES", needs: []string{"colTable", "otherCol"},
			apply:  []string{"GRANT REFERENCES ({otherCol}) ON public.{colTable} TO {role}"},
			revert: []string{"REVOKE REFERENCES ({otherCol}) ON public.{colTable} FROM {role}"}},
		{name: "column-scoped undeclared column via PUBLIC", needs: []string{"colTable", "otherCol"},
			apply:  []string{"GRANT SELECT ({otherCol}) ON public.{colTable} TO PUBLIC"},
			revert: []string{"REVOKE SELECT ({otherCol}) ON public.{colTable} FROM PUBLIC"}},
		{name: "column-scoped declared privilege other column of declared kind", needs: []string{"colTable", "otherCol"},
			apply:  []string{"GRANT INSERT ({otherCol}) ON public.{colTable} TO {role}"},
			revert: []string{"REVOKE INSERT ({otherCol}) ON public.{colTable} FROM {role}"}},
		{name: "column-scoped table-wide via PUBLIC", needs: []string{"colTable"},
			apply:  []string{"GRANT SELECT ON public.{colTable} TO PUBLIC"},
			revert: []string{"REVOKE SELECT ON public.{colTable} FROM PUBLIC"}},
	}
}

type oracleEnv struct {
	admin  *pgxpool.Pool
	pools  map[string]*pgxpool.Pool // role name -> pool authenticated as it
	dbName string
	other  string
}

func oraclePlaceholders(env oracleEnv, role string, posture RolePosture) map[string]string {
	values := map[string]string{
		"role": pgx.Identifier{role}.Sanitize(), "db": pgx.Identifier{env.dbName}.Sanitize(),
		"other": pgx.Identifier{env.other}.Sanitize(),
	}
	set := func(key, value string) {
		if _, taken := values[key]; !taken && value != "" {
			values[key] = value
		}
	}
	for _, table := range posture.RequiredTables {
		set("req", table.TableName)
		if !table.AllowInsert {
			set("reqNoIns", table.TableName)
		} else {
			set("reqIns", table.TableName)
		}
		if !table.AllowUpdate {
			set("reqNoUpd", table.TableName)
		} else {
			set("reqUpd", table.TableName)
		}
		if !table.AllowDelete {
			set("reqNoDel", table.TableName)
		} else {
			set("reqDel", table.TableName)
		}
	}
	if len(posture.RequiredSequences) > 0 {
		set("seq", posture.RequiredSequences[0])
	}
	if len(posture.ColumnScoped) > 0 {
		set("colTable", posture.ColumnScoped[0].TableName)
		set("colName", posture.ColumnScoped[0].ColumnName)
	}
	return values
}

func expandOracle(statement string, values map[string]string) (string, bool) {
	for {
		start := strings.IndexByte(statement, '{')
		if start < 0 {
			return statement, true
		}
		end := strings.IndexByte(statement[start:], '}')
		key := statement[start+1 : start+end]
		value, ok := values[key]
		if !ok {
			return "", false
		}
		statement = statement[:start] + value + statement[start+end+1:]
	}
}

// evaluateOracle runs one query as the pool's role, through the production
// argument construction.
func evaluateOracle(ctx context.Context, pool *pgxpool.Pool, query, role string, posture RolePosture) (bool, error) {
	return evaluateOracleIn(ctx, pool, query, role, "river", posture)
}

func evaluateOracleIn(ctx context.Context, pool *pgxpool.Pool, query, role, riverSchema string, posture RolePosture) (bool, error) {
	args, err := rolePostureArgs(role, riverSchema, posture)
	if err != nil {
		return false, err
	}
	var authorized bool
	err = pool.QueryRow(ctx, query, args...).Scan(&authorized)
	return authorized, err
}

func startOracleEnv(t *testing.T, ctx context.Context) (oracleEnv, grantRoleNames, string) {
	t.Helper()
	admin, uri, roles := startGrantHarness(t, ctx)
	dbName := ""
	if err := admin.QueryRow(ctx, "SELECT current_database()").Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	roleURI := func(role, password string) string {
		parsed, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		parsed.User = url.UserPassword(role, password)
		return parsed.String()
	}
	env := oracleEnv{admin: admin, pools: map[string]*pgxpool.Pool{}, dbName: dbName, other: roles.queue}
	for role, password := range map[string]string{
		roles.domain: grantDomainPass, roles.coordinator: grantCoordinatorPass, roles.queue: grantQueuePass,
	} {
		pool, err := pgxpool.New(ctx, roleURI(role, password))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		env.pools[role] = pool
	}
	for _, statement := range []string{
		"CREATE TABLE public.oracle_extra (id bigint PRIMARY KEY, note text)",
		"CREATE VIEW public.oracle_extra_view AS SELECT id FROM public.oracle_extra",
		"CREATE MATERIALIZED VIEW public.oracle_extra_matview AS SELECT id FROM public.oracle_extra",
		"CREATE TABLE public.oracle_extra_part (id bigint, note text) PARTITION BY RANGE (id)",
		"CREATE SEQUENCE public.oracle_extra_seq",
		"CREATE FUNCTION public.oracle_public_fn() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
		"REVOKE ALL ON FUNCTION public.oracle_public_fn() FROM PUBLIC",
		"CREATE TABLE river.oracle_river_table (id bigint PRIMARY KEY)",
		"CREATE SEQUENCE river.oracle_river_seq",
		"CREATE FUNCTION river.oracle_river_fn() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
		"REVOKE ALL ON FUNCTION river.oracle_river_fn() FROM PUBLIC",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	return env, roles, uri
}

func TestRolePostureQueryAgreesWithTheLegacyQueryAcrossTheGrantMatrix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env, roles, _ := startOracleEnv(t, ctx)

	type subject struct {
		role    string
		posture RolePosture
		isDom   bool
	}
	subjects := []subject{
		{roles.domain, domainPosture(), true},
		{roles.coordinator, coordinatorPosture(), false},
	}
	answers := map[string]int{"true": 0, "false": 0}
	for _, subj := range subjects {
		subj := subj
		values := oraclePlaceholders(env, subj.role, subj.posture)
		if table := values["colTable"]; table != "" {
			var other string
			if err := env.admin.QueryRow(ctx, `
				SELECT attname FROM pg_attribute
				WHERE attrelid = ('public.' || $1)::regclass AND attnum > 0 AND NOT attisdropped AND attname <> $2
				ORDER BY attnum LIMIT 1`, table, values["colName"]).Scan(&other); err == nil {
				values["otherCol"] = pgx.Identifier{other}.Sanitize()
			}
		}
		pool := env.pools[subj.role]
		for _, scenario := range oracleScenarios() {
			scenario := scenario
			t.Run(fmt.Sprintf("%s/%s", map[bool]string{true: "domain", false: "coordinator"}[subj.isDom], scenario.name), func(t *testing.T) {
				for _, need := range scenario.needs {
					if _, ok := values[need]; !ok {
						t.Skipf("posture has no %s", need)
					}
				}
				run := func(statements []string) {
					for _, statement := range statements {
						expanded, ok := expandOracle(statement, values)
						if !ok {
							t.Skipf("unfilled placeholder in %q", statement)
						}
						if _, err := env.admin.Exec(ctx, expanded); err != nil {
							t.Fatalf("%s: %v", expanded, err)
						}
					}
				}
				run(scenario.apply)
				defer run(scenario.revert)
				legacy, legacyErr := evaluateOracle(ctx, pool, legacyRolePostureQuery, subj.role, subj.posture)
				current, currentErr := evaluateOracle(ctx, pool, rolePostureQuery, subj.role, subj.posture)
				if legacyErr != nil || currentErr != nil {
					t.Fatalf("query errors: legacy=%v current=%v", legacyErr, currentErr)
				}
				if legacy != current {
					t.Fatalf("DISAGREE: legacy=%v current=%v", legacy, current)
				}
				if subj.isDom {
					if current != scenario.domainWant {
						t.Fatalf("answer = %v, scenario is built to give %v", current, scenario.domainWant)
					}
				}
				answers[fmt.Sprint(current)]++
			})
		}
	}
	// The matrix must exercise both outcomes, or it proves nothing.
	if answers["true"] == 0 || answers["false"] == 0 {
		t.Fatalf("matrix answers = %v, want both outcomes exercised", answers)
	}
}

// CHAOS-6937. On the production catalog (1251 pg_class rows, 5376 live
// pg_attribute rows, 250 public relations) the pre-rewrite query took 1.8 s;
// the cost was the per-row has_*_privilege calls and two expansions of
// information_schema.column_privileges (21.3k column-ACL rows sorted). This
// builds a catalog of that shape, runs both queries as the domain role, and
// requires the rewrite to be the cheaper by a wide margin while agreeing.
func TestRolePostureQueryIsCheaperOnAProdSizedCatalog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env, roles, _ := startOracleEnv(t, ctx)
	if _, err := env.admin.Exec(ctx, `
		DO $$
		DECLARE i int;
		BEGIN
			FOR i IN 1..190 LOOP
				EXECUTE format('CREATE TABLE public.bulk_%s (id bigint PRIMARY KEY, %s)', i,
					(SELECT string_agg(format('c%s text', c), ', ') FROM generate_series(1, 19) AS c));
			END LOOP;
		END $$`); err != nil {
		t.Fatal(err)
	}
	pool := env.pools[roles.domain]
	posture := domainPosture()
	measure := func(query string) (time.Duration, bool) {
		var answer bool
		var samples []time.Duration
		for i := 0; i < 7; i++ {
			started := time.Now()
			got, err := evaluateOracle(ctx, pool, query, roles.domain, posture)
			if err != nil {
				t.Fatal(err)
			}
			answer = got
			samples = append(samples, time.Since(started))
		}
		sort.Slice(samples, func(a, b int) bool { return samples[a] < samples[b] })
		return samples[len(samples)/2], answer
	}
	legacy, legacyAnswer := measure(legacyRolePostureQuery)
	current, currentAnswer := measure(rolePostureQuery)
	t.Logf("median of 7: legacy %s, rewritten %s", legacy, current)
	if !legacyAnswer || !currentAnswer {
		t.Fatalf("baseline posture must hold on both: legacy=%v current=%v", legacyAnswer, currentAnswer)
	}
	if current*3 > legacy {
		t.Fatalf("rewritten query (%s) is not at least 3x cheaper than the legacy one (%s) on a prod-shaped catalog", current, legacy)
	}
}

// The catalog angle of the column-scoped check is redundant, by design, with
// the has_column_privilege sweeps beside it (its own comment says it is a second
// angle), so no boolean scenario can pin the rewrite of it: a wrong
// implementation would still leave the query's answer unchanged. Pin the ROWS
// instead. caller_column_grants must be exactly what
// information_schema.column_privileges shows for the calling role on the
// column-scoped relations, for every grant shape.
func TestCallerColumnGrantsAreTheRowsInformationSchemaShowsForTheCaller(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env, roles, _ := startOracleEnv(t, ctx)
	posture := domainPosture()
	if len(posture.ColumnScoped) == 0 {
		t.Fatal("the domain posture has no column-scoped pair; the oracle needs one")
	}
	values := oraclePlaceholders(env, roles.domain, posture)
	var other string
	if err := env.admin.QueryRow(ctx, `
		SELECT attname FROM pg_attribute
		WHERE attrelid = ('public.' || $1)::regclass AND attnum > 0 AND NOT attisdropped AND attname <> $2
		ORDER BY attnum LIMIT 1`, values["colTable"], values["colName"]).Scan(&other); err != nil {
		t.Fatal(err)
	}
	values["otherCol"] = pgx.Identifier{other}.Sanitize()

	// The production statement, cut before its final SELECT, with that SELECT
	// replaced by one that returns the CTE's rows. $1 must still be referenced.
	cut := strings.Index(rolePostureQuery, "\nSELECT\n\t"+roleacl.IdentityPredicateSQL)
	if cut < 0 {
		t.Fatal("cannot find the final SELECT of rolePostureQuery to cut at")
	}
	rowsQuery := rolePostureQuery[:cut] + `
SELECT table_name::text, column_name::text, privilege_type::text, is_grantable
FROM caller_column_grants
WHERE $1::text IS NOT NULL
ORDER BY 1, 2, 3, 4`
	args, err := rolePostureArgs(roles.domain, "river", posture)
	if err != nil {
		t.Fatal(err)
	}
	const legacyRows = `
SELECT table_name::text, column_name::text, privilege_type::text, (is_grantable = 'YES')
FROM information_schema.column_privileges
WHERE table_schema = 'public' AND grantee = current_user AND table_name = $1
ORDER BY 1, 2, 3, 4`
	read := func(pool *pgxpool.Pool, query string, params ...any) []string {
		rows, err := pool.Query(ctx, query, params...)
		if err != nil {
			t.Fatalf("%v", err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var table, column, privilege string
			var grantable bool
			if err := rows.Scan(&table, &column, &privilege, &grantable); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprintf("%s.%s %s grantable=%v", table, column, privilege, grantable))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	pool := env.pools[roles.domain]
	shapes := []struct {
		name          string
		apply, revert []string
		minRows       int
	}{
		{name: "declared pairs only"},
		{name: "table-wide SELECT", apply: []string{"GRANT SELECT ON public.{colTable} TO {role}"},
			revert: []string{"REVOKE SELECT ON public.{colTable} FROM {role}"}, minRows: 1},
		{name: "table-wide INSERT with grant option", apply: []string{"GRANT INSERT ON public.{colTable} TO {role} WITH GRANT OPTION"},
			revert: []string{"REVOKE INSERT ON public.{colTable} FROM {role}"}, minRows: 1},
		{name: "table-wide UPDATE and REFERENCES", apply: []string{"GRANT UPDATE, REFERENCES ON public.{colTable} TO {role}"},
			revert: []string{"REVOKE UPDATE, REFERENCES ON public.{colTable} FROM {role}"}, minRows: 1},
		{name: "table-wide DELETE and TRUNCATE (not column privileges)", apply: []string{"GRANT DELETE, TRUNCATE, TRIGGER ON public.{colTable} TO {role}"},
			revert: []string{"REVOKE DELETE, TRUNCATE, TRIGGER ON public.{colTable} FROM {role}"}},
		{name: "declared column grantable", apply: []string{"GRANT SELECT ({colName}) ON public.{colTable} TO {role} WITH GRANT OPTION"},
			revert: []string{"REVOKE SELECT ({colName}) ON public.{colTable} FROM {role}", "GRANT SELECT ({colName}) ON public.{colTable} TO {role}"}, minRows: 1},
		{name: "undeclared column, each privilege", apply: []string{
			"GRANT SELECT ({otherCol}), INSERT ({otherCol}), UPDATE ({otherCol}), REFERENCES ({otherCol}) ON public.{colTable} TO {role}"},
			revert: []string{"REVOKE SELECT ({otherCol}), INSERT ({otherCol}), UPDATE ({otherCol}), REFERENCES ({otherCol}) ON public.{colTable} FROM {role}"}, minRows: 1},
		{name: "granted to PUBLIC only", apply: []string{"GRANT SELECT ON public.{colTable} TO PUBLIC", "GRANT UPDATE ({otherCol}) ON public.{colTable} TO PUBLIC"},
			revert: []string{"REVOKE SELECT ON public.{colTable} FROM PUBLIC", "REVOKE UPDATE ({otherCol}) ON public.{colTable} FROM PUBLIC"}},
	}
	sawRows := false
	for _, shape := range shapes {
		shape := shape
		t.Run(shape.name, func(t *testing.T) {
			run := func(statements []string) {
				for _, statement := range statements {
					expanded, ok := expandOracle(statement, values)
					if !ok {
						t.Fatalf("unfilled placeholder in %q", statement)
					}
					if _, err := env.admin.Exec(ctx, expanded); err != nil {
						t.Fatalf("%s: %v", expanded, err)
					}
				}
			}
			run(shape.apply)
			defer run(shape.revert)
			got := read(pool, rowsQuery, args...)
			want := read(pool, legacyRows, values["colTable"])
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("caller_column_grants rows differ from information_schema.column_privileges:\n got  %v\n want %v", got, want)
			}
			if len(got) < shape.minRows {
				t.Fatalf("shape produced %d rows, want at least %d: the scenario does not bite", len(got), shape.minRows)
			}
			if len(got) > 0 {
				sawRows = true
			}
		})
	}
	if !sawRows {
		t.Fatal("no shape produced a row")
	}
}

// The rewrite skips dropped columns by attisdropped; that only matters if a
// dropped column can keep a column ACL. PostgreSQL clears it on DROP COLUMN, so
// the filter is a belt, not a behaviour: pinned here so a server that changes
// this shows up as a failure, not a silent divergence.
func TestDroppingAColumnClearsItsColumnAcl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env, roles, _ := startOracleEnv(t, ctx)
	for _, statement := range []string{
		"ALTER TABLE public.oracle_extra ADD COLUMN doomed text",
		"GRANT SELECT (doomed) ON public.oracle_extra TO " + pgx.Identifier{roles.domain}.Sanitize(),
		"ALTER TABLE public.oracle_extra DROP COLUMN doomed",
	} {
		if _, err := env.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	var remaining int
	if err := env.admin.QueryRow(ctx, `
		SELECT count(*) FROM pg_attribute
		WHERE attrelid = 'public.oracle_extra'::regclass AND attisdropped AND attacl IS NOT NULL`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("%d dropped column(s) still carry an ACL", remaining)
	}
}

// A function with a NULL proacl carries the built-in default, which lets PUBLIC
// EXECUTE it. The harness's river schema has default privileges, so a function
// created there always has an explicit ACL; a schema with none is where the
// NULL case (the one acldefault() exists for in the rewrite) is reachable for
// the River-schema function check.
func TestBuiltInDefaultFunctionAclCountsAsEffectiveInTheRiverSchemaCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env, roles, _ := startOracleEnv(t, ctx)
	for _, statement := range []string{
		"CREATE SCHEMA oracle_alt",
		"CREATE FUNCTION oracle_alt.fn() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
	} {
		if _, err := env.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	var acl *string
	if err := env.admin.QueryRow(ctx, "SELECT proacl::text FROM pg_proc WHERE proname = 'fn' AND pronamespace = 'oracle_alt'::regnamespace").Scan(&acl); err != nil || acl != nil {
		t.Fatalf("the function must have a NULL proacl for this test to mean anything: acl=%v err=%v", acl, err)
	}
	pool := env.pools[roles.domain]
	posture := domainPosture()
	for _, step := range []struct {
		name string
		sql  string
		want bool
	}{
		{"default ACL: PUBLIC may EXECUTE", "SELECT 1", false},
		{"PUBLIC EXECUTE revoked", "REVOKE EXECUTE ON FUNCTION oracle_alt.fn() FROM PUBLIC", true},
		{"granted to the role", "GRANT EXECUTE ON FUNCTION oracle_alt.fn() TO " + pgx.Identifier{roles.domain}.Sanitize(), false},
	} {
		if _, err := env.admin.Exec(ctx, step.sql); err != nil {
			t.Fatalf("%s: %v", step.sql, err)
		}
		legacy, lerr := evaluateOracleIn(ctx, pool, legacyRolePostureQuery, roles.domain, "oracle_alt", posture)
		current, cerr := evaluateOracleIn(ctx, pool, rolePostureQuery, roles.domain, "oracle_alt", posture)
		if lerr != nil || cerr != nil {
			t.Fatalf("%s: legacy=%v current=%v", step.name, lerr, cerr)
		}
		if legacy != current || current != step.want {
			t.Fatalf("%s: legacy=%v current=%v, want both %v", step.name, legacy, current, step.want)
		}
	}
}
