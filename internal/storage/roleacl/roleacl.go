// Package roleacl is the ONE definition of what a least-privilege runtime role
// holds and how to tell (CHAOS-6804, lead D2616). It exists so the readiness check
// and the migration leg cannot disagree: both read the same enumeration.
//
//   - IdentityPredicateSQL: the pool AUTHENTICATED as the role (not merely acting
//     as it), and the role is an unprivileged, membership-free login.
//   - OwnsNothingSQL: the role owns no object.
//   - Enumerate: every EFFECTIVE grant the role holds, from every ACL-bearing
//     catalog, PUBLIC counted as granted to the role, plus its role-level settings.
//     "Hold exactly the manifest" is set equality against this list, so a
//     privilege KIND nobody thought to check cannot exist by construction.
//   - Enumerate also yields, for every grant the role holds in its OWN name, the
//     statement that removes it: the migration leg runs those and then grants the
//     manifest.
//
// Scope, stated (lead D2616, reconciled): every schema except the system schemas
// (pg_catalog, information_schema, pg_toast*, pg_temp_*), and every object except
// those an extension owns (pg_depend deptype 'e': relations, schemas, functions,
// types, languages and foreign data wrappers), which are the server's or the
// extension author's, not the application's. The PUBLIC defaults on types
// and languages (USAGE) and on functions (EXECUTE) are ambient in every catalog
// and are not counted (once any explicit grant materialises an object's ACL, the
// default PUBLIC entry inside it is still not counted), EXCEPT PUBLIC EXECUTE on a
// SECURITY DEFINER function, which lets any caller run code with its owner's
// privileges. Every other ACL entry for the role or PUBLIC, of any class, is
// counted. Grants on
// OTHER databases are out of scope (the catalogs are per database).
package roleacl

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// RoleAttributesSQL is true when $1 names an unprivileged login: it can log in and
// has none of SUPERUSER, BYPASSRLS, CREATEROLE, CREATEDB, REPLICATION.
const RoleAttributesSQL = `EXISTS (
	SELECT 1 FROM pg_catalog.pg_roles AS identity
	WHERE identity.rolname = $1
		AND identity.rolcanlogin
		AND NOT identity.rolsuper
		AND NOT identity.rolbypassrls
		AND NOT identity.rolcreaterole
		AND NOT identity.rolcreatedb
		AND NOT identity.rolreplication
)`

// MembershipFreeSQL is true when $1 is a member of no role (an inherited privilege
// is a privilege the role's own ACL entries do not show).
const MembershipFreeSQL = `NOT EXISTS (
	SELECT 1 FROM pg_catalog.pg_auth_members AS membership
	JOIN pg_catalog.pg_roles AS identity ON identity.oid = membership.member
	WHERE identity.rolname = $1
)`

// OwnsNothingSQL is true when $1 owns no database, schema, relation, function or
// procedure. Ownership carries every grant option, so it is checked apart from
// the ACLs, and the migration leg refuses a role that owns anything (a REVOKE
// would strip an owner of its own objects).
const OwnsNothingSQL = `(
	NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_roles AS identity
		JOIN pg_catalog.pg_database AS object ON object.datdba = identity.oid
		WHERE identity.rolname = $1
	)
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_roles AS identity
		JOIN pg_catalog.pg_namespace AS object ON object.nspowner = identity.oid
		WHERE identity.rolname = $1
	)
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_roles AS identity
		JOIN pg_catalog.pg_class AS object ON object.relowner = identity.oid
			AND object.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
		WHERE identity.rolname = $1
	)
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_roles AS identity
		JOIN pg_catalog.pg_proc AS object ON object.proowner = identity.oid
		WHERE identity.rolname = $1
	)
)`

// IdentityPredicateSQL is the single definition of "this pool IS the role": the
// AUTHENTICATED user (pg_stat_activity.usename of this backend, which neither SET
// ROLE nor SET SESSION AUTHORIZATION changes), session_user and current_user all
// equal $1, and $1 is an unprivileged login that is a member of no role.
const IdentityPredicateSQL = `(
	(SELECT usename FROM pg_catalog.pg_stat_activity WHERE pid = pg_backend_pid()) = $1
	AND session_user = $1
	AND current_user = $1
	AND ` + RoleAttributesSQL + `
	AND ` + MembershipFreeSQL + `
)`

// Querier is the slice of pgx the enumeration needs (a pool, a connection or a
// transaction).
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Grant is one effective privilege (or setting) a role holds.
type Grant struct {
	// Class is relation, column, schema, function, type, language, large object,
	// default privileges, database, tablespace, foreign data wrapper, foreign
	// server, parameter or setting.
	Class string
	// Object identifies the object (schema-qualified where it has a schema).
	Object string
	// Privilege is the privilege type, "... WITH GRANT OPTION" when grantable; the
	// setting text for a setting.
	Privilege string
	// ViaPublic: the grantee is PUBLIC (grantee 0), not the role itself.
	ViaPublic bool
	// Revoke removes this grant when it is held in the role's OWN name; empty for
	// PUBLIC grants (other roles rely on them: the check names them, an operator
	// decides) and for implicit defaults.
	Revoke string
}

// String is the redacted, log-safe form: class, object, privilege, and whether it
// arrives through PUBLIC. Object names are catalog identifiers, never secrets.
func (g Grant) String() string {
	via := ""
	if g.ViaPublic {
		via = " (through PUBLIC)"
	}
	if g.Class == "setting" {
		return "role setting " + g.Privilege
	}
	return fmt.Sprintf("%s on %s %s%s", g.Privilege, g.Class, g.Object, via)
}

const parameterACLMinVersion = 150000

// enumerationSQL builds the one enumeration query. Every branch yields
// (class, object, privilege, via_public, revoke). $1 is the role name. The
// parameter branch exists only on servers that have pg_parameter_acl.
func enumerationSQL(withParameters bool) string {
	const systemSchemas = `n.nspname NOT IN ('pg_catalog', 'information_schema')
		AND n.nspname NOT LIKE 'pg\_toast%' AND n.nspname NOT LIKE 'pg\_temp\_%'`
	// notExtension excludes objects an extension owns (pg_depend deptype 'e'): an
	// extension's own catalog objects are its author's, not the application's.
	notExtension := func(catalog, oid string) string {
		return fmt.Sprintf(`NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend AS dep WHERE dep.classid = '%s'::regclass AND dep.objid = %s AND dep.deptype = 'e')`, catalog, oid)
	}
	// grantee is the role's own oid or PUBLIC (0).
	const grantee = `a.grantee IN (0, (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = $1))`
	const priv = `a.privilege_type || CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END`
	// revoke wraps a statement so a REVOKE the migration identity is not entitled
	// to run (an ALTER DEFAULT PRIVILEGES for another owner) warns instead of
	// aborting the migration; the readiness check then refuses the leftover loudly.
	const tolerant = `format('DO $r$ BEGIN EXECUTE %%L; EXCEPTION WHEN insufficient_privilege THEN RAISE WARNING %%L; END $r$', %s, 'roleacl: not permitted to revoke, readiness will refuse the leftover')`
	own := func(inner string) string {
		return fmt.Sprintf(`CASE WHEN a.grantee <> 0 THEN `+tolerant+` END`, inner)
	}
	query := `
SELECT class, object, privilege, via_public, revoke_stmt FROM (
	-- relations and sequences (table-level ACL; defaults are owner-only)
	SELECT 'relation' AS class, format('%I.%I', n.nspname, c.relname) AS object, ` + priv + ` AS privilege,
		(a.grantee = 0) AS via_public,
		` + own(`format('REVOKE ALL PRIVILEGES ON %s %I.%I FROM %I', CASE WHEN c.relkind = 'S' THEN 'SEQUENCE' ELSE 'TABLE' END, n.nspname, c.relname, $1::text)`) + ` AS revoke_stmt
	FROM pg_catalog.pg_class AS c
	JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
	CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,
		pg_catalog.acldefault(CASE WHEN c.relkind = 'S' THEN 's'::"char" ELSE 'r'::"char" END, c.relowner))) AS a
	WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S') AND ` + systemSchemas + ` AND ` + grantee + `
		AND ` + notExtension("pg_class", "c.oid") + `
	UNION ALL
	-- column-level ACLs (explicit only)
	SELECT 'column', format('%I.%I.%I', n.nspname, c.relname, t.attname), ` + priv + `,
		(a.grantee = 0),
		` + own(`format('REVOKE ALL PRIVILEGES ON TABLE %I.%I FROM %I', n.nspname, c.relname, $1::text)`) + `
	FROM pg_catalog.pg_attribute AS t
	JOIN pg_catalog.pg_class AS c ON c.oid = t.attrelid
	JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
	CROSS JOIN LATERAL pg_catalog.aclexplode(t.attacl) AS a
	WHERE t.attacl IS NOT NULL AND t.attnum > 0 AND NOT t.attisdropped AND ` + systemSchemas + ` AND ` + grantee + `
		AND ` + notExtension("pg_class", "c.oid") + `
	UNION ALL
	-- schemas
	SELECT 'schema', n.nspname::text, ` + priv + `,
		(a.grantee = 0),
		` + own(`format('REVOKE ALL PRIVILEGES ON SCHEMA %I FROM %I', n.nspname, $1::text)`) + `
	FROM pg_catalog.pg_namespace AS n
	CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(n.nspacl, pg_catalog.acldefault('n'::"char", n.nspowner))) AS a
	WHERE ` + systemSchemas + ` AND ` + grantee + `
		AND ` + notExtension("pg_namespace", "n.oid") + `
	UNION ALL
	-- functions and procedures: explicit ACL entries always; the implicit PUBLIC
	-- EXECUTE only on a SECURITY DEFINER function (ambient otherwise)
	SELECT 'function', format('%I.%I(%s)', n.nspname, p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid)), ` + priv + `,
		(a.grantee = 0),
		` + own(`format('REVOKE ALL PRIVILEGES ON %s %I.%I(%s) FROM %I', CASE WHEN p.prokind = 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END, n.nspname, p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid), $1::text)`) + `
	FROM pg_catalog.pg_proc AS p
	JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
	CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(p.proacl, pg_catalog.acldefault('f'::"char", p.proowner))) AS a
	WHERE (p.proacl IS NOT NULL OR p.prosecdef) AND ` + systemSchemas + ` AND ` + grantee + `
		AND NOT (a.grantee = 0 AND a.privilege_type = 'EXECUTE' AND NOT a.is_grantable AND NOT p.prosecdef)
		AND ` + notExtension("pg_proc", "p.oid") + `
	UNION ALL
	-- types (explicit only)
	SELECT 'type', format('%I.%I', n.nspname, t.typname), ` + priv + `,
		(a.grantee = 0),
		` + own(`format('REVOKE ALL PRIVILEGES ON TYPE %I.%I FROM %I', n.nspname, t.typname, $1::text)`) + `
	FROM pg_catalog.pg_type AS t
	JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
	CROSS JOIN LATERAL pg_catalog.aclexplode(t.typacl) AS a
	WHERE t.typacl IS NOT NULL AND ` + systemSchemas + ` AND ` + grantee + `
		AND NOT (a.grantee = 0 AND a.privilege_type = 'USAGE' AND NOT a.is_grantable)
		AND ` + notExtension("pg_type", "t.oid") + `
	UNION ALL
	-- languages (explicit only)
	SELECT 'language', l.lanname::text, ` + priv + `,
		(a.grantee = 0),
		` + own(`format('REVOKE ALL PRIVILEGES ON LANGUAGE %I FROM %I', l.lanname, $1::text)`) + `
	FROM pg_catalog.pg_language AS l
	CROSS JOIN LATERAL pg_catalog.aclexplode(l.lanacl) AS a
	WHERE l.lanacl IS NOT NULL AND ` + grantee + `
		AND NOT (a.grantee = 0 AND a.privilege_type = 'USAGE' AND NOT a.is_grantable)
		AND ` + notExtension("pg_language", "l.oid") + `
	UNION ALL
	-- large objects (explicit only; defaults are owner-only)
	SELECT 'large object', m.oid::text, ` + priv + `,
		(a.grantee = 0),
		` + own(`format('REVOKE ALL PRIVILEGES ON LARGE OBJECT %s FROM %I', m.oid::text, $1::text)`) + `
	FROM pg_catalog.pg_largeobject_metadata AS m
	CROSS JOIN LATERAL pg_catalog.aclexplode(m.lomacl) AS a
	WHERE m.lomacl IS NOT NULL AND ` + grantee + `
	UNION ALL
	-- default privileges (ALTER DEFAULT PRIVILEGES) that name the role or PUBLIC
	SELECT 'default privileges', format('%s for %I%s', d.defaclobjtype, r.rolname,
			CASE WHEN d.defaclnamespace = 0 THEN '' ELSE ' in ' || n.nspname END), ` + priv + `,
		(a.grantee = 0),
		` + own(`format('ALTER DEFAULT PRIVILEGES FOR ROLE %I%s REVOKE ALL ON %s FROM %I', r.rolname,
			CASE WHEN d.defaclnamespace = 0 THEN '' ELSE format(' IN SCHEMA %I', n.nspname) END,
			CASE d.defaclobjtype WHEN 'r' THEN 'TABLES' WHEN 'S' THEN 'SEQUENCES' WHEN 'f' THEN 'FUNCTIONS' WHEN 'T' THEN 'TYPES' ELSE 'SCHEMAS' END,
			$1::text)`) + `
	FROM pg_catalog.pg_default_acl AS d
	JOIN pg_catalog.pg_roles AS r ON r.oid = d.defaclrole
	LEFT JOIN pg_catalog.pg_namespace AS n ON n.oid = d.defaclnamespace
	CROSS JOIN LATERAL pg_catalog.aclexplode(d.defaclacl) AS a
	WHERE ` + grantee + `
	UNION ALL
	-- this database (defaults count: PUBLIC holds CONNECT and TEMPORARY)
	SELECT 'database', d.datname::text, ` + priv + `,
		(a.grantee = 0),
		CASE WHEN a.grantee <> 0 AND d.datacl IS NOT NULL THEN ` + fmt.Sprintf(tolerant, `format('REVOKE ALL PRIVILEGES ON DATABASE %I FROM %I', d.datname, $1::text)`) + ` END
	FROM pg_catalog.pg_database AS d
	CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(d.datacl, pg_catalog.acldefault('d'::"char", d.datdba))) AS a
	WHERE d.datname = current_database() AND ` + grantee + `
	UNION ALL
	-- tablespaces (explicit only)
	SELECT 'tablespace', t.spcname::text, ` + priv + `,
		(a.grantee = 0),
		` + own(`format('REVOKE ALL PRIVILEGES ON TABLESPACE %I FROM %I', t.spcname, $1::text)`) + `
	FROM pg_catalog.pg_tablespace AS t
	CROSS JOIN LATERAL pg_catalog.aclexplode(t.spcacl) AS a
	WHERE t.spcacl IS NOT NULL AND ` + grantee + `
	UNION ALL
	-- foreign data wrappers and foreign servers (explicit only)
	SELECT 'foreign data wrapper', w.fdwname::text, ` + priv + `,
		(a.grantee = 0),
		` + own(`format('REVOKE ALL PRIVILEGES ON FOREIGN DATA WRAPPER %I FROM %I', w.fdwname, $1::text)`) + `
	FROM pg_catalog.pg_foreign_data_wrapper AS w
	CROSS JOIN LATERAL pg_catalog.aclexplode(w.fdwacl) AS a
	WHERE w.fdwacl IS NOT NULL AND ` + grantee + `
		AND ` + notExtension("pg_foreign_data_wrapper", "w.oid") + `
	UNION ALL
	SELECT 'foreign server', s.srvname::text, ` + priv + `,
		(a.grantee = 0),
		` + own(`format('REVOKE ALL PRIVILEGES ON FOREIGN SERVER %I FROM %I', s.srvname, $1::text)`) + `
	FROM pg_catalog.pg_foreign_server AS s
	CROSS JOIN LATERAL pg_catalog.aclexplode(s.srvacl) AS a
	WHERE s.srvacl IS NOT NULL AND ` + grantee + `
	UNION ALL
	-- role-level settings (ALTER ROLE ... SET): the manifest declares none
	SELECT 'setting', CASE WHEN s.setdatabase = 0 THEN 'all databases' ELSE 'database ' || s.setdatabase::text END,
		cfg.setting, false,
		format('ALTER ROLE %I%s RESET ALL', $1::text,
			CASE WHEN s.setdatabase = 0 THEN '' ELSE ' IN DATABASE ' || quote_ident((SELECT datname FROM pg_catalog.pg_database WHERE oid = s.setdatabase)) END)
	FROM pg_catalog.pg_db_role_setting AS s
	CROSS JOIN LATERAL unnest(s.setconfig) AS cfg(setting)
	WHERE s.setrole = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = $1)`
	if withParameters {
		query += `
	UNION ALL
	-- parameter privileges (PostgreSQL 15+)
	SELECT 'parameter', p.parname, ` + priv + `,
		(a.grantee = 0),
		` + own(`format('REVOKE ALL PRIVILEGES ON PARAMETER %I FROM %I', p.parname, $1::text)`) + `
	FROM pg_catalog.pg_parameter_acl AS p
	CROSS JOIN LATERAL pg_catalog.aclexplode(p.paracl) AS a
	WHERE ` + grantee
	}
	query += `
) AS effective
ORDER BY class, object, privilege, via_public`
	return query
}

// Enumerate returns every effective grant $role holds (see the package doc for
// scope), ordered, with the statement that removes each one held in the role's own
// name. It is read-only.
func Enumerate(ctx context.Context, q Querier, role string) ([]Grant, error) {
	var versionText string
	if err := q.QueryRow(ctx, "SELECT current_setting('server_version_num')").Scan(&versionText); err != nil {
		return nil, fmt.Errorf("roleacl: reading the server version: %w", err)
	}
	version, err := strconv.Atoi(versionText)
	if err != nil {
		return nil, fmt.Errorf("roleacl: unreadable server version %q", versionText)
	}
	rows, err := q.Query(ctx, enumerationSQL(version >= parameterACLMinVersion), role)
	if err != nil {
		return nil, fmt.Errorf("roleacl: enumerating effective grants: %w", err)
	}
	defer rows.Close()
	var grants []Grant
	for rows.Next() {
		var g Grant
		var revoke *string
		if err := rows.Scan(&g.Class, &g.Object, &g.Privilege, &g.ViaPublic, &revoke); err != nil {
			return nil, fmt.Errorf("roleacl: scanning an effective grant: %w", err)
		}
		if revoke != nil {
			g.Revoke = *revoke
		}
		grants = append(grants, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("roleacl: reading effective grants: %w", err)
	}
	return grants, nil
}

// RevokeStatements returns the distinct statements that remove every grant the
// role holds in its OWN name (never PUBLIC's), in enumeration order. Run them,
// then grant the manifest: the role's posture becomes a function of the migration
// alone.
func RevokeStatements(ctx context.Context, q Querier, role string) ([]string, error) {
	grants, err := Enumerate(ctx, q, role)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	var statements []string
	for _, grant := range grants {
		if grant.Revoke == "" {
			continue
		}
		if _, dup := seen[grant.Revoke]; dup {
			continue
		}
		seen[grant.Revoke] = struct{}{}
		statements = append(statements, grant.Revoke)
	}
	return statements, nil
}
