package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// domainAuthorizationQuery proves the configured domain identity has exactly
// the semantic runtime posture. The expected role and River schema are query
// parameters, never interpolated identifiers. Catalog predicates use
// effective privileges so inherited administrator or DDL access closes
// readiness as well as direct grants.
const domainAuthorizationQuery = `
WITH domain_tables AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind IN ('r', 'p')
		AND class.relname <> 'alembic_version'
), domain_sequences AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind = 'S'
), river_relations AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = $2
		AND class.relkind IN ('r', 'p', 'S', 'v', 'm', 'f')
), river_functions AS (
	SELECT procedure.oid
	FROM pg_catalog.pg_proc AS procedure
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
	WHERE namespace.nspname = $2
), elevated_roles AS (
	SELECT role.oid
	FROM pg_catalog.pg_roles AS role
	WHERE role.rolsuper
		OR role.rolcreatedb
		OR role.rolcreaterole
		OR role.rolreplication
		OR role.rolbypassrls
)
SELECT
	current_user = $1
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_roles
		WHERE rolname = current_user
			AND rolcanlogin
			AND NOT rolsuper
			AND NOT rolcreatedb
			AND NOT rolcreaterole
			AND NOT rolreplication
			AND NOT rolbypassrls
	)
	AND NOT EXISTS (
		SELECT 1 FROM elevated_roles
		WHERE pg_has_role(current_user, oid, 'USAGE')
	)
	AND NOT has_database_privilege(current_user, current_database(), 'CREATE')
	AND has_schema_privilege(current_user, 'public', 'USAGE')
	AND NOT has_schema_privilege(current_user, 'public', 'CREATE')
	AND EXISTS (SELECT 1 FROM domain_tables)
	AND NOT EXISTS (
		SELECT 1
		FROM domain_tables
		WHERE NOT has_table_privilege(current_user, oid, 'SELECT')
			OR NOT has_table_privilege(current_user, oid, 'INSERT')
			OR NOT has_table_privilege(current_user, oid, 'UPDATE')
			OR NOT has_table_privilege(current_user, oid, 'DELETE')
	)
	AND NOT EXISTS (
		SELECT 1
		FROM domain_sequences
		WHERE NOT has_sequence_privilege(current_user, oid, 'USAGE')
			OR NOT has_sequence_privilege(current_user, oid, 'SELECT')
			OR NOT has_sequence_privilege(current_user, oid, 'UPDATE')
	)
	AND NOT EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'alembic_version'
			AND has_table_privilege(current_user, class.oid, 'SELECT')
	)
	AND NOT has_schema_privilege(current_user, $2, 'USAGE')
	AND NOT EXISTS (
		SELECT 1
		FROM river_relations
		WHERE has_table_privilege(current_user, oid, 'SELECT')
			OR has_table_privilege(current_user, oid, 'INSERT')
			OR has_table_privilege(current_user, oid, 'UPDATE')
			OR has_table_privilege(current_user, oid, 'DELETE')
	)
	AND NOT EXISTS (
		SELECT 1
		FROM river_functions
		WHERE has_function_privilege(current_user, oid, 'EXECUTE')
	)`

// CheckDomainAuthorization is a read-only readiness check for the semantic
// PostgreSQL pool. It binds the active login to the declared domain role and
// never exposes catalog or driver details that could contain connection
// material.
func CheckDomainAuthorization(ctx context.Context, pool *pgxpool.Pool, expectedRole, riverSchema string) error {
	if pool == nil || !validRuntimeIdentifier(expectedRole) || !validRuntimeIdentifier(riverSchema) {
		return ErrUnavailable
	}
	var authorized bool
	if err := pool.QueryRow(ctx, domainAuthorizationQuery, expectedRole, riverSchema).Scan(&authorized); err != nil || !authorized {
		return ErrUnavailable
	}
	return nil
}
