package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// queueAuthorizationQuery proves the queue-control login has only the River
// and outbox capabilities it requires. In particular, it cannot create
// database objects, touch arbitrary semantic tables, or insert producer-owned
// outbox rows. Every predicate operates on effective privileges.
const queueAuthorizationQuery = `
WITH river_tables AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = $2
		AND class.relkind IN ('r', 'p')
), river_sequences AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = $2
		AND class.relkind = 'S'
), river_functions AS (
	SELECT procedure.oid
	FROM pg_catalog.pg_proc AS procedure
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
	WHERE namespace.nspname = $2
), other_public_relations AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind IN ('r', 'p', 'S', 'v', 'm', 'f')
		AND class.relname NOT IN ('worker_job_outbox', 'alembic_version')
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
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'worker_job_outbox'
			AND class.relkind IN ('r', 'p')
			AND has_table_privilege(current_user, class.oid, 'SELECT')
			AND has_table_privilege(current_user, class.oid, 'UPDATE')
			AND has_table_privilege(current_user, class.oid, 'DELETE')
			AND NOT has_table_privilege(current_user, class.oid, 'INSERT')
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
	)
	AND NOT EXISTS (
		SELECT 1
		FROM other_public_relations
		WHERE has_table_privilege(current_user, oid, 'SELECT')
			OR has_table_privilege(current_user, oid, 'INSERT')
			OR has_table_privilege(current_user, oid, 'UPDATE')
			OR has_table_privilege(current_user, oid, 'DELETE')
			OR has_table_privilege(current_user, oid, 'TRUNCATE')
			OR has_table_privilege(current_user, oid, 'REFERENCES')
			OR has_table_privilege(current_user, oid, 'TRIGGER')
	)
	AND has_schema_privilege(current_user, $2, 'USAGE')
	AND NOT has_schema_privilege(current_user, $2, 'CREATE')
	AND EXISTS (SELECT 1 FROM river_tables)
	AND NOT EXISTS (
		SELECT 1
		FROM river_tables
		WHERE NOT has_table_privilege(current_user, oid, 'SELECT')
			OR NOT has_table_privilege(current_user, oid, 'INSERT')
			OR NOT has_table_privilege(current_user, oid, 'UPDATE')
			OR NOT has_table_privilege(current_user, oid, 'DELETE')
			OR has_table_privilege(current_user, oid, 'TRUNCATE')
			OR has_table_privilege(current_user, oid, 'REFERENCES')
			OR has_table_privilege(current_user, oid, 'TRIGGER')
	)
	AND NOT EXISTS (
		SELECT 1
		FROM river_sequences
		WHERE NOT has_sequence_privilege(current_user, oid, 'USAGE')
			OR NOT has_sequence_privilege(current_user, oid, 'SELECT')
			OR NOT has_sequence_privilege(current_user, oid, 'UPDATE')
	)
	AND NOT EXISTS (
		SELECT 1
		FROM river_functions
		WHERE NOT has_function_privilege(current_user, oid, 'EXECUTE')
	)`

// CheckQueueAuthorization is a read-only readiness check for the dedicated
// River queue-control pool. It returns a stable failure category only.
func CheckQueueAuthorization(ctx context.Context, pool *pgxpool.Pool, expectedRole, riverSchema string) error {
	if pool == nil || !validRuntimeIdentifier(expectedRole) || !validRuntimeIdentifier(riverSchema) {
		return ErrUnavailable
	}
	var authorized bool
	if err := pool.QueryRow(ctx, queueAuthorizationQuery, expectedRole, riverSchema).Scan(&authorized); err != nil || !authorized {
		return ErrUnavailable
	}
	return nil
}
