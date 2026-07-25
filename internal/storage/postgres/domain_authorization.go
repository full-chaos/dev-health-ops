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
WITH required_table_privileges(table_name, allow_insert, allow_update) AS (
	VALUES
		('integrations', false, false),
		('integration_sources', false, false),
		('integration_datasets', false, false),
		('integration_credentials', false, false),
		('sync_runs', false, false),
		('worker_job_routes', false, false),
		('sync_dispatch_transport_routes', false, false),
		('sync_run_units', false, true),
		('sync_watermarks', true, true),
		('sync_dispatch_outbox', true, true),
		('worker_job_outbox', true, false)
), required_tables AS (
	SELECT
		class.oid,
		required.allow_insert,
		required.allow_update
	FROM required_table_privileges AS required
	JOIN pg_catalog.pg_class AS class ON class.relname = required.table_name
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind IN ('r', 'p')
), other_public_relations AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind IN ('r', 'p', 'v', 'm', 'f')
		AND NOT EXISTS (
			SELECT 1
			FROM required_table_privileges AS required
			WHERE required.table_name = class.relname
		)
), public_sequences AS (
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
		AND class.relkind IN ('r', 'p', 'v', 'm', 'f')
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
), public_functions AS (
	SELECT procedure.oid
	FROM pg_catalog.pg_proc AS procedure
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
	WHERE namespace.nspname = 'public'
), member_roles AS (
	SELECT role.oid
	FROM pg_catalog.pg_roles AS role
	WHERE role.rolname <> current_user
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
		SELECT 1 FROM member_roles
		WHERE pg_has_role(current_user, oid, 'MEMBER')
	)
	AND NOT has_database_privilege(current_user, current_database(), 'CREATE')
	AND NOT has_database_privilege(current_user, current_database(), 'TEMPORARY')
	AND has_schema_privilege(current_user, 'public', 'USAGE')
	AND NOT has_schema_privilege(current_user, 'public', 'CREATE')
	AND (
		SELECT count(*) FROM required_tables
	) = (
		SELECT count(*) FROM required_table_privileges
	)
	AND NOT EXISTS (
		SELECT 1
		FROM required_tables
		WHERE NOT has_table_privilege(current_user, oid, 'SELECT')
			OR has_table_privilege(current_user, oid, 'INSERT') <> allow_insert
			OR (
				NOT allow_insert
				AND has_any_column_privilege(current_user, oid, 'INSERT')
			)
			OR has_table_privilege(current_user, oid, 'UPDATE') <> allow_update
			OR (
				NOT allow_update
				AND has_any_column_privilege(current_user, oid, 'UPDATE')
			)
			OR has_table_privilege(current_user, oid, 'DELETE')
			OR has_table_privilege(current_user, oid, 'TRUNCATE')
			OR has_table_privilege(current_user, oid, 'REFERENCES')
			OR has_any_column_privilege(current_user, oid, 'REFERENCES')
			OR has_table_privilege(current_user, oid, 'TRIGGER')
			OR CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND NOT EXISTS (
		SELECT 1
		FROM other_public_relations
		WHERE has_table_privilege(current_user, oid, 'SELECT')
			OR has_table_privilege(current_user, oid, 'INSERT')
			OR has_table_privilege(current_user, oid, 'UPDATE')
			OR has_table_privilege(current_user, oid, 'DELETE')
			OR has_any_column_privilege(
				current_user, oid, 'SELECT, INSERT, UPDATE, REFERENCES'
			)
			OR has_table_privilege(current_user, oid, 'TRUNCATE')
			OR has_table_privilege(current_user, oid, 'REFERENCES')
			OR has_table_privilege(current_user, oid, 'TRIGGER')
			OR CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND NOT EXISTS (
		SELECT 1
		FROM public_sequences
		WHERE has_sequence_privilege(current_user, oid, 'USAGE')
			OR has_sequence_privilege(current_user, oid, 'SELECT')
			OR has_sequence_privilege(current_user, oid, 'UPDATE')
	)
	AND NOT has_schema_privilege(current_user, $2, 'USAGE')
	AND NOT EXISTS (
		SELECT 1
		FROM river_relations
		WHERE has_table_privilege(current_user, oid, 'SELECT')
			OR has_table_privilege(current_user, oid, 'INSERT')
			OR has_table_privilege(current_user, oid, 'UPDATE')
			OR has_table_privilege(current_user, oid, 'DELETE')
			OR has_any_column_privilege(
				current_user, oid, 'SELECT, INSERT, UPDATE, REFERENCES'
			)
			OR has_table_privilege(current_user, oid, 'TRUNCATE')
			OR has_table_privilege(current_user, oid, 'REFERENCES')
			OR has_table_privilege(current_user, oid, 'TRIGGER')
			OR CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND NOT EXISTS (
		SELECT 1
		FROM river_sequences
		WHERE has_sequence_privilege(current_user, oid, 'USAGE')
			OR has_sequence_privilege(current_user, oid, 'SELECT')
			OR has_sequence_privilege(current_user, oid, 'UPDATE')
	)
	AND NOT EXISTS (
		SELECT 1
		FROM river_functions
		WHERE has_function_privilege(current_user, oid, 'EXECUTE')
	)
	AND NOT EXISTS (
		SELECT 1
		FROM public_functions
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
