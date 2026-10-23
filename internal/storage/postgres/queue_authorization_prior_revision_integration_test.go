//go:build integration

package postgres

import "github.com/full-chaos/dev-health-ops/internal/storage/roleacl"

// queueAuthorizationQueryPriorRevision is the queue role posture query as it
// stood before the owns-nothing and no-grant-option predicates joined it. It is
// kept only so the integration suite can show, in one run, that each planted
// posture is answered "authorized" by this revision and refused by
// queueAuthorizationQuery. It is never run by production code.
const queueAuthorizationQueryPriorRevision = `
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
), other_public_tables AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind IN ('r', 'p', 'v', 'm', 'f')
		AND class.relname NOT IN (
			'worker_job_outbox',
			'worker_job_delivery_abandonments',
			'worker_job_completion_fences',
			'sync_dispatch_outbox',
			'sync_dispatch_transport_routes',
			'sync_runs',
			'sync_run_units',
			'daily_metrics_runs',
			'daily_metrics_partitions',
			'work_graph_execution_requests'
		)
), public_sequences AS (
	SELECT class.oid
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
		AND class.relkind = 'S'
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
	` + roleacl.IdentityPredicateSQL + `
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
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'sync_run_units'
			AND class.relkind IN ('r', 'p')
			AND has_table_privilege(current_user, class.oid, 'SELECT')
			AND NOT has_table_privilege(current_user, class.oid, 'INSERT')
			AND NOT has_table_privilege(current_user, class.oid, 'UPDATE')
			AND NOT has_any_column_privilege(
				current_user, class.oid, 'INSERT, UPDATE, REFERENCES'
			)
			AND NOT has_table_privilege(current_user, class.oid, 'DELETE')
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
			AND NOT CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, class.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'sync_runs'
			AND class.relkind IN ('r', 'p')
			AND has_table_privilege(current_user, class.oid, 'SELECT')
			AND NOT has_table_privilege(current_user, class.oid, 'INSERT')
			AND NOT has_table_privilege(current_user, class.oid, 'UPDATE')
			AND NOT has_any_column_privilege(
				current_user, class.oid, 'INSERT, UPDATE, REFERENCES'
			)
			AND NOT has_table_privilege(current_user, class.oid, 'DELETE')
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
			AND NOT CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, class.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'daily_metrics_runs'
			AND class.relkind IN ('r', 'p')
			AND has_table_privilege(current_user, class.oid, 'SELECT')
			AND NOT has_table_privilege(current_user, class.oid, 'INSERT')
			AND NOT has_table_privilege(current_user, class.oid, 'UPDATE')
			AND NOT has_any_column_privilege(
				current_user, class.oid, 'INSERT, UPDATE, REFERENCES'
			)
			AND NOT has_table_privilege(current_user, class.oid, 'DELETE')
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
			AND NOT CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, class.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'daily_metrics_partitions'
			AND class.relkind IN ('r', 'p')
			AND has_table_privilege(current_user, class.oid, 'SELECT')
			AND NOT has_table_privilege(current_user, class.oid, 'INSERT')
			AND NOT has_table_privilege(current_user, class.oid, 'UPDATE')
			AND NOT has_any_column_privilege(
				current_user, class.oid, 'INSERT, UPDATE, REFERENCES'
			)
			AND NOT has_table_privilege(current_user, class.oid, 'DELETE')
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
			AND NOT CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, class.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'work_graph_execution_requests'
			AND class.relkind IN ('r', 'p')
			AND has_table_privilege(current_user, class.oid, 'SELECT')
			AND NOT has_table_privilege(current_user, class.oid, 'INSERT')
			AND NOT has_table_privilege(current_user, class.oid, 'UPDATE')
			AND NOT has_any_column_privilege(
				current_user, class.oid, 'INSERT, UPDATE, REFERENCES'
			)
			AND NOT has_table_privilege(current_user, class.oid, 'DELETE')
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
			AND NOT CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, class.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND NOT EXISTS (
		SELECT 1 FROM member_roles
		WHERE pg_has_role(current_user, oid, 'MEMBER')
	)
	AND NOT has_database_privilege(current_user, current_database(), 'CREATE')
	AND NOT has_database_privilege(current_user, current_database(), 'TEMPORARY')
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
			AND NOT has_any_column_privilege(
				current_user, class.oid, 'INSERT, REFERENCES'
			)
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
			AND NOT CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, class.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'worker_job_delivery_abandonments'
			AND class.relkind IN ('r', 'p')
			AND has_table_privilege(current_user, class.oid, 'SELECT')
			AND has_table_privilege(current_user, class.oid, 'INSERT')
			AND NOT has_table_privilege(current_user, class.oid, 'UPDATE')
			AND NOT has_any_column_privilege(
				current_user, class.oid, 'UPDATE, REFERENCES'
			)
			AND NOT has_table_privilege(current_user, class.oid, 'DELETE')
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
			AND NOT CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, class.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'worker_job_completion_fences'
			AND class.relkind IN ('r', 'p')
			AND has_table_privilege(current_user, class.oid, 'SELECT')
			AND has_table_privilege(current_user, class.oid, 'UPDATE')
			AND has_table_privilege(current_user, class.oid, 'DELETE')
			AND NOT has_table_privilege(current_user, class.oid, 'INSERT')
			AND NOT has_any_column_privilege(
				current_user, class.oid, 'INSERT, REFERENCES'
			)
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
			AND NOT CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, class.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'sync_dispatch_outbox'
			AND class.relkind IN ('r', 'p')
			AND has_table_privilege(current_user, class.oid, 'SELECT')
			AND has_table_privilege(current_user, class.oid, 'UPDATE')
			AND NOT has_table_privilege(current_user, class.oid, 'INSERT')
			AND NOT has_any_column_privilege(
				current_user, class.oid, 'INSERT, REFERENCES'
			)
			AND NOT has_table_privilege(current_user, class.oid, 'DELETE')
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
			AND NOT CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, class.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'public'
			AND class.relname = 'sync_dispatch_transport_routes'
			AND class.relkind IN ('r', 'p')
			AND has_table_privilege(current_user, class.oid, 'SELECT')
			AND NOT has_table_privilege(current_user, class.oid, 'INSERT')
			AND NOT has_table_privilege(current_user, class.oid, 'UPDATE')
			AND NOT has_any_column_privilege(
				current_user, class.oid, 'INSERT, UPDATE, REFERENCES'
			)
			AND NOT has_table_privilege(current_user, class.oid, 'DELETE')
			AND NOT has_table_privilege(current_user, class.oid, 'TRUNCATE')
			AND NOT has_table_privilege(current_user, class.oid, 'REFERENCES')
			AND NOT has_table_privilege(current_user, class.oid, 'TRIGGER')
			AND NOT CASE
				WHEN current_setting('server_version_num')::integer >= 170000
				THEN has_table_privilege(current_user, class.oid, 'MAINTAIN')
				ELSE false
			END
	)
	AND NOT EXISTS (
		SELECT 1
		FROM other_public_tables
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
	AND NOT EXISTS (
		SELECT 1
		FROM public_functions
		WHERE has_function_privilege(current_user, oid, 'EXECUTE')
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
