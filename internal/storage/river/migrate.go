package riverstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

const (
	PinnedRiverVersion  = "0.40.0"
	PinnedSchemaVersion = 7
	migrationLockKey    = int64(30330037)
)

var (
	ErrPinnedMigrationMismatch = errors.New("bundled River migrations do not match the pinned schema version")
	ErrMigrationConfiguration  = errors.New("invalid River migration role configuration")
	ErrMigrationFailed         = errors.New("River migration failed")
	ErrSchemaNotCurrent        = errors.New("River schema is not at the pinned version")
)

type MigrationOptions struct {
	Schema     string
	DomainRole string
	QueueRole  string
	Logger     *slog.Logger
}

type MigrationResult struct {
	AppliedVersions []int
	CurrentVersion  int
}

// ApplyPinnedMigrations is the only production schema-changing River API in
// this repository. It is intended exclusively for the one-shot migration
// command. Runtime pool/client construction never calls it.
func ApplyPinnedMigrations(
	ctx context.Context,
	pool *pgxpool.Pool,
	options MigrationOptions,
) (MigrationResult, error) {
	if pool == nil || pool.Config().MaxConns < 2 || ValidateMigrationOptions(options) != nil {
		return MigrationResult{}, ErrMigrationConfiguration
	}
	driver := riverpgxv5.New(pool)
	migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
		Schema: options.Schema,
		Logger: options.Logger,
	})
	if err != nil || validatePinnedBundle(migrator.AllVersions()) != nil {
		return MigrationResult{}, ErrPinnedMigrationMismatch
	}

	lockConnection, err := pool.Acquire(ctx)
	if err != nil {
		return MigrationResult{}, migrationStageError("acquire migration connection")
	}
	defer lockConnection.Release()

	if _, err := lockConnection.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return MigrationResult{}, migrationStageError("acquire migration lock")
	}
	defer func() {
		unlockContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = lockConnection.Exec(unlockContext, "SELECT pg_advisory_unlock($1)", migrationLockKey)
	}()
	var migrationRole string
	var domainRoleEligible, queueRoleEligible bool
	if err := lockConnection.QueryRow(
		ctx,
		`SELECT
			current_user,
			EXISTS (
				SELECT 1 FROM pg_catalog.pg_roles
				WHERE rolname = $1
					AND rolcanlogin
					AND NOT rolsuper
					AND NOT rolcreatedb
					AND NOT rolcreaterole
					AND NOT rolreplication
					AND NOT rolbypassrls
			),
			EXISTS (
				SELECT 1 FROM pg_catalog.pg_roles
				WHERE rolname = $2
					AND rolcanlogin
					AND NOT rolsuper
					AND NOT rolcreatedb
					AND NOT rolcreaterole
					AND NOT rolreplication
					AND NOT rolbypassrls
			)`,
		options.DomainRole,
		options.QueueRole,
	).Scan(&migrationRole, &domainRoleEligible, &queueRoleEligible); err != nil {
		return MigrationResult{}, migrationStageError("read migration role")
	}
	if err := validateRuntimeRolePreflight(migrationRole, domainRoleEligible, queueRoleEligible, options); err != nil {
		return MigrationResult{}, err
	}

	schema := pgx.Identifier{options.Schema}.Sanitize()
	if _, err := lockConnection.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
		return MigrationResult{}, migrationStageError("create River schema")
	}

	result, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{
		TargetVersion: PinnedSchemaVersion,
	})
	if err != nil {
		return MigrationResult{}, migrationStageError("apply pinned schema")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return MigrationResult{}, migrationStageError("begin privilege transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := applyRuntimeGrants(ctx, tx, options); err != nil {
		return MigrationResult{}, migrationStageError("apply runtime grants")
	}
	if err := tx.Commit(ctx); err != nil {
		return MigrationResult{}, migrationStageError("commit transaction")
	}

	status, err := CheckSchema(ctx, pool, options.Schema, options.Logger)
	if err != nil {
		return MigrationResult{}, err
	}
	applied := make([]int, 0, len(result.Versions))
	for _, version := range result.Versions {
		applied = append(applied, version.Version)
	}
	return MigrationResult{AppliedVersions: applied, CurrentVersion: status}, nil
}

func migrationStageError(stage string) error {
	return fmt.Errorf("%w during %s", ErrMigrationFailed, stage)
}

// CheckSchema is read-only and requires the exact pinned migration prefix.
func CheckSchema(ctx context.Context, pool *pgxpool.Pool, schema string, logger *slog.Logger) (int, error) {
	if pool == nil || !validIdentifier(schema) {
		return 0, ErrMigrationConfiguration
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Schema: schema, Logger: logger})
	if err != nil || validatePinnedBundle(migrator.AllVersions()) != nil {
		return 0, ErrPinnedMigrationMismatch
	}
	versions, err := migrator.ExistingVersions(ctx)
	if err != nil || len(versions) != PinnedSchemaVersion {
		return 0, ErrSchemaNotCurrent
	}
	for index, version := range versions {
		if version.Version != index+1 {
			return 0, ErrSchemaNotCurrent
		}
	}
	return versions[len(versions)-1].Version, nil
}

func validatePinnedBundle(versions []rivermigrate.Migration) error {
	if len(versions) != PinnedSchemaVersion {
		return ErrPinnedMigrationMismatch
	}
	for index, version := range versions {
		if version.Version != index+1 {
			return ErrPinnedMigrationMismatch
		}
	}
	return nil
}

func ValidateMigrationOptions(options MigrationOptions) error {
	if !validIdentifier(options.Schema) || !validIdentifier(options.DomainRole) || !validIdentifier(options.QueueRole) {
		return ErrMigrationConfiguration
	}
	if options.DomainRole == options.QueueRole {
		return ErrMigrationConfiguration
	}
	return nil
}

func validateRuntimeRolePreflight(
	migrationRole string,
	domainRoleEligible bool,
	queueRoleEligible bool,
	options MigrationOptions,
) error {
	if migrationRole == options.DomainRole || migrationRole == options.QueueRole || !domainRoleEligible || !queueRoleEligible {
		return ErrMigrationConfiguration
	}
	return nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 63 {
		return false
	}
	for index, char := range value {
		if (char >= 'a' && char <= 'z') || char == '_' || (index > 0 && char >= '0' && char <= '9') {
			continue
		}
		return false
	}
	return true
}

func applyRuntimeGrants(ctx context.Context, tx pgx.Tx, options MigrationOptions) error {
	for _, statement := range runtimeGrantStatements(options) {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("apply River runtime privilege policy")
		}
	}
	return nil
}

func runtimeGrantStatements(options MigrationOptions) []string {
	schema := pgx.Identifier{options.Schema}.Sanitize()
	domainRole := pgx.Identifier{options.DomainRole}.Sanitize()
	queueRole := pgx.Identifier{options.QueueRole}.Sanitize()
	return []string{
		"DO $$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC, %I, %I', current_database(), '" + options.DomainRole + "', '" + options.QueueRole + "'); END $$",
		"GRANT USAGE ON SCHEMA public TO " + domainRole,
		"REVOKE CREATE ON SCHEMA public FROM " + domainRole,
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM " + domainRole,
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM " + domainRole,
		"DO $$ BEGIN IF to_regclass('public.alembic_version') IS NOT NULL THEN REVOKE ALL PRIVILEGES ON TABLE public.alembic_version FROM " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.integrations') IS NOT NULL THEN GRANT SELECT ON TABLE public.integrations TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.integration_sources') IS NOT NULL THEN GRANT SELECT ON TABLE public.integration_sources TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.integration_datasets') IS NOT NULL THEN GRANT SELECT ON TABLE public.integration_datasets TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.integration_credentials') IS NOT NULL THEN GRANT SELECT ON TABLE public.integration_credentials TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_runs') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_runs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_routes') IS NOT NULL THEN GRANT SELECT ON TABLE public.worker_job_routes TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_transport_routes') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_units') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.sync_run_units TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_watermarks') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_watermarks TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_outbox') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_dispatch_outbox TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_outbox') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.worker_job_outbox TO " + domainRole + "; END IF; END $$",
		"GRANT USAGE ON SCHEMA public TO " + queueRole,
		"REVOKE CREATE ON SCHEMA public FROM " + queueRole,
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM " + queueRole,
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM " + queueRole,
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, " + domainRole + ", " + queueRole,
		"DO $$ BEGIN IF to_regclass('public.worker_job_outbox') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_completion_fences') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_completion_fences TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_outbox') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.sync_dispatch_outbox TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_transport_routes') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO " + queueRole + "; END IF; END $$",
		"REVOKE ALL PRIVILEGES ON SCHEMA " + schema + " FROM PUBLIC",
		"REVOKE ALL PRIVILEGES ON SCHEMA " + schema + " FROM " + domainRole,
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schema + " FROM PUBLIC, " + domainRole,
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA " + schema + " FROM PUBLIC, " + domainRole,
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA " + schema + " FROM PUBLIC, " + domainRole,
		"GRANT USAGE ON SCHEMA " + schema + " TO " + queueRole,
		"REVOKE CREATE ON SCHEMA " + schema + " FROM " + queueRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA " + schema + " TO " + queueRole,
		"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA " + schema + " TO " + queueRole,
		"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA " + schema + " TO " + queueRole,
		"ALTER DEFAULT PRIVILEGES IN SCHEMA " + schema + " GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO " + queueRole,
		"ALTER DEFAULT PRIVILEGES IN SCHEMA " + schema + " GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO " + queueRole,
		"ALTER DEFAULT PRIVILEGES IN SCHEMA " + schema + " REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
		"ALTER DEFAULT PRIVILEGES IN SCHEMA " + schema + " GRANT EXECUTE ON FUNCTIONS TO " + queueRole,
	}
}
