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

// TableGrant declares one relation's table-level DML posture for a runtime
// role. SELECT is always implied and always granted; the flags add
// INSERT/UPDATE/DELETE. TRUNCATE, REFERENCES, TRIGGER and MAINTAIN are not
// expressible and are never granted, which matches
// postgres.TablePrivilege's own contract.
//
// This package deliberately does NOT own the coordinator's table list. The
// authority is postgres.CoordinatorPosture(), and it cannot be imported here:
// internal/storage/postgres's own test files import this package, so a
// production import in the other direction would be an import cycle. The grant
// set is therefore INJECTED (see MigrationOptions.CoordinatorGrants) and
// derived from that posture by the caller, rather than transcribed into a
// second hand-maintained list that could silently drift from readiness.
type TableGrant struct {
	TableName   string
	AllowInsert bool
	AllowUpdate bool
	AllowDelete bool
}

// ColumnGrant declares one column-scoped privilege for a runtime role on a
// relation that is deliberately NOT granted table-wide. It exists for the same
// reason postgres.ColumnPrivilege does: worker_job_completion_fences.
// completed_at is server-owned, so a table-wide grant would let a runtime role
// forge a fence retention never reaps. A relation may appear in ColumnGrants
// or in TableGrants, never in both — a table-wide privilege on a
// column-scoped relation is exactly what the readiness posture check refuses.
//
// Privilege is one of SELECT, INSERT, UPDATE, REFERENCES: the four PostgreSQL
// column-grantable privileges. It is validated against that closed set rather
// than sanitized, because a privilege keyword cannot be quoted as an
// identifier.
type ColumnGrant struct {
	TableName  string
	ColumnName string
	Privilege  string
}

type MigrationOptions struct {
	Schema     string
	DomainRole string
	QueueRole  string
	// CoordinatorRole is optional. When empty no coordinator role is touched
	// at all, which keeps every pre-split caller behaving identically. When
	// set, CoordinatorGrants must be non-empty: a coordinator role that got a
	// REVOKE ALL and no GRANTs would leave its binaries fail-closed forever,
	// so an empty grant set is rejected rather than applied.
	CoordinatorRole   string
	CoordinatorGrants []TableGrant
	// CoordinatorColumnGrants is the column-scoped half of the same injected
	// posture. It is optional even when CoordinatorRole is set, because a
	// posture with no column-scoped privileges is legitimate; what is NOT
	// legitimate is supplying it without a role, which is rejected alongside
	// CoordinatorGrants for the same reason.
	CoordinatorColumnGrants []ColumnGrant
	Logger                  *slog.Logger
}

// columnGrantablePrivileges is PostgreSQL's closed set of column-level
// privileges. DELETE and TRUNCATE are not column-grantable at all, so a caller
// asking for either is a construction bug rather than a tightening.
var columnGrantablePrivileges = map[string]struct{}{
	"SELECT": {}, "INSERT": {}, "UPDATE": {}, "REFERENCES": {},
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
	var domainRoleEligible, queueRoleEligible, coordinatorRoleEligible bool
	// The coordinator arm is parameterized on options.CoordinatorRole and
	// short-circuits to TRUE when no coordinator role is configured, so a
	// pre-split caller sees the identical preflight it always did.
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
			),
			(
				$3 = ''
				OR EXISTS (
					SELECT 1 FROM pg_catalog.pg_roles
					WHERE rolname = $3
						AND rolcanlogin
						AND NOT rolsuper
						AND NOT rolcreatedb
						AND NOT rolcreaterole
						AND NOT rolreplication
						AND NOT rolbypassrls
				)
			)`,
		options.DomainRole,
		options.QueueRole,
		options.CoordinatorRole,
	).Scan(&migrationRole, &domainRoleEligible, &queueRoleEligible, &coordinatorRoleEligible); err != nil {
		return MigrationResult{}, migrationStageError("read migration role")
	}
	if err := validateRuntimeRolePreflight(
		migrationRole, domainRoleEligible, queueRoleEligible, coordinatorRoleEligible, options,
	); err != nil {
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
	if options.CoordinatorRole == "" {
		// No coordinator provisioning requested. Grants supplied without a role
		// are a caller bug, not a no-op: it would silently skip the grants the
		// caller believed it was applying.
		if len(options.CoordinatorGrants) != 0 || len(options.CoordinatorColumnGrants) != 0 {
			return ErrMigrationConfiguration
		}
		return nil
	}
	if !validIdentifier(options.CoordinatorRole) ||
		options.CoordinatorRole == options.DomainRole ||
		options.CoordinatorRole == options.QueueRole {
		return ErrMigrationConfiguration
	}
	if len(options.CoordinatorGrants) == 0 {
		return ErrMigrationConfiguration
	}
	seen := make(map[string]struct{}, len(options.CoordinatorGrants))
	for _, grant := range options.CoordinatorGrants {
		if !validIdentifier(grant.TableName) {
			return ErrMigrationConfiguration
		}
		if _, duplicate := seen[grant.TableName]; duplicate {
			return ErrMigrationConfiguration
		}
		seen[grant.TableName] = struct{}{}
	}
	seenColumns := make(map[string]struct{}, len(options.CoordinatorColumnGrants))
	for _, grant := range options.CoordinatorColumnGrants {
		if !validIdentifier(grant.TableName) || !validIdentifier(grant.ColumnName) {
			return ErrMigrationConfiguration
		}
		if _, grantable := columnGrantablePrivileges[grant.Privilege]; !grantable {
			return ErrMigrationConfiguration
		}
		// A relation granted table-wide AND column-scoped fails the readiness
		// posture check outright (it forbids any table-wide privilege on a
		// column-scoped relation), so applying both here would provision a
		// role that can never report ready.
		if _, tableWide := seen[grant.TableName]; tableWide {
			return ErrMigrationConfiguration
		}
		key := grant.TableName + "." + grant.ColumnName + ":" + grant.Privilege
		if _, duplicate := seenColumns[key]; duplicate {
			return ErrMigrationConfiguration
		}
		seenColumns[key] = struct{}{}
	}
	return nil
}

func validateRuntimeRolePreflight(
	migrationRole string,
	domainRoleEligible bool,
	queueRoleEligible bool,
	coordinatorRoleEligible bool,
	options MigrationOptions,
) error {
	if migrationRole == options.DomainRole || migrationRole == options.QueueRole || !domainRoleEligible || !queueRoleEligible {
		return ErrMigrationConfiguration
	}
	// The migration identity must not BE the coordinator role either: it holds
	// DDL and ownership, and granting the runtime posture to the role that owns
	// the objects would make the coordinator's own readiness check fail on the
	// "owns nothing" predicate while quietly holding every grant option.
	if options.CoordinatorRole != "" &&
		(migrationRole == options.CoordinatorRole || !coordinatorRoleEligible) {
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
	return append([]string{
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
		// worker_job_routes, scheduled_jobs, scheduled_sync_occurrences, and
		// fixed_schedule_occurrences are coordinator-exclusive under the
		// Option B two-role split (role-partition manifest, removed in
		// e23ede618; see git history at eda2d6b91) and deliberately have no
		// domain-role GRANT here. Their
		// COORDINATOR-side grants are no longer deploy-deferred: when
		// MigrationOptions.CoordinatorRole is set, coordinatorGrantStatements
		// below emits them from the injected posture in this same transaction,
		// so local dev and CI are self-provisioning for all three roles.
		// sync_configurations is SELECT-only for the domain role: its
		// coordinator-side FOR UPDATE row-locking use does not make the
		// domain role's own posture require UPDATE. sync_runs, by contrast,
		// is one of the six dual-grant ("both") tables and genuinely needs
		// UPDATE on the domain side (Fanout's FOR SHARE + the providersync
		// hot path).
		"DO $$ BEGIN IF to_regclass('public.sync_runs') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.sync_runs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_transport_routes') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_units') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.sync_run_units TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_watermarks') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_watermarks TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_outbox') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_dispatch_outbox TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_outbox') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.worker_job_outbox TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_configurations') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_configurations TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.organizations') IS NOT NULL THEN GRANT SELECT ON TABLE public.organizations TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.remaining_metric_runs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.remaining_metric_runs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.remaining_metric_partitions') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.remaining_metric_partitions TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.work_graph_execution_requests') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.work_graph_execution_requests TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.work_graph_execution_ledger') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.work_graph_execution_ledger TO " + domainRole + "; END IF; END $$",
		// Column-scoped, not table-wide: completed_at is server-owned and no
		// domain statement ever touches it. A table-wide grant would let the
		// domain role forge completed_at and mint a fence retention never
		// reaps.
		"DO $$ BEGIN IF to_regclass('public.worker_job_completion_fences') IS NOT NULL THEN GRANT SELECT (completion_key), INSERT (completion_key) ON TABLE public.worker_job_completion_fences TO " + domainRole + "; END IF; END $$",
		// CHAOS-3033 Option B manifest additions — domain-exclusive tables
		// (role-partition manifest, removed in e23ede618; see git history at eda2d6b91).
		"DO $$ BEGIN IF to_regclass('public.billing_notifications') IS NOT NULL THEN GRANT SELECT ON TABLE public.billing_notifications TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.daily_metrics_partitions') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.daily_metrics_partitions TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.daily_metrics_runs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.daily_metrics_runs TO " + domainRole + "; END IF; END $$",
		// UPDATE is required by PostgreSQL for SELECT ... FOR UPDATE row
		// locking; retention never updates conversation columns.
		"DO $$ BEGIN IF to_regclass('public.dev_conversations') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.dev_conversations TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.dev_conversation_tombstones') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.dev_conversation_tombstones TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_batch_payloads') IS NOT NULL THEN GRANT SELECT, DELETE ON TABLE public.external_ingest_batch_payloads TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_batches') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.external_ingest_batches TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_recompute_jobs') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.external_ingest_recompute_jobs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_rejections') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.external_ingest_rejections TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_sources') IS NOT NULL THEN GRANT SELECT ON TABLE public.external_ingest_sources TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.feature_flags') IS NOT NULL THEN GRANT SELECT ON TABLE public.feature_flags TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.org_feature_overrides') IS NOT NULL THEN GRANT SELECT ON TABLE public.org_feature_overrides TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.org_licenses') IS NOT NULL THEN GRANT SELECT ON TABLE public.org_licenses TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.provider_rate_limit_observations') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.provider_rate_limit_observations TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.report_runs') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.report_runs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.saved_reports') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.saved_reports TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.webhook_deliveries') IS NOT NULL THEN GRANT SELECT ON TABLE public.webhook_deliveries TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_runs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.worker_job_runs TO " + domainRole + "; END IF; END $$",
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
	}, coordinatorGrantStatements(options)...)
}

// coordinatorGrantStatements emits the coordinator role's privilege policy
// using exactly the technique the domain block above uses: REVOKE ALL first so
// the resulting posture is a function of this migration alone and never of
// whatever a previous revision or a hand-run script left behind, then selective
// GRANTs guarded by to_regclass so a table that does not exist yet is skipped
// instead of failing the migration, with every identifier sanitized.
//
// The table list is never written here. It is derived from
// options.CoordinatorGrants, which the caller builds from
// postgres.CoordinatorPosture() — the same declaration
// CheckCoordinatorAuthorization asserts against at readiness. That is what
// makes the grant side and the assertion side incapable of drifting: they are
// one list, not two that happen to agree.
//
// Returns nil when no coordinator role is configured, so every pre-split
// caller produces a byte-identical statement list to before.
func coordinatorGrantStatements(options MigrationOptions) []string {
	if options.CoordinatorRole == "" {
		return nil
	}
	coordinatorRole := pgx.Identifier{options.CoordinatorRole}.Sanitize()
	statements := []string{
		"DO $$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM %I', current_database(), '" + options.CoordinatorRole + "'); END $$",
		"GRANT USAGE ON SCHEMA public TO " + coordinatorRole,
		"REVOKE CREATE ON SCHEMA public FROM " + coordinatorRole,
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM " + coordinatorRole,
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM " + coordinatorRole,
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM " + coordinatorRole,
		// The coordinator is a public-schema control-plane role only. It never
		// touches River's own tables, so it gets the same fail-closed treatment
		// the domain role gets on the River schema.
		"REVOKE ALL PRIVILEGES ON SCHEMA " + pgx.Identifier{options.Schema}.Sanitize() + " FROM " + coordinatorRole,
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + pgx.Identifier{options.Schema}.Sanitize() + " FROM " + coordinatorRole,
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA " + pgx.Identifier{options.Schema}.Sanitize() + " FROM " + coordinatorRole,
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA " + pgx.Identifier{options.Schema}.Sanitize() + " FROM " + coordinatorRole,
	}
	for _, grant := range options.CoordinatorGrants {
		privileges := "SELECT"
		if grant.AllowInsert {
			privileges += ", INSERT"
		}
		if grant.AllowUpdate {
			privileges += ", UPDATE"
		}
		if grant.AllowDelete {
			privileges += ", DELETE"
		}
		qualified := "public." + grant.TableName
		statements = append(statements,
			"DO $$ BEGIN IF to_regclass('"+qualified+"') IS NOT NULL THEN GRANT "+
				privileges+" ON TABLE "+pgx.Identifier{"public", grant.TableName}.Sanitize()+
				" TO "+coordinatorRole+"; END IF; END $$",
		)
	}
	// Column-scoped grants are emitted after the table-wide ones and never
	// instead of them: the two sets are disjoint by construction
	// (ValidateMigrationOptions rejects a relation appearing in both), so
	// order carries no meaning beyond keeping the statement list stable and
	// diffable. The privilege keyword is taken from the validated closed set
	// above rather than sanitized, because it is a keyword and not an
	// identifier; the table and column are sanitized identifiers.
	for _, grant := range options.CoordinatorColumnGrants {
		qualified := "public." + grant.TableName
		statements = append(statements,
			"DO $$ BEGIN IF to_regclass('"+qualified+"') IS NOT NULL THEN GRANT "+
				grant.Privilege+" ("+pgx.Identifier{grant.ColumnName}.Sanitize()+
				") ON TABLE "+pgx.Identifier{"public", grant.TableName}.Sanitize()+
				" TO "+coordinatorRole+"; END IF; END $$",
		)
	}
	return statements
}
