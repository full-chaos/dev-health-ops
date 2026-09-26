package riverstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
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
	// ErrSchemaCheckUnavailable reports that CheckSchema's own query against
	// the queue-control pool failed -- a connection/pool error (e.g.
	// CHAOS-5469's pgbouncer pool exhaustion), never a real schema mismatch.
	// Before this existed, CheckSchema collapsed both causes into
	// ErrSchemaNotCurrent, which read as "the schema is genuinely wrong" on
	// a fleet whose schema had never actually been checked -- the readiness
	// log line and the CHAOS-5435 logDependencyCheckFailure caller both
	// name the check by its wrapped error text, so this sentinel is what
	// lets an operator tell "pool exhausted" apart from "run migrate".
	ErrSchemaCheckUnavailable = errors.New("River schema check could not run")
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
	// CoordinatorSequences are the exact public-schema sequences the
	// coordinator may use. Each receives USAGE only; all other sequence
	// privileges remain revoked.
	CoordinatorSequences []string
	// APIRole is the dho api Service's Postgres role (API_DATABASE_ROLE).
	// Optional, and applied only when the role exists: it is provisioned
	// once by an operator (scripts/worker/provision_river_roles.sql with
	// api_role), so a migration run before that skips it and says so. When
	// it exists it must be an eligible least-privilege login, and it gets the
	// same REVOKE-ALL-then-GRANT treatment as the coordinator, from
	// APIGrants/APIColumnGrants/APISequences, which the caller derives from
	// postgres.APIPosture() -- the list the api's readiness check asserts.
	// Unlike CoordinatorGrants, an empty APIGrants is valid: the api posture
	// starts empty and grows one route at a time.
	APIRole         string
	APIGrants       []TableGrant
	APIColumnGrants []ColumnGrant
	APISequences    []string
	// QueryAPIRole is the query-api service's Postgres role
	// (QUERY_API_DATABASE_ROLE). Optional, and applied only when the role
	// exists: an empty name, or a role not yet provisioned, skips the leg.
	//
	// Since CHAOS-6804 this leg has the same policy as the api role: REVOKE ALL
	// then GRANT the full manifest, so the role's posture is a function of this
	// migration alone. It therefore refuses (ErrMigrationConfiguration) a role
	// that is not an eligible least-privilege login, that is the migration
	// identity, or that owns any database, schema, relation or function: a
	// REVOKE would strip an owner (the registry-table owner query-api logs in
	// as today) of the privileges it needs on its own objects. QueryAPIGrants is
	// derived by the caller from postgres.QueryAPIPosture(), the list the
	// readiness check asserts.
	QueryAPIRole   string
	QueryAPIGrants []TableGrant
	// PostureManifestDigest is the sha256 hex digest CHAOS-5437's lockstep
	// guard stamps into worker_posture_manifest_applied on every run
	// (postgres.PostureManifestDigest() -- injected the same way
	// CoordinatorGrants is, since this package cannot import
	// internal/storage/postgres). Optional: empty skips the table entirely,
	// so every pre-existing caller that does not set it (the large body of
	// integration tests exercising ApplyPinnedMigrations directly) behaves
	// exactly as before.
	PostureManifestDigest string
	// PostureManifestBuildID identifies the migrate binary that applied
	// PostureManifestDigest, for operator debugging only -- never compared
	// programmatically. Ignored when PostureManifestDigest is empty; when
	// PostureManifestDigest is set and this is empty, PostureManifestDigest
	// itself is used as the build identity.
	PostureManifestBuildID string
	// NativeRiverRoutes are the job kinds whose checked-in migration policy is
	// route "river" with rollback "none". Each one without a
	// public.worker_job_routes row gets one (transport river, unpaused,
	// generation 1); an existing row is never read for anything but its
	// presence and never changed. A registered kind with no row makes
	// internal/jobroute resolve it as unknown, which fails the relay and the
	// reconciler for every kind, so this is what lets a kind introduced in
	// Go reach production without a hand-written seed. Optional: empty seeds
	// nothing.
	NativeRiverRoutes []string
	Logger            *slog.Logger
}

// postureManifestAppliedTable mirrors postgres.PostureManifestAppliedTable's
// string value. It cannot be imported (see the CoordinatorGrants doc comment
// on why this package never imports internal/storage/postgres); the two are
// kept honest by internal/storage/river's own integration test, which
// creates this table via ApplyPinnedMigrations and then reads it back
// through postgres.CheckPostureManifestLockstep -- a name mismatch would
// make that call see no applied row at all and fail the test.
const postureManifestAppliedTable = "worker_posture_manifest_applied"

// jobKindPattern is the registry's own kind grammar
// (contracts/jobs/v1/registry.schema.json).
var jobKindPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)

// columnGrantablePrivileges is PostgreSQL's closed set of column-level
// privileges. DELETE and TRUNCATE are not column-grantable at all, so a caller
// asking for either is a construction bug rather than a tightening.
var columnGrantablePrivileges = map[string]struct{}{
	"SELECT": {}, "INSERT": {}, "UPDATE": {}, "REFERENCES": {},
}

type MigrationResult struct {
	AppliedVersions []int
	CurrentVersion  int
	// SeededRoutes are the NativeRiverRoutes this run inserted; PresentRoutes
	// already had a row and were left untouched. RouteTableAbsent reports
	// that public.worker_job_routes does not exist yet (the application
	// schema migration has not run), so nothing was seeded.
	SeededRoutes     []string
	PresentRoutes    []string
	RouteTableAbsent bool
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
	var apiRoleExists, apiRoleEligible bool
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
			),
			$4 <> '' AND EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $4),
			EXISTS (
				SELECT 1 FROM pg_catalog.pg_roles
				WHERE rolname = $4
					AND rolcanlogin
					AND NOT rolsuper
					AND NOT rolcreatedb
					AND NOT rolcreaterole
					AND NOT rolreplication
					AND NOT rolbypassrls
			)`,
		options.DomainRole,
		options.QueueRole,
		options.CoordinatorRole,
		options.APIRole,
	).Scan(&migrationRole, &domainRoleEligible, &queueRoleEligible, &coordinatorRoleEligible,
		&apiRoleExists, &apiRoleEligible); err != nil {
		return MigrationResult{}, migrationStageError("read migration role")
	}
	if err := validateRuntimeRolePreflight(
		migrationRole, domainRoleEligible, queueRoleEligible, coordinatorRoleEligible, options,
	); err != nil {
		return MigrationResult{}, err
	}
	options, err = resolveAPIRole(ctx, options, migrationRole, apiRoleExists, apiRoleEligible)
	if err != nil {
		return MigrationResult{}, err
	}
	if options.QueryAPIRole != "" {
		var queryAPIRoleExists, queryAPIRoleEligible, queryAPIRoleOwnsNothing bool
		if err := lockConnection.QueryRow(
			ctx,
			`SELECT
				EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1),
				`+roleacl.RoleAttributesSQL+` AND `+roleacl.MembershipFreeSQL+`,
				`+roleacl.OwnsNothingSQL,
			options.QueryAPIRole,
		).Scan(&queryAPIRoleExists, &queryAPIRoleEligible, &queryAPIRoleOwnsNothing); err != nil {
			return MigrationResult{}, migrationStageError("read query-api role")
		}
		options, err = resolveQueryAPIRole(
			ctx, options, migrationRole, queryAPIRoleExists, queryAPIRoleEligible && queryAPIRoleOwnsNothing,
		)
		if err != nil {
			return MigrationResult{}, err
		}
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
	// CHAOS-5437: created before applyRuntimeGrants, in the SAME transaction,
	// so the grant statement below (guarded by to_regclass, like every other
	// grant in this file) sees the table as already existing on the very
	// first run -- DDL is visible to later statements in the same
	// transaction. This table is Go-migrate's own schema, never Alembic's:
	// nothing outside ApplyPinnedMigrations creates or writes it.
	if options.PostureManifestDigest != "" {
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.`+postureManifestAppliedTable+` (
			manifest_digest text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now(),
			migrate_build text NOT NULL
		)`); err != nil {
			logMigrationStageFailure(ctx, options.Logger, "create posture manifest table", err)
			return MigrationResult{}, migrationStageError("create posture manifest table")
		}
	}
	// CHAOS-5615: queued_contract_versions readiness
	// (QueueTelemetrySampler.CheckAvailableContractVersions) filters
	// river_job down to state='available' rows in the configured queues, then
	// extracts args->>'contract_version' from every one of them. Without a
	// supporting index this is a sequential scan over the WHOLE river_job
	// table -- every completed/discarded job ever run, not just the live
	// backlog -- so its cost scales with total table history, not with the
	// available backlog. On a restored backlog (11,428 available `metrics`
	// rows, the live incident this closes) that scan blew the query's
	// timeout budget. This partial index bounds the scan to exactly the
	// state='available' rows in the configured queues (measured on a
	// synthetic 12k-row backlog against a 400k-row table: the planner
	// switches from a sequential scan of the whole table to an index scan
	// touching only the matching subset -- see
	// TestQueueTelemetryContractVersionScanBoundsWorkToTheBacklogNotTheWholeTable).
	// It does NOT make the scan an index-ONLY scan -- measured directly,
	// Postgres 18 does not serve an indexed jsonb-extraction expression's
	// value from the index alone at execution time, so args is still read
	// (and detoasted if large) per matching row; the win is bounding which
	// rows are visited at all, from the whole table down to the backlog.
	// Built inside the same transaction and IF NOT EXISTS like every other
	// DDL in this function -- see the posture manifest table above for why
	// that is safe on a repeat run.
	riverJobRelation := pgx.Identifier{options.Schema, "river_job"}.Sanitize()
	if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS river_job_available_contract_version_idx
		ON `+riverJobRelation+` (queue, kind, (args ->> 'contract_version'))
		WHERE state = 'available'`); err != nil {
		logMigrationStageFailure(ctx, options.Logger, "create queue telemetry contract version index", err)
		return MigrationResult{}, migrationStageError("create queue telemetry contract version index")
	}
	if err := applyRuntimeGrants(ctx, tx, options); err != nil {
		return MigrationResult{}, migrationStageError("apply runtime grants")
	}
	routes, err := seedNativeRiverRoutes(ctx, tx, options.NativeRiverRoutes)
	if err != nil {
		logMigrationStageFailure(ctx, options.Logger, "seed native river routes", err)
		return MigrationResult{}, migrationStageError("seed native river routes")
	}
	if options.PostureManifestDigest != "" {
		buildID := options.PostureManifestBuildID
		if buildID == "" {
			buildID = options.PostureManifestDigest
		}
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO public.`+postureManifestAppliedTable+` (manifest_digest, applied_at, migrate_build)
			VALUES ($1, now(), $2)
			ON CONFLICT (manifest_digest) DO UPDATE SET applied_at = EXCLUDED.applied_at, migrate_build = EXCLUDED.migrate_build`,
			options.PostureManifestDigest, buildID,
		); err != nil {
			logMigrationStageFailure(ctx, options.Logger, "stamp posture manifest applied", err)
			return MigrationResult{}, migrationStageError("stamp posture manifest applied")
		}
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
	return MigrationResult{
		AppliedVersions:  applied,
		CurrentVersion:   status,
		SeededRoutes:     routes.seeded,
		PresentRoutes:    routes.present,
		RouteTableAbsent: routes.tableAbsent,
	}, nil
}

type routeSeedResult struct {
	seeded      []string
	present     []string
	tableAbsent bool
}

// seedNativeRiverRoutes inserts the missing route rows inside the migration's
// privilege transaction. ON CONFLICT DO NOTHING is what leaves an existing
// row -- paused, rolled back, or at a later generation -- exactly as an
// operator or an earlier migration left it; RETURNING tells an insert from a
// row that was already there.
func seedNativeRiverRoutes(ctx context.Context, tx pgx.Tx, kinds []string) (routeSeedResult, error) {
	var result routeSeedResult
	if len(kinds) == 0 {
		return result, nil
	}
	var exists bool
	if err := tx.QueryRow(ctx,
		"SELECT to_regclass('public.worker_job_routes') IS NOT NULL").Scan(&exists); err != nil {
		return result, err
	}
	if !exists {
		result.tableAbsent = true
		return result, nil
	}
	for _, kind := range kinds {
		var inserted string
		err := tx.QueryRow(ctx, `
INSERT INTO public.worker_job_routes (job_kind, transport, paused, generation, updated_at)
VALUES ($1, 'river', FALSE, 1, now())
ON CONFLICT (job_kind) DO NOTHING
RETURNING job_kind`, kind).Scan(&inserted)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			result.present = append(result.present, kind)
		case err != nil:
			return result, fmt.Errorf("seed route %s: %w", kind, err)
		default:
			result.seeded = append(result.seeded, kind)
		}
	}
	return result, nil
}

// isSchemaCheckConnectivityError positively identifies a connection/pool/
// context failure -- the ONLY class CheckSchema treats as
// ErrSchemaCheckUnavailable. Deliberately positive, not negative-by-default:
// a negative test ("assume Unavailable unless proven otherwise") is exactly
// what round 1's regression did, and rivermigrate's own
// "migration N not found in migrator bundle" error (a genuine schema fact)
// carries no underlying driver error at all, so there is nothing for a
// negative test to find -- it would (and did) misclassify by default.
func isSchemaCheckConnectivityError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return true
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	return false
}

func migrationStageError(stage string) error {
	return fmt.Errorf("%w during %s", ErrMigrationFailed, stage)
}

// logMigrationStageFailure logs a stage's underlying driver error before it
// is discarded by migrationStageError (codex review finding, CHAOS-5437
// round 1, P2): every OTHER migrationStageError call site in this function
// discards its underlying error too, an established (if not ideal) pattern
// this change does not attempt to rewrite wholesale -- but chris's standing
// rule is that a swallowed error on a TOUCHED failure path is a review
// finding, and CHAOS-5437's own two new stages (creating/stamping the
// posture manifest table) are exactly that. The caller (cmd/dev-health-
// worker-migrate) runs its logger through logging.NewJSON against stderr,
// so this reaches the operator's deploy log without needing
// ApplyPinnedMigrations' return value to carry more than the bounded stage
// name it already does.
func logMigrationStageFailure(ctx context.Context, logger *slog.Logger, stage string, err error) {
	if logger == nil || err == nil {
		return
	}
	logger.ErrorContext(ctx, "migration stage failed", "stage", stage, "error", err.Error())
}

// CheckSchema is read-only and requires the exact pinned migration prefix.
// It returns ErrSchemaCheckUnavailable (wrapping the real driver/pool error)
// only when ExistingVersions' failure is POSITIVELY identified as a
// connection/pool/context problem (see isSchemaCheckConnectivityError) --
// never a schema fact -- and ErrSchemaNotCurrent for every other cause,
// including a query that ran and returned a version set that is genuinely
// wrong, incomplete, or (round 2 of CHAOS-5469, a real regression in round
// 1's fix) unrecognized by this binary's own migration bundle. Classifying
// unrecognized-as-unavailable was the round-1 bug: a database AHEAD of the
// binary (an unknown migration version already applied) is a schema FACT,
// not an outage, and rivermigrate's own versionsFromDriver reports it as a
// bare fmt.Errorf with no underlying driver error to classify at all.
func CheckSchema(ctx context.Context, pool *pgxpool.Pool, schema string, logger *slog.Logger) (int, error) {
	if pool == nil || !validIdentifier(schema) {
		return 0, ErrMigrationConfiguration
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Schema: schema, Logger: logger})
	if err != nil || validatePinnedBundle(migrator.AllVersions()) != nil {
		return 0, ErrPinnedMigrationMismatch
	}
	versions, err := migrator.ExistingVersions(ctx)
	if err != nil {
		if isSchemaCheckConnectivityError(err) {
			return 0, fmt.Errorf("%w: %w", ErrSchemaCheckUnavailable, err)
		}
		// Not a connection/pool/context failure: ExistingVersions can also
		// fail on a genuine schema FACT with no underlying driver error at
		// all -- rivermigrate.versionsFromDriver returns a bare
		// fmt.Errorf("migration %d not found in migrator bundle", ...) when
		// the database holds a migration row this binary's bundle does not
		// recognize (the database is AHEAD of the binary). That is exactly
		// what ErrSchemaNotCurrent means: found live-migrate-integration-test
		// regression, CHAOS-5469 round 2 -- classifying every ExistingVersions
		// error as unavailable made this genuine mismatch unreadable as a
		// mismatch.
		return 0, fmt.Errorf("%w: %w", ErrSchemaNotCurrent, err)
	}
	if len(versions) != PinnedSchemaVersion {
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
	seenRoutes := make(map[string]struct{}, len(options.NativeRiverRoutes))
	for _, kind := range options.NativeRiverRoutes {
		if len(kind) > 96 || !jobKindPattern.MatchString(kind) {
			return ErrMigrationConfiguration
		}
		if _, duplicate := seenRoutes[kind]; duplicate {
			return ErrMigrationConfiguration
		}
		seenRoutes[kind] = struct{}{}
	}
	if options.DomainRole == options.QueueRole {
		return ErrMigrationConfiguration
	}
	if err := validateCoordinatorOptions(options); err != nil {
		return err
	}
	if err := validateAPIOptions(options); err != nil {
		return err
	}
	return validateQueryAPIOptions(options)
}

// validateQueryAPIOptions checks the optional query-api role leg.
// Grants without a role are a caller bug, as for the coordinator; a role
// without grants is a leg that would report success having granted nothing.
// The role must not be any of the four roles that carry a full, exact posture
// of their own: this leg's REVOKE ALL and GRANT would overwrite theirs.
func validateQueryAPIOptions(options MigrationOptions) error {
	if options.QueryAPIRole == "" {
		if len(options.QueryAPIGrants) != 0 {
			return ErrMigrationConfiguration
		}
		return nil
	}
	if !validIdentifier(options.QueryAPIRole) ||
		options.QueryAPIRole == options.DomainRole ||
		options.QueryAPIRole == options.QueueRole ||
		options.QueryAPIRole == options.CoordinatorRole ||
		options.QueryAPIRole == options.APIRole {
		return ErrMigrationConfiguration
	}
	if len(options.QueryAPIGrants) == 0 {
		return ErrMigrationConfiguration
	}
	return validateGrantSet(options.QueryAPIGrants, nil, nil)
}

func validateCoordinatorOptions(options MigrationOptions) error {
	if options.CoordinatorRole == "" {
		// No coordinator provisioning requested. Grants supplied without a role
		// are a caller bug, not a no-op: it would silently skip the grants the
		// caller believed it was applying.
		if len(options.CoordinatorGrants) != 0 || len(options.CoordinatorColumnGrants) != 0 || len(options.CoordinatorSequences) != 0 {
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
	return validateGrantSet(options.CoordinatorGrants, options.CoordinatorColumnGrants, options.CoordinatorSequences)
}

// validateAPIOptions checks the optional api role leg. Grants without a role
// are a caller bug, as for the coordinator.
func validateAPIOptions(options MigrationOptions) error {
	if options.APIRole == "" {
		if len(options.APIGrants) != 0 || len(options.APIColumnGrants) != 0 || len(options.APISequences) != 0 {
			return ErrMigrationConfiguration
		}
		return nil
	}
	if !validIdentifier(options.APIRole) ||
		options.APIRole == options.DomainRole ||
		options.APIRole == options.QueueRole ||
		options.APIRole == options.CoordinatorRole {
		return ErrMigrationConfiguration
	}
	return validateGrantSet(options.APIGrants, options.APIColumnGrants, options.APISequences)
}

// validateGrantSet checks one role's injected posture: valid identifiers, no
// duplicates, grantable column privileges, and no relation granted both
// table-wide and column-scoped.
func validateGrantSet(tables []TableGrant, columns []ColumnGrant, sequences []string) error {
	seen := make(map[string]struct{}, len(tables))
	for _, grant := range tables {
		if !validIdentifier(grant.TableName) {
			return ErrMigrationConfiguration
		}
		if _, duplicate := seen[grant.TableName]; duplicate {
			return ErrMigrationConfiguration
		}
		seen[grant.TableName] = struct{}{}
	}
	seenColumns := make(map[string]struct{}, len(columns))
	for _, grant := range columns {
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
	seenSequences := make(map[string]struct{}, len(sequences))
	for _, sequence := range sequences {
		if !validIdentifier(sequence) {
			return ErrMigrationConfiguration
		}
		if _, duplicate := seenSequences[sequence]; duplicate {
			return ErrMigrationConfiguration
		}
		seenSequences[sequence] = struct{}{}
	}
	return nil
}

// resolveAPIRole decides the api role leg after the preflight read. An api
// role that does not exist yet is skipped (logged): it is provisioned once,
// by an operator, and the api's readiness stays false until a later
// migration grants it. One that exists must be an eligible least-privilege
// login distinct from the migration identity, or the migration stops.
func resolveAPIRole(
	ctx context.Context,
	options MigrationOptions,
	migrationRole string,
	exists bool,
	eligible bool,
) (MigrationOptions, error) {
	if options.APIRole == "" {
		return options, nil
	}
	if !exists {
		if options.Logger != nil {
			options.Logger.WarnContext(ctx, "api Postgres role does not exist; api grants skipped",
				"api_role", options.APIRole)
		}
		options.APIRole = ""
		options.APIGrants, options.APIColumnGrants, options.APISequences = nil, nil, nil
		return options, nil
	}
	if !eligible || migrationRole == options.APIRole {
		return options, ErrMigrationConfiguration
	}
	return options, nil
}

// resolveQueryAPIRole decides the query-api role leg after the preflight
// read. A role that does not exist yet is skipped and logged, like the api role:
// it is provisioned once by an operator, and query-api's readiness stays false
// (when it names the role) until a later migration grants it. One that exists
// must be an eligible least-privilege login that owns nothing and is not the
// migration identity, or the migration stops: this leg REVOKEs, so pointing it
// at the registry owner (or a superuser) must never reach the statements.
func resolveQueryAPIRole(
	ctx context.Context,
	options MigrationOptions,
	migrationRole string,
	exists bool,
	eligible bool,
) (MigrationOptions, error) {
	if options.QueryAPIRole == "" {
		return options, nil
	}
	if !exists {
		if options.Logger != nil {
			options.Logger.WarnContext(ctx, "query-api Postgres role does not exist; query-api grants skipped",
				"query_api_role", options.QueryAPIRole)
		}
		options.QueryAPIRole, options.QueryAPIGrants = "", nil
		return options, nil
	}
	if !eligible || migrationRole == options.QueryAPIRole {
		return options, ErrMigrationConfiguration
	}
	return options, nil
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

// ValidIdentifier is the ONE rule for a role or schema name this package accepts
// ([a-z_][a-z0-9_]*, at most 63 bytes). `dho migrate roles` applies it to the runtime
// roles before it creates them, so it cannot provision a login that `dho migrate
// river` would then refuse (CHAOS-6901).
func ValidIdentifier(value string) bool { return validIdentifier(value) }

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
	if options.QueryAPIRole != "" {
		// CHAOS-6804 (lead D2616): the query-api role is provisioned from the SAME
		// enumeration the readiness check reads. Remove every grant it holds in its
		// own name, in every catalog class and schema (roleacl.RevokeStatements),
		// then grant exactly the manifest below: its posture is a function of this
		// migration alone. PUBLIC's grants are not the role's to lose and stay for
		// the check to name.
		revokes, err := roleacl.RevokeStatements(ctx, tx, options.QueryAPIRole)
		if err != nil {
			return fmt.Errorf("enumerate the query-api role's grants")
		}
		for _, statement := range revokes {
			if _, err := tx.Exec(ctx, statement); err != nil {
				return fmt.Errorf("revoke the query-api role's grants")
			}
		}
	}
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
		"DO $$ BEGIN IF to_regclass('public.integration_sources') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.integration_sources TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.integration_datasets') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.integration_datasets TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.integration_credentials') IS NOT NULL THEN GRANT SELECT ON TABLE public.integration_credentials TO " + domainRole + "; END IF; END $$",
		// CHAOS-6695: the webhook worker's installation routing read and
		// installation upsert, and its scoped-sync request row. Each flag list
		// is exactly what domainPosture() declares.
		"DO $$ BEGIN IF to_regclass('public.github_app_installations') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.github_app_installations TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.webhook_sync_requests') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.webhook_sync_requests TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.provider_oauth_credentials') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.provider_oauth_credentials TO " + domainRole + "; END IF; END $$",
		// worker_job_routes, scheduled_sync_occurrences, and
		// fixed_schedule_occurrences are coordinator-exclusive under the
		// Option B two-role split (role-partition manifest, removed in
		// e23ede618; see git history at eda2d6b91) and deliberately have no
		// domain-role GRANT here. Their
		// COORDINATOR-side grants are no longer deploy-deferred: when
		// MigrationOptions.CoordinatorRole is set, coordinatorGrantStatements
		// below emits them from the injected posture in this same transaction,
		// so local dev and CI are self-provisioning for all three roles.
		// scheduled_jobs is dual-role. The native sync-coverage projector reads
		// schedule facts, and the report worker clears next_run_at through the
		// immutable scheduled occurrence whenever a run terminalizes so the fixed
		// scheduler recomputes the next cron instant. That repair also reads the
		// scheduled_report_occurrences link; it never mutates the occurrence row.
		// sync_configurations is SELECT-only for the domain role: its
		// coordinator-side FOR UPDATE row-locking use does not make the
		// domain role's own posture require UPDATE. sync_runs, by contrast,
		// is one of the six dual-grant ("both") tables and genuinely needs
		// UPDATE on the domain side (Fanout's FOR SHARE + the providersync
		// hot path). CHAOS-3145 adds INSERT on sync_runs and sync_run_units
		// for the native scheduler materializer's separate domain transaction.
		"DO $$ BEGIN IF to_regclass('public.sync_runs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_runs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_transport_routes') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_units') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_run_units TO " + domainRole + "; END IF; END $$",
		// CHAOS-4209. These four are the grants the CHAOS-4175 native ports need
		// and never got: NativeReferenceDiscoveryService and
		// NativeFinalizeSyncRunService both run on pools.Domain and were issuing
		// statements against tables the domain role held nothing on. Each flag
		// list is exactly what domainPosture() declares -- the two must agree or
		// TestDomainAuthorizationAcceptsTheGrantsItIsPairedWith fails, since
		// CheckDomainAuthorization asserts the privilege set is neither smaller
		// nor larger than the manifest.
		"DO $$ BEGIN IF to_regclass('public.sync_run_reference_discoveries') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_run_reference_discoveries TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_post_dispatches') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.sync_run_post_dispatches TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_compute_checkpoints') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.sync_compute_checkpoints TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.job_runs') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.job_runs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_unit_chunk_checkpoints') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.sync_run_unit_chunk_checkpoints TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_unit_effect_chunks') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.sync_run_unit_effect_chunks TO " + domainRole + "; END IF; END $$",
		// Prepared recovery snapshots are transient state cleared in the same
		// transaction that terminalizes their unit, never updated in place, so
		// they receive DELETE but no UPDATE. Worker lifecycle tables have their
		// own bounded DELETE grants below.
		"DO $$ BEGIN IF to_regclass('public.sync_run_unit_effect_snapshots') IS NOT NULL THEN GRANT SELECT, INSERT, DELETE ON TABLE public.sync_run_unit_effect_snapshots TO " + domainRole + "; END IF; END $$",
		// CHAOS-4114: the executed-proof ledger is written by the same domain
		// transactions that write sync_run_units and read by the scheduler's
		// evidence refresh. No DELETE: the ledger is monotone by construction.
		"DO $$ BEGIN IF to_regclass('public.sync_executed_proof_ledger') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_executed_proof_ledger TO " + domainRole + "; END IF; END $$",
		// CHAOS-6622: DELETE for the Jira rename watermark move (see
		// domainPosture's sync_watermarks entry).
		"DO $$ BEGIN IF to_regclass('public.sync_watermarks') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.sync_watermarks TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_outbox') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_dispatch_outbox TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_outbox') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.worker_job_outbox TO " + domainRole + "; END IF; END $$",
		// UPDATE added by CHAOS-4209 for stampCanonicalSyncConfig's last_sync_*
		// write on the domain transaction.
		"DO $$ BEGIN IF to_regclass('public.sync_configurations') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.sync_configurations TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.scheduled_jobs') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.scheduled_jobs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.scheduled_report_occurrences') IS NOT NULL THEN GRANT SELECT ON TABLE public.scheduled_report_occurrences TO " + domainRole + "; END IF; END $$",
		// UPDATE added by CHAOS-4209 for observeTerminalSyncRun's backfill
		// terminalization. INSERT deliberately stays off.
		"DO $$ BEGIN IF to_regclass('public.backfill_jobs') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.backfill_jobs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_coverage_projections') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_coverage_projections TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.organizations') IS NOT NULL THEN GRANT SELECT ON TABLE public.organizations TO " + domainRole + "; END IF; END $$",
		// The native phone-home heartbeat runs on the domain pool
		// (internal/jobs/system/heartbeat_native.go): it counts users
		// alongside organizations and records each run as an audit_logs row.
		// Without these two the daily job fails on its first statement and,
		// at one attempt, is discarded -- the bridge it replaced ran as the
		// application role and never needed them. audit_logs gets no UPDATE
		// or DELETE: the worker appends its own row and never edits the trail.
		"DO $$ BEGIN IF to_regclass('public.users') IS NOT NULL THEN GRANT SELECT ON TABLE public.users TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.audit_logs') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.audit_logs TO " + domainRole + "; END IF; END $$",
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
		// CHAOS-4405 (codex review finding on #1971): the finalize-redrive
		// provenance ledger. Without this grant, CompleteFinalize/
		// ReleaseFinalize's own close-out UPDATE and
		// ReconcileOrphanedFinalizeRedriveRuns's read+close-out both fail
		// 42501 in any real least-privilege deployment -- every ordinary
		// finalize transition would roll back the instant this table has
		// an 'open' row for its run. See domainPosture()'s matching entry.
		"DO $$ BEGIN IF to_regclass('public.daily_metrics_finalize_redrive_events') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.daily_metrics_finalize_redrive_events TO " + domainRole + "; END IF; END $$",
		// CHAOS-4459: the partition-recompute provenance ledger.
		// INSERT-only -- see domainPosture()'s matching entry.
		"DO $$ BEGIN IF to_regclass('public.daily_metrics_partition_recompute_events') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.daily_metrics_partition_recompute_events TO " + domainRole + "; END IF; END $$",
		// The compatibility-bridge execution ledger. UPDATE only -- see
		// domainPosture()'s matching entry for why no INSERT and no DELETE.
		"DO $$ BEGIN IF to_regclass('public.metric_compatibility_executions') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.metric_compatibility_executions TO " + domainRole + "; END IF; END $$",
		// UPDATE is required by PostgreSQL for SELECT ... FOR UPDATE row
		// locking; retention never updates conversation columns.
		"DO $$ BEGIN IF to_regclass('public.dev_conversations') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.dev_conversations TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.dev_conversation_tombstones') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.dev_conversation_tombstones TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_batch_payloads') IS NOT NULL THEN GRANT SELECT, DELETE ON TABLE public.external_ingest_batch_payloads TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_batches') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.external_ingest_batches TO " + domainRole + "; END IF; END $$",
		// UPDATE was added by CHAOS-5296: the native external-recompute drain
		// claims a row (bridge_pending -> bridge_claimed) and marks it terminal
		// (-> bridge_dispatched/bridge_failed) from this role. Before the port
		// the only reader was Celery, running as a different role entirely,
		// which is why SELECT+INSERT sufficed. No DELETE: nothing in the drain
		// or the replay command removes a row -- the ledger is the evidence
		// that a recompute was dispatched, and retention owns its lifetime.
		"DO $$ BEGIN IF to_regclass('public.external_ingest_recompute_jobs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.external_ingest_recompute_jobs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_rejections') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.external_ingest_rejections TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.external_ingest_sources') IS NOT NULL THEN GRANT SELECT ON TABLE public.external_ingest_sources TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.feature_flags') IS NOT NULL THEN GRANT SELECT ON TABLE public.feature_flags TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.org_feature_overrides') IS NOT NULL THEN GRANT SELECT ON TABLE public.org_feature_overrides TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.org_licenses') IS NOT NULL THEN GRANT SELECT ON TABLE public.org_licenses TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.provider_rate_limit_observations') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.provider_rate_limit_observations TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.report_runs') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.report_runs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.saved_reports') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.saved_reports TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.tier_limits') IS NOT NULL THEN GRANT SELECT ON TABLE public.tier_limits TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.webhook_deliveries') IS NOT NULL THEN GRANT SELECT ON TABLE public.webhook_deliveries TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_runs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE public.worker_job_runs TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_concurrency_leases') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.worker_concurrency_leases TO " + domainRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_instances') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.worker_instances TO " + domainRole + "; END IF; END $$",
		// CHAOS-5437: SELECT-only -- this migration is the table's only
		// writer (the CREATE TABLE + upsert above, same transaction).
		"DO $$ BEGIN IF to_regclass('public." + postureManifestAppliedTable + "') IS NOT NULL THEN GRANT SELECT ON TABLE public." + postureManifestAppliedTable + " TO " + domainRole + "; END IF; END $$",
		"GRANT USAGE ON SCHEMA public TO " + queueRole,
		"REVOKE CREATE ON SCHEMA public FROM " + queueRole,
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM " + queueRole,
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM " + queueRole,
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, " + domainRole + ", " + queueRole,
		"DO $$ BEGIN IF to_regclass('public.worker_job_outbox') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_delivery_abandonments') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE public.worker_job_delivery_abandonments TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.worker_job_completion_fences') IS NOT NULL THEN GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_completion_fences TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_outbox') IS NOT NULL THEN GRANT SELECT, UPDATE ON TABLE public.sync_dispatch_outbox TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_dispatch_transport_routes') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_runs') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_runs TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.sync_run_units') IS NOT NULL THEN GRANT SELECT ON TABLE public.sync_run_units TO " + queueRole + "; END IF; END $$",
		// CHAOS-3997: read-only domain access bounding the stranded-delivery
		// repair. This list is the AUTHORITY -- the REVOKE ALL above wipes the
		// queue role every run, so a grant that exists only in
		// scripts/worker/provision_river_roles.sql is erased by this migration
		// before the reconciler ever starts.
		"DO $$ BEGIN IF to_regclass('public.daily_metrics_runs') IS NOT NULL THEN GRANT SELECT ON TABLE public.daily_metrics_runs TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.daily_metrics_partitions') IS NOT NULL THEN GRANT SELECT ON TABLE public.daily_metrics_partitions TO " + queueRole + "; END IF; END $$",
		"DO $$ BEGIN IF to_regclass('public.work_graph_execution_requests') IS NOT NULL THEN GRANT SELECT ON TABLE public.work_graph_execution_requests TO " + queueRole + "; END IF; END $$",
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
	}, append(append(coordinatorGrantStatements(options), apiGrantStatements(options)...),
		queryAPIGrantStatements(options)...)...)
}

// queryAPIGrantStatements is the GRANT half of the query-api role's policy: the
// baseline (CONNECT on this database, USAGE on the public schema) and one guarded
// GRANT per manifest table. The REVOKE half is not a fixed list: it is derived at
// run time from roleacl.Enumerate (see applyRuntimeGrants), so it covers every
// grant the role holds, in every class, however it got there. Returns nil when no
// query-api role applies.
func queryAPIGrantStatements(options MigrationOptions) []string {
	if options.QueryAPIRole == "" {
		return nil
	}
	role := pgx.Identifier{options.QueryAPIRole}.Sanitize()
	statements := []string{
		"DO $$ BEGIN EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), '" + options.QueryAPIRole + "'); END $$",
		"GRANT USAGE ON SCHEMA public TO " + role,
	}
	for _, grant := range options.QueryAPIGrants {
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
		statements = append(statements,
			"DO $$ BEGIN IF to_regclass('public."+grant.TableName+"') IS NOT NULL THEN GRANT "+
				privileges+" ON TABLE "+pgx.Identifier{"public", grant.TableName}.Sanitize()+
				" TO "+role+"; END IF; END $$",
		)
	}
	return statements
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
	return postureGrantStatements(options.CoordinatorRole, options.Schema,
		options.CoordinatorGrants, options.CoordinatorColumnGrants, options.CoordinatorSequences)
}

// apiGrantStatements is the api role's privilege policy, built exactly as
// the coordinator's: REVOKE ALL, then the grants injected from
// postgres.APIPosture(). Returns nil when no api role applies.
func apiGrantStatements(options MigrationOptions) []string {
	if options.APIRole == "" {
		return nil
	}
	return postureGrantStatements(options.APIRole, options.Schema,
		options.APIGrants, options.APIColumnGrants, options.APISequences)
}

// postureGrantStatements emits one role's policy: REVOKE ALL on the public
// and River schemas, then each injected grant guarded by to_regclass.
func postureGrantStatements(roleName, riverSchema string, tables []TableGrant, columns []ColumnGrant, sequences []string) []string {
	role := pgx.Identifier{roleName}.Sanitize()
	statements := []string{
		"DO $$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM %I', current_database(), '" + roleName + "'); END $$",
		"GRANT USAGE ON SCHEMA public TO " + role,
		"REVOKE CREATE ON SCHEMA public FROM " + role,
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM " + role,
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM " + role,
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM " + role,
		// The coordinator and the api are public-schema roles only. Neither
		// touches River's own tables, so each gets the same fail-closed
		// treatment the domain role gets on the River schema.
		"REVOKE ALL PRIVILEGES ON SCHEMA " + pgx.Identifier{riverSchema}.Sanitize() + " FROM " + role,
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + pgx.Identifier{riverSchema}.Sanitize() + " FROM " + role,
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA " + pgx.Identifier{riverSchema}.Sanitize() + " FROM " + role,
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA " + pgx.Identifier{riverSchema}.Sanitize() + " FROM " + role,
	}
	for _, grant := range tables {
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
				" TO "+role+"; END IF; END $$",
		)
	}
	// Column-scoped grants are emitted after the table-wide ones and never
	// instead of them: the two sets are disjoint by construction
	// (ValidateMigrationOptions rejects a relation appearing in both), so
	// order carries no meaning beyond keeping the statement list stable and
	// diffable. The privilege keyword is taken from the validated closed set
	// above rather than sanitized, because it is a keyword and not an
	// identifier; the table and column are sanitized identifiers.
	for _, grant := range columns {
		qualified := "public." + grant.TableName
		statements = append(statements,
			"DO $$ BEGIN IF to_regclass('"+qualified+"') IS NOT NULL THEN GRANT "+
				grant.Privilege+" ("+pgx.Identifier{grant.ColumnName}.Sanitize()+
				") ON TABLE "+pgx.Identifier{"public", grant.TableName}.Sanitize()+
				" TO "+role+"; END IF; END $$",
		)
	}
	for _, sequence := range sequences {
		qualified := "public." + sequence
		statements = append(statements,
			"DO $$ BEGIN IF to_regclass('"+qualified+"') IS NOT NULL THEN GRANT USAGE ON SEQUENCE "+
				pgx.Identifier{"public", sequence}.Sanitize()+" TO "+role+"; END IF; END $$",
		)
	}
	return statements
}
