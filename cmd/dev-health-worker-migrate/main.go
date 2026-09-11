// dev-health-worker-migrate is the one-shot River schema migration command.
// It is intentionally separate from every long-running runtime binary.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

const (
	serviceName = "dev-health-worker-migrate"
	// Kept in step with internal/platform/config's
	// defaultCoordinatorDatabaseRole so the migration grants the same role the
	// runtime binaries connect as when neither side overrides the env var.
	defaultCoordinatorRole = "devhealth_coordinator"
)

func main() {
	os.Exit(execute(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

func execute(
	parent context.Context,
	args []string,
	lookup platformsecrets.LookupEnv,
	stdout io.Writer,
	stderr io.Writer,
) int {
	flags := flag.NewFlagSet(serviceName, flag.ContinueOnError)
	flags.SetOutput(stderr)
	check := flags.Bool("check", false, "verify the pinned River schema without applying DDL")
	showVersion := flags.Bool("version", false, "print build metadata as JSON and exit")
	// CHAOS-5560 round-2 P3: this binary's own flag.NewFlagSet documented
	// zero environment variables (not even the pre-existing
	// MIGRATION_DATABASE_URI) -- the only place either DSN form was
	// discoverable was this file's source or the PR history. defaultUsage
	// is flag's own generated text; appending the env section keeps it
	// rather than replacing it.
	defaultUsage := flags.Usage
	flags.Usage = func() {
		defaultUsage()
		fmt.Fprint(stderr, "\nEnvironment:\n"+
			"  MIGRATION_DATABASE_URI (or _FILE)   pre-built PostgreSQL DSN\n"+
			"  DEV_HEALTH_MIGRATION_PG_HOST         component form: host (also enables _PORT/_USER/_PASSWORD/_DB below)\n"+
			"  DEV_HEALTH_MIGRATION_PG_PORT         component form: port (default 5432)\n"+
			"  DEV_HEALTH_MIGRATION_PG_USER         component form: user\n"+
			"  DEV_HEALTH_MIGRATION_PG_PASSWORD     component form: password\n"+
			"  DEV_HEALTH_MIGRATION_PG_DB           component form: database name (default postgres)\n"+
			"  RIVER_DOMAIN_DATABASE_ROLE, RIVER_QUEUE_DATABASE_ROLE, RIVER_COORDINATOR_DATABASE_ROLE (optional)\n"+
			"  RIVER_DATABASE_SCHEMA (optional)\n"+
			"MIGRATION_DATABASE_URI and the DEV_HEALTH_MIGRATION_PG_* component form are mutually exclusive; set exactly one.\n")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "argument error: positional arguments are not accepted")
		return 2
	}
	if *showVersion {
		if err := version.Current(serviceName).WriteJSON(stdout); err != nil {
			fmt.Fprintln(stderr, "could not write version metadata")
			return 1
		}
		return 0
	}

	migrationURI, ok := resolveMigrationDatabaseURI(lookup, stderr)
	if !ok {
		return 1
	}
	// CHAOS-5560 round 5 finding #5, redesigned per round 6's ruling:
	// successful resolution returned with no observable record of which
	// form (uri|components) or database it reached -- a silent regression
	// there (a wrong, but reachable, database) would have been invisible
	// even after this ticket's other fixes. Form presence-checks
	// DEV_HEALTH_MIGRATION_PG_HOST the same way
	// resolveMigrationDatabaseURI/config.ResolveDSN's own hostSet check
	// does. Round 6 ruling (chris): no telemetry field is ever derived by
	// parsing a DSN -- for the URI form, "database" is omitted entirely;
	// for the component form, config.ComponentDatabaseName reads the
	// separate, non-secret DEV_HEALTH_MIGRATION_PG_DB env value directly,
	// needing no parsing of the assembled DSN.
	infoLogger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if host, present := lookup(migrationDatabaseSpec.HostKey); present && host != "" {
		infoLogger.InfoContext(parent, "migration database resolved",
			"form", "components", "database", config.ComponentDatabaseName(lookup, migrationDatabaseSpec))
	} else {
		infoLogger.InfoContext(parent, "migration database resolved", "form", "uri")
	}
	migrationRole, err := postgresstore.ConnectionUser(migrationURI.Reveal())
	if err != nil {
		config.WriteConfigError(stderr, errors.New("invalid MIGRATION_DATABASE_URI"))
		return 1
	}
	domainRole, ok := requiredName("RIVER_DOMAIN_DATABASE_ROLE", lookup, stderr)
	if !ok {
		return 1
	}
	queueRole, ok := requiredName("RIVER_QUEUE_DATABASE_ROLE", lookup, stderr)
	if !ok {
		return 1
	}

	schema := "river"
	if value, present := lookup("RIVER_DATABASE_SCHEMA"); present && strings.TrimSpace(value) != "" {
		schema = value
	}
	// Defaulted rather than required, matching RIVER_DATABASE_SCHEMA above and
	// internal/platform/config's own default: making it required would break
	// every existing environment's migration the moment this ships, for no
	// safety gain. Safety comes from the preflight instead — the coordinator
	// role must already exist as an eligible least-privilege login, so a
	// migration run before the role is provisioned fails loudly rather than
	// granting nothing and reporting success. See the coordinator-pool
	// activation design doc (removed in e23ede618; see git history) for the
	// required order of operations.
	coordinatorRole := defaultCoordinatorRole
	if value, present := lookup("RIVER_COORDINATOR_DATABASE_ROLE"); present && strings.TrimSpace(value) != "" {
		coordinatorRole = value
	}
	coordinatorTableGrants, coordinatorColumnGrants, coordinatorSequences := coordinatorGrants()
	// CHAOS-5437: postureManifestDigest is the SAME value every go-* runtime
	// binary recomputes at startup (postgres.PostureManifestDigest()) --
	// stamping it here is what lets each of them prove, without a live
	// database round trip through anything but this one small table,
	// whether it is running a posture manifest older than the one this
	// migrate run just applied. migrateBuildID prefers the binary's real
	// commit; Commit is "unknown" only on a build with no ldflags AND no VCS
	// info available to debug.ReadBuildInfo (see internal/platform/version),
	// in which case the manifest digest itself is the honest fallback
	// identity -- it is what changes, and it is always present.
	postureManifestDigest := postgresstore.PostureManifestDigest()
	migrateBuildID := version.Current(serviceName).Commit
	if migrateBuildID == "" || migrateBuildID == "unknown" {
		migrateBuildID = "manifest:" + postureManifestDigest
	}
	migrationOptions := riverstore.MigrationOptions{
		Schema:                  schema,
		DomainRole:              domainRole,
		QueueRole:               queueRole,
		CoordinatorRole:         coordinatorRole,
		CoordinatorGrants:       coordinatorTableGrants,
		CoordinatorColumnGrants: coordinatorColumnGrants,
		CoordinatorSequences:    coordinatorSequences,
		PostureManifestDigest:   postureManifestDigest,
		PostureManifestBuildID:  migrateBuildID,
	}
	if err := riverstore.ValidateMigrationOptions(migrationOptions); err != nil ||
		migrationRole == domainRole || migrationRole == queueRole || migrationRole == coordinatorRole {
		config.WriteConfigError(stderr, errors.New("migration, domain, queue-control, and coordinator PostgreSQL roles must be distinct"))
		return 1
	}

	poolConfig := postgresstore.DefaultConfig(migrationURI.Reveal())
	// One connection holds the migration advisory lock while River applies
	// commit-separated migrations through the second connection.
	poolConfig.MaxConns = 2
	poolConfig.MaxConnIdleTime = time.Minute
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	defer cancel()
	pool, err := postgresstore.Open(ctx, poolConfig)
	if err != nil {
		fmt.Fprintln(stderr, "migration error: PostgreSQL migration endpoint unavailable")
		return 1
	}
	defer pool.Close()

	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if *check {
		current, err := riverstore.CheckSchema(ctx, pool, schema, logger)
		return reportSchemaCheck(ctx, logger, stdout, stderr, current, err)
	}

	migrationOptions.Logger = logger
	result, err := riverstore.ApplyPinnedMigrations(ctx, pool, migrationOptions)
	if err != nil {
		fmt.Fprintln(stderr, "migration failed: River schema or privilege policy was not applied")
		return 1
	}

	// CHAOS-4261: ApplyPinnedMigrations returning nil proves the GRANT
	// statements it issued succeeded, not that the live database now holds
	// the full declared posture -- every domain/coordinator/queue GRANT is
	// guarded by to_regclass and silently no-ops on a table that does not
	// exist yet (a database behind on Alembic head; see
	// deploy/go-workers/README.md). This executed-proof gate re-derives the
	// real posture against the database that was just migrated and fails
	// the command loudly, naming the missing (table, privilege) pairs,
	// instead of reporting success on a partially-applied grant set the
	// way the prod incident's `go-river-provision` REVOKE ALL once did
	// silently.
	postureResult := checkExecutedGrantPosture(ctx, pool, domainRole, queueRole, coordinatorRole, logger)
	writePostureTelemetry(stdout, postureResult)
	if !postureResult.OK {
		fmt.Fprintln(
			stderr,
			"migration failed: executed grant posture check found "+postureFailureKind(postureResult)+" "+
				"after go-river-migrate; see the preceding structured log lines for the "+
				"affected role(s) and table(s)",
		)
		return 1
	}

	fmt.Fprintf(
		stdout,
		"River %s schema current at pinned version %d (%d applied)\n",
		riverstore.PinnedRiverVersion,
		result.CurrentVersion,
		len(result.AppliedVersions),
	)
	return 0
}

// reportSchemaCheck is --check's reporting half, split out from execute so
// the logging behavior below is unit-testable without a live pool (CheckSchema
// itself needs one; this does not). Before CHAOS-5469, this call site
// discarded CheckSchema's error entirely -- a pool/connection failure
// (riverstore.ErrSchemaCheckUnavailable) and a genuine version mismatch
// (riverstore.ErrSchemaNotCurrent) both surfaced only as the same generic
// "River schema is not current" line, with the real cause nowhere in the
// output at all. The generic stderr line stays unchanged (an operator script
// parsing it must not break), but the underlying cause is now logged first,
// the same logDependencyCheckFailure-style pattern the readiness paths use.
func reportSchemaCheck(
	ctx context.Context,
	logger *slog.Logger,
	stdout, stderr io.Writer,
	current int,
	err error,
) int {
	if err != nil {
		logger.ErrorContext(ctx, "river schema check failed",
			"check", "river_schema",
			"error", err.Error(),
		)
		fmt.Fprintln(stderr, "migration check failed: River schema is not current")
		return 1
	}
	fmt.Fprintf(stdout, "River schema current at pinned version %d\n", current)
	return 0
}

func requiredName(key string, lookup platformsecrets.LookupEnv, stderr io.Writer) (string, bool) {
	value, configured := lookup(key)
	if !configured || strings.TrimSpace(value) == "" {
		config.WriteConfigError(stderr, fmt.Errorf("%s is required", key))
		return "", false
	}
	return value, true
}

// resolveMigrationDatabaseURI is CHAOS-5560's component alternative to a
// pre-built MIGRATION_DATABASE_URI: compose.yml's own entrypoint already
// assembles a fallback DSN by raw shell interpolation
// (postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/$POSTGRES_DB)
// when MIGRATION_DATABASE_URI is unset -- exactly the unescaped-password
// class this ticket fixes, just one shell layer further out.
//
// The component var names are deliberately NOT POSTGRES_HOST/_PORT/_USER/
// _PASSWORD/_DB: deploy/docker-compose/compose.go-workers.yml (not touched
// by this PR) already sets every one of those, unconditionally, for its own
// pre-existing shell fallback -- a round-1 review (2026-09-11) proved that
// reusing those names made this function activate every time that compose
// service ran, silently discarding a perfectly valid, already-working
// MIGRATION_DATABASE_URI override. DEV_HEALTH_MIGRATION_PG_* is a prefix
// swept against compose.yml, both overlays, deploy/helm, and docs before
// being chosen (zero hits) so setting it can never collide with anything
// today, deployed or documented. See config.ResolveDSN for the shared
// mutual-exclusion contract this now defers to instead of picking a
// precedence winner.
// migrationDatabaseSpec is the ONE ComponentSpec for MIGRATION_DATABASE_URI,
// shared by resolveMigrationDatabaseURI and execute's own Info-resolution
// record (config.ComponentDatabaseName) so the two never risk drifting
// into two different definitions of "the migration database's component
// form".
var migrationDatabaseSpec = config.ComponentSpec{
	HostKey: "DEV_HEALTH_MIGRATION_PG_HOST", PortKey: "DEV_HEALTH_MIGRATION_PG_PORT", DefaultPort: "5432",
	UserKey: "DEV_HEALTH_MIGRATION_PG_USER", PasswordKey: "DEV_HEALTH_MIGRATION_PG_PASSWORD",
	DBKey: "DEV_HEALTH_MIGRATION_PG_DB", DefaultDB: "postgres", Scheme: "postgresql",
}

func resolveMigrationDatabaseURI(
	lookup platformsecrets.LookupEnv,
	stderr io.Writer,
) (platformsecrets.Value, bool) {
	value, configured, err := config.ResolveDSN(lookup, "MIGRATION_DATABASE_URI", migrationDatabaseSpec)
	if err != nil {
		config.WriteConfigError(stderr, err)
		return platformsecrets.Value{}, false
	}
	if !configured {
		config.WriteConfigError(stderr, errors.New("MIGRATION_DATABASE_URI is required"))
		return platformsecrets.Value{}, false
	}
	return value, true
}

// coordinatorGrants derives the coordinator role's GRANT set from
// postgresstore.CoordinatorPosture() — the same declaration
// postgresstore.CheckCoordinatorAuthorization asserts against at readiness — so
// the migration cannot grant a privilege set the readiness check would then
// reject, and cannot omit one it requires. Nothing about the coordinator's
// table list is written here; this only translates between the two packages'
// types, because internal/storage/river cannot import internal/storage/postgres
// (that direction is an import cycle).
//
// Column-scoped privileges are translated too, and both halves come from the
// same posture. An earlier revision returned nil the moment ColumnScoped was
// non-empty, so that a coordinator column privilege would fail the migration
// loudly rather than being silently dropped. CHAOS-3114 added the first such
// entry (worker_job_completion_fences.completion_key, reached transitively
// from the fixed-schedule engine's replay arms), so the guard has served its
// purpose and is replaced by the real translation: dropping a declared column
// privilege here would leave readiness demanding a grant the migration never
// emitted, which is precisely the drift this indirection exists to prevent.
func coordinatorGrants() ([]riverstore.TableGrant, []riverstore.ColumnGrant, []string) {
	posture := postgresstore.CoordinatorPosture()
	grants := make([]riverstore.TableGrant, 0, len(posture.RequiredTables))
	for _, table := range posture.RequiredTables {
		grants = append(grants, riverstore.TableGrant{
			TableName:   table.TableName,
			AllowInsert: table.AllowInsert,
			AllowUpdate: table.AllowUpdate,
			AllowDelete: table.AllowDelete,
		})
	}
	columns := make([]riverstore.ColumnGrant, 0, len(posture.ColumnScoped))
	for _, column := range posture.ColumnScoped {
		columns = append(columns, riverstore.ColumnGrant{
			TableName:  column.TableName,
			ColumnName: column.ColumnName,
			Privilege:  column.Privilege,
		})
	}
	return grants, columns, append([]string(nil), posture.RequiredSequences...)
}
