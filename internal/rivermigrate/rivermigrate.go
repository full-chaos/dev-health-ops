// Package rivermigrate is the one-shot River schema migration command:
// `dho migrate river` (spec S4). cmd/dev-health-worker-migrate is a thin main
// over the same Execute, kept only because the Python image's
// `dev-hops migrate postgres` execs it by that name; it is deleted with S10.
package rivermigrate

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

	jobsv1 "github.com/full-chaos/dev-health-ops/contracts/jobs/v1"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

const (
	// Kept in step with internal/platform/config's
	// defaultCoordinatorDatabaseRole so the migration grants the same role the
	// runtime binaries connect as when neither side overrides the env var.
	defaultCoordinatorRole = "devhealth_coordinator"
	defaultAPIRole         = "devhealth_api"
)

// Command is `dho migrate`: the `postgres` and `clickhouse` groups (each
// schema's head, internal/pgmigrate and internal/chmigrate), the `upgrade`
// verb that runs the migrate Job's steps in order, the `river` verb, and the flat
// aliases of the Alembic verbs the Python CLI registers directly under `migrate`.
func Command() cli.Command {
	return cli.Command{
		Name:    "migrate",
		Summary: "apply or check database schemas",
		Kind:    cli.Group,
		Children: append([]cli.Command{
			migrationPostgresCommand(),
			chmigrate.Command(),
			{
				Name:    "upgrade",
				Summary: "run the migrate Job's steps in order: postgres upgrade, features seed, clickhouse upgrade",
				Kind:    cli.Verb,
				Run:     runUpgrade,
			},
			{
				Name:    "river",
				Summary: "apply the pinned River schema and runtime grant posture, or check it (--check, --apply-and-check)",
				Kind:    cli.Verb,
				Run: func(ctx context.Context, env cli.Env) int {
					return Execute(ctx, "dho", env.Args, env.Lookup, env.Stdout, env.Stderr)
				},
			},
			// The Python CLI's flat aliases of the Alembic verbs (dev-hops migrate
			// current|heads|history|status|downgrade).
		}, pgmigrate.Aliases(migrationDatabaseResolver)...),
	}
}

// Main is cmd/dev-health-worker-migrate's whole main.
func Main() {
	os.Exit(Execute(context.Background(), "dev-health-worker-migrate", os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

// noMigrationDatabaseMessage is the refusal the chart hook's shell wrapper
// printed, verbatim, when --apply-and-check finds neither DSN.
const noMigrationDatabaseMessage = config.NoMigrationDatabaseMessage

func execute(parent context.Context, args []string, lookup platformsecrets.LookupEnv, stdout, stderr io.Writer) int {
	return Execute(parent, "dev-health-worker-migrate", args, lookup, stdout, stderr)
}

// Execute runs the migration command. service names the binary in --version
// output and help. Modes:
//
//   - default: apply the pinned River schema and the runtime grant posture.
//   - --check: verify the schema is current, without applying DDL.
//   - --apply-and-check: apply, then verify on a fresh connection pool, in
//     one process. This is the chart hook's and Compose's mode, and the only
//     one that falls back to POSTGRES_URI when MIGRATION_DATABASE_URI is not
//     configured (the fallback the hook's shell wrapper used to perform).
func Execute(
	parent context.Context,
	service string,
	args []string,
	lookup platformsecrets.LookupEnv,
	stdout io.Writer,
	stderr io.Writer,
) int {
	flags := flag.NewFlagSet(service, flag.ContinueOnError)
	flags.SetOutput(stderr)
	check := flags.Bool("check", false, "verify the pinned River schema without applying DDL")
	applyAndCheck := flags.Bool("apply-and-check", false, "apply, then verify the schema on a fresh connection; falls back to POSTGRES_URI when MIGRATION_DATABASE_URI is not configured")
	showVersion := flags.Bool("version", false, "print build metadata as JSON and exit")
	// This binary's own flag.NewFlagSet must document both DSN forms --
	// leaving MIGRATION_DATABASE_URI (and the component form)
	// undocumented would make this file's source the only place either
	// was discoverable. defaultUsage
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
	if *check && *applyAndCheck {
		fmt.Fprintln(stderr, "argument error: --check and --apply-and-check are mutually exclusive")
		return 2
	}
	if *showVersion {
		if err := version.Current(service).WriteJSON(stdout); err != nil {
			fmt.Fprintln(stderr, "could not write version metadata")
			return 1
		}
		return 0
	}

	migrationURI, source, ok := resolveMigrationDatabaseURI(lookup, stderr, *applyAndCheck)
	if !ok {
		return 1
	}
	// A successful resolution must leave an observable record of which
	// form (uri|components) or database it reached -- otherwise a silent
	// regression there (a wrong, but reachable, database) would be
	// invisible even after this ticket's other fixes. Form presence-checks
	// DEV_HEALTH_MIGRATION_PG_HOST the same way
	// resolveMigrationDatabaseURI/config.ResolveDSN's own hostSet check
	// does. No telemetry field is ever derived by
	// parsing a DSN -- for the URI form, "database" is omitted entirely;
	// for the component form, config.ComponentDatabaseIdentity asks the driver for the
	// separate, non-secret DEV_HEALTH_MIGRATION_PG_DB env value directly,
	// needing no parsing of the assembled DSN.
	infoLogger := logging.NewJSON(stderr, slog.LevelInfo)
	if host, present := lookup(migrationDatabaseSpec.HostKey); present && host != "" && source == "MIGRATION_DATABASE_URI" {
		infoLogger.InfoContext(parent, "migration database resolved",
			"form", "components", "source", source,
			"database", config.ComponentDatabaseIdentity(migrationDatabaseSpec.Scheme, migrationURI))
	} else {
		infoLogger.InfoContext(parent, "migration database resolved", "form", "uri", "source", source)
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
	// The api role (CHAOS-6269) is applied only when it exists; see
	// riverstore.MigrationOptions.APIRole.
	apiRole := defaultAPIRole
	if value, present := lookup("API_DATABASE_ROLE"); present && strings.TrimSpace(value) != "" {
		apiRole = value
	}
	apiTableGrants, apiColumnGrants, apiSequences := postureGrants(postgresstore.APIPosture())
	queryAPIRole, queryAPIWriteGrants := queryAPILeg(lookup)
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
	migrateBuildID := version.Current(service).Commit
	if migrateBuildID == "" || migrateBuildID == "unknown" {
		migrateBuildID = "manifest:" + postureManifestDigest
	}
	nativeRoutes, err := jobcontract.NativeRiverRouteKinds(jobsv1.MigrationState)
	if err != nil {
		fmt.Fprintln(stderr, "migration error: embedded job migration policy is invalid")
		return 1
	}
	migrationOptions := riverstore.MigrationOptions{
		Schema:                  schema,
		DomainRole:              domainRole,
		QueueRole:               queueRole,
		CoordinatorRole:         coordinatorRole,
		CoordinatorGrants:       coordinatorTableGrants,
		CoordinatorColumnGrants: coordinatorColumnGrants,
		CoordinatorSequences:    coordinatorSequences,
		APIRole:                 apiRole,
		APIGrants:               apiTableGrants,
		APIColumnGrants:         apiColumnGrants,
		APISequences:            apiSequences,
		QueryAPIRole:            queryAPIRole,
		QueryAPIWriteGrants:     queryAPIWriteGrants,
		PostureManifestDigest:   postureManifestDigest,
		PostureManifestBuildID:  migrateBuildID,
		NativeRiverRoutes:       nativeRoutes,
	}
	if err := riverstore.ValidateMigrationOptions(migrationOptions); err != nil ||
		migrationRole == domainRole || migrationRole == queueRole || migrationRole == coordinatorRole ||
		migrationRole == apiRole {
		config.WriteConfigError(stderr, errors.New("migration, domain, queue-control, coordinator, and api PostgreSQL roles must be distinct"))
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

	logger := logging.NewJSON(stderr, slog.LevelWarn)
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
	logRouteSeed(ctx, infoLogger, result)
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
	if !*applyAndCheck {
		return 0
	}
	// The check runs on its own pool, as the separate `--check` process the
	// hook used to start did: it asserts what a fresh connection sees, not
	// what the applying session left behind.
	pool.Close()
	checkPool, err := postgresstore.Open(ctx, poolConfig)
	if err != nil {
		fmt.Fprintln(stderr, "migration error: PostgreSQL migration endpoint unavailable for the check")
		return 1
	}
	defer checkPool.Close()
	infoLogger.InfoContext(ctx, "river schema check after apply", "check", "river_schema")
	current, err := riverstore.CheckSchema(ctx, checkPool, schema, logger)
	return reportSchemaCheck(ctx, logger, stdout, stderr, current, err)
}

// logRouteSeed records, per kind, whether this run created its route row or
// found one already there. A route table that does not exist yet is a warning:
// every River-only kind then still lacks its row, and the relay and reconciler
// resolve such a kind as unknown until the next migrate run after the
// application schema migration has created the table.
func logRouteSeed(ctx context.Context, logger *slog.Logger, result riverstore.MigrationResult) {
	if result.RouteTableAbsent {
		logger.WarnContext(ctx, "worker job route seeding skipped",
			"reason", "worker_job_routes_absent")
		return
	}
	for _, kind := range result.SeededRoutes {
		logger.InfoContext(ctx, "worker job route seeded", "job_kind", kind, "transport", "river")
	}
	for _, kind := range result.PresentRoutes {
		logger.InfoContext(ctx, "worker job route present", "job_kind", kind)
	}
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

// resolveMigrationDatabaseURI resolves the elevated migration DSN; the rules
// live in config.ResolveMigrationDatabase, shared with every migrate step.
func resolveMigrationDatabaseURI(
	lookup platformsecrets.LookupEnv,
	stderr io.Writer,
	fallbackToPostgres bool,
) (platformsecrets.Value, string, bool) {
	return config.ResolveMigrationDatabase(lookup, stderr, fallbackToPostgres)
}

// migrationDatabaseSpec is config's one ComponentSpec for
// MIGRATION_DATABASE_URI, named here for execute's Info record.
var migrationDatabaseSpec = config.MigrationDatabaseSpec

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
	return postureGrants(postgresstore.CoordinatorPosture())
}

// postureGrants converts one role's declared posture into the migration's
// grant options, so the grant side and the readiness side are one list.
// queryAPILeg reads the query-api role leg (CHAOS-6803). It is opt-in: with
// QUERY_API_DATABASE_ROLE unset or blank nothing is granted and nothing
// changes. When it names a role that exists, the migration adds the ADDITIVE
// write grants postgres.QueryAPIWritePosture() declares -- see
// riverstore.MigrationOptions.QueryAPIRole for why that leg never revokes.
func queryAPILeg(lookup func(string) (string, bool)) (string, []riverstore.TableGrant) {
	value, _ := lookup("QUERY_API_DATABASE_ROLE")
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	grants, _, _ := postureGrants(postgresstore.QueryAPIWritePosture())
	return value, grants
}

func postureGrants(posture postgresstore.RolePosture) ([]riverstore.TableGrant, []riverstore.ColumnGrant, []string) {
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
