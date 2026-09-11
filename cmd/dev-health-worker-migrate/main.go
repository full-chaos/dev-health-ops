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
	migrationRole, err := postgresstore.ConnectionUser(migrationURI.Reveal())
	if err != nil {
		fmt.Fprintln(stderr, "configuration error: invalid MIGRATION_DATABASE_URI")
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
		fmt.Fprintln(stderr, "configuration error: migration, domain, queue-control, and coordinator PostgreSQL roles must be distinct")
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
		fmt.Fprintf(stderr, "configuration error: %s is required\n", key)
		return "", false
	}
	return value, true
}

// resolveMigrationDatabaseURI is CHAOS-5560's component alternative to a
// pre-built MIGRATION_DATABASE_URI: compose.yml's own entrypoint already
// assembles a fallback DSN by raw shell interpolation
// (postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/$POSTGRES_DB)
// when MIGRATION_DATABASE_URI is unset -- exactly the unescaped-password
// class this ticket fixes, just one shell layer further out. Setting
// POSTGRES_HOST here builds the same URI safely instead, sharing
// config.ResolveDSNFromComponents with the runtime binaries rather than a
// second hand-rolled DSN builder.
//
// MIGRATION_DATABASE_URI wins whenever it is set; POSTGRES_HOST is a
// fallback used only when no MIGRATION_DATABASE_URI is present at all.
// This is deliberately the OPPOSITE precedence from
// internal/platform/config's four runtime DSNs, where a set HOST var wins
// outright -- because deploy/docker-compose/compose.go-workers.yml (not
// touched by this PR) ALWAYS sets POSTGRES_HOST, defaulted to "postgres",
// for its own pre-existing shell-fallback entrypoint. A round-1 review
// (2026-09-11) proved that "HOST wins outright" here silently discarded a
// perfectly valid, already-working MIGRATION_DATABASE_URI override the
// moment that compose service ran, because its POSTGRES_HOST default is
// present unconditionally, not just when an operator means to opt into
// components. Preferring the explicit URI when set restores today's
// behavior (the entrypoint's own shell fallback, or an operator's
// GO_WORKER_MIGRATION_DATABASE_URI override, both arrive as a non-empty
// MIGRATION_DATABASE_URI and win as before); components remain available
// for a caller -- a future Kubernetes Job, say -- that sets POSTGRES_HOST
// and genuinely never sets MIGRATION_DATABASE_URI at all.
func resolveMigrationDatabaseURI(
	lookup platformsecrets.LookupEnv,
	stderr io.Writer,
) (platformsecrets.Value, bool) {
	uri, configured, err := platformsecrets.Resolve("MIGRATION_DATABASE_URI", lookup)
	if err != nil {
		fmt.Fprintln(stderr, "configuration error: could not resolve MIGRATION_DATABASE_URI")
		return platformsecrets.Value{}, false
	}
	if configured {
		return uri, true
	}
	built, used, err := config.ResolveDSNFromComponents(lookup, config.ComponentSpec{
		HostKey: "POSTGRES_HOST", PortKey: "POSTGRES_PORT", DefaultPort: "5432",
		UserKey: "POSTGRES_USER", PasswordKey: "POSTGRES_PASSWORD",
		DBKey: "POSTGRES_DB", DefaultDB: "postgres", Scheme: "postgresql",
	})
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: %v\n", err)
		return platformsecrets.Value{}, false
	}
	if used {
		return built, true
	}
	fmt.Fprintln(stderr, "configuration error: MIGRATION_DATABASE_URI is required")
	return platformsecrets.Value{}, false
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
