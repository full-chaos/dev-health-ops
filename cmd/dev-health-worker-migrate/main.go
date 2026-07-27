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

	migrationURI, ok := requiredSecret("MIGRATION_DATABASE_URI", lookup, stderr)
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
	coordinatorTableGrants, coordinatorColumnGrants := coordinatorGrants()
	migrationOptions := riverstore.MigrationOptions{
		Schema:                  schema,
		DomainRole:              domainRole,
		QueueRole:               queueRole,
		CoordinatorRole:         coordinatorRole,
		CoordinatorGrants:       coordinatorTableGrants,
		CoordinatorColumnGrants: coordinatorColumnGrants,
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
		if err != nil {
			fmt.Fprintln(stderr, "migration check failed: River schema is not current")
			return 1
		}
		fmt.Fprintf(stdout, "River schema current at pinned version %d\n", current)
		return 0
	}

	migrationOptions.Logger = logger
	result, err := riverstore.ApplyPinnedMigrations(ctx, pool, migrationOptions)
	if err != nil {
		fmt.Fprintln(stderr, "migration failed: River schema or privilege policy was not applied")
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

func requiredName(key string, lookup platformsecrets.LookupEnv, stderr io.Writer) (string, bool) {
	value, configured := lookup(key)
	if !configured || strings.TrimSpace(value) == "" {
		fmt.Fprintf(stderr, "configuration error: %s is required\n", key)
		return "", false
	}
	return value, true
}

func requiredSecret(
	key string,
	lookup platformsecrets.LookupEnv,
	stderr io.Writer,
) (platformsecrets.Value, bool) {
	value, configured, err := platformsecrets.Resolve(key, lookup)
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: could not resolve %s\n", key)
		return platformsecrets.Value{}, false
	}
	if !configured {
		fmt.Fprintf(stderr, "configuration error: %s is required\n", key)
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
func coordinatorGrants() ([]riverstore.TableGrant, []riverstore.ColumnGrant) {
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
	return grants, columns
}
