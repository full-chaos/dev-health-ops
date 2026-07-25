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

const serviceName = "dev-health-worker-migrate"

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
	migrationOptions := riverstore.MigrationOptions{
		Schema:     schema,
		DomainRole: domainRole,
		QueueRole:  queueRole,
	}
	if err := riverstore.ValidateMigrationOptions(migrationOptions); err != nil || migrationRole == domainRole || migrationRole == queueRole {
		fmt.Fprintln(stderr, "configuration error: migration, domain, and queue-control PostgreSQL roles must be distinct")
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
