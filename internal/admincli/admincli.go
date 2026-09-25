// Package admincli is the `admin` group of dho: operator verbs over the
// application database. Its first verb is `admin features seed`, which the
// migrate Job runs after the PostgreSQL migrations.
package admincli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// Command is the `admin` group.
func Command() cli.Command {
	return cli.Command{
		Name:    "admin",
		Summary: "operator verbs over the application database",
		Kind:    cli.Group,
		Children: []cli.Command{{
			Name:    "features",
			Summary: "the standard feature flags",
			Kind:    cli.Group,
			Children: []cli.Command{{
				Name:    "seed",
				Summary: "insert every standard feature flag the database does not hold yet",
				Kind:    cli.Verb,
				Run:     runSeed,
			}},
		}},
	}
}

// SeedResult is what one seed did.
type SeedResult struct {
	Created []string `json:"created"`
}

// Seed inserts every feature of StandardFeatures whose key feature_flags does
// not hold, as the Python seed_feature_flags_async does: a fresh uuid4 id,
// enabled, not beta, not deprecated, config_schema the JSON value null (the
// model's JSON column writes Python None that way, not as SQL NULL), created
// and updated now. A key already present is left as it is. It runs in one
// transaction.
func Seed(ctx context.Context, conn *pgx.Conn, now func() time.Time) (SeedResult, error) {
	result := SeedResult{Created: []string{}}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, "SELECT key FROM feature_flags")
	if err != nil {
		return result, fmt.Errorf("read feature_flags: %w", err)
	}
	keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return result, fmt.Errorf("read feature_flags: %w", err)
	}
	existing := make(map[string]bool, len(keys))
	for _, key := range keys {
		existing[key] = true
	}
	for _, feature := range StandardFeatures {
		if existing[feature.Key] {
			continue
		}
		stamp := now().UTC()
		if _, err := tx.Exec(ctx, "INSERT INTO feature_flags "+
			"(id, key, name, description, category, min_tier, is_enabled, is_beta, is_deprecated, config_schema, created_at, updated_at) "+
			"VALUES ($1, $2, $3, $4, $5, $6, true, false, false, 'null', $7, $7)",
			uuid.New(), feature.Key, feature.Name, feature.Description, feature.Category, feature.MinTier, stamp,
		); err != nil {
			return result, fmt.Errorf("insert feature %s: %w", feature.Key, err)
		}
		result.Created = append(result.Created, feature.Key)
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func runSeed(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho admin features seed", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	defaultUsage := flags.Usage
	flags.Usage = func() {
		defaultUsage()
		fmt.Fprint(env.Stderr, "\nEnvironment:\n"+
			"  MIGRATION_DATABASE_URI (or _FILE, or the DEV_HEALTH_MIGRATION_PG_* component form)   the database, as the migrate Job gives it\n"+
			"  POSTGRES_URI (or _FILE)   used when MIGRATION_DATABASE_URI is not configured\n")
	}
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted")
		return cli.ExitUsage
	}
	dsn, source, ok := config.ResolveMigrationDatabase(env.Lookup, env.Stderr, true)
	if !ok {
		return cli.ExitFailure
	}
	boundary := pgstorage.Boundary(dsn.Reveal())
	conn, err := pgx.Connect(ctx, dsn.Reveal())
	if err != nil {
		return writeError(env.Stderr, "postgres_unavailable", boundary.Redact(err).Error())
	}
	defer conn.Close(context.Background())
	// Which database the seed writes, named without credentials: the
	// setting it came from, the host and the database name.
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	logger.Info("feature seed database", "source", source, "host", conn.Config().Host, "database", conn.Config().Database)
	started := time.Now()
	result, err := Seed(ctx, conn, time.Now)
	if err != nil {
		return writeError(env.Stderr, "seed_failed", boundary.Redact(err).Error())
	}
	logger.Info("feature seed done", "created", len(result.Created), "duration_ms", time.Since(started).Milliseconds())
	if err := json.NewEncoder(env.Stdout).Encode(result); err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	return cli.ExitOK
}

func writeError(stderr io.Writer, code, detail string) int {
	_ = json.NewEncoder(stderr).Encode(map[string]any{"error": map[string]string{"code": code, "detail": detail}})
	return cli.ExitFailure
}
