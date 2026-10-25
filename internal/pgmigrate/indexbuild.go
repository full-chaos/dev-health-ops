package pgmigrate

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// FailureStreakIndexName is the index revision 0159 creates and the build-index verb builds first, without
// blocking writes, on a database whose sync_run_units is large.
const FailureStreakIndexName = "ix_sync_run_units_failure_streak"

// FailureStreakIndexBody is the definition shared by the verb and revision 0159 (TestFailureStreakIndexDefinitionMatchesRevision0159
// holds the two together): the equality columns of providersync.RecentTerminalUnitsSQL, then its ORDER BY, over its status predicate.
const FailureStreakIndexBody = FailureStreakIndexName + ` ON public.sync_run_units (org_id, integration_id, source_id, dataset_key, updated_at DESC, id DESC)
WHERE status IN ('success', 'failed')`

const failureStreakBuildSQL = "CREATE INDEX CONCURRENTLY IF NOT EXISTS " + FailureStreakIndexBody

const failureStreakDropSQL = "DROP INDEX CONCURRENTLY IF EXISTS public." + FailureStreakIndexName

// IndexBuildOptions bound how long one attempt may wait for a lock and how often the build is retried.
type IndexBuildOptions struct {
	LockTimeout time.Duration
	Attempts    int
	RetryDelay  time.Duration
}

// IndexBuildResult is what one build-index run did. Action is "present" (a valid index already existed),
// "built", or "rebuilt" (an invalid leftover was dropped first).
type IndexBuildResult struct {
	Index    string `json:"index"`
	Action   string `json:"action"`
	Attempts int    `json:"attempts"`
	Valid    bool   `json:"valid"`
}

// BuildFailureStreakIndex builds the index with CREATE INDEX CONCURRENTLY, outside any transaction. Each attempt runs
// under lock_timeout, so a stuck lock queue ends the attempt instead of stalling the writers behind it; a failed attempt
// leaves an INVALID index, which the next attempt drops before it builds again. It returns nil only when the index is
// present, valid and ready.
func BuildFailureStreakIndex(ctx context.Context, conn *pgx.Conn, options IndexBuildOptions, logger *slog.Logger) (IndexBuildResult, error) {
	result := IndexBuildResult{Index: FailureStreakIndexName}
	if options.Attempts < 1 {
		options.Attempts = 1
	}
	if _, err := conn.Exec(ctx, "SET statement_timeout = 0"); err != nil {
		return result, fmt.Errorf("set statement_timeout: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT set_config('lock_timeout', $1, false)", strconv.FormatInt(options.LockTimeout.Milliseconds(), 10)); err != nil {
		return result, fmt.Errorf("set lock_timeout: %w", err)
	}
	var hasTable bool
	if err := conn.QueryRow(ctx, "SELECT to_regclass('public.sync_run_units') IS NOT NULL").Scan(&hasTable); err != nil {
		return result, fmt.Errorf("look for sync_run_units: %w", err)
	}
	if !hasTable {
		return result, errors.New("public.sync_run_units does not exist: the database is not at the baseline")
	}
	valid, exists, err := failureStreakIndexState(ctx, conn)
	if err != nil {
		return result, err
	}
	if exists && valid {
		result.Action, result.Valid = "present", true
		return result, nil
	}
	result.Action = "built"
	var lastErr error
	for attempt := 1; attempt <= options.Attempts; attempt++ {
		result.Attempts = attempt
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			case <-time.After(options.RetryDelay):
			}
		}
		if _, exists, err = failureStreakIndexState(ctx, conn); err != nil {
			return result, err
		}
		if exists {
			// IF NOT EXISTS would skip an INVALID leftover and report success.
			result.Action = "rebuilt"
			if _, err := conn.Exec(ctx, failureStreakDropSQL); err != nil {
				lastErr = err
				logger.Warn("build-index: drop of the invalid index failed", "attempt", attempt, "sqlstate", sqlState(err))
				continue
			}
		}
		if _, err := conn.Exec(ctx, failureStreakBuildSQL); err != nil {
			lastErr = err
			logger.Warn("build-index: attempt failed", "attempt", attempt, "sqlstate", sqlState(err))
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		return result, fmt.Errorf("the index was not built after %d attempts: %w", result.Attempts, lastErr)
	}
	valid, exists, err = failureStreakIndexState(ctx, conn)
	if err != nil {
		return result, err
	}
	if !exists || !valid {
		return result, fmt.Errorf("index %s is not valid after the build (exists=%t)", FailureStreakIndexName, exists)
	}
	result.Valid = true
	return result, nil
}

// failureStreakIndexState reads whether the index exists and whether it is valid and ready.
func failureStreakIndexState(ctx context.Context, conn *pgx.Conn) (valid, exists bool, err error) {
	rows, err := conn.Query(ctx, `
SELECT i.indisvalid AND i.indisready
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relnamespace = 'public'::regnamespace AND c.relname = $1 AND i.indrelid = 'public.sync_run_units'::regclass`,
		FailureStreakIndexName)
	if err != nil {
		return false, false, fmt.Errorf("read the index state: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		exists = true
		if err := rows.Scan(&valid); err != nil {
			return false, false, err
		}
	}
	return valid, exists, rows.Err()
}

func buildIndex(ctx context.Context, resolve ResolveDSN, env cli.Env) int {
	flags := flag.NewFlagSet("dho migrate postgres build-index", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	options := IndexBuildOptions{}
	flags.DurationVar(&options.LockTimeout, "lock-timeout", 5*time.Second, "lock_timeout of each attempt")
	flags.IntVar(&options.Attempts, "attempts", 20, "attempts before the verb fails")
	flags.DurationVar(&options.RetryDelay, "retry-delay", 15*time.Second, "pause between attempts")
	defaultUsage := flags.Usage
	flags.Usage = func() {
		defaultUsage()
		fmt.Fprint(env.Stderr, "\nUsage: dho migrate postgres build-index failure-streak\n"+
			"\nBuilds "+FailureStreakIndexName+" CONCURRENTLY (revision 0159 then finds it and does nothing).\n"+
			"Exit 0 only when the index is present and valid.\n"+
			"\nEnvironment:\n"+
			"  MIGRATION_DATABASE_URI (or _FILE, or the DEV_HEALTH_MIGRATION_PG_* component form)   elevated DSN, direct to PostgreSQL\n"+
			"  POSTGRES_URI (or _FILE)                   used when MIGRATION_DATABASE_URI is not configured\n")
	}
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	positional := flags.Args()
	if len(positional) > 0 {
		// The standard parser stops at the first positional argument: read the flags that follow it too.
		if err := flags.Parse(positional[1:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return cli.ExitOK
			}
			return cli.ExitUsage
		}
		positional = append(positional[:1], flags.Args()...)
	}
	if len(positional) != 1 || positional[0] != "failure-streak" {
		fmt.Fprintln(env.Stderr, "argument error: the one index this verb builds is named failure-streak")
		return cli.ExitUsage
	}
	if options.LockTimeout < time.Second || options.LockTimeout > time.Minute || options.Attempts < 1 || options.RetryDelay < 0 {
		fmt.Fprintln(env.Stderr, "argument error: --lock-timeout must be 1s to 1m, --attempts at least 1, --retry-delay not negative")
		return cli.ExitUsage
	}
	dsn, _, ok := resolve(env.Lookup, env.Stderr)
	if !ok {
		return cli.ExitFailure
	}
	boundary := pgstorage.Boundary(dsn.Reveal())
	conn, err := pgx.Connect(ctx, dsn.Reveal())
	if err != nil {
		return writeError(env.Stderr, "postgres_unavailable", boundary.Redact(err).Error())
	}
	defer conn.Close(context.Background())
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	result, err := BuildFailureStreakIndex(ctx, conn, options, logger)
	if err != nil {
		return writeError(env.Stderr, "build_index_failed", boundary.Redact(err).Error())
	}
	return writeResult(env.Stdout, env.Stderr, result)
}
