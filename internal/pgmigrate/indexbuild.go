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

const failureStreakBuildSQL = "CREATE INDEX CONCURRENTLY " + FailureStreakIndexBody

const failureStreakDropSQL = "DROP INDEX CONCURRENTLY IF EXISTS public." + FailureStreakIndexName

// IndexBuildOptions bound how long one attempt may wait for the table lock, how long the whole build may wait for
// older transactions, and how often a failed attempt is retried.
type IndexBuildOptions struct {
	// LockTimeout bounds the probe that takes the table lock (SHARE UPDATE EXCLUSIVE) before each attempt.
	LockTimeout time.Duration
	Attempts    int
	RetryDelay  time.Duration
	// Deadline is the window of the whole build: CREATE/DROP INDEX CONCURRENTLY wait for older transactions up to it.
	// Zero means DefaultIndexBuildDeadline.
	Deadline time.Duration
	// CleanupBudget bounds the drop that follows a final failure. Zero means DefaultIndexCleanupBudget.
	CleanupBudget time.Duration
	// SampleEvery and HeartbeatEvery set the blocker sampling and the "waiting" log cadence (1 s and 30 s when zero).
	SampleEvery    time.Duration
	HeartbeatEvery time.Duration
}

const (
	DefaultIndexBuildDeadline  = 30 * time.Minute
	DefaultIndexCleanupBudget  = 5 * time.Minute
	defaultSampleEvery         = time.Second
	defaultHeartbeatEvery      = 30 * time.Second
	statementTimeoutResetLimit = 10 * time.Second
)

// IndexBuildResult is what one build-index run did. Action is "present" (a valid index already existed),
// "built", or "rebuilt" (an invalid leftover was dropped first).
type IndexBuildResult struct {
	Index    string `json:"index"`
	Action   string `json:"action"`
	Attempts int    `json:"attempts"`
	Valid    bool   `json:"valid"`
}

// BuildFailureStreakIndex builds the index with CREATE INDEX CONCURRENTLY, outside any transaction.
//
// The lock_timeout of the session caps every lock wait of a statement, and CREATE/DROP INDEX CONCURRENTLY wait for every
// older transaction through lock waits too (virtualxid): one transaction older than lock_timeout failed each attempt.
// Those waits block no writer, so they run under lock_timeout = 0 bounded by the build window (statement_timeout and a
// context deadline). The table lock is taken first by a short probe under lock_timeout, so a stuck lock queue still ends
// the attempt. A failed attempt leaves an INVALID index, which the next attempt drops before it builds again. It returns
// nil only when the index is present, valid and ready.
func BuildFailureStreakIndex(ctx context.Context, conn *pgx.Conn, options IndexBuildOptions, logger *slog.Logger) (IndexBuildResult, error) {
	result := IndexBuildResult{Index: FailureStreakIndexName}
	if options.Attempts < 1 {
		options.Attempts = 1
	}
	if options.Deadline <= 0 {
		options.Deadline = DefaultIndexBuildDeadline
	}
	if _, err := conn.Exec(ctx, "SET statement_timeout = 0"); err != nil {
		return result, fmt.Errorf("set statement_timeout: %w", err)
	}
	if err := setWaitBounds(ctx, conn, options.LockTimeout, 0); err != nil {
		return result, fmt.Errorf("set lock_timeout: %w", err)
	}
	var hasTable bool
	if err := conn.QueryRow(ctx, "SELECT to_regclass('public.sync_run_units') IS NOT NULL").Scan(&hasTable); err != nil {
		return result, fmt.Errorf("look for sync_run_units: %w", err)
	}
	if !hasTable {
		return result, errors.New("public.sync_run_units does not exist: the database is not at the baseline")
	}
	windowEnd := time.Now().Add(options.Deadline)
	watch := startBlockerWatch(ctx, conn, options, logger)
	defer watch.stop()

	info, err := readIndexInfo(ctx, conn)
	if err != nil {
		return result, err
	}
	logIndexInfo(logger, "start", 0, info)
	if info.ready() {
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
		if info, err = readIndexInfo(ctx, conn); err != nil {
			return result, err
		}
		if attempt > 1 {
			logIndexInfo(logger, "retry", attempt, info)
		}
		if info.ready() {
			lastErr = nil
			break
		}
		stage, err := runBuildAttempt(ctx, conn, info, &result, windowEnd, options.LockTimeout)
		if err == nil {
			lastErr = nil
			break
		}
		lastErr = err
		if stage == "drop" {
			logger.Warn("build-index: drop of the invalid index failed", "attempt", attempt, "sqlstate", sqlState(err))
		} else {
			logger.Warn("build-index: attempt failed", "attempt", attempt, "stage", stage, "sqlstate", sqlState(err))
		}
		watch.logBlockers(stage, attempt)
		if post, perr := readIndexInfo(context.WithoutCancel(ctx), conn); perr == nil {
			logIndexInfo(logger, "after_failed_attempt", attempt, post)
		}
		if windowExpired(err, windowEnd) {
			break
		}
	}
	if lastErr != nil {
		dropInvalidFailureStreakIndex(ctx, conn, options, logger)
		return result, fmt.Errorf("the index was not built after %d attempts: %w", result.Attempts, lastErr)
	}
	info, err = readIndexInfo(ctx, conn)
	if err != nil {
		return result, err
	}
	if !info.ready() {
		return result, fmt.Errorf("index %s is not valid after the build (exists=%t)", FailureStreakIndexName, info.exists)
	}
	result.Valid = true
	return result, nil
}

// runBuildAttempt runs one attempt: probe the table lock, drop an INVALID leftover, build. stage names where it failed.
func runBuildAttempt(ctx context.Context, conn *pgx.Conn, info indexInfo, result *IndexBuildResult, windowEnd time.Time, lock time.Duration) (stage string, err error) {
	if err := probeTableLock(ctx, conn); err != nil {
		return "probe", err
	}
	if info.exists {
		result.Action = "rebuilt"
		if err := execWithinWindow(ctx, conn, failureStreakDropSQL, windowEnd, lock); err != nil {
			return "drop", err
		}
	}
	if err := execWithinWindow(ctx, conn, failureStreakBuildSQL, windowEnd, lock); err != nil {
		return "build", err
	}
	return "", nil
}

// probeTableLock takes and releases SHARE UPDATE EXCLUSIVE on the table under the session lock_timeout. That is the lock
// CREATE/DROP INDEX CONCURRENTLY hold: no writer conflicts with it, but a pending request that waits behind VACUUM or
// DDL can queue other DDL, so its wait stays short.
func probeTableLock(ctx context.Context, conn *pgx.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "LOCK TABLE public.sync_run_units IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return err
	}
	return tx.Commit(ctx)
}

func ms(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }

func setWaitBounds(ctx context.Context, conn *pgx.Conn, lock, statement time.Duration) error {
	_, err := conn.Exec(ctx, "SELECT set_config('lock_timeout', $1, false), set_config('statement_timeout', $2, false)", ms(lock), ms(statement))
	return err
}

var errWindowExpired = errors.New("the build window ended")

// execWithinWindow runs one statement with lock_timeout = 0 and statement_timeout = the rest of the window, then puts the
// session lock_timeout back. The context deadline is a backstop behind statement_timeout.
func execWithinWindow(ctx context.Context, conn *pgx.Conn, sql string, windowEnd time.Time, restoreLock time.Duration) error {
	remaining := time.Until(windowEnd)
	if remaining < time.Second {
		return errWindowExpired
	}
	if err := setWaitBounds(ctx, conn, 0, remaining); err != nil {
		return err
	}
	runCtx, cancel := context.WithDeadline(ctx, windowEnd.Add(statementTimeoutResetLimit))
	defer cancel()
	_, err := conn.Exec(runCtx, sql)
	resetCtx, resetCancel := context.WithTimeout(context.WithoutCancel(ctx), statementTimeoutResetLimit)
	defer resetCancel()
	_ = setWaitBounds(resetCtx, conn, restoreLock, 0)
	return err
}

func windowExpired(err error, windowEnd time.Time) bool {
	if errors.Is(err, errWindowExpired) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return sqlState(err) == "57014" && !time.Now().Before(windowEnd.Add(-2*time.Second))
}

const (
	failureStreakDropAttempts = 3
	failureStreakDropGap      = 2 * time.Second
)

// failureStreakDropBudget bounds the whole cleanup; it ignores RetryDelay and LockTimeout so no option can push it past the
// Job deadline.
func failureStreakDropBudget(options IndexBuildOptions) time.Duration {
	if options.CleanupBudget > 0 {
		return options.CleanupBudget
	}
	return DefaultIndexCleanupBudget
}

// dropInvalidFailureStreakIndex runs after the final failed build: an INVALID index is still maintained by every writer
// and must not stay. DROP INDEX CONCURRENTLY waits on the same open transactions that can fail the build, so it runs under
// lock_timeout = 0 within a bounded budget (it outlives a cancelled ctx). A VALID index is never dropped. A failure here is
// logged with its SQLSTATE only; the next run drops the leftover at the start of its first attempt.
func dropInvalidFailureStreakIndex(ctx context.Context, conn *pgx.Conn, options IndexBuildOptions, logger *slog.Logger) {
	budget := failureStreakDropBudget(options)
	windowEnd := time.Now().Add(budget)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget+statementTimeoutResetLimit)
	defer cancel()
	for attempt := 1; attempt <= failureStreakDropAttempts; attempt++ {
		info, err := readIndexInfo(ctx, conn)
		if err != nil {
			logger.Warn("build-index: invalid index left; the next run drops it", "stage", "state", "attempt", attempt, "sqlstate", sqlState(err), "index", FailureStreakIndexName)
			return
		}
		if !info.exists || info.ready() {
			return
		}
		if err := execWithinWindow(ctx, conn, failureStreakDropSQL, windowEnd, options.LockTimeout); err == nil {
			logger.Info("build-index: dropped the invalid index after the final failure", "attempt", attempt)
			return
		} else {
			logger.Warn("build-index: drop of the invalid index failed", "attempt", attempt, "sqlstate", sqlState(err))
			if attempt == failureStreakDropAttempts || windowExpired(err, windowEnd) {
				logger.Warn("build-index: invalid index left; the next run drops it", "stage", "drop", "attempt", attempt, "sqlstate", sqlState(err), "index", FailureStreakIndexName)
				return
			}
		}
		select {
		case <-ctx.Done():
			logger.Warn("build-index: invalid index left; the next run drops it", "stage", "budget", "attempt", attempt, "index", FailureStreakIndexName)
			return
		case <-time.After(failureStreakDropGap):
		}
	}
}

// indexInfo is the catalog state of the index.
type indexInfo struct {
	exists, valid, isReady bool
}

func (i indexInfo) ready() bool { return i.exists && i.valid && i.isReady }

func (i indexInfo) state() string {
	switch {
	case !i.exists:
		return "none"
	case !i.valid:
		return "invalid"
	case !i.isReady:
		return "not_ready"
	}
	return "valid"
}

func logIndexInfo(logger *slog.Logger, when string, attempt int, i indexInfo) {
	logger.Info("build-index: existing index", "when", when, "attempt", attempt, "state", i.state(), "indisvalid", i.valid, "indisready", i.isReady)
}

// readIndexInfo reads whether the index exists and its indisvalid and indisready flags.
func readIndexInfo(ctx context.Context, conn *pgx.Conn) (indexInfo, error) {
	rows, err := conn.Query(ctx, `
SELECT i.indisvalid, i.indisready
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relnamespace = 'public'::regnamespace AND c.relname = $1 AND i.indrelid = 'public.sync_run_units'::regclass`,
		FailureStreakIndexName)
	if err != nil {
		return indexInfo{}, fmt.Errorf("read the index state: %w", err)
	}
	defer rows.Close()
	info := indexInfo{}
	if rows.Next() {
		info.exists = true
		if err := rows.Scan(&info.valid, &info.isReady); err != nil {
			return indexInfo{}, err
		}
	}
	return info, rows.Err()
}

func buildIndex(ctx context.Context, resolve ResolveDSN, env cli.Env) int {
	flags := flag.NewFlagSet("dho migrate postgres build-index", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	options := IndexBuildOptions{}
	flags.DurationVar(&options.LockTimeout, "lock-timeout", 5*time.Second, "lock_timeout of the table-lock probe before each attempt")
	flags.DurationVar(&options.Deadline, "deadline", DefaultIndexBuildDeadline, "window of the whole build: the waits for older transactions run up to it")
	flags.IntVar(&options.Attempts, "attempts", 3, "attempts before the verb fails (retries of transient failures; waiting does not use them)")
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
	if options.LockTimeout < time.Second || options.LockTimeout > time.Minute || options.Attempts < 1 || options.RetryDelay < 0 || options.Deadline < 10*time.Second || options.Deadline > 4*time.Hour {
		fmt.Fprintln(env.Stderr, "argument error: --lock-timeout must be 1s to 1m, --deadline 10s to 4h, --attempts at least 1, --retry-delay not negative")
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
