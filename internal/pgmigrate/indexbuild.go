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

// OpenUnitsIndexName is the partial index over the units that are not final. The materializer's dispatch and
// finalize candidate reads probe sync_run_units once per active run; this index holds only the open units, so each
// probe reads a small index. Revision 0160 creates it; the build-index verb builds it first without blocking writes.
const OpenUnitsIndexName = "ix_sync_run_units_open_run"

// OpenUnitsIndexBody is the definition shared by the verb and revision 0160 (TestOpenUnitsIndexDefinitionMatchesRevision0160
// holds the two together). Its predicate is the complement of the materializer's finalize predicate (unit.status NOT IN
// ('success', 'failed')); the dispatch statuses are a subset of it.
const OpenUnitsIndexBody = OpenUnitsIndexName + ` ON public.sync_run_units (sync_run_id)
WHERE status NOT IN ('success', 'failed')`

// unitsIndex is one index of sync_run_units the build-index verb knows how to build. Each has its own build lock key, so
// a run for one index never reads another index's run as the holder of its lock.
type unitsIndex struct {
	verb    string
	name    string
	body    string
	lockKey int64
}

var unitsIndexes = []unitsIndex{
	{verb: "failure-streak", name: FailureStreakIndexName, body: FailureStreakIndexBody, lockKey: buildIndexLockKey},
	{verb: "open-units", name: OpenUnitsIndexName, body: OpenUnitsIndexBody, lockKey: openUnitsBuildIndexLockKey},
}

func unitsIndexByVerb(verb string) (unitsIndex, bool) {
	for _, index := range unitsIndexes {
		if index.verb == verb {
			return index, true
		}
	}
	return unitsIndex{}, false
}

func (index unitsIndex) buildSQL() string { return "CREATE INDEX CONCURRENTLY " + index.body }

func (index unitsIndex) dropSQL() string {
	return "DROP INDEX CONCURRENTLY IF EXISTS public." + index.name
}

// buildIndexLockKey is the session advisory lock ("dho_ix") one build-index run holds from its first state read to its
// return. It is not the migrator's lockKey, so it never holds up a migration.
const buildIndexLockKey int64 = 0x64686f5f6978

// openUnitsBuildIndexLockKey is the same lock for the open-units index ("dho_iy"): the two indexes build independently.
const openUnitsBuildIndexLockKey int64 = 0x64686f5f6979

// ErrBuildLockHeld is the refusal of a run that finds another build-index run holding the build lock while the index is not
// valid. The run sends no DDL: an INVALID index then may be the other run's build in progress.
var ErrBuildLockHeld = errors.New("another build-index run holds the lock")

// ErrBackendChanged refuses a run whose session moved to another PostgreSQL backend under the build lock, as a
// transaction-mode pooler moves it: the lock then guards nothing, so the run sends no further DDL.
var ErrBackendChanged = errors.New("the database backend changed under the build lock: build-index needs a direct or session-mode PostgreSQL connection")

// ErrBuildLockLost reports a release that found the build lock not held by the run's backend.
var ErrBuildLockLost = errors.New("the build lock was not held at release")

// lockedSession is the one connection that holds the build lock and sends every DDL and the unlock, with the backend pid
// read in the statement that took the lock.
type lockedSession struct {
	conn *pgx.Conn
	pid  int32
}

func (s lockedSession) sameBackend(ctx context.Context) error {
	var pid int32
	if err := s.conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		return fmt.Errorf("read the backend pid: %w", err)
	}
	if pid != s.pid {
		return fmt.Errorf("%w (pid %d at the lock, %d now)", ErrBackendChanged, s.pid, pid)
	}
	return nil
}

// exec runs one DDL statement within the window, after it checks that the session is still on the backend that holds the lock.
func (s lockedSession) exec(ctx context.Context, sql string, windowEnd time.Time, restoreLock time.Duration) error {
	if err := s.sameBackend(ctx); err != nil {
		return err
	}
	return execWithinWindow(ctx, s.conn, sql, windowEnd, restoreLock)
}

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

// BuildFailureStreakIndex builds ix_sync_run_units_failure_streak (see buildUnitsIndex).
func BuildFailureStreakIndex(ctx context.Context, conn *pgx.Conn, options IndexBuildOptions, logger *slog.Logger) (IndexBuildResult, error) {
	return buildUnitsIndex(ctx, conn, unitsIndexes[0], options, logger)
}

// BuildOpenUnitsIndex builds ix_sync_run_units_open_run (see buildUnitsIndex).
func BuildOpenUnitsIndex(ctx context.Context, conn *pgx.Conn, options IndexBuildOptions, logger *slog.Logger) (IndexBuildResult, error) {
	return buildUnitsIndex(ctx, conn, unitsIndexes[1], options, logger)
}

// buildUnitsIndex builds the index with CREATE INDEX CONCURRENTLY, outside any transaction.
//
// The lock_timeout of the session caps every lock wait of a statement, and CREATE/DROP INDEX CONCURRENTLY wait for every
// older transaction through lock waits too (virtualxid): one transaction older than lock_timeout failed each attempt.
// Those waits block no writer, so they run under lock_timeout = 0 bounded by the build window (statement_timeout and a
// context deadline). The table lock is taken first by a short probe under lock_timeout, so a stuck lock queue still ends
// the attempt. A failed attempt leaves an INVALID index, which the next attempt drops before it builds again. It returns
// nil only when the index is present, valid and ready.
//
// One run at a time acts: a run takes the build lock without waiting. A run that misses it reads the state once and
// returns "present" over a valid index, else ErrBuildLockHeld, with no DDL. The winner reads the state again after each
// table-lock probe, so an index a builder outside the lock made valid during the probe wait is kept.
//
// The lock is a session lock: the connection must stay on one backend (a direct or session-mode connection). The winner
// reads pg_backend_pid() with the lock, before each DDL and before the unlock; a change returns ErrBackendChanged with no
// further DDL. An unlock that answers false returns ErrBuildLockLost.
func buildUnitsIndex(ctx context.Context, conn *pgx.Conn, index unitsIndex, options IndexBuildOptions, logger *slog.Logger) (IndexBuildResult, error) {
	result := IndexBuildResult{Index: index.name}
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
	var locked bool
	var pid int32
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1), pg_backend_pid()", index.lockKey).Scan(&locked, &pid); err != nil {
		return result, fmt.Errorf("take the build lock: %w", err)
	}
	if !locked {
		info, err := readIndexInfo(ctx, conn, index.name)
		if err != nil {
			return result, err
		}
		logIndexInfo(logger, "build_lock_held", 0, info)
		if info.ready() {
			result.Action, result.Valid = "present", true
			return result, nil
		}
		return result, ErrBuildLockHeld
	}
	session := lockedSession{conn: conn, pid: pid}
	err := buildUnderLock(ctx, session, index, options, logger, &result)
	if releaseErr := releaseBuildLock(ctx, session, index.lockKey, logger); releaseErr != nil && err == nil {
		err = releaseErr
	}
	return result, err
}

// buildUnderLock is the winner's run: every statement goes through the session that holds the build lock.
func buildUnderLock(ctx context.Context, session lockedSession, index unitsIndex, options IndexBuildOptions, logger *slog.Logger, result *IndexBuildResult) error {
	conn := session.conn
	windowEnd := time.Now().Add(options.Deadline)
	watch := startBlockerWatch(ctx, conn, options, logger)
	defer watch.stop()

	info, err := readIndexInfo(ctx, conn, index.name)
	if err != nil {
		return err
	}
	logIndexInfo(logger, "start", 0, info)
	if info.ready() {
		result.Action, result.Valid = "present", true
		return nil
	}
	result.Action = "built"
	var lastErr error
	for attempt := 1; attempt <= options.Attempts; attempt++ {
		result.Attempts = attempt
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(options.RetryDelay):
			}
		}
		if info, err = readIndexInfo(ctx, conn, index.name); err != nil {
			return err
		}
		if attempt > 1 {
			logIndexInfo(logger, "retry", attempt, info)
		}
		if info.ready() {
			lastErr = nil
			break
		}
		stage, err := runBuildAttempt(ctx, session, index, result, windowEnd, options.LockTimeout)
		if err == nil {
			lastErr = nil
			break
		}
		if errors.Is(err, ErrBackendChanged) {
			logger.Warn("build-index: the backend changed under the build lock; no further DDL", "attempt", attempt, "stage", stage)
			return fmt.Errorf("attempt %d: %w", attempt, err)
		}
		lastErr = err
		if stage == "drop" {
			logger.Warn("build-index: drop of the invalid index failed", "attempt", attempt, "sqlstate", sqlState(err))
		} else {
			logger.Warn("build-index: attempt failed", "attempt", attempt, "stage", stage, "sqlstate", sqlState(err))
		}
		watch.logBlockers(stage, attempt)
		if post, perr := readIndexInfo(context.WithoutCancel(ctx), conn, index.name); perr == nil {
			logIndexInfo(logger, "after_failed_attempt", attempt, post)
		}
		if windowExpired(err, windowEnd) {
			break
		}
	}
	if lastErr != nil {
		dropInvalidUnitsIndex(ctx, session, index, options, logger)
		return fmt.Errorf("the index was not built after %d attempts: %w", result.Attempts, lastErr)
	}
	info, err = readIndexInfo(ctx, conn, index.name)
	if err != nil {
		return err
	}
	if !info.ready() {
		return fmt.Errorf("index %s is not valid after the build (exists=%t)", index.name, info.exists)
	}
	result.Valid = true
	return nil
}

// runBuildAttempt runs one attempt: probe the table lock, read the state, drop an INVALID leftover, build. stage names
// where it failed. The state is read after the probe: the probe can wait behind a builder that ends with a valid index.
func runBuildAttempt(ctx context.Context, session lockedSession, index unitsIndex, result *IndexBuildResult, windowEnd time.Time, lock time.Duration) (stage string, err error) {
	if err := probeTableLock(ctx, session.conn); err != nil {
		return "probe", err
	}
	info, err := readIndexInfo(ctx, session.conn, index.name)
	if err != nil {
		return "state", err
	}
	if info.ready() {
		result.Action = "present"
		return "", nil
	}
	if info.exists {
		result.Action = "rebuilt"
		if err := session.exec(ctx, index.dropSQL(), windowEnd, lock); err != nil {
			return "drop", err
		}
	}
	if err := session.exec(ctx, index.buildSQL(), windowEnd, lock); err != nil {
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

// releaseBuildLock unlocks on the backend that took the lock. A changed backend (ErrBackendChanged) and an unlock that
// answers false (ErrBuildLockLost) are errors with their own reason; a failed statement is only logged, as the session end
// releases the lock.
func releaseBuildLock(ctx context.Context, session lockedSession, lockKey int64, logger *slog.Logger) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), statementTimeoutResetLimit)
	defer cancel()
	if err := session.sameBackend(releaseCtx); err != nil {
		if errors.Is(err, ErrBackendChanged) {
			logger.Warn("build-index: build lock not released", "reason", "backend_changed")
			return err
		}
		logger.Warn("build-index: build lock not released; the session end releases it", "reason", "backend_pid_unread", "sqlstate", sqlState(err))
		return nil
	}
	var released bool
	if err := session.conn.QueryRow(releaseCtx, "SELECT pg_advisory_unlock($1)", lockKey).Scan(&released); err != nil {
		logger.Warn("build-index: build lock not released; the session end releases it", "reason", "unlock_failed", "sqlstate", sqlState(err))
		return nil
	}
	if !released {
		logger.Warn("build-index: build lock was not held at release", "reason", "unlock_returned_false")
		return ErrBuildLockLost
	}
	return nil
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
	unitsIndexDropAttempts = 3
	unitsIndexDropGap      = 2 * time.Second
)

// unitsIndexDropBudget bounds the whole cleanup; it ignores RetryDelay and LockTimeout so no option can push it past the
// Job deadline.
func unitsIndexDropBudget(options IndexBuildOptions) time.Duration {
	if options.CleanupBudget > 0 {
		return options.CleanupBudget
	}
	return DefaultIndexCleanupBudget
}

// dropInvalidUnitsIndex runs after the final failed build: an INVALID index is still maintained by every writer
// and must not stay. DROP INDEX CONCURRENTLY waits on the same open transactions that can fail the build, so it runs under
// lock_timeout = 0 within a bounded budget (it outlives a cancelled ctx). A VALID index is never dropped. A failure here is
// logged with its SQLSTATE only; the next run drops the leftover at the start of its first attempt.
func dropInvalidUnitsIndex(ctx context.Context, session lockedSession, index unitsIndex, options IndexBuildOptions, logger *slog.Logger) {
	budget := unitsIndexDropBudget(options)
	windowEnd := time.Now().Add(budget)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget+statementTimeoutResetLimit)
	defer cancel()
	for attempt := 1; attempt <= unitsIndexDropAttempts; attempt++ {
		info, err := readIndexInfo(ctx, session.conn, index.name)
		if err != nil {
			logger.Warn("build-index: invalid index left; the next run drops it", "stage", "state", "attempt", attempt, "sqlstate", sqlState(err), "index", index.name)
			return
		}
		if !info.exists || info.ready() {
			return
		}
		if err := session.exec(ctx, index.dropSQL(), windowEnd, options.LockTimeout); err == nil {
			logger.Info("build-index: dropped the invalid index after the final failure", "attempt", attempt)
			return
		} else if errors.Is(err, ErrBackendChanged) {
			logger.Warn("build-index: invalid index left; the next run drops it", "stage", "backend", "attempt", attempt, "index", index.name)
			return
		} else {
			logger.Warn("build-index: drop of the invalid index failed", "attempt", attempt, "sqlstate", sqlState(err))
			if attempt == unitsIndexDropAttempts || windowExpired(err, windowEnd) {
				logger.Warn("build-index: invalid index left; the next run drops it", "stage", "drop", "attempt", attempt, "sqlstate", sqlState(err), "index", index.name)
				return
			}
		}
		select {
		case <-ctx.Done():
			logger.Warn("build-index: invalid index left; the next run drops it", "stage", "budget", "attempt", attempt, "index", index.name)
			return
		case <-time.After(unitsIndexDropGap):
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
func readIndexInfo(ctx context.Context, conn *pgx.Conn, name string) (indexInfo, error) {
	rows, err := conn.Query(ctx, `
SELECT i.indisvalid, i.indisready
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relnamespace = 'public'::regnamespace AND c.relname = $1 AND i.indrelid = 'public.sync_run_units'::regclass`,
		name)
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
		fmt.Fprint(env.Stderr, "\nUsage: dho migrate postgres build-index failure-streak|open-units\n"+
			"\nBuilds "+FailureStreakIndexName+" (failure-streak) or "+OpenUnitsIndexName+" (open-units) CONCURRENTLY\n"+
			"(the upgrade revision that carries it, 0159 or 0160, then finds it and does nothing).\n"+
			"Exit 0 only when the index is present and valid.\n"+
			"A run that finds another run holding the build lock, while the index is not valid, exits non-zero (build_index_lock_held) with no DDL.\n"+
			"\nEnvironment:\n"+
			"  MIGRATION_DATABASE_URI (or _FILE, or the DEV_HEALTH_MIGRATION_PG_* component form)   elevated DSN, direct to PostgreSQL\n"+
			"  POSTGRES_URI (or _FILE)                   used when MIGRATION_DATABASE_URI is not configured\n"+
			"\nBoth must reach PostgreSQL directly or through a session-mode pooler: the build lock is a session lock. Under a\n"+
			"transaction-mode pooler the session can move to another backend; the run then exits non-zero\n"+
			"(build_index_session_changed) with no further DDL. An unlock that answers false exits non-zero (build_index_lock_lost).\n")
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
	var index unitsIndex
	found := false
	if len(positional) == 1 {
		index, found = unitsIndexByVerb(positional[0])
	}
	if !found {
		fmt.Fprintln(env.Stderr, "argument error: the index to build is named failure-streak or open-units")
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
	result, err := buildUnitsIndex(ctx, conn, index, options, logger)
	if errors.Is(err, ErrBuildLockHeld) {
		return writeError(env.Stderr, "build_index_lock_held", err.Error())
	}
	if errors.Is(err, ErrBackendChanged) {
		return writeError(env.Stderr, "build_index_session_changed", boundary.Redact(err).Error())
	}
	if errors.Is(err, ErrBuildLockLost) {
		return writeError(env.Stderr, "build_index_lock_lost", boundary.Redact(err).Error())
	}
	if err != nil {
		return writeError(env.Stderr, "build_index_failed", boundary.Redact(err).Error())
	}
	return writeResult(env.Stdout, env.Stderr, result)
}
