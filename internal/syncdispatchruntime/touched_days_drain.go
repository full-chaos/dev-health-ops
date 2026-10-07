package syncdispatchruntime

import (
	"context"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The drain of the pending touched days (CHAOS-8846).
//
// A post-sync fan-out starts a daily run for the PostSyncTouchedDaysPerFanout
// newest pending days and leaves the rest. Without the drain only a later
// fan-out takes them, so an organization with no later sync keeps them for
// ever. A drain pass takes the newest pending days too, in the order of the
// fan-out, so the derived rows grow back from today in one piece and never
// leave a hole between two filled ranges. Because a chain of passes goes on
// until nothing is pending, the old days are reached when no new touches
// arrive; the age of the oldest pending touch is exported for the case that
// they keep arriving.
//
// A pass is triggered by the dispatch of the nightly run of the organization
// (the floor: once a day, with or without a sync) and by the end of every
// daily run of the organization (the continuation). A chain of passes ends by
// itself: a pass that starts no run produces no run end.
//
// Design, delivery guarantees and limits:
// .github/docs-legacy/architecture/data-pipeline.md, "Drain of the pending
// touched days".

const (
	// TouchedDaysPerDrainPass is the number of days one pass starts a run
	// for. Each is a whole daily_metrics_runs pipeline, as in the fan-out.
	TouchedDaysPerDrainPass = PostSyncTouchedDaysPerFanout

	// touchedDrainInFlightWindow bounds how long a drain run that has not
	// ended holds back the next pass. The nightly trigger comes once in this
	// time, so a run that never ends delays the drain by one night and no
	// more; the blocked-run marker reports such a run.
	touchedDrainInFlightWindow = 24 * time.Hour

	// touchedDrainReturnRunLimit bounds the runs without a result whose keys
	// one pass returns. The read has no bound on the age of a run; a pass that
	// hits this bound logs an error and counts it, and the runs behind it are
	// reached when the keys of the returned ones were taken again. An
	// organization has none or a few such runs; the bound is the same number
	// as the bound of the pending days, for a backlog where every run fails.
	touchedDrainReturnRunLimit = postSyncTouchedPendingDayReadLimit

	// touchedDrainRetryAfter is how long after the newest run of a skipped
	// day the drain starts one more run for it. A skipped day therefore costs
	// one run in this time, and comes back without an operator when the cause
	// of its failures is gone.
	touchedDrainRetryAfter = 24 * time.Hour

	// TouchedDrainFailedRunsBeforeSkip is the number of newest runs of a day
	// that must all have ended without a result (failed, canceled, or not
	// ended in touchedDrainInFlightWindow) for the drain to stop starting
	// runs for it. The day stays pending and is reported. It is started again
	// touchedDrainRetryAfter after its newest run, and a run of any other
	// trigger that succeeds makes it startable at once.
	TouchedDrainFailedRunsBeforeSkip = 3
)

// TouchedDaysDrainStore is the part of the touched-day record the drain
// reads and writes. Every failure is an error: an implementation never
// answers "no day".
type TouchedDaysDrainStore interface {
	Backlog(ctx context.Context, organizationID string, limit int) (TouchedDaysBacklog, error)
	PendingRepositories(ctx context.Context, organizationID string, days []time.Time, limitPerDay int) (map[string][]string, error)
	MarkDispatched(ctx context.Context, organizationID string, at time.Time, fullDays []time.Time, keys []TouchedDayKey) error
	// ReturnToPending makes the listed keys pending again that are not
	// pending, and returns the number of days it did that for.
	ReturnToPending(ctx context.Context, organizationID string, runs []TouchedRunKeys) (int, error)
	// RunsWithEveryKeyPending counts the runs whose listed keys are all
	// pending.
	RunsWithEveryKeyPending(ctx context.Context, organizationID string, runs []TouchedRunKeys) (int, error)
}

// TouchedDaysDrainRuns is the daily-run state the drain reads and writes.
type TouchedDaysDrainRuns interface {
	// RepositoryLimit is the largest repository list one run accepts.
	RepositoryLimit() int
	// OwnedKeysOfRunsWithoutResult are the keys whose owner run ended without
	// a result. The owner of a key is the newest run of a fan-out or of the
	// drain that lists it; a run without a result is failed, canceled, or not
	// ended notEndedAfter after its creation. At most limit runs, newest
	// first; the flag says that more exist.
	OwnedKeysOfRunsWithoutResult(ctx context.Context, organizationID string, notEndedAfter time.Duration, limit int) ([]TouchedRunKeys, bool, error)
	// RunsWithResultOfPass are the runs with a result of the drain pass that
	// started the run endedRunID, with the keys each lists. None when
	// endedRunID is not a run of the drain.
	RunsWithResultOfPass(ctx context.Context, organizationID, endedRunID string) ([]TouchedRunKeys, error)
	// DaysWithOnlyFailedRuns are the days among days (keys 2006-01-02) whose
	// newest threshold runs all ended without a result. The value is true
	// when the newest run of the day is older than retryAfter.
	DaysWithOnlyFailedRuns(ctx context.Context, organizationID string, days []time.Time, threshold int, notEndedAfter, retryAfter time.Duration) (map[string]bool, error)
	// RunsStateTx returns a value that is another one after any daily run of
	// the organization was created. A pass reads it before its first read
	// and again under its lock: the same value says that no run exists that
	// the reads of the pass did not see.
	RunsStateTx(ctx context.Context, tx pgx.Tx, organizationID string) (string, error)
	// InFlightTx counts the drain runs of the organization that are not
	// ended and were created at or after since.
	InFlightTx(ctx context.Context, tx pgx.Tx, organizationID string, since time.Time) (int, error)
	// StartTx starts the run of day for repositoryIDs under the generation of
	// the pass. It returns false, and starts nothing, when the run exists.
	StartTx(ctx context.Context, tx pgx.Tx, organizationID string, day time.Time, passID string, repositoryIDs []string) (bool, error)
}

// TouchedDaysDrain starts daily runs for the pending touched days of an
// organization, newest first.
type TouchedDaysDrain struct {
	pool     *pgxpool.Pool
	store    TouchedDaysDrainStore
	runs     TouchedDaysDrainRuns
	logger   *synclog.Logger
	observer jobruntime.TouchedDaysDrainObserver
	now      func() time.Time
}

// NewTouchedDaysDrain builds the drain. Every collaborator but the logger is
// required.
func NewTouchedDaysDrain(
	pool *pgxpool.Pool, store TouchedDaysDrainStore, runs TouchedDaysDrainRuns, logger *synclog.Logger,
) (*TouchedDaysDrain, error) {
	if pool == nil || store == nil || runs == nil || runs.RepositoryLimit() < 1 {
		return nil, ErrPostSyncUnavailable
	}
	if logger == nil {
		logger = synclog.Default()
	}
	return &TouchedDaysDrain{pool: pool, store: store, runs: runs, logger: logger, now: time.Now}, nil
}

// SetObserver wires the optional counters and the pending-age gauge. A nil
// observer (the default) means a pass still logs its line.
func (drain *TouchedDaysDrain) SetObserver(observer jobruntime.TouchedDaysDrainObserver) {
	if drain != nil {
		drain.observer = observer
	}
}

// touchedDrainStart is one run a pass will start: a day and the pending
// repositories it takes of it.
type touchedDrainStart struct {
	day          time.Time
	repositories []string
	// split is true when the day has more pending repositories than the
	// list: the rest stays pending for a later pass.
	split bool
	// retry is true for a day whose newest runs all ended without a result
	// and whose newest run is older than touchedDrainRetryAfter.
	retry bool
}

// touchedDrainPass is what one pass did, for its report.
type touchedDrainPass struct {
	organizationID string
	passID         string
	returned       int
	// returnTruncated is true when more runs without a result own keys than
	// the pass read.
	returnTruncated bool
	backlog         TouchedDaysBacklog
	skipped         []time.Time
	starts          []touchedDrainStart
	started         []touchedDrainStart
	alreadyStarted  int
	inFlight        int
	// runsState is the state of the daily runs of the organization, read
	// before every other read of the pass.
	runsState string
	// runsChanged is true when a run of the organization was created between
	// the reads of the pass and its lock: the pass started nothing.
	runsChanged bool
}

// DrainTouchedDays runs one pass for the organization. passID names the
// trigger (the same trigger always gives the same passID), and with it the
// generation of the runs the pass starts.
//
// It returns nothing and fails nothing: every failure is one Error line and
// one counter, and leaves each day pending or its run started.
func (drain *TouchedDaysDrain) DrainTouchedDays(ctx context.Context, organizationID, passID string) {
	if drain == nil || ctx == nil || organizationID == "" || passID == "" {
		return
	}
	pass := &touchedDrainPass{organizationID: organizationID, passID: passID}
	// A cheap first look, outside the lock: while runs of an earlier pass are
	// not ended this trigger does nothing, and the end of each of those runs
	// is a trigger of its own.
	inFlight, runsState, err := drain.inFlight(ctx, organizationID)
	if err != nil {
		drain.fail(ctx, pass, "in_flight_read")
		return
	}
	pass.runsState = runsState
	if inFlight > 0 {
		drain.observe(jobruntime.TouchedDaysDrainInFlight, 1)
		return
	}
	if stopped, err := drain.markOfPassMissing(ctx, pass); err != nil {
		drain.fail(ctx, pass, "mark_read")
		return
	} else if stopped {
		return
	}
	if err := drain.returnFailedDays(ctx, pass); err != nil {
		drain.fail(ctx, pass, "return_failed_days")
		return
	}
	if err := drain.take(ctx, pass); err != nil {
		drain.fail(ctx, pass, "read")
		return
	}
	drain.observe(jobruntime.TouchedDaysDrainPasses, 1)
	if len(pass.starts) > 0 {
		if err := drain.start(ctx, pass); err != nil {
			drain.fail(ctx, pass, "start")
			return
		}
		drain.mark(ctx, pass)
	}
	drain.report(ctx, pass)
}

// inFlight reads the state of the daily runs of the organization and then
// counts its drain runs that are not ended, in a transaction of its own. The
// state is the first read of a pass: every later read of the pass sees at
// least the runs it stands for.
func (drain *TouchedDaysDrain) inFlight(ctx context.Context, organizationID string) (int, string, error) {
	tx, err := drain.pool.Begin(ctx)
	if err != nil {
		return 0, "", ErrPostSyncUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	runsState, err := drain.runs.RunsStateTx(ctx, tx, organizationID)
	if err != nil {
		return 0, "", err
	}
	inFlight, err := drain.runs.InFlightTx(ctx, tx, organizationID, drain.now().UTC().Add(-touchedDrainInFlightWindow))
	return inFlight, runsState, err
}

// touchedDrainEndTrigger is the prefix of the passID of a pass that the end
// of a daily run triggered. The id of that run follows it.
const touchedDrainEndTrigger = "e:"

// markOfPassMissing stops the chain when the mark of the pass before this one
// did not reach the touched-day record.
//
// A chain goes on because the end of a run of one pass triggers the next
// pass. The runs of a pass are committed before its mark, so a mark that fails
// leaves their days pending, and the next pass would start the same newest
// days again, for as long as the mark fails, and never reach an older day.
// The pass therefore looks at the pass of the run whose end triggered it: a
// run of that pass that has a result and whose every key is pending was not
// marked. It then starts nothing, so the chain ends; the nightly pass (its
// trigger is not the end of a drain run) and the fan-out of the next sync
// start runs again.
//
// A run whose every key was touched again while it ran looks the same and
// stops the chain too. The sync that touched them brings a fan-out of its own.
func (drain *TouchedDaysDrain) markOfPassMissing(ctx context.Context, pass *touchedDrainPass) (bool, error) {
	endedRunID, ok := strings.CutPrefix(pass.passID, touchedDrainEndTrigger)
	if !ok {
		return false, nil
	}
	runs, err := drain.runs.RunsWithResultOfPass(ctx, pass.organizationID, endedRunID)
	if err != nil || len(runs) == 0 {
		return false, err
	}
	unmarked, err := drain.store.RunsWithEveryKeyPending(ctx, pass.organizationID, runs)
	if err != nil || unmarked == 0 {
		return false, err
	}
	drain.logger.Error(ctx, synclog.MsgTouchedDaysDrainFailed,
		synclog.Text(synclog.KeyPhase, synclog.ParseLabel("chain_stopped_mark_missing")),
		synclog.Org(synclog.ParseID(pass.organizationID)),
		drainPassAttr(pass.passID),
		synclog.Count(synclog.KeyDrainDaysNotMarked, unmarked),
	)
	drain.observe(jobruntime.TouchedDaysDrainChainStopped, 1)
	return true, nil
}

// returnFailedDays makes the keys pending again whose owner run ended without
// a result: it failed, it was canceled, or it is not ended after
// touchedDrainInFlightWindow. The keys were marked when the run started;
// without this step they would be lost.
//
// The run state names the keys (the list of the run, and "newest run that
// lists the key" for the owner) and the touched-day record is asked only
// which of them are not pending. No time of one is compared with a time of the
// other, so the two clocks can differ by any amount.
func (drain *TouchedDaysDrain) returnFailedDays(ctx context.Context, pass *touchedDrainPass) error {
	runs, truncated, err := drain.runs.OwnedKeysOfRunsWithoutResult(
		ctx, pass.organizationID, touchedDrainInFlightWindow, touchedDrainReturnRunLimit)
	if err != nil {
		return err
	}
	pass.returnTruncated = truncated
	if truncated {
		// Reported here and not with the pass line: a later step of the pass
		// can fail, and the runs behind the bound must have their line then
		// too.
		drain.logger.Error(ctx, synclog.MsgTouchedDaysDrainFailed,
			synclog.Text(synclog.KeyPhase, synclog.ParseLabel("return_read_truncated")),
			synclog.Org(synclog.ParseID(pass.organizationID)), drainPassAttr(pass.passID))
		drain.observe(jobruntime.TouchedDaysDrainReturnReadTruncated, 1)
	}
	if len(runs) == 0 {
		return nil
	}
	returned, err := drain.store.ReturnToPending(ctx, pass.organizationID, runs)
	if err != nil {
		return err
	}
	pass.returned = returned
	return nil
}

// take reads the pending days, newest first, and chooses the runs of the pass.
// It holds no Postgres transaction while it talks to ClickHouse.
//
// A day whose newest runs all ended without a result gets no run: it would
// fail again, and its end would trigger the next pass, which would start it
// again. It stays pending, it is reported, and it never holds one of the
// slots. When its newest run is older than touchedDrainRetryAfter it gets one
// run: that run is then its newest, so the passes of the same chain skip the
// day again, whatever the end of the run.
//
// A day with more pending repositories than one run accepts is split: the pass
// takes as many as one run accepts and leaves the rest pending. The next pass
// has another generation, so it can start a run of the same day with the next
// repositories. A part of a day is a run of listed repositories, which is what
// the fan-out starts for every touched day: what such a run writes for a row
// that spans repositories (a work scope) is the rule of the work-item
// families, named in data-pipeline.md.
func (drain *TouchedDaysDrain) take(ctx context.Context, pass *touchedDrainPass) error {
	backlog, err := drain.store.Backlog(ctx, pass.organizationID, postSyncTouchedPendingDayReadLimit)
	if err != nil {
		return err
	}
	pass.backlog = backlog
	if len(backlog.Days) == 0 {
		return nil
	}
	onlyFailed, err := drain.runs.DaysWithOnlyFailedRuns(
		ctx, pass.organizationID, backlog.Days, TouchedDrainFailedRunsBeforeSkip,
		touchedDrainInFlightWindow, touchedDrainRetryAfter)
	if err != nil {
		return err
	}
	candidates := make([]time.Time, 0, len(backlog.Days))
	for _, day := range backlog.Days {
		if retryDue, failed := onlyFailed[day.UTC().Format("2006-01-02")]; failed && !retryDue {
			pass.skipped = append(pass.skipped, day)
			continue
		}
		candidates = append(candidates, day)
	}
	limit := drain.runs.RepositoryLimit()
	for scanned := 0; scanned < len(candidates) && len(pass.starts) < TouchedDaysPerDrainPass; {
		end := min(scanned+TouchedDaysPerDrainPass, len(candidates))
		chunk := candidates[scanned:end]
		repositories, err := drain.store.PendingRepositories(ctx, pass.organizationID, chunk, limit+1)
		if err != nil {
			return err
		}
		for _, day := range chunk {
			if len(pass.starts) >= TouchedDaysPerDrainPass {
				break
			}
			scanned++
			identifiers := repositories[day.UTC().Format("2006-01-02")]
			if len(identifiers) == 0 {
				// A fan-out dispatched the day between the two reads.
				continue
			}
			start := touchedDrainStart{day: day, repositories: identifiers}
			start.retry = onlyFailed[day.UTC().Format("2006-01-02")]
			if len(identifiers) > limit {
				start.repositories, start.split = identifiers[:limit], true
			}
			pass.starts = append(pass.starts, start)
		}
	}
	return nil
}

// touchedDrainLockNamespace is the first key of the advisory lock of a pass.
// The second key is a hash of the organization id.
const touchedDrainLockNamespace = 8846

// start starts the runs of the pass in one transaction.
//
// The advisory lock puts two passes of one organization in sequence, and the
// count under the lock is what makes the second one start nothing: it sees the
// committed runs of the first, which hold the same days until they are marked.
//
// Everything the pass decided before the lock (the keys it returned, the days
// and repositories it chose, the days it skips or starts once more) was read
// from a state of the runs that can be old by now: another pass or a fan-out
// can have started a run since, and that run can have ended already, so the
// count of the runs in flight does not show it. The pass therefore reads the
// state of the runs again under the lock and starts nothing when it is not the
// one of its first read. The days stay pending, and the end of the run that
// changed the state triggers the next pass, which reads again. This is what
// keeps a skipped day at one more run in touchedDrainRetryAfter when two
// passes find it due at the same time.
//
// A fan-out does not take this lock: a run it commits after this check is not
// seen, and costs one more recompute of its days.
func (drain *TouchedDaysDrain) start(ctx context.Context, pass *touchedDrainPass) error {
	tx, err := drain.pool.Begin(ctx)
	if err != nil {
		return ErrPostSyncUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, hashtext($2))`,
		touchedDrainLockNamespace, pass.organizationID); err != nil {
		return ErrPostSyncUnavailable
	}
	inFlight, err := drain.runs.InFlightTx(
		ctx, tx, pass.organizationID, drain.now().UTC().Add(-touchedDrainInFlightWindow))
	if err != nil {
		return err
	}
	if inFlight > 0 {
		pass.inFlight = inFlight
		return tx.Commit(ctx)
	}
	runsState, err := drain.runs.RunsStateTx(ctx, tx, pass.organizationID)
	if err != nil {
		return err
	}
	if runsState != pass.runsState {
		pass.runsChanged = true
		return tx.Commit(ctx)
	}
	var started []touchedDrainStart
	alreadyStarted := 0
	for _, start := range pass.starts {
		created, err := drain.runs.StartTx(ctx, tx, pass.organizationID, start.day, pass.passID, start.repositories)
		if err != nil {
			return err
		}
		if !created {
			alreadyStarted++
			continue
		}
		started = append(started, start)
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrPostSyncUnavailable
	}
	pass.started, pass.alreadyStarted = started, alreadyStarted
	return nil
}

// mark runs after the transaction of the pass committed. It marks exactly the
// keys the started runs list, at the time the pending days were read.
//
// A failed mark is not a failure of the pass: the runs are committed and the
// days stay pending, so a later pass computes them once more. It ends the
// chain: the pass that the end of one of these runs triggers finds the keys
// not marked and starts nothing (markOfPassMissing).
func (drain *TouchedDaysDrain) mark(ctx context.Context, pass *touchedDrainPass) {
	var keys []TouchedDayKey
	for _, start := range pass.started {
		for _, repositoryID := range start.repositories {
			keys = append(keys, TouchedDayKey{Day: start.day, RepositoryID: repositoryID})
		}
	}
	if len(keys) == 0 {
		return
	}
	if err := drain.store.MarkDispatched(ctx, pass.organizationID, pass.backlog.TakenAt, nil, keys); err != nil {
		drain.logger.Error(ctx, synclog.MsgTouchedDaysDrainFailed,
			synclog.Text(synclog.KeyPhase, synclog.ParseLabel("mark")),
			synclog.Org(synclog.ParseID(pass.organizationID)),
			drainPassAttr(pass.passID),
		)
		drain.observe(jobruntime.TouchedDaysDrainMarkFailed, 1)
	}
}

// report logs and counts one pass that read the pending days.
func (drain *TouchedDaysDrain) report(ctx context.Context, pass *touchedDrainPass) {
	org := synclog.Org(synclog.ParseID(pass.organizationID))
	passAttr := drainPassAttr(pass.passID)
	split, retried := 0, 0
	for _, start := range pass.started {
		if start.retry {
			retried++
		}
		if !start.split {
			continue
		}
		split++
		// One line for each split day: its numbers are whole only after the
		// passes that take the rest of its repositories.
		drain.logger.Warn(ctx, synclog.MsgTouchedDaysDrainSplit, org, passAttr,
			synclog.Instant(synclog.KeyDrainOldestPendingDay, start.day.UTC()),
			synclog.Count(synclog.KeyRepoCount, len(start.repositories)),
		)
	}
	if len(pass.skipped) > 0 {
		// One line for each pass, never one for each day: the same days are
		// found again by every pass until a run of them succeeds.
		drain.logger.Error(ctx, synclog.MsgTouchedDaysDrainSkipped, org, passAttr,
			synclog.Count(synclog.KeyDrainDaysSkipped, len(pass.skipped)),
			synclog.Instant(synclog.KeyDrainOldestPendingDay, pass.skipped[len(pass.skipped)-1].UTC()),
			synclog.Instant(synclog.KeyDrainNewestSkippedDay, pass.skipped[0].UTC()),
		)
	}
	if pass.backlog.Truncated {
		drain.logger.Error(ctx, synclog.MsgTouchedDaysDrainFailed,
			synclog.Text(synclog.KeyPhase, synclog.ParseLabel("read_truncated")), org, passAttr)
		drain.observe(jobruntime.TouchedDaysDrainReadTruncated, 1)
	}
	// A day is left pending when the pass did not take every pending key of
	// it. The count is of the days the read returned: with Truncated it is a
	// lower bound.
	whole := 0
	for _, start := range pass.started {
		if !start.split {
			whole++
		}
	}
	pendingLeft := len(pass.backlog.Days) - whole
	var age time.Duration
	if pendingLeft > 0 && !pass.backlog.OldestTouchedAt.IsZero() {
		age = max(pass.backlog.TakenAt.Sub(pass.backlog.OldestTouchedAt), time.Millisecond)
	}
	outcome := "started"
	switch {
	case len(pass.backlog.Days) == 0:
		outcome = "nothing_pending"
	case pass.inFlight > 0:
		outcome = "in_flight"
	case pass.runsChanged:
		outcome = "runs_changed_since_read"
	case len(pass.started) == 0:
		outcome = "started_none"
	}
	if outcome == "nothing_pending" && pass.returned == 0 {
		// The usual pass of an organization with no backlog: counted, not
		// logged, because every end of a daily run triggers one.
		drain.observe(jobruntime.TouchedDaysDrainNothingPending, 1)
		drain.observeAge(pass.organizationID, 0)
		return
	}
	attrs := []synclog.Attr{
		org, passAttr,
		synclog.Text(synclog.KeyOutcome, synclog.ParseLabel(outcome)),
		synclog.Count(synclog.KeyDrainDaysStarted, len(pass.started)),
		synclog.Count(synclog.KeyDrainDaysPendingLeft, pendingLeft),
		synclog.Count(synclog.KeyDrainDaysSplit, split),
		synclog.Count(synclog.KeyDrainDaysAlreadyStarted, pass.alreadyStarted),
		synclog.Count(synclog.KeyDrainDaysReturned, pass.returned),
		synclog.Count(synclog.KeyDrainDaysSkipped, len(pass.skipped)),
		synclog.Count(synclog.KeyDrainDaysRetried, retried),
		synclog.Count(synclog.KeyDrainRunsInFlight, pass.inFlight),
		synclog.Elapsed(synclog.KeyDrainOldestPendingAge, age),
		synclog.Flag(synclog.KeyDrainReadTruncated, pass.backlog.Truncated),
	}
	if len(pass.backlog.Days) > 0 {
		// The days are newest first: the last one is the oldest the read
		// returned (with Truncated, older ones exist).
		attrs = append(attrs, synclog.Instant(
			synclog.KeyDrainOldestPendingDay, pass.backlog.Days[len(pass.backlog.Days)-1].UTC()))
	}
	drain.logger.Info(ctx, synclog.MsgTouchedDaysDrainPass, attrs...)
	drain.observe(jobruntime.TouchedDaysDrainDaysStarted, uint64(len(pass.started)))
	drain.observe(jobruntime.TouchedDaysDrainDaysSplit, uint64(split))
	drain.observe(jobruntime.TouchedDaysDrainDaysAlreadyStarted, uint64(pass.alreadyStarted))
	drain.observe(jobruntime.TouchedDaysDrainDaysReturned, uint64(pass.returned))
	drain.observe(jobruntime.TouchedDaysDrainDaysSkipped, uint64(len(pass.skipped)))
	drain.observe(jobruntime.TouchedDaysDrainDaysRetried, uint64(retried))
	if pass.inFlight > 0 {
		drain.observe(jobruntime.TouchedDaysDrainInFlight, 1)
	}
	if len(pass.backlog.Days) == 0 {
		drain.observe(jobruntime.TouchedDaysDrainNothingPending, 1)
	}
	drain.observeAge(pass.organizationID, age)
}

// drainPassAttr is the pass of a line: the trigger letter and the id of the run
// that triggered it. A label of a log line has no "-", so the id is written
// with "_" in its place; without that every line would say "invalid".
func drainPassAttr(passID string) synclog.Attr {
	return synclog.Text(synclog.KeyDrainPass, synclog.ParseLabel(strings.ReplaceAll(passID, "-", "_")))
}

// fail logs and counts one pass that failed before its runs were committed.
// phase is a fixed word of this file, never an error text.
func (drain *TouchedDaysDrain) fail(ctx context.Context, pass *touchedDrainPass, phase string) {
	drain.logger.Error(ctx, synclog.MsgTouchedDaysDrainFailed,
		synclog.Text(synclog.KeyPhase, synclog.ParseLabel(phase)),
		synclog.Org(synclog.ParseID(pass.organizationID)),
		drainPassAttr(pass.passID),
	)
	drain.observe(jobruntime.TouchedDaysDrainPassFailed, 1)
}

func (drain *TouchedDaysDrain) observe(event jobruntime.TouchedDaysDrainEvent, count uint64) {
	if drain.observer != nil {
		_ = drain.observer.ObserveTouchedDaysDrain(event, count)
	}
}

func (drain *TouchedDaysDrain) observeAge(organizationID string, age time.Duration) {
	if drain.observer != nil {
		_ = drain.observer.ObserveTouchedDaysOldestPendingAge(organizationID, age)
	}
}
