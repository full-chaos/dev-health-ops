package syncdispatchruntime

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/jackc/pgx/v5"
)

// PostSyncTouchedDaysPerFanout is the number of touched days outside the
// full-organization window that one post-sync fan-out starts a daily run for,
// newest first. Each day is a whole daily_metrics_runs pipeline, so one sync
// of a long history must not start hundreds at once. The remainder stays
// pending and each later fan-out of the organization takes this many more.
const PostSyncTouchedDaysPerFanout = 31

// postSyncTouchedPendingDayReadLimit bounds the read of the pending days of
// one organization (ten years of days). A fan-out that hits the bound logs an
// error and counts it: the days older than the bound are not seen until the
// newer ones are dispatched.
const postSyncTouchedPendingDayReadLimit = 3660

// postSyncTouchedClockMargin is subtracted from the start time of the sync run
// before the fan-out reads the raw rows the run wrote.
//
// The two times come from two clocks: sync_runs.started_at is the clock of the
// coordinator process that queued the first unit, and last_synced of a raw row
// is the clock of the worker process that wrote it. A unit starts only after
// the coordinator committed started_at, so without clock skew every row of the
// run has last_synced >= started_at. The margin is the largest skew between
// two processes of the platform that the read tolerates. A larger margin only
// records rows of an earlier run again, which costs one more recompute of
// their days; a margin smaller than the real skew loses a day.
const postSyncTouchedClockMargin = 5 * time.Minute

// TouchedDayKey is one (day, repository) key of the touched-day record. The
// nil UUID is the repository of the work items that have none.
type TouchedDayKey struct {
	Day          time.Time
	RepositoryID string
}

// TouchedDaysPending is one read of the pending days of an organization.
type TouchedDaysPending struct {
	// TakenAt is the time of the store's own clock, read before the days.
	TakenAt time.Time
	// Days are the pending days, newest first.
	Days []time.Time
	// Truncated is true when older pending days exist that the read did not
	// return.
	Truncated bool
}

// TouchedDaysStore is the record of the days that stored raw rows touched.
// Every failure is an error: an implementation never answers "no day".
type TouchedDaysStore interface {
	RecordTouched(ctx context.Context, organizationID string, since time.Time) (uint64, error)
	PendingDays(ctx context.Context, organizationID string, limit int) (TouchedDaysPending, error)
	PendingRepositories(ctx context.Context, organizationID string, days []time.Time, limitPerDay int) (map[string][]string, error)
	MarkDispatched(ctx context.Context, organizationID string, at time.Time, fullDays []time.Time, keys []TouchedDayKey) error
}

// TouchedDayPostSyncWriter starts the daily run of one touched day inside the
// fan-out transaction.
type TouchedDayPostSyncWriter interface {
	// FullOrganizationDays are the days DailyPostSyncWriter.StartRunTx starts
	// a run of every repository for. The fan-out starts no second run for
	// them.
	FullOrganizationDays(plan PostSyncPlan) []time.Time
	// RepositoryLimit is the largest repository list one run accepts. A day
	// with more touched repositories gets a run of every repository.
	RepositoryLimit() int
	// StartTouchedDayTx starts the run of day for repositoryIDs (none = every
	// repository). It returns false, and starts nothing, when a run of this
	// sync run exists for the day already.
	StartTouchedDayTx(ctx context.Context, tx pgx.Tx, plan PostSyncPlan, day time.Time, repositoryIDs []string) (bool, error)
}

// SetTouchedDays wires the touched-day recompute (CHAOS-8813): the fan-out
// then records the days that the raw rows of its sync run touched and starts
// a daily run for each pending day. Without it the fan-out recomputes only
// the window of the sync run.
//
// Design, delivery guarantees and limits:
// .github/docs-legacy/architecture/data-pipeline.md, "Post-sync recompute of
// the touched days".
func (service *NativePostSyncService) SetTouchedDays(store TouchedDaysStore, writer TouchedDayPostSyncWriter) error {
	if service == nil || store == nil || writer == nil || writer.RepositoryLimit() < 1 {
		return ErrPostSyncUnavailable
	}
	service.touched = store
	service.touchedWriter = writer
	return nil
}

// SetTouchedDaysObserver wires the optional touched-day counters. A nil
// observer (the default) means the fan-out still logs its line.
func (service *NativePostSyncService) SetTouchedDaysObserver(observer jobruntime.PostSyncTouchedDaysObserver) {
	if service == nil {
		return
	}
	service.touchedObserver = observer
}

// touchedDayStart is one day the fan-out will start a run for. An empty
// repository list is a run of every repository.
type touchedDayStart struct {
	day          time.Time
	repositories []string
}

// touchedDaysTake is what one fan-out read from the touched-day record before
// its Postgres transaction.
type touchedDaysTake struct {
	organizationID string
	recorded       uint64
	takenAt        time.Time
	starts         []touchedDayStart
	// windowPending are the pending days that DailyPostSyncWriter.StartRunTx
	// computes for every repository.
	windowPending []time.Time
	carriedOver   int
	truncated     bool
	// overLimit are the days with more pending repositories than one run
	// accepts. No run is started for them and they stay pending.
	overLimit []time.Time
}

// touchedDaysResult is what the fan-out transaction did with a take.
type touchedDaysResult struct {
	// dailyStarted is true when DailyPostSyncWriter.StartRunTx ran in the
	// transaction, so the window days have their run.
	dailyStarted   bool
	started        []touchedDayStart
	alreadyStarted int
}

// takeTouchedDays records the days that the raw rows of the sync run touched
// and reads the pending days this fan-out will start a run for.
//
// It runs before the fan-out transaction and holds no Postgres transaction
// while it talks to ClickHouse. It returns nil with no error when the fan-out
// will start no daily run: the route is not current, or the sync run has
// nothing that a daily run reads.
//
// The record is complete because a post_sync job exists only after every
// unit of its sync run is success or failed (NativeFinalizeSyncRunService
// commits nothing else while one unit is in another state), so no unit of the
// run writes a raw row after this read.
func (service *NativePostSyncService) takeTouchedDays(
	ctx context.Context, args PostSyncArgs, now time.Time,
) (*touchedDaysTake, error) {
	plan, err := service.touchedDaysPlan(ctx, args, now)
	if err != nil || plan == nil {
		return nil, err
	}
	take := &touchedDaysTake{organizationID: plan.OrganizationID}
	if plan.WorkItems {
		if plan.RunStartedAt.IsZero() {
			return nil, ErrPostSyncUnavailable
		}
		take.recorded, err = service.touched.RecordTouched(
			ctx, plan.OrganizationID, plan.RunStartedAt.Add(-postSyncTouchedClockMargin),
		)
		if err != nil {
			return nil, err
		}
	}
	pending, err := service.touched.PendingDays(ctx, plan.OrganizationID, postSyncTouchedPendingDayReadLimit)
	if err != nil {
		return nil, err
	}
	take.takenAt = pending.TakenAt
	take.truncated = pending.Truncated
	window := make(map[string]struct{})
	for _, day := range service.touchedWriter.FullOrganizationDays(*plan) {
		window[day.UTC().Format("2006-01-02")] = struct{}{}
	}
	var taken []time.Time
	for _, day := range pending.Days {
		if _, inWindow := window[day.UTC().Format("2006-01-02")]; inWindow {
			take.windowPending = append(take.windowPending, day)
			continue
		}
		if len(taken) < PostSyncTouchedDaysPerFanout {
			taken = append(taken, day)
			continue
		}
		take.carriedOver++
	}
	limit := service.touchedWriter.RepositoryLimit()
	repositories, err := service.touched.PendingRepositories(ctx, plan.OrganizationID, taken, limit+1)
	if err != nil {
		return nil, err
	}
	for _, day := range taken {
		identifiers := repositories[day.UTC().Format("2006-01-02")]
		if len(identifiers) == 0 {
			// Another fan-out dispatched the day between the two reads.
			continue
		}
		if len(identifiers) > limit {
			// A run of every repository is refused above the cap of the daily
			// job, and a refused run must not end the day: it stays pending,
			// is reported, and is not started here.
			take.overLimit = append(take.overLimit, day)
			continue
		}
		take.starts = append(take.starts, touchedDayStart{day: day, repositories: identifiers})
	}
	return take, nil
}

// touchedDaysPlan loads the plan of the sync run in a transaction of its own,
// which ends before any ClickHouse call.
func (service *NativePostSyncService) touchedDaysPlan(
	ctx context.Context, args PostSyncArgs, now time.Time,
) (*PostSyncPlan, error) {
	tx, err := service.pool.Begin(ctx)
	if err != nil {
		return nil, ErrPostSyncUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, err := currentPostSyncReference(ctx, tx, args)
	if err != nil || !current {
		return nil, err
	}
	plan, err := loadPostSyncPlan(ctx, tx, args, now)
	if err != nil || plan == nil || !plan.Daily {
		return nil, err
	}
	return plan, nil
}

// startTouchedDaysTx starts the runs of a take inside the fan-out transaction.
func (service *NativePostSyncService) startTouchedDaysTx(
	ctx context.Context, tx pgx.Tx, plan PostSyncPlan, take *touchedDaysTake, result *touchedDaysResult,
) error {
	result.dailyStarted = true
	for _, start := range take.starts {
		started, err := service.touchedWriter.StartTouchedDayTx(ctx, tx, plan, start.day, start.repositories)
		if err != nil {
			return err
		}
		if !started {
			result.alreadyStarted++
			continue
		}
		result.started = append(result.started, start)
	}
	return nil
}

// finishTouchedDays runs after the fan-out transaction committed. It marks
// the started days as dispatched and reports the fan-out.
//
// A failed mark is not an error of the fan-out: the runs are committed, and
// the days stay pending, so a later fan-out computes them once more. Nothing
// here can lose a day.
func (service *NativePostSyncService) finishTouchedDays(
	ctx context.Context, args PostSyncArgs, take *touchedDaysTake, result touchedDaysResult,
) {
	var (
		fullDays []time.Time
		keys     []TouchedDayKey
	)
	if result.dailyStarted {
		fullDays = append(fullDays, take.windowPending...)
	}
	for _, start := range result.started {
		if len(start.repositories) == 0 {
			fullDays = append(fullDays, start.day)
			continue
		}
		for _, repositoryID := range start.repositories {
			keys = append(keys, TouchedDayKey{Day: start.day, RepositoryID: repositoryID})
		}
	}
	if len(fullDays) > 0 || len(keys) > 0 {
		if err := service.touched.MarkDispatched(ctx, take.organizationID, take.takenAt, fullDays, keys); err != nil {
			service.observeTouchedDaysFailure(ctx, args, "mark", jobruntime.PostSyncTouchedDaysMarkFailed)
		}
	}
	for range take.overLimit {
		service.observeTouchedDaysFailure(ctx, args, touchedDaysPhaseOverRepositoryLimit,
			jobruntime.PostSyncTouchedDaysOverRepositoryLimit)
	}
	if take.truncated {
		service.logger.Error(ctx, synclog.MsgPostSyncTouchedDaysFailed,
			synclog.Text(synclog.KeyPhase, synclog.ParseLabel("read_truncated")),
			synclog.Org(synclog.ParseID(args.OrganizationID())),
			synclog.Run(synclog.ParseID(args.SyncRunID())),
		)
	}
	service.logger.Info(ctx, synclog.MsgPostSyncTouchedDays,
		synclog.Org(synclog.ParseID(args.OrganizationID())),
		synclog.Run(synclog.ParseID(args.SyncRunID())),
		synclog.Count(synclog.KeyTouchedKeysRecorded, int64(take.recorded)),
		synclog.Count(synclog.KeyTouchedDaysDispatched, len(result.started)),
		synclog.Count(synclog.KeyTouchedDaysCarriedOver, take.carriedOver),
		synclog.Count(synclog.KeyTouchedDaysAlreadyStarted, result.alreadyStarted),
		synclog.Flag(synclog.KeyTouchedDaysReadTruncated, take.truncated),
	)
	if service.touchedObserver == nil {
		return
	}
	_ = service.touchedObserver.ObservePostSyncTouchedDays(jobruntime.PostSyncTouchedDaysKeysRecorded, take.recorded)
	_ = service.touchedObserver.ObservePostSyncTouchedDays(jobruntime.PostSyncTouchedDaysDispatched, uint64(len(result.started)))
	_ = service.touchedObserver.ObservePostSyncTouchedDays(jobruntime.PostSyncTouchedDaysCarriedOver, uint64(take.carriedOver))
	_ = service.touchedObserver.ObservePostSyncTouchedDays(jobruntime.PostSyncTouchedDaysAlreadyStarted, uint64(result.alreadyStarted))
	if take.truncated {
		_ = service.touchedObserver.ObservePostSyncTouchedDays(jobruntime.PostSyncTouchedDaysReadTruncated, 1)
	}
}

// touchedDaysPhaseOverRepositoryLimit is the phase word of the report of a day
// that has more pending repositories than one run accepts.
const touchedDaysPhaseOverRepositoryLimit = "over_repository_limit"

// observeTouchedDaysFailure logs and counts one failed step of the touched-day
// record. phase is a fixed word of this file, never an error text.
func (service *NativePostSyncService) observeTouchedDaysFailure(
	ctx context.Context, args PostSyncArgs, phase string, event jobruntime.PostSyncTouchedDaysEvent,
) {
	service.logger.Error(ctx, synclog.MsgPostSyncTouchedDaysFailed,
		synclog.Text(synclog.KeyPhase, synclog.ParseLabel(phase)),
		synclog.Org(synclog.ParseID(args.OrganizationID())),
		synclog.Run(synclog.ParseID(args.SyncRunID())),
	)
	if service.touchedObserver != nil {
		_ = service.touchedObserver.ObservePostSyncTouchedDays(event, 1)
	}
}
