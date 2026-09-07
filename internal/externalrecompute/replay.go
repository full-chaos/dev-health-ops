package externalrecompute

// replay.go is the one-shot drain for the rows that accumulated while nobody
// was reading them (CHAOS-5296).
//
// Between 2026-08-19 (Celery stopped) and this port, the stream runner kept
// writing external-ingest recompute rows addressed to a Celery task nothing was
// running. Those rows carry LegacyCeleryTaskName and are invisible to the
// drain, which only ever selects NativeDrainTaskName. That is deliberate: a
// worker restart must not replay ~2.5 weeks of backlog as a side effect of
// deploying the fix.
//
// This code is what an operator runs, once, by hand, instead
// (`dev-health-workerctl external-recompute replay`).
//
// IT COLLAPSES RATHER THAN REPLAYS. Replaying each row individually would mean
// one bounded plan per ingested batch across the whole gap -- thousands of
// daily runs, most of them for the same (org, day). Collapsing to one widest
// plan per (org, source system, source instance) covers the same days and the
// same repositories with a single pass, and the daily/investment layers are
// idempotent per (org, day) anyway, so the narrower per-row plans would add
// nothing but fan-out.

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BacklogRow is one unconsumed legacy row plus the scope it referred to.
type BacklogRow struct {
	JobID          uuid.UUID
	BridgeID       string
	OrgID          string
	SourceSystem   string
	SourceInstance string
	DispatchedAt   time.Time
	// Scope is nil when no pending batch row still carries this bridge id --
	// an already-terminal row that only needs its status corrected.
	Scope        *PlanScope
	IngestionIDs []string
	// LoadFailed distinguishes "this row's scope could not be READ" from
	// "this row has no pending scope left". Both leave Scope nil, and
	// collapsing them is a data-loss bug: a transient connection reset while
	// reading a row's scope would otherwise retire that row with no work
	// enqueued, dropping the recompute permanently (r1 P2). A row marked here
	// is reported and left alone for the next invocation.
	LoadFailed bool
	LoadError  string
}

// CollapsedGroup is the union of every backlog row for one debounce grain.
type CollapsedGroup struct {
	OrgID          string
	SourceSystem   string
	SourceInstance string
	Scope          PlanScope
	// JobIDs are every legacy row this group retires, INCLUDING the
	// scope-less ones: they are the same grain and were superseded by the
	// union, so leaving them behind would strand rows that can never be
	// consumed by anything.
	JobIDs       []uuid.UUID
	IngestionIDs []string
	Rows         int
	// Skipped counts rows in this grain whose scope could not be read. They
	// are deliberately absent from JobIDs: an unreadable row must survive to
	// be retried, not be retired as though it had been drained.
	Skipped int
	Oldest  time.Time
	Newest  time.Time
}

// CollapseBacklog folds backlog rows into one widest plan per (org, source
// system, source instance). Pure, so the collapse is unit-testable without a
// database: the union rules below are the whole correctness argument for
// running this against production, and they should not need a Postgres instance
// to check.
func CollapseBacklog(rows []BacklogRow) []CollapsedGroup {
	type key struct{ org, system, instance string }
	order := make([]key, 0, len(rows))
	groups := make(map[key]*CollapsedGroup, len(rows))
	for _, row := range rows {
		id := key{row.OrgID, row.SourceSystem, row.SourceInstance}
		group, ok := groups[id]
		if !ok {
			group = &CollapsedGroup{
				OrgID:          row.OrgID,
				SourceSystem:   row.SourceSystem,
				SourceInstance: row.SourceInstance,
				Scope:          PlanScope{OrgID: row.OrgID},
				Oldest:         row.DispatchedAt,
				Newest:         row.DispatchedAt,
			}
			groups[id] = group
			order = append(order, id)
		}
		group.Rows++
		if row.LoadFailed {
			// NOT added to JobIDs: retiring a row whose scope we failed to
			// read would drop its recompute for good.
			group.Skipped++
		} else {
			group.JobIDs = append(group.JobIDs, row.JobID)
		}
		if row.DispatchedAt.Before(group.Oldest) {
			group.Oldest = row.DispatchedAt
		}
		if row.DispatchedAt.After(group.Newest) {
			group.Newest = row.DispatchedAt
		}
		if row.Scope == nil {
			continue
		}
		group.IngestionIDs = append(group.IngestionIDs, row.IngestionIDs...)
		group.Scope.RepoIDs = append(group.Scope.RepoIDs, row.Scope.RepoIDs...)
		group.Scope.TeamIDs = append(group.Scope.TeamIDs, row.Scope.TeamIDs...)
		group.Scope.RecordKinds = append(group.Scope.RecordKinds, row.Scope.RecordKinds...)
		// Widest window, not the newest row's: the point of collapsing is to
		// cover every day any row in the group asked for. The planner's own
		// backfill cap then bounds the result, so "widest" cannot become
		// unbounded here.
		group.Scope.WindowStart = earlier(group.Scope.WindowStart, row.Scope.WindowStart)
		group.Scope.WindowEnd = later(group.Scope.WindowEnd, row.Scope.WindowEnd)
	}
	collapsed := make([]CollapsedGroup, 0, len(order))
	for _, id := range order {
		group := groups[id]
		group.Scope.RepoIDs = sortedUnique(group.Scope.RepoIDs)
		group.Scope.TeamIDs = sortedUnique(group.Scope.TeamIDs)
		group.Scope.RecordKinds = sortedUnique(group.Scope.RecordKinds)
		group.IngestionIDs = sortedUnique(group.IngestionIDs)
		slices.SortFunc(group.JobIDs, func(left, right uuid.UUID) int {
			return slices.Compare(left[:], right[:])
		})
		collapsed = append(collapsed, *group)
	}
	return collapsed
}

func earlier(current, candidate *time.Time) *time.Time {
	if candidate == nil {
		return current
	}
	if current == nil || candidate.Before(*current) {
		value := candidate.UTC()
		return &value
	}
	return current
}

func later(current, candidate *time.Time) *time.Time {
	if candidate == nil {
		return current
	}
	if current == nil || candidate.After(*current) {
		value := candidate.UTC()
		return &value
	}
	return current
}

// LoadBacklog reads every unconsumed legacy row and the scope it referred to.
// It selects on LegacyCeleryTaskName only, so it can never touch a row the live
// drain owns.
func LoadBacklog(ctx context.Context, pool *pgxpool.Pool, limit int) ([]BacklogRow, error) {
	if pool == nil || limit < 1 {
		return nil, ErrInvalidConfig
	}
	rows, err := pool.Query(ctx, `
SELECT id, celery_task_id, org_id, source_system, source_instance, dispatched_at
FROM external_ingest_recompute_jobs
WHERE celery_task_name = $1
  AND status IN ($2, $3)
ORDER BY dispatched_at, id
LIMIT $4`, LegacyCeleryTaskName, statusPending, statusClaimed, limit)
	if err != nil {
		return nil, fmt.Errorf("load external recompute backlog: %w", err)
	}
	defer rows.Close()
	backlog := make([]BacklogRow, 0, limit)
	for rows.Next() {
		var row BacklogRow
		var bridgeID *string
		if err := rows.Scan(&row.JobID, &bridgeID, &row.OrgID, &row.SourceSystem,
			&row.SourceInstance, &row.DispatchedAt); err != nil {
			return nil, fmt.Errorf("scan external recompute backlog row: %w", err)
		}
		if bridgeID != nil {
			row.BridgeID = *bridgeID
		}
		backlog = append(backlog, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate external recompute backlog: %w", err)
	}
	for index := range backlog {
		scope, ingestionIDs, err := loadBridgeScope(ctx, pool, backlog[index].OrgID,
			backlog[index].SourceSystem, backlog[index].SourceInstance, backlog[index].BridgeID)
		if err != nil {
			// One unreadable row must not stop the whole replay -- a single bad
			// payload from three weeks ago should not block every good row
			// behind it -- but it must not be RETIRED either. Marking it
			// LoadFailed keeps it out of the group's JobIDs, so the next
			// invocation sees it again, and surfaces it in the report instead
			// of hiding it in the scopeless count.
			backlog[index].LoadFailed = true
			backlog[index].LoadError = err.Error()
			backlog[index].Scope = nil
			continue
		}
		backlog[index].Scope = scope
		backlog[index].IngestionIDs = ingestionIDs
	}
	return backlog, nil
}

// ReplayGroupReport is what one collapsed group did (or would do, under a dry
// run). It is deliberately the SAME struct for both, so a dry run's output and
// the real run's output are comparable line for line.
type ReplayGroupReport struct {
	OrgID          string    `json:"org_id"`
	SourceSystem   string    `json:"source_system"`
	SourceInstance string    `json:"source_instance"`
	Rows           int       `json:"rows"`
	IngestionIDs   int       `json:"ingestion_ids"`
	Oldest         time.Time `json:"oldest_dispatched_at"`
	Newest         time.Time `json:"newest_dispatched_at"`
	Trigger        bool      `json:"trigger"`
	Day            string    `json:"day,omitempty"`
	BackfillDays   int       `json:"backfill_days,omitempty"`
	FromDate       string    `json:"from_date,omitempty"`
	ToDate         string    `json:"to_date,omitempty"`
	RepoIDs        int       `json:"repo_ids"`
	TeamIDs        int       `json:"team_ids"`
	// SkippedRows are rows in this grain left un-retired because their scope
	// could not be read. A non-zero value means this grain is NOT fully
	// drained, however healthy the rest of the report looks.
	SkippedRows  int      `json:"skipped_rows,omitempty"`
	CappedDays   bool     `json:"capped_days"`
	CappedRepos  bool     `json:"capped_repos"`
	DailyRunIDs  []string `json:"daily_run_ids,omitempty"`
	InvestmentID string   `json:"investment_request_id,omitempty"`
	Error        string   `json:"error,omitempty"`
}

// ReplayReport is the whole run's outcome.
type ReplayReport struct {
	DryRun bool `json:"dry_run"`
	Rows   int  `json:"rows"`
	// ScopelessRows are legitimately terminal: no pending batch row still
	// carries their bridge id, so there is nothing left to enqueue for them.
	ScopelessRows int `json:"scopeless_rows"`
	// UnreadableRows could not have their scope READ. They are left untouched
	// for a later invocation. Counted apart from ScopelessRows because the two
	// look identical in the data and mean opposite things.
	UnreadableRows int                 `json:"unreadable_rows"`
	Groups         []ReplayGroupReport `json:"groups"`
	Retired        int                 `json:"rows_retired"`
	Failed         int                 `json:"groups_failed"`
}

// Incomplete reports whether this run left work behind, for any reason. The
// command uses it to choose a non-zero exit: a replay that retired some rows
// and failed on others must not look like a success to whoever ran it (r1 P2).
func (report ReplayReport) Incomplete() bool {
	return report.Failed > 0 || report.UnreadableRows > 0
}

// Replay collapses and drains the legacy backlog. dryRun stops before any
// write, after producing the identical per-group report the real run produces,
// so an operator can see exactly which orgs, which windows and how many rows
// are about to move before moving them.
func Replay(
	ctx context.Context,
	pool *pgxpool.Pool,
	enqueuer Enqueuer,
	now time.Time,
	limit int,
	dryRun bool,
) (ReplayReport, error) {
	backlog, err := LoadBacklog(ctx, pool, limit)
	if err != nil {
		return ReplayReport{}, err
	}
	report := ReplayReport{DryRun: dryRun, Rows: len(backlog)}
	for _, row := range backlog {
		switch {
		case row.LoadFailed:
			report.UnreadableRows++
		case row.Scope == nil:
			report.ScopelessRows++
		}
	}
	for _, group := range CollapseBacklog(backlog) {
		plan := PlanRecompute(group.Scope, now)
		groupReport := ReplayGroupReport{
			OrgID:          group.OrgID,
			SourceSystem:   group.SourceSystem,
			SourceInstance: group.SourceInstance,
			Rows:           group.Rows,
			IngestionIDs:   len(group.IngestionIDs),
			Oldest:         group.Oldest.UTC(),
			Newest:         group.Newest.UTC(),
			Trigger:        plan.Trigger,
			Day:            planDayString(plan),
			BackfillDays:   plan.BackfillDays,
			FromDate:       planDateString(plan.FromDate),
			ToDate:         planDateString(plan.ToDate),
			RepoIDs:        len(plan.RepoIDs),
			TeamIDs:        len(plan.TeamIDs),
			CappedDays:     plan.CappedDays,
			CappedRepos:    plan.CappedRepos,
			SkippedRows:    group.Skipped,
		}
		if dryRun {
			report.Groups = append(report.Groups, groupReport)
			continue
		}
		enqueued, err := replayGroup(ctx, pool, enqueuer, group, plan, now)
		if err != nil {
			groupReport.Error = err.Error()
			report.Failed++
			report.Groups = append(report.Groups, groupReport)
			continue
		}
		groupReport.DailyRunIDs = enqueued.DailyRunIDs
		groupReport.InvestmentID = enqueued.InvestmentRequestID
		report.Retired += len(group.JobIDs)
		report.Groups = append(report.Groups, groupReport)
	}
	return report, nil
}

// replayGroupNamespace derives one stable correlation per collapsed group, so a
// re-run of the replay command lands on the same daily generation and the same
// investment request id as the first run instead of starting a second set.
var replayGroupNamespace = uuid.MustParse("9d4b2e51-70a3-5c86-8f19-4a7d6b2c1e58")

func replayGroup(
	ctx context.Context,
	pool *pgxpool.Pool,
	enqueuer Enqueuer,
	group CollapsedGroup,
	plan Plan,
	now time.Time,
) (Enqueued, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return Enqueued{}, fmt.Errorf("begin external recompute replay: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	correlation := "ext-recompute:" + uuid.NewSHA1(replayGroupNamespace,
		[]byte(group.OrgID+":"+group.SourceSystem+":"+group.SourceInstance)).String()

	var enqueued Enqueued
	outcome := outcomeNotApplicable
	if plan.Trigger {
		enqueued, err = enqueuer.Enqueue(ctx, tx, plan, correlation)
		if err != nil {
			return Enqueued{}, err
		}
		outcome = outcomeSkippedNoScope
		if len(enqueued.DailyRunIDs) > 0 || enqueued.InvestmentRequestID != "" {
			outcome = outcomeDispatched
		}
	}
	if err := writeBatchOutcome(ctx, tx, group.OrgID, group.IngestionIDs,
		group.Scope, plan, outcome, now.UTC()); err != nil {
		return Enqueued{}, err
	}
	if err := writeJobLog(ctx, tx, drainClaim{
		BridgeID:       correlation,
		OrgID:          group.OrgID,
		SourceSystem:   group.SourceSystem,
		SourceInstance: group.SourceInstance,
	}, enqueued, now.UTC()); err != nil {
		return Enqueued{}, err
	}
	if err := retireBacklogRows(ctx, tx, group.JobIDs); err != nil {
		return Enqueued{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Enqueued{}, fmt.Errorf("commit external recompute replay: %w", err)
	}
	committed = true
	return enqueued, nil
}

func retireBacklogRows(ctx context.Context, tx pgx.Tx, jobIDs []uuid.UUID) error {
	for _, jobID := range jobIDs {
		// Scoped to the LEGACY task name even here. The replay command must be
		// incapable of touching a live row no matter what it is handed.
		if _, err := tx.Exec(ctx, `
UPDATE external_ingest_recompute_jobs
SET status = $2
WHERE id = $1 AND celery_task_name = $3`,
			jobID, statusDispatched, LegacyCeleryTaskName); err != nil {
			return fmt.Errorf("retire external recompute backlog row: %w", err)
		}
	}
	return nil
}
