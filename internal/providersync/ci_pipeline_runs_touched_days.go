package providersync

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// storedPipelineFinishDaysSQL reads, for the keys of one write batch, the UTC finish day of the stored version of
// each run (FINAL: the newest synced version) and the ClickHouse clock. One statement for the batch: the batch's
// repositories and run ids bound as two arrays, the pairs matched in Go.
const storedPipelineFinishDaysSQL = `
SELECT toString(repo_id), run_id, toDate(finished_at, 'UTC'), toUnixTimestamp64Milli(now64(3))
FROM ci_pipeline_runs FINAL
WHERE org_id = {org_id:String}
  AND repo_id IN {repo_ids:Array(UUID)}
  AND run_id IN {run_ids:Array(String)}
  AND finished_at IS NOT NULL`

// recordMovedPipelineFinishDays appends a 'touched' event to daily_metrics_touched_days (the record the post-sync
// fan-out starts daily runs from, syncdispatchruntime.ClickHouseTouchedDaysStore) for the OLD finish day of each run
// of the batch whose stored finish day differs from the one the batch writes (CHAOS-14016). A rerun that moves a run's
// finish from D to D+1 leaves D counted until D is computed again, and the window of the sync that writes the rerun
// often does not hold D. A run with no stored version, or one that finishes on the same day, records nothing; a run
// whose new version has no finish (it runs again) records its old day.
//
// It runs before the batch is written: once the new version is stored, the old day is no longer known. A failure is
// returned, so the write fails and is retried: a day is never lost; a retry, or a write that fails after the record,
// only adds a recompute. The event time is the ClickHouse clock, as for every event of the table.
func recordMovedPipelineFinishDays(ctx context.Context, conn driver.Conn, orgID string, rows []githubTestsPipelineRow) error {
	if len(rows) == 0 {
		return nil
	}
	newDay := make(map[[2]string]string, len(rows))
	repoSet, runSet := map[uuid.UUID]struct{}{}, map[string]struct{}{}
	for _, row := range rows {
		repo, err := uuid.Parse(row.RepoID)
		if err != nil {
			return fmt.Errorf("ci_pipeline_runs repo_id %q: %w", row.RepoID, err)
		}
		day := ""
		if row.FinishedAt != nil {
			day = row.FinishedAt.UTC().Format(time.DateOnly)
		}
		newDay[[2]string{repo.String(), row.RunID}] = day
		repoSet[repo] = struct{}{}
		runSet[row.RunID] = struct{}{}
	}
	repos := make([]uuid.UUID, 0, len(repoSet))
	for repo := range repoSet {
		repos = append(repos, repo)
	}
	runs := make([]string, 0, len(runSet))
	for run := range runSet {
		runs = append(runs, run)
	}
	result, err := conn.Query(ctx, storedPipelineFinishDaysSQL,
		clickhouse.Named("org_id", orgID), clickhouse.Named("repo_ids", repos), clickhouse.Named("run_ids", runs))
	if err != nil {
		return fmt.Errorf("read stored ci_pipeline_runs finish days: %w", err)
	}
	defer result.Close()
	type touched struct {
		day  time.Time
		repo uuid.UUID
	}
	keys := map[touched]struct{}{}
	var clock int64
	for result.Next() {
		var repo, run string
		var stored time.Time
		var now int64
		if err := result.Scan(&repo, &run, &stored, &now); err != nil {
			return fmt.Errorf("scan stored ci_pipeline_runs finish day: %w", err)
		}
		clock = now
		next, inBatch := newDay[[2]string{repo, run}]
		storedDay := stored.UTC().Format(time.DateOnly)
		if !inBatch || next == storedDay {
			continue
		}
		parsed, err := uuid.Parse(repo)
		if err != nil {
			return fmt.Errorf("stored ci_pipeline_runs repo_id %q: %w", repo, err)
		}
		keys[touched{day: time.Date(stored.Year(), stored.Month(), stored.Day(), 0, 0, 0, 0, time.UTC), repo: parsed}] = struct{}{}
	}
	if err := result.Err(); err != nil {
		return fmt.Errorf("read stored ci_pipeline_runs finish days: %w", err)
	}
	if len(keys) == 0 {
		return nil
	}
	ordered := make([]touched, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if !ordered[i].day.Equal(ordered[j].day) {
			return ordered[i].day.Before(ordered[j].day)
		}
		return ordered[i].repo.String() < ordered[j].repo.String()
	})
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)`)
	if err != nil {
		return fmt.Errorf("prepare touched days of moved ci_pipeline_runs: %w", err)
	}
	defer batch.Abort()
	at := time.UnixMilli(clock).UTC()
	for _, key := range ordered {
		if err := batch.Append(orgID, key.day, key.repo, "touched", at); err != nil {
			return fmt.Errorf("append touched day of a moved ci_pipeline_run: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send touched days of moved ci_pipeline_runs: %w", err)
	}
	return nil
}
