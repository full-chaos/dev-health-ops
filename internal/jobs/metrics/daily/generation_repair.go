package daily

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// legacyRedriveGenerationPrefix is the generation the range finalize redrive
// wrote before CHAOS-4442 (#4316): it REPLACED a run's whole generation with
// "redrive:<nonce>", which dropped the producer classification prefix that
// partition recompute, manual daily-start coverage and the recommendations
// readiness gate find a day's authoritative run by (CHAOS-10072).
const legacyRedriveGenerationPrefix = "redrive:"

const (
	// generationRepairLookback and generationRepairLookahead bound the
	// occurrence times tried for one row, around its created_at: the scheduler
	// stamps the occurrence it was due at, and the run row is inserted at or
	// after that moment.
	generationRepairLookback  = 36 * time.Hour
	generationRepairLookahead = 5 * time.Minute
)

// GenerationRepairRow is one legacy "redrive:" run and what the repair did.
type GenerationRepairRow struct {
	RunID         string `json:"run_id"`
	TargetDay     string `json:"target_day"`
	OldGeneration string `json:"old_generation"`
	NewGeneration string `json:"new_generation,omitempty"`
	Family        string `json:"family,omitempty"`
	FullOrgFlip   bool   `json:"full_org_flip,omitempty"`
	Outcome       string `json:"outcome"`
}

// GenerationRepairOutcome reports one repair pass. Examined counts every
// legacy row in scope; each lands in exactly one of the other counters.
type GenerationRepairOutcome struct {
	Examined       int `json:"examined"`
	Repaired       int `json:"repaired"`
	WouldRepair    int `json:"would_repair"`
	NotProvable    int `json:"not_provable"`
	NotSettled     int `json:"not_settled"`
	Raced          int `json:"raced"`
	FullOrgFlipped int `json:"full_org_flipped"`
	// ByFamily counts the proven rows per generation family
	// (fixed-schedule, post-sync, touched-drain, manual-daily).
	ByFamily map[string]int        `json:"by_family"`
	Rows     []GenerationRepairRow `json:"rows"`
}

// provenFanoutGeneration finds the scheduled fan-out generation a run was
// created with. A run id is uuid5(org|day|generation) (newRun), so the id
// proves the original generation: a candidate counts only when it hashes to
// the run's own id. Nothing is guessed.
func provenFanoutGeneration(orgID, day string, createdAt time.Time, runID uuid.UUID) (string, bool) {
	latest := createdAt.UTC().Truncate(time.Second).Add(generationRepairLookahead)
	earliest := createdAt.UTC().Add(-generationRepairLookback)
	prefix := orgID + "|" + day + "|" + ScheduledFanoutGenerationPrefix
	for occurrence := latest; !occurrence.Before(earliest); occurrence = occurrence.Add(-time.Second) {
		stamp := occurrence.Format(time.RFC3339)
		if uuid.NewSHA1(dailyRunNamespace, []byte(prefix+stamp)) == runID {
			return ScheduledFanoutGenerationPrefix + stamp, true
		}
	}
	return "", false
}

// generationRepairSources are the ids a generation of the other families
// embeds: the sync runs of the organization (post-sync:<sync run id>) and its
// daily runs (touched-drain:<n|e>:<run id>).
type generationRepairSources struct {
	syncRuns []repairSyncRun
	dailyIDs []string
}

type repairSyncRun struct {
	id        string
	createdAt time.Time
}

const generationRepairSyncLookback = 14 * 24 * time.Hour

func proveGeneration(
	orgID, day string, createdAt time.Time, runID uuid.UUID, sources generationRepairSources,
) (generation, family string, ok bool) {
	if generation, ok = provenFanoutGeneration(orgID, day, createdAt, runID); ok {
		return generation, "fixed-schedule", true
	}
	matches := func(candidate string) bool {
		return uuid.NewSHA1(dailyRunNamespace, []byte(orgID+"|"+day+"|"+candidate)) == runID
	}
	if candidate := ManualDailyRunGeneration(orgID, day, nil); matches(candidate) {
		return candidate, "manual-daily", true
	}
	for _, syncRun := range sources.syncRuns {
		if syncRun.createdAt.After(createdAt) || createdAt.Sub(syncRun.createdAt) > generationRepairSyncLookback {
			continue
		}
		if candidate := postSyncGenerationPrefix + syncRun.id; matches(candidate) {
			return candidate, "post-sync", true
		}
	}
	for _, id := range sources.dailyIDs {
		for _, trigger := range []string{"n:", "e:"} {
			if candidate := TouchedDrainGenerationPrefix + trigger + id; matches(candidate) {
				return candidate, "touched-drain", true
			}
		}
	}
	return "", "", false
}

func (store *PostgresStore) loadGenerationRepairSources(ctx context.Context, orgID string) (generationRepairSources, error) {
	var sources generationRepairSources
	syncRows, err := store.pool.Query(ctx, `SELECT id::text, created_at FROM public.sync_runs WHERE org_id = $1::uuid`, orgID)
	if err != nil {
		return sources, ErrUnavailable
	}
	for syncRows.Next() {
		var syncRun repairSyncRun
		if err := syncRows.Scan(&syncRun.id, &syncRun.createdAt); err != nil {
			syncRows.Close()
			return sources, ErrUnavailable
		}
		sources.syncRuns = append(sources.syncRuns, syncRun)
	}
	syncErr := syncRows.Err()
	syncRows.Close()
	if syncErr != nil {
		return sources, ErrUnavailable
	}
	idRows, err := store.pool.Query(ctx, `SELECT id::text FROM public.daily_metrics_runs WHERE org_id = $1::uuid`, orgID)
	if err != nil {
		return sources, ErrUnavailable
	}
	for idRows.Next() {
		var id string
		if err := idRows.Scan(&id); err != nil {
			idRows.Close()
			return sources, ErrUnavailable
		}
		sources.dailyIDs = append(sources.dailyIDs, id)
	}
	idErr := idRows.Err()
	idRows.Close()
	if idErr != nil {
		return sources, ErrUnavailable
	}
	return sources, nil
}

type legacyRedriveRun struct {
	id, day, generation string
	createdAt           time.Time
	settled, fullOrg    bool
}

// RepairLegacyRedriveGenerations restores the scheduled fan-out generation of
// runs the pre-CHAOS-4442 finalize redrive rewrote to "redrive:<nonce>"
// (CHAOS-10072). A row is rewritten only when its original generation is
// proven by its own run id (proveGeneration: fixed-schedule, post-sync,
// touched-drain, manual-daily without a repository list or rerun tag); the rewrite is a
// compare-and-set on the old generation, so a second pass finds nothing to do.
// A run still being redriven (not succeeded/succeeded, or with an open
// redrive event) is left alone: its generation is the live ledger identity.
// Every row is logged at Warn with the old and new value.
func (store *PostgresStore) RepairLegacyRedriveGenerations(
	ctx context.Context, orgID string, from, to time.Time, dryRun bool, reason string,
) (GenerationRepairOutcome, error) {
	outcome := GenerationRepairOutcome{Rows: []GenerationRepairRow{}, ByFamily: map[string]int{}}
	orgID, ok := canonicalOrgID(orgID)
	if !store.valid() || !ok || to.Before(from) {
		return outcome, ErrUnavailable
	}
	from = from.UTC().Truncate(24 * time.Hour)
	to = to.UTC().Truncate(24 * time.Hour)

	rows, err := store.pool.Query(ctx, `
SELECT run.id::text, run.target_day::text, run.generation, run.created_at, run.full_org,
       (run.status = 'succeeded' AND run.finalization_status = 'succeeded'
        AND NOT EXISTS (
            SELECT 1 FROM public.daily_metrics_finalize_redrive_events AS event
            WHERE event.run_id = run.id AND event.status = 'open'))
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid AND run.target_day BETWEEN $2 AND $3
  AND starts_with(run.generation, $4)
ORDER BY run.target_day, run.id`, orgID, from, to, legacyRedriveGenerationPrefix)
	if err != nil {
		return outcome, ErrUnavailable
	}
	var runs []legacyRedriveRun
	for rows.Next() {
		var run legacyRedriveRun
		if err := rows.Scan(&run.id, &run.day, &run.generation, &run.createdAt, &run.fullOrg, &run.settled); err != nil {
			rows.Close()
			return outcome, ErrUnavailable
		}
		runs = append(runs, run)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return outcome, ErrUnavailable
	}

	var sources generationRepairSources
	if len(runs) > 0 {
		if sources, err = store.loadGenerationRepairSources(ctx, orgID); err != nil {
			return outcome, err
		}
	}
	for _, run := range runs {
		outcome.Examined++
		row := GenerationRepairRow{RunID: run.id, TargetDay: run.day, OldGeneration: run.generation}
		runID, err := uuid.Parse(run.id)
		if err != nil {
			return outcome, ErrInvalidState
		}
		switch {
		case !run.settled:
			row.Outcome = "not_settled"
			outcome.NotSettled++
		default:
			generation, family, proven := proveGeneration(orgID, run.day, run.createdAt, runID, sources)
			if !proven {
				row.Outcome = "not_provable"
				outcome.NotProvable++
				break
			}
			row.NewGeneration = generation
			row.Family = family
			// full_org follows migration 0148: only a fan-out is known to be
			// organization-wide. Another family's history cannot prove it.
			row.FullOrgFlip = family == "fixed-schedule" && !run.fullOrg
			if dryRun {
				row.Outcome = "would_repair"
				outcome.WouldRepair++
				break
			}
			applied, err := store.rewriteLegacyRedriveGeneration(ctx, orgID, run.id, run.generation, generation, row.FullOrgFlip)
			if err != nil {
				return outcome, err
			}
			if !applied {
				row.Outcome = "raced"
				outcome.Raced++
				break
			}
			row.Outcome = "repaired"
			outcome.Repaired++
		}
		if row.Family != "" && (row.Outcome == "repaired" || row.Outcome == "would_repair") {
			outcome.ByFamily[row.Family]++
		}
		if row.FullOrgFlip && (row.Outcome == "repaired" || row.Outcome == "would_repair") {
			outcome.FullOrgFlipped++
		} else {
			row.FullOrgFlip = false
		}
		slog.WarnContext(ctx, "daily metrics legacy redrive generation",
			"run_id", run.id, "org_id", orgID, "target_day", run.day, "outcome", row.Outcome,
			"old_generation", run.generation, "new_generation", row.NewGeneration,
			"family", row.Family, "full_org_flip", row.FullOrgFlip, "dry_run", dryRun, "reason", reason)
		outcome.Rows = append(outcome.Rows, row)
	}
	slog.InfoContext(ctx, "daily metrics legacy redrive generation repair pass",
		"org_id", orgID, "from", from.Format(dailyTargetDayLayout), "to", to.Format(dailyTargetDayLayout),
		"dry_run", dryRun, "examined", outcome.Examined, "repaired", outcome.Repaired,
		"would_repair", outcome.WouldRepair, "not_provable", outcome.NotProvable,
		"not_settled", outcome.NotSettled, "raced", outcome.Raced)
	return outcome, nil
}

// rewriteLegacyRedriveGeneration swaps one run's generation back to its proven
// proven value. full_org becomes true for a fan-out: that generation is what
// migration 0148 treated as an organization-wide run. updated_at is left as it
// is, because this restores a classification and is not a run state change.
func (store *PostgresStore) rewriteLegacyRedriveGeneration(
	ctx context.Context, orgID, runID, oldGeneration, newGeneration string, setFullOrg bool,
) (bool, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	tag, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_runs AS run
SET generation = $4, full_org = (run.full_org OR $5)
WHERE run.id = $1::uuid AND run.org_id = $2::uuid AND run.generation = $3
  AND run.status = 'succeeded' AND run.finalization_status = 'succeeded'
  AND NOT EXISTS (
      SELECT 1 FROM public.daily_metrics_finalize_redrive_events AS event
      WHERE event.run_id = run.id AND event.status = 'open')`,
		runID, orgID, oldGeneration, newGeneration, setFullOrg)
	if err != nil {
		return false, ErrUnavailable
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, ErrUnavailable
	}
	return true, nil
}
