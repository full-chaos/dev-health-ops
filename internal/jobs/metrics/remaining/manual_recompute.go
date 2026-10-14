package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RecomputeDisposition names what a manual recompute did (or would do) for
// one day.
type RecomputeDisposition string

const (
	// RecomputeStarted: a new run was inserted and published for the day.
	RecomputeStarted RecomputeDisposition = "started"
	// RecomputeWouldStart: dry run only; StartManualRecomputeRun would start a run.
	RecomputeWouldStart RecomputeDisposition = "would_start"
	// RecomputeAlreadyRecomputed: this basis already recomputed the day
	// (a succeeded run under the same generation). The second run is a no-op.
	RecomputeAlreadyRecomputed RecomputeDisposition = "already_recomputed"
	// RecomputeAlreadyRan: this basis already started the day and the run is
	// still pending or running (an in-flight retry of the same command).
	RecomputeAlreadyRan RecomputeDisposition = "already_ran"
	// RecomputeNotCovered: no succeeded partition covers the day with output,
	// so there is nothing to replace. `metrics remaining start` (without
	// --recompute) computes such a day.
	RecomputeNotCovered RecomputeDisposition = "not_covered"
	// RecomputeInProgress: an automatic run covering the day is pending or
	// running. Nothing is written; try again when it has finished.
	RecomputeInProgress RecomputeDisposition = "in_progress"
	// RecomputeExhausted: every generation of this basis for the day failed
	// or was canceled; the operator must look at the runs.
	RecomputeExhausted RecomputeDisposition = "exhausted"
)

// ManualRecomputeFamilies lists the families `start --recompute` accepts. Only
// dora: its automatic triggers (post-sync, fixed schedule) take the same
// (org, day) advisory lock and treat a covered closed day as covered
// (StartRunTx), so a recompute cannot race them into a second run. complexity
// and release_impact have no such cross-trigger lock or dedup on a post-sync
// run, so a recompute of them could not promise "never concurrent with an
// automatic run of the same day".
var ManualRecomputeFamilies = []string{"dora"}

// ManualRecomputeOutcome reports what StartManualRecomputeRun (or the dry-run
// preview) decided for one day.
type ManualRecomputeOutcome struct {
	Day         string
	Disposition RecomputeDisposition
	RunID       string
	PartitionID string
	Generation  string
}

// maxRecomputeBasisLength keeps "manual-recompute:<basis>:<family>:<org>" plus
// the ":retry-N" suffix inside maxGenerationLength (128).
const maxRecomputeBasisLength = 40

var recomputeBasisPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// ValidRecomputeBasis reports whether basis is a usable basis label: 1 to 40
// of lowercase letters, digits, '.', '_' and '-', starting with a letter or digit.
func ValidRecomputeBasis(basis string) bool {
	return len(basis) >= 1 && len(basis) <= maxRecomputeBasisLength && recomputeBasisPattern.MatchString(basis)
}

// ManualRecomputeGeneration is the generation of every run one basis change
// starts for (family, org). It carries no day and no window: the day is the
// run's scope key, so the same basis never recomputes the same day twice,
// whatever window an operator names on each call. A new basis change takes a
// new label and so a new generation.
func ManualRecomputeGeneration(basis, family, organizationID string) string {
	return "manual-recompute:" + basis + ":" + family + ":" + organizationID
}

// StartManualRecomputeRun rewrites one COVERED closed day after a basis
// change (CHAOS-9674). `metrics remaining start` refuses a covered day on
// purpose; this is the explicit, operator-only path past that refusal.
//
// What it does for (family, org, day):
//   - an automatic run (pending/running, other generation) whose window holds
//     the day -> RecomputeInProgress, nothing written (the same
//     findManualBackfillBlocker check and the same advisory lock `start`
//     uses, so it cannot race a scheduled or post-sync run of the day);
//   - a run of this basis already succeeded -> RecomputeAlreadyRecomputed (no-op);
//   - a run of this basis pending/running -> RecomputeAlreadyRan;
//   - no succeeded partition covers the day with output -> RecomputeNotCovered;
//   - otherwise a new run for the day under ManualRecomputeGeneration, which
//     the worker executes like any other partition (ClaimPartition and its
//     lease), -> RecomputeStarted.
//
// The rewrite appends: dora_metrics_daily keeps every generation and readers
// take the newest computed_at per key (internal/jobs/report/tablereads.go).
// Only ManualRecomputeFamilies are accepted.
func (store *PostgresStore) StartManualRecomputeRun(
	ctx context.Context,
	family, organizationID, day, basis string,
	publisher PartitionPublisher,
) (ManualRecomputeOutcome, error) {
	return store.manualRecompute(ctx, family, organizationID, day, basis, publisher, false)
}

// PreviewManualRecompute returns the outcome StartManualRecomputeRun would
// reach for the day (RecomputeStarted is RecomputeWouldStart) and writes nothing.
func (store *PostgresStore) PreviewManualRecompute(
	ctx context.Context, family, organizationID, day, basis string,
) (ManualRecomputeOutcome, error) {
	return store.manualRecompute(ctx, family, organizationID, day, basis, nil, true)
}

func (store *PostgresStore) manualRecompute(
	ctx context.Context,
	family, organizationID, day, basis string,
	publisher PartitionPublisher,
	dryRun bool,
) (ManualRecomputeOutcome, error) {
	if !store.valid() {
		return ManualRecomputeOutcome{}, ErrUnavailable
	}
	if !validUUID(organizationID) || !validDate(day) || !ValidRecomputeBasis(basis) {
		return ManualRecomputeOutcome{}, ErrInvalidState
	}
	if !slices.Contains(ManualRecomputeFamilies, family) {
		return ManualRecomputeOutcome{}, ErrInvalidState
	}
	organizationID = uuid.MustParse(organizationID).String()
	scope, err := manualBackfillDayScope(family, day)
	if err != nil {
		return ManualRecomputeOutcome{}, err
	}
	generation := ManualRecomputeGeneration(basis, family, organizationID)
	request, err := normalizeStartRunRequest(StartRunRequest{
		OrganizationID: organizationID,
		Family:         family,
		Generation:     generation,
		ScopeKey:       day,
		Scopes:         []json.RawMessage{scope},
	})
	if err != nil {
		store.observeScopeRefused(family, scopeRefusalReason(err))
		return ManualRecomputeOutcome{}, err
	}

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return ManualRecomputeOutcome{}, ErrUnavailable
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

	// The same lock keyspaces StartManualBackfillRun takes (see there for why
	// dora's has no family segment), so a recompute serializes against a
	// concurrent dispatch of the same org+day.
	lockNamespace, lockKey := "remaining_metrics_manual_backfill_day", request.OrganizationID+":"+request.Family+":"+day
	if request.Family == "dora" {
		lockNamespace, lockKey = "remaining_metrics_dora_day", request.OrganizationID+":"+day
	}
	if !dryRun {
		if _, err := tx.Exec(ctx,
			"SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))", lockNamespace, lockKey,
		); err != nil {
			return ManualRecomputeOutcome{}, ErrUnavailable
		}
	}

	blockingRunID, reason, err := store.findManualBackfillBlocker(
		ctx, tx, request.OrganizationID, request.Family, day, generation, request.Scopes[0])
	if err != nil {
		return ManualRecomputeOutcome{}, err
	}
	if reason == blockReasonInProgress {
		return store.finishRecompute(family, ManualRecomputeOutcome{
			Day: day, Disposition: RecomputeInProgress, RunID: blockingRunID, Generation: generation,
		}, dryRun), nil
	}

	attempt := request
	for bump := 0; ; bump++ {
		runID := deterministicRunID(attempt)
		var status string
		err := tx.QueryRow(ctx,
			"SELECT status FROM public.remaining_metric_runs WHERE id = $1::uuid", runID).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return ManualRecomputeOutcome{}, store.wrapUnavailable(ctx, "load recompute run", err, "family", family)
		}
		switch status {
		case "succeeded":
			return store.finishRecompute(family, ManualRecomputeOutcome{
				Day: day, Disposition: RecomputeAlreadyRecomputed, RunID: runID,
				PartitionID: deterministicPartitionID(runID, 1), Generation: attempt.Generation,
			}, dryRun), nil
		case "pending", "running":
			return store.finishRecompute(family, ManualRecomputeOutcome{
				Day: day, Disposition: RecomputeAlreadyRan, RunID: runID,
				PartitionID: deterministicPartitionID(runID, 1), Generation: attempt.Generation,
			}, dryRun), nil
		}
		if bump >= maxManualBackfillGenerationBumps {
			return store.finishRecompute(family, ManualRecomputeOutcome{
				Day: day, Disposition: RecomputeExhausted, RunID: runID, Generation: attempt.Generation,
			}, dryRun), nil
		}
		attempt.Generation = fmt.Sprintf("%s:retry-%d", generation, bump+1)
	}

	if reason != blockReasonCovered {
		return store.finishRecompute(family, ManualRecomputeOutcome{
			Day: day, Disposition: RecomputeNotCovered, Generation: generation,
		}, dryRun), nil
	}
	if dryRun {
		return store.finishRecompute(family, ManualRecomputeOutcome{
			Day: day, Disposition: RecomputeWouldStart, Generation: attempt.Generation,
		}, true), nil
	}

	run, created, err := store.insertRun(ctx, tx, attempt, publisher)
	if err != nil {
		return ManualRecomputeOutcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ManualRecomputeOutcome{}, ErrUnavailable
	}
	committed = true
	disposition := RecomputeStarted
	if !created {
		disposition = RecomputeAlreadyRan
	}
	return store.finishRecompute(family, ManualRecomputeOutcome{
		Day: day, Disposition: disposition, RunID: run.ID,
		PartitionID: deterministicPartitionID(run.ID, 1), Generation: attempt.Generation,
	}, false), nil
}

func (store *PostgresStore) finishRecompute(
	family string, outcome ManualRecomputeOutcome, dryRun bool,
) ManualRecomputeOutcome {
	store.logger.Info("remaining metrics manual recompute",
		"family", family, "day", outcome.Day, "disposition", string(outcome.Disposition),
		"run_id", outcome.RunID, "generation", outcome.Generation, "dry_run", dryRun)
	return outcome
}
