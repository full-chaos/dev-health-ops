package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	readyFinalizeCompletedEvidence = "ready_finalize_delivery_completed"
	readyFinalizeMissingEvidence   = "ready_finalize_delivery_missing"
)

// # Guard matrix, CHAOS-5456
//
// Every predicate below was mutated one clause at a time against the real
// PostgreSQL 18 + migration-0049 harness, and each mutation was required to
// turn a specific test red. 21 of 25 did. The four that did not are recorded
// here rather than left as an unexplained gap, because "no test went red" and
// "the clause cannot be reached" look identical in a matrix that only counts:
//
//   - job.finalized_at IS NOT NULL -- River's own river_job CHECK constraint
//     forbids a completed row with a NULL finalized_at (SQLSTATE 23514), so
//     the excluded shape cannot be seeded at all. Pinned instead by
//     TestReadyFinalizeRepairPreservesDeliveryAndDomainFences' "river forbids
//     a completed job with no finalized_at" subtest, which fails the day a
//     River migration drops that constraint and makes this clause load-bearing.
//   - the `if !completed` verdict on the FOR UPDATE re-read -- unreachable
//     except when another actor changes the job's state between the candidate
//     select (which locks the outbox, not the job) and this lock. That is a
//     genuine race guard, and pinning it deterministically would mean
//     injecting the race. The RULE it re-checks is shared, not restated
//     (readyFinalizeJobLivenessPredicate), so it cannot drift from the filter.
//   - the RowsAffected() != 1 assertion -- the UPDATE is keyed on a primary
//     key held under FOR UPDATE in this transaction, so no reachable state
//     produces a count other than 1. It stays as the cheap detector for a
//     future predicate change that makes it reachable.
//   - limit-outcome.Recovered as the budget handed to this pass -- exercising
//     it needs one of the three River-terminal branches to consume part of the
//     same Step's budget first, which no single fixture seeds today. It bounds
//     one pass, not correctness of what is re-armed.
//
// Full matrix, including the mutation and the killing subtest for each of the
// 21, is in the lane's mutation-matrix.json evidence.
//
// readyFinalizeOutcome is the per-Step tally the telemetry line reports. Every
// field is emitted on EVERY pass, including the zero-work pass: CHAOS-5456's
// whole cost was that a finalize backstop which selected nothing and wrote
// nothing was indistinguishable, in the log, from one that was never reached.
// A counter that only appears when it is non-zero cannot answer "did this run
// at all", which is the first question an operator asks about a stranded run.
type readyFinalizeOutcome struct {
	Candidates         int
	Recovered          int
	CompletedDelivery  int
	MissingDelivery    int
	SkippedNotReady    int
	SkippedLiveJob     int
	SkippedLockedJob   int
	RemainingBudget    int
	CoordinatorReadSec float64
}

// repairReadyFinalizers complements the materializer's deliberate refusal to
// replace a River delivery without queue-side liveness evidence. The queue
// transaction owns outbox/job locks and all writes. The coordinator connection
// reads the same run/discovery readiness contract as materializeFinalizeSQL;
// the queue role must never acquire access to coordinator-only ledgers. This
// split is the shape docs/contribute/architecture/go-worker-runtime.md's
// "Components that hold more than one pool" table mandates: statements that
// span two jurisdictions take two pools and move the foreign read out of the
// other pool's transaction, rather than widening either role.
//
// Completion of units/discovery is monotonic within one run. A finalizer is
// eligible only after those terminal facts are committed. The outbox lock
// prevents a new delivery while their readiness is checked, and a present
// River row is locked before its completed state can license recovery.
// See .github/docs-legacy/architecture/go-worker-cutover-trd.md §9.
func (repair *TerminalDeliveryRepair) repairReadyFinalizers(
	ctx context.Context, tx pgx.Tx, now time.Time, limit int,
) (readyFinalizeOutcome, error) {
	outcome := readyFinalizeOutcome{RemainingBudget: limit}
	if limit == 0 {
		// Still reported: a budget fully consumed by the three River-terminal
		// branches is a real reason this backstop did nothing, and it is
		// invisible unless the zero pass says so.
		repair.logReadyFinalizeOutcome(ctx, now, outcome)
		return outcome, nil
	}
	staleBefore := now.Add(-repair.finalizeStaleAge)
	rows, err := tx.Query(ctx, repair.readyFinalizeQuery, now, staleBefore, limit)
	if err != nil {
		return outcome, repair.readyFinalizeError(ctx, "candidates", err)
	}
	type candidate struct {
		id, runID string
		jobID     int64
		present   bool
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.runID, &c.jobID, &c.present); err != nil {
			rows.Close()
			return outcome, repair.readyFinalizeError(ctx, "candidate_scan", err)
		}
		candidates = append(candidates, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return outcome, repair.readyFinalizeError(ctx, "candidate_rows", err)
	}
	if len(candidates) > limit {
		return outcome, fmt.Errorf("ready-finalizer candidate count: %w", ErrUnavailable)
	}
	outcome.Candidates = len(candidates)
	for _, c := range candidates {
		var ready bool
		started := time.Now()
		readErr := repair.coordinator.QueryRow(ctx, readyFinalizeDomainSQL, c.runID).Scan(&ready)
		outcome.CoordinatorReadSec += time.Since(started).Seconds()
		if readErr != nil {
			return outcome, repair.readyFinalizeError(ctx, "coordinator_readiness", readErr)
		}
		if !ready {
			outcome.SkippedNotReady++
			continue
		}
		evidence := readyFinalizeMissingEvidence
		if c.present {
			var completed bool
			err := tx.QueryRow(ctx, repair.lockFinalizeJobSQL, c.jobID).Scan(&completed)
			if errors.Is(err, pgx.ErrNoRows) {
				// SKIP LOCKED yielded nothing: another replica holds this
				// job's row, so its liveness cannot be read here.
				outcome.SkippedLockedJob++
				continue
			}
			if err != nil {
				return outcome, repair.readyFinalizeError(ctx, "river_liveness_lock", err)
			}
			if !completed {
				outcome.SkippedLiveJob++
				continue
			}
			evidence = readyFinalizeCompletedEvidence
		}
		tag, err := tx.Exec(ctx, rearmReadyFinalizeSQL, c.id, now, evidence)
		if err != nil {
			return outcome, repair.readyFinalizeError(ctx, "rearm", err)
		}
		if tag.RowsAffected() != 1 {
			return outcome, fmt.Errorf("ready-finalizer rearm count: %w", ErrUnavailable)
		}
		outcome.Recovered++
		if c.present {
			outcome.CompletedDelivery++
		} else {
			outcome.MissingDelivery++
		}
	}
	outcome.RemainingBudget = limit - outcome.Recovered
	repair.logReadyFinalizeOutcome(ctx, now, outcome)
	return outcome, nil
}

// readyFinalizeError logs the failing step WITH the driver's SQLSTATE before
// collapsing it to ErrUnavailable. The collapse is deliberate -- the caller
// classifies this stage as continue-safe on any failure -- but a bare step
// name told an operator nothing about WHY: a permission denial (42501), a
// missing relation (42P01) and a serialization failure (40001) all read the
// same. Five minutes of logging here is the difference between reading the
// cause and re-deriving it from a live database.
func (repair *TerminalDeliveryRepair) readyFinalizeError(ctx context.Context, step string, cause error) error {
	sqlstate := ""
	var pgErr *pgconn.PgError
	if errors.As(cause, &pgErr) {
		sqlstate = pgErr.Code
	}
	slog.ErrorContext(ctx, "syncreconciler.ready_finalize_failed",
		"step", step,
		"sqlstate", sqlstate,
		"error", cause.Error(),
	)
	return fmt.Errorf("ready-finalizer %s: %w", step, ErrUnavailable)
}

func (repair *TerminalDeliveryRepair) logReadyFinalizeOutcome(
	ctx context.Context, now time.Time, outcome readyFinalizeOutcome,
) {
	slog.InfoContext(ctx, "syncreconciler.ready_finalize_pass",
		"candidates", outcome.Candidates,
		"recovered", outcome.Recovered,
		"completed_delivery", outcome.CompletedDelivery,
		"missing_delivery", outcome.MissingDelivery,
		"skipped_not_ready", outcome.SkippedNotReady,
		"skipped_live_job", outcome.SkippedLiveJob,
		"skipped_locked_job", outcome.SkippedLockedJob,
		"remaining_budget", outcome.RemainingBudget,
		"stale_age_seconds", int64(repair.finalizeStaleAge.Seconds()),
		"coordinator_read_seconds", outcome.CoordinatorReadSec,
		"now", now.UTC().Format(time.RFC3339Nano),
	)
}

// nonterminalSyncRunStatusPredicate is the ONE definition of "this run has not
// reached a terminal state", shared by the materializer's readiness predicate
// and by the queue-side candidate filter below.
//
// It used to be written twice, once as this denylist and once as the
// complementary allowlist `run.status IN ('planned','dispatching','running')`.
// Over models.integrations.SyncRunStatus those two are exactly equivalent, so
// neither copy could ever be pinned on its own: a mutation of either survived
// the whole guard matrix because the other still refused the row. One shared
// definition is one mutable predicate with a red test behind it, which is
// worth more than two copies that only look like defence in depth.
const nonterminalSyncRunStatusPredicate = `run.status NOT IN ('success', 'partial_failed', 'failed')`

// readyFinalizeJobLivenessPredicate is the ONE definition of "River retired
// this exact finalize delivery without the domain being re-armed", shared by
// the candidate filter and by the FOR UPDATE re-read that licenses the write.
// Same reason as above: two hand-kept copies of a liveness rule are two
// predicates that mask each other's mutations, and the re-read exists to close
// a select-then-write race, not to state a second, independently-drifting rule.
const readyFinalizeJobLivenessPredicate = `job.kind='finalize_sync_run' AND job.state='completed' AND job.finalized_at IS NOT NULL`

// River's integer primary key is compared without a cast on the indexed
// column. Missing means a recorded, well-formed job identity whose row was
// removed; malformed/missing identities are not permission to repair.
const selectReadyFinalizeSQL = `
SELECT outbox.id::text, outbox.sync_run_id::text,
    outbox.transport_job_id::bigint, job.id IS NOT NULL
FROM public.sync_dispatch_outbox AS outbox
JOIN public.sync_runs AS run ON run.id=outbox.sync_run_id
JOIN public.sync_dispatch_transport_routes AS route ON route.kind=outbox.kind
LEFT JOIN %s AS job ON job.id=CASE
    WHEN outbox.transport_job_id ~ '^[1-9][0-9]{0,17}$' THEN outbox.transport_job_id::bigint END
WHERE outbox.kind='finalize_sync_run'
    AND outbox.status='dispatched'
    AND outbox.last_error IS DISTINCT FROM 'feature_disabled'
    AND outbox.dispatched_transport='river'
    AND outbox.dispatched_route_generation=route.generation
    AND route.transport='river' AND NOT route.paused
    AND ` + nonterminalSyncRunStatusPredicate + `
    AND outbox.dispatched_at <= $2
    AND (outbox.claim_expires_at IS NULL OR outbox.claim_expires_at <= $1)
    AND outbox.transport_job_id ~ '^[1-9][0-9]{0,17}$'
    AND (job.id IS NULL OR (` + readyFinalizeJobLivenessPredicate + `))
ORDER BY outbox.dispatched_at,outbox.id
FOR UPDATE OF outbox SKIP LOCKED
LIMIT $3`

// This is the materializer's readiness predicate, evaluated through its own
// coordinator role. The queue-side candidate never substitutes unit counts
// for the discovery or scheduled-occurrence contract.
const readyFinalizeDomainSQL = `SELECT EXISTS (
    SELECT 1 FROM public.sync_runs AS run WHERE run.id=$1 AND ` + finalizeReadyRunPredicate + `
)`

const lockReadyFinalizeJobSQL = `
SELECT ` + readyFinalizeJobLivenessPredicate + `
FROM %s AS job WHERE job.id=$1
FOR UPDATE SKIP LOCKED`

const rearmReadyFinalizeSQL = `
UPDATE public.sync_dispatch_outbox
SET status='pending',available_at=$2,last_error=$3,
    dispatched_at=NULL,dispatched_transport=NULL,dispatched_route_generation=NULL,transport_job_id=NULL,
    claim_token=NULL,claim_expires_at=NULL,claim_transport=NULL,claim_route_generation=NULL,updated_at=$2
WHERE id=$1`
