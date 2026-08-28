package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// maxDayScopedBackfillDays mirrors validateFamilyScope's own bound on
// dora/release_impact's BackfillDays (scopes.go: 1-90) -- the widest window
// a single succeeded partition could possibly cover, and therefore the
// furthest ahead of the requested day findNonZeroRowCoverage needs to look
// for an anchor that might contain it.
const maxDayScopedBackfillDays = 90

// ErrDayAlreadyCovered is returned by StartManualBackfillRun when a
// succeeded partition already provides UNAMBIGUOUS non-zero-row output for
// the requested day, or an AMBIGUOUS multi-day partition's window contains
// it (see findManualBackfillBlocker's doc comment for why an ambiguous
// aggregate is treated as covered rather than guessed at). Retrying would
// risk inserting a genuine duplicate row into an append-only table with no
// dedup on replay (CHAOS-4242's dora_metrics_daily finding) -- this is the
// one case the command refuses.
var ErrDayAlreadyCovered = errors.New("remaining metrics day is already covered by a succeeded partition")

// ErrDayInProgress is returned by StartManualBackfillRun when an automatic
// (post-sync or fixed-schedule) run for the requested day is still
// pending/running under a DIFFERENT generation. Its eventual completion is
// invisible to findManualBackfillBlocker's succeeded-only coverage check, so
// inserting a manual run alongside it risks both writing the same day
// (codex review, P1) -- this refuses rather than race it.
var ErrDayInProgress = errors.New("remaining metrics day already has an in-progress automatic run")

// ErrManualBackfillGenerationExhausted is returned by StartManualBackfillRun
// when every generation it tried (the base one plus
// maxManualBackfillGenerationBumps numbered retries) already exists in a
// terminal, not-usefully-covering state (failed/canceled, or succeeded with
// an unambiguous 0-row completion). Committing anyway and reporting
// "already_ran" would claim success while dispatching nothing (codex
// review, P2) -- a run stuck failing/canceling on every attempt is a real
// problem to surface, not paper over.
var ErrManualBackfillGenerationExhausted = errors.New(
	"remaining metrics manual backfill exhausted its retry-generation budget without dispatching new work",
)

// ErrUnsupportedManualBackfillFamily is returned for a family this command
// has no day-scoped default partition scope for.
var ErrUnsupportedManualBackfillFamily = errors.New(
	"remaining metrics family is not day-scoped for manual backfill (supported: dora, complexity, release_impact)",
)

// ManualBackfillDayScopedFamilies lists the families StartManualBackfillRun
// accepts, in the stable order the CLI prints them in help text. capacity,
// recommendations, and membership_backfill are valid remaining-metrics
// families (families.json) but their scopes are not day-keyed (capacity
// needs a GenerationSeed; recommendations/membership_backfill scope by
// window/repo set, not a calendar day), so a "--day" backfill has no
// well-defined meaning for them.
var ManualBackfillDayScopedFamilies = []string{"complexity", "dora", "release_impact"}

// manualBackfillDayScope builds a day-scoped family's default partition
// scope for a single day, byte-for-byte matching the field values
// internal/scheduler/fixed/producers.go's fixed-schedule fanout uses for the
// same family (backfill_days=1; dora's sink="auto"/interval="daily";
// release_impact's recomputation_window_days=7) -- so a manually started
// partition is indistinguishable, once dispatched, from one the fixed
// schedule would have created for that day.
func manualBackfillDayScope(family, day string) (json.RawMessage, error) {
	switch family {
	case "dora":
		return json.Marshal(map[string]any{
			"version": 1, "day": day, "backfill_days": 1,
			"sink": "auto", "interval": "daily",
		})
	case "complexity":
		return json.Marshal(map[string]any{
			"version": 1, "day": day, "backfill_days": 1,
		})
	case "release_impact":
		return json.Marshal(map[string]any{
			"version": 1, "day": day, "backfill_days": 1,
			"recomputation_window_days": 7,
		})
	default:
		return nil, ErrUnsupportedManualBackfillFamily
	}
}

// ManualBackfillOutcome reports what StartManualBackfillRun did for one day.
type ManualBackfillOutcome struct {
	Day         string
	RunID       string
	PartitionID string
	// Generation is the ACTUAL generation the run was inserted (or found)
	// under -- the base generation the caller passed in, or one of its
	// numbered ":retry-N" bumps if the base was exhausted by a prior
	// terminal, not-usefully-covering run (codex review, P2). Callers
	// building a durable lookup query (e.g. `WHERE run.generation = ...`)
	// must use THIS value, not the generation they originally passed in --
	// they can differ.
	Generation string
	// AlreadyRan is true when this exact (org, family, generation, day)
	// request had already been started by a prior call (e.g. a retried CLI
	// invocation reusing the same generation) -- StartRunTx's own ON
	// CONFLICT DO NOTHING idempotency, surfaced here rather than treated as
	// an error.
	AlreadyRan bool
}

// StartManualBackfillRun starts an operator-triggered remaining-metrics run
// for one historical (organization, family, day) that no automatic trigger
// ever dispatched (CHAOS-4254 -- the prod recovery path for CHAOS-4384's
// frozen dora days). generation MUST be deterministic per LOGICAL request
// (the CLI derives "manual-backfill:<family>:<org>:<from>..<to>" from its
// own flags, not a wall-clock timestamp) so a retried CLI invocation reuses
// the SAME generation and lands on insertRun's ON CONFLICT DO NOTHING path
// instead of inserting a second run while the first is still
// pending/running (codex review, P1: coverage only recognizes SUCCEEDED
// partitions, so a timestamp-derived generation made a same-second retry
// indistinguishable from a genuinely new request). It must still differ
// from any post-sync/fixed-schedule generation, which the "manual-backfill:"
// prefix guarantees.
//
// Coverage rule -- deliberately NOT the same one StartRunTx's automatic
// family=="dora" block applies: that block treats ANY succeeded partition
// for a day that has already CLOSED as terminal coverage (CHAOS-4384),
// because for the automatic triggers a genuinely quiet closed day and a
// day nobody ever computed are indistinguishable, and reopening every
// closed 0-row day forever would burn a fresh run on every later trigger.
// This command refuses ONLY a NON-zero-row succeeded partition. A 0-row day
// -- open or closed -- is exactly the CHAOS-4384 failure shape this command
// exists to recover: the day was computed before its source data existed.
// Recomputing it is the entire point, so a closed 0-row day is backfillable
// here even though the automatic path would refuse to touch it again.
func (store *PostgresStore) StartManualBackfillRun(
	ctx context.Context,
	family, organizationID, day, generation string,
	publisher PartitionPublisher,
) (ManualBackfillOutcome, error) {
	if !store.valid() {
		return ManualBackfillOutcome{}, ErrUnavailable
	}
	if !validUUID(organizationID) {
		return ManualBackfillOutcome{}, ErrInvalidState
	}
	if !validDate(day) {
		return ManualBackfillOutcome{}, ErrInvalidState
	}
	scope, err := manualBackfillDayScope(family, day)
	if err != nil {
		return ManualBackfillOutcome{}, err
	}
	request, err := normalizeStartRunRequest(StartRunRequest{
		OrganizationID: organizationID,
		Family:         family,
		Generation:     generation,
		ScopeKey:       day,
		Scopes:         []json.RawMessage{scope},
	})
	if err != nil {
		return ManualBackfillOutcome{}, err
	}

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return ManualBackfillOutcome{}, ErrUnavailable
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

	// Serialize against a concurrent dispatch for the SAME (org, family,
	// day). For "dora" this MUST be the exact same advisory-lock keyspace
	// StartRunTx's own family=="dora" block uses (postgres.go) --
	// ("remaining_metrics_dora_day", org+":"+day), no family segment --
	// otherwise the two never actually serialize against each other (codex
	// review, P1): a manual backfill and a concurrent post-sync/fixed-
	// schedule dispatch for the same org+day would each see no coverage and
	// insert their own append-only computation. complexity/release_impact
	// have no such automatic cross-generation collision, so they use a
	// manual-only key scoped by family, purely to self-serialize concurrent
	// manual invocations.
	lockNamespace, lockKey := "remaining_metrics_manual_backfill_day", request.OrganizationID+":"+request.Family+":"+day
	if request.Family == "dora" {
		lockNamespace, lockKey = "remaining_metrics_dora_day", request.OrganizationID+":"+day
	}
	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))", lockNamespace, lockKey,
	); err != nil {
		return ManualBackfillOutcome{}, ErrUnavailable
	}

	blockingRunID, reason, err := store.findManualBackfillBlocker(ctx, tx, request.OrganizationID, request.Family, day, generation)
	if err != nil {
		return ManualBackfillOutcome{}, err
	}
	if reason == blockReasonInProgress {
		store.observeManualBackfill(family, "in_progress")
		return ManualBackfillOutcome{Day: day, RunID: blockingRunID}, ErrDayInProgress
	}
	if reason == blockReasonCovered {
		store.observeManualBackfill(family, "already_covered")
		return ManualBackfillOutcome{Day: day, RunID: blockingRunID}, ErrDayAlreadyCovered
	}

	// codex review, P1: generation is deterministic per LOGICAL request (see
	// the doc comment above), which is exactly what makes an in-flight retry
	// idempotent -- but it also means a PRIOR manual attempt that already
	// reached a terminal state under this same generation would otherwise be
	// stuck forever: insertRun's ON CONFLICT DO NOTHING always reloads that
	// same row, even after it legitimately needs recomputing (e.g. it
	// itself completed with 0 rows before source data existed, and the
	// operator is intentionally re-running the identical command once it
	// has). Bounded retry: each pass either inserts fresh (done) or finds an
	// existing row. A 'pending'/'running' reload is a genuine in-flight
	// retry -- stop and report it as "already_ran". Anything else
	// (failed/canceled, or succeeded) is safe to bump past: a succeeded
	// reload can only be an UNAMBIGUOUS 0-row completion here, because
	// findManualBackfillBlocker above already scanned every succeeded
	// partition for this org+family+window (across ALL generations, not
	// just this one) and would have refused this call outright had any real
	// or ambiguous coverage existed.
	attemptRequest := request
	var run Run
	var created, exhausted bool
	for attempt := 0; ; attempt++ {
		run, created, err = store.insertRun(ctx, tx, attemptRequest, publisher)
		if err != nil {
			return ManualBackfillOutcome{}, err
		}
		if created || run.Status == "pending" || run.Status == "running" {
			break
		}
		if attempt >= maxManualBackfillGenerationBumps {
			// codex review round 3, P2: every generation tried is terminal
			// and not usefully covering (findManualBackfillBlocker above
			// already ruled out real/ambiguous coverage, so this can only
			// be failed/canceled or an exhausted unambiguous 0-row chain).
			// Committing anyway and reporting "already_ran" would claim
			// success while dispatching nothing.
			exhausted = true
			break
		}
		attemptRequest.Generation = fmt.Sprintf("%s:retry-%d", generation, attempt+1)
	}

	if err := tx.Commit(ctx); err != nil {
		return ManualBackfillOutcome{}, ErrUnavailable
	}
	committed = true
	if exhausted {
		store.observeManualBackfill(family, "exhausted")
		return ManualBackfillOutcome{Day: day, RunID: run.ID, Generation: run.Generation}, ErrManualBackfillGenerationExhausted
	}
	// created is false when insertRun's own ON CONFLICT DO NOTHING branch
	// loaded an existing (still pending/running) run rather than inserting a
	// fresh one -- this exact (org, family, generation, day) request was
	// already started by a prior call and is genuinely in flight.
	outcome := "started"
	if !created {
		outcome = "already_ran"
	}
	store.observeManualBackfill(family, outcome)
	return ManualBackfillOutcome{
		Day:         day,
		RunID:       run.ID,
		PartitionID: deterministicPartitionID(run.ID, 1),
		Generation:  attemptRequest.Generation,
		AlreadyRan:  outcome == "already_ran",
	}, nil
}

// maxManualBackfillGenerationBumps bounds how many numbered-suffix
// generations StartManualBackfillRun will try before giving up: a run stuck
// failing/canceling on every attempt is a real problem worth surfacing as an
// error, not retrying indefinitely inside one call.
const maxManualBackfillGenerationBumps = 5

// isSameManualBackfillRequest reports whether runGeneration belongs to the
// SAME logical manual-backfill request as generation -- either the exact
// base generation, or one of its bounded ":retry-N" bumps StartManualBackfillRun
// mints when a prior attempt under this same request exhausted a terminal,
// not-usefully-covering generation (codex review round 3, P2). Without this,
// findManualBackfillBlocker would treat an operator's own still-pending
// retry-1 run as a FOREIGN in-progress collision on a subsequent identical
// invocation, refusing a request that should instead report "already_ran".
func isSameManualBackfillRequest(runGeneration, generation string) bool {
	return runGeneration == generation || strings.HasPrefix(runGeneration, generation+":retry-")
}

// blockReason names why findManualBackfillBlocker refused a day.
type blockReason int

const (
	blockReasonNone blockReason = iota
	blockReasonInProgress
	blockReasonCovered
)

// findManualBackfillBlocker decides whether StartManualBackfillRun must
// refuse (organizationID, family, day), and why.
//
// blockReasonInProgress: a run under a DIFFERENT generation is still
// pending/running and its scope's [anchor-backfill_days+1, anchor] window
// contains day. Its eventual completion is invisible to the succeeded-only
// checks below, so inserting a manual run alongside it risks both writing
// the same day once each finishes (codex review, P1) -- even the advisory
// lock above does not help here, since it only serializes concurrent
// transactions, not a run that already committed and is still executing.
// A pending/running row under THIS SAME generation is excluded from this
// check -- that is simply this call's own prior insert not yet observed,
// which insertRun's ON CONFLICT DO NOTHING path already handles correctly
// (reported as "already_ran"); treating it as a foreign blocker would make
// a genuine idempotent retry refuse itself.
//
// blockReasonCovered: a succeeded partition's window contains day, and
// either:
//   - its own backfill_days == 1 -- an EXACT single-day partition, where
//     output_evidence's rows_written IS that day's own total,
//     unambiguously -- with non-zero evidence; or
//   - its backfill_days > 1 -- an aggregate spanning MULTIPLE days, which
//     CANNOT prove day itself got zero or non-zero rows (codex review, P1:
//     DORAExecutor.ComputePartition accumulates ONE total across the whole
//     window in dora_native.go, and complexity's compatibility-bridge
//     evidence carries no row count at all). An ambiguous multi-day
//     aggregate is treated as covered regardless of its zero/non-zero
//     value -- guessing "definitely 0 rows on this specific day" from a
//     multi-day total risks the opposite failure this command exists to
//     fix (silently skipping a day that already has real, duplicable
//     output). An operator who hits this should verify via the
//     readback_hint and, if genuinely uncovered, request a narrower
//     --day/--to range that does not land inside the ambiguous window.
//
// Bounded to anchors in [day, day+maxDayScopedBackfillDays-1] so this stays
// a targeted range read on the (org_id, family) index shape, not a full
// per-organization table scan.
func (store *PostgresStore) findManualBackfillBlocker(
	ctx context.Context, tx pgx.Tx, organizationID, family, day, generation string,
) (runID string, reason blockReason, err error) {
	requested, parseErr := time.Parse("2006-01-02", day)
	if parseErr != nil {
		return "", blockReasonNone, ErrInvalidState
	}
	upperBound := requested.AddDate(0, 0, maxDayScopedBackfillDays-1).Format("2006-01-02")
	rows, err := tx.Query(ctx, `
SELECT run.id::text, run.generation, run.status, partition.status, partition.scope->>'day',
       coalesce((partition.scope->>'backfill_days')::int, 1), partition.output_evidence
FROM public.remaining_metric_partitions AS partition
JOIN public.remaining_metric_runs AS run ON run.id = partition.run_id
WHERE run.org_id = $1::uuid AND run.family = $2
  AND run.status IN ('pending', 'running', 'succeeded')
  AND partition.scope->>'day' >= $3 AND partition.scope->>'day' <= $4`,
		organizationID, family, day, upperBound,
	)
	if err != nil {
		return "", blockReasonNone, ErrUnavailable
	}
	defer rows.Close()

	var coveredRunID string
	for rows.Next() {
		var id, runGeneration, runStatus, partitionStatus, anchorDay string
		var backfillDays int
		var outputEvidence *string
		if err := rows.Scan(&id, &runGeneration, &runStatus, &partitionStatus, &anchorDay, &backfillDays, &outputEvidence); err != nil {
			return "", blockReasonNone, ErrUnavailable
		}
		anchor, parseErr := time.Parse("2006-01-02", anchorDay)
		if parseErr != nil {
			continue
		}
		windowStart := anchor.AddDate(0, 0, -(backfillDays - 1))
		if requested.Before(windowStart) || requested.After(anchor) {
			continue // this partition's window does not contain the requested day
		}
		if (runStatus == "pending" || runStatus == "running") && !isSameManualBackfillRequest(runGeneration, generation) {
			// The most actionable signal -- return immediately rather than
			// keep scanning for a "covered" reason that matters less.
			return id, blockReasonInProgress, nil
		}
		if runStatus != "succeeded" || partitionStatus != "succeeded" {
			continue
		}
		if backfillDays > 1 || !isZeroRowEvidence(outputEvidence) {
			coveredRunID = id
		}
	}
	if rows.Err() != nil {
		return "", blockReasonNone, ErrUnavailable
	}
	if coveredRunID != "" {
		return coveredRunID, blockReasonCovered, nil
	}
	return "", blockReasonNone, nil
}
