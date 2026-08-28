package remaining

import (
	"context"
	"encoding/json"
	"errors"
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
// non-zero-row succeeded partition already covers the requested
// organization/family/day. Retrying would insert a genuine duplicate row
// into an append-only table with no dedup on replay (CHAOS-4242's
// dora_metrics_daily finding) -- this is the one case the command refuses.
var ErrDayAlreadyCovered = errors.New("remaining metrics day is already covered by a non-zero-row succeeded partition")

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

	coveringRunID, covered, err := store.findNonZeroRowCoverage(ctx, tx, request.OrganizationID, request.Family, day)
	if err != nil {
		return ManualBackfillOutcome{}, err
	}
	if covered {
		store.observeManualBackfill(family, "already_covered")
		return ManualBackfillOutcome{Day: day, RunID: coveringRunID}, ErrDayAlreadyCovered
	}

	run, created, err := store.insertRun(ctx, tx, request, publisher)
	if err != nil {
		return ManualBackfillOutcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ManualBackfillOutcome{}, ErrUnavailable
	}
	committed = true
	// created is false when insertRun's own ON CONFLICT DO NOTHING branch
	// loaded an existing run rather than inserting a fresh one -- this exact
	// (org, family, generation, day) request was already started by a prior
	// call (a retried CLI invocation reusing the same generation).
	outcome := "started"
	if !created {
		outcome = "already_ran"
	}
	store.observeManualBackfill(family, outcome)
	return ManualBackfillOutcome{
		Day:         day,
		RunID:       run.ID,
		PartitionID: deterministicPartitionID(run.ID, 1),
		AlreadyRan:  outcome == "already_ran",
	}, nil
}

// findNonZeroRowCoverage reports whether a NON-zero-row succeeded partition
// for (organizationID, family) already covers day -- true coverage this
// command must refuse, versus a 0-row completion it exists to recompute.
//
// Unlike loadRunCoveringDay's exact `scope->>'day' = day` match, this checks
// CONTAINMENT in the partition's whole [anchor - backfill_days + 1, anchor]
// window (codex review, P1): DORAExecutor.ComputePartition and
// job_release_impact.py's `_date_range` both write rows for EVERY day in
// that window from ONE partition anchored on the LATEST day, not one
// partition per day. A post-sync catch-up run anchored on day E with
// backfill_days=N therefore already covers every day in
// [E-N+1, E] -- requesting an interior day of that range must see it as
// covered, or this command would insert a genuine duplicate for data
// already written. (complexity's BackfillDays is pinned to 1 by
// validateFamilyScope, so its window is always a single day -- this still
// reduces to an exact match for it, no special-casing needed.)
//
// Bounded to anchors in [day, day+maxDayScopedBackfillDays-1] so this stays
// a targeted range read keyed on the (org_id, family) index shape the exact-
// match query already uses, not a full per-organization table scan.
func (store *PostgresStore) findNonZeroRowCoverage(
	ctx context.Context, tx pgx.Tx, organizationID, family, day string,
) (runID string, covered bool, err error) {
	requested, parseErr := time.Parse("2006-01-02", day)
	if parseErr != nil {
		return "", false, ErrInvalidState
	}
	upperBound := requested.AddDate(0, 0, maxDayScopedBackfillDays-1).Format("2006-01-02")
	rows, err := tx.Query(ctx, `
SELECT run.id::text, partition.scope->>'day', coalesce((partition.scope->>'backfill_days')::int, 1), partition.output_evidence
FROM public.remaining_metric_partitions AS partition
JOIN public.remaining_metric_runs AS run ON run.id = partition.run_id
WHERE run.org_id = $1::uuid AND run.family = $2
  AND run.status = 'succeeded' AND partition.status = 'succeeded'
  AND partition.scope->>'day' >= $3 AND partition.scope->>'day' <= $4`,
		organizationID, family, day, upperBound,
	)
	if err != nil {
		return "", false, ErrUnavailable
	}
	defer rows.Close()
	for rows.Next() {
		var id, anchorDay string
		var backfillDays int
		var outputEvidence *string
		if err := rows.Scan(&id, &anchorDay, &backfillDays, &outputEvidence); err != nil {
			return "", false, ErrUnavailable
		}
		if isZeroRowEvidence(outputEvidence) {
			continue
		}
		anchor, parseErr := time.Parse("2006-01-02", anchorDay)
		if parseErr != nil {
			continue
		}
		windowStart := anchor.AddDate(0, 0, -(backfillDays - 1))
		if !requested.Before(windowStart) && !requested.After(anchor) {
			return id, true, nil
		}
	}
	if rows.Err() != nil {
		return "", false, ErrUnavailable
	}
	return "", false, nil
}
