package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

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
// frozen dora days). generation MUST be distinct per operator invocation
// (the CLI mints "manual-backfill:<RFC3339 timestamp>") so a manual run
// never collides with a post-sync or fixed-schedule run's own generation.
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

	// Serialize against a concurrent automatic (post-sync / fixed-schedule)
	// or manual dispatch for the SAME (org, family, day): the same
	// advisory-lock keyspace shape StartRunTx's dora block uses, widened
	// with the family so complexity/release_impact -- which have no
	// cross-generation coverage collision in the automatic path -- still
	// never race a concurrent manual invocation for the same day.
	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
		"remaining_metrics_manual_backfill_day", request.OrganizationID+":"+request.Family+":"+day,
	); err != nil {
		return ManualBackfillOutcome{}, ErrUnavailable
	}

	covering, outputEvidence, found, err := store.loadRunCoveringDay(
		ctx, tx, request.OrganizationID, request.Family, day, 1,
	)
	if err != nil {
		return ManualBackfillOutcome{}, err
	}
	if found && !isZeroRowEvidence(outputEvidence) {
		store.observeManualBackfill(family, "already_covered")
		return ManualBackfillOutcome{Day: day, RunID: covering.ID}, ErrDayAlreadyCovered
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
