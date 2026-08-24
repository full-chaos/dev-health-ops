package syncdispatchruntime

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// unitLogAttrs ports _unit_log_context verbatim: the identifying fields
// every BudgetGuard log line in this family carries.
func unitLogAttrs(syncRunID string, unit budgetUnit) []slog.Attr {
	return []slog.Attr{
		slog.String("sync_run_id", syncRunID),
		slog.String("unit_id", unit.id),
		slog.String("source_id", unit.sourceID),
		slog.String("dataset_key", unit.datasetKey),
		slog.String("provider", unit.provider),
		slog.String("cost_class", unit.costClass),
	}
}

// bucketObservationFields mirrors BudgetEstimateBucket.to_dict() verbatim
// -- the shape every observation dict in this family embeds under "bucket".
func bucketObservationFields(bucket budgetEstimateBucket) map[string]any {
	return map[string]any{
		"provider":               bucket.Provider,
		"org_id":                 bucket.OrgID,
		"host":                   bucket.Host,
		"credential_fingerprint": bucket.CredentialFingerprint,
		"dimension":              bucket.Dimension,
	}
}

// admitSurplusRetries ports _admit_surplus_retries verbatim: spend this
// pass's leftover budget on deferred units, longest-deferred-first. See
// that function's docstring (mirrored on surplusRetryCandidates and this
// function's Python source) for the full counter-semantics rationale --
// summarized: a surplus attempt that does not succeed is a complete no-op
// (no episode column moves), and a successful one writes ONLY available_at,
// because every other episode column is owned elsewhere in the lifecycle.
//
// consumedByBucket and slotHeadroom are mutated in place (Go maps are
// reference types, matching Python's mutable dict-by-reference semantics
// here exactly); observations is returned, not mutated in place, since a Go
// append can reallocate.
func admitSurplusRetries(
	ctx context.Context, tx pgx.Tx, logger *slog.Logger, syncRunID string,
	candidates []budgetUnit,
	estimatesByUnit map[string][]budgetEstimate,
	consumedByBucket map[string]int,
	limits map[string]int,
	defaultLimit int,
	slotHeadroom map[dispatchBucket]int,
	familyCooldowns, dimensionCooldowns map[cooldownKey]time.Time,
	observations []map[string]any,
	now time.Time,
) (admitted map[string]time.Time, updatedObservations []map[string]any, err error) {
	if logger == nil {
		logger = slog.Default()
	}
	admitted = map[string]time.Time{}

	for _, unit := range candidates {
		logAttrs := unitLogAttrs(syncRunID, unit)
		estimates := estimatesByUnit[unit.id]
		if len(estimates) == 0 {
			continue
		}

		if _, found := matchingCooldownExpiry(estimates, unit.orgID, unit.provider, unit.integrationID, familyCooldowns, dimensionCooldowns); found {
			logger.InfoContext(ctx, "dispatch_sync_run.budget_surplus_skipped",
				attrsToAny(append(logAttrs, slog.String("reason", "cooldown_active")))...)
			continue
		}

		slotKey := dispatchBucket{orgID: unit.orgID, provider: unit.provider, costClass: unit.costClass}
		if slotHeadroom[slotKey] <= 0 {
			logger.InfoContext(ctx, "dispatch_sync_run.budget_surplus_skipped",
				attrsToAny(append(logAttrs, slog.String("reason", "no_concurrency_slot")))...)
			continue
		}

		// Fit is decided across ALL of the unit's estimates before ANY of
		// them is charged, mirroring the admission loop's whole-unit
		// semantics: a unit that fits three buckets and overflows a fourth
		// must not leave three buckets charged for work that never ran.
		surplusObservations := make([]map[string]any, 0, len(estimates))
		fits := true
		for _, estimate := range estimates {
			budgetKey := budgetKeyFor(estimate.Bucket, estimate.RouteFamily)
			limit := limitForBucket(estimate.Bucket, estimate.RouteFamily, limits, defaultLimit)
			projectedUnits := consumedByBucket[budgetKey] + estimate.EstimatedUnits
			if projectedUnits > limit {
				fits = false
			}
			observation := map[string]any{
				"sync_run_id":      syncRunID,
				"unit_id":          unit.id,
				"source_id":        unit.sourceID,
				"dataset_key":      unit.datasetKey,
				"provider":         unit.provider,
				"cost_class":       unit.costClass,
				"decision":         "surplus_admitted",
				"bucket":           bucketObservationFields(estimate.Bucket),
				"budget_key":       budgetKey,
				"estimated_units":  estimate.EstimatedUnits,
				"projected_units":  projectedUnits,
				"budget_limit":     limit,
				"confidence":       estimate.Confidence,
				"route_family":     estimate.RouteFamily,
				"budget_deferrals": unit.budgetDeferrals,
			}
			surplusObservations = append(surplusObservations, observation)
		}
		if !fits {
			logger.InfoContext(ctx, "dispatch_sync_run.budget_surplus_skipped",
				attrsToAny(append(logAttrs, slog.String("reason", "insufficient_surplus")))...)
			continue
		}

		// Captured BEFORE the promotion overwrites it: this is what a later
		// withdrawal restores. The selection query requires available_at >
		// now, so a candidate always has one; if that ever stops holding,
		// skip rather than promote a unit we could not put back.
		if unit.availableAt == nil {
			logger.WarnContext(ctx, "dispatch_sync_run.budget_surplus_skipped",
				attrsToAny(append(logAttrs, slog.String("reason", "no_prior_available_at")))...)
			continue
		}
		priorAvailableAt := *unit.availableAt

		admittedNow, admitErr := admitUnitFromSurplus(ctx, tx, logger, syncRunID, unit, now)
		if admitErr != nil {
			return nil, nil, admitErr
		}
		if !admittedNow {
			// CAS lost: the unit moved on concurrently. Its budget stays
			// unspent and is offered to the next candidate.
			continue
		}

		for _, estimate := range estimates {
			budgetKey := budgetKeyFor(estimate.Bucket, estimate.RouteFamily)
			consumedByBucket[budgetKey] += estimate.EstimatedUnits
		}
		slotHeadroom[slotKey]--
		admitted[unit.id] = priorAvailableAt
		observations = append(observations, surplusObservations...)
		for _, observation := range surplusObservations {
			logger.InfoContext(ctx, "dispatch_sync_run.budget_surplus_admitted", observationToAnyArgs(observation)...)
		}
	}
	return admitted, observations, nil
}

// admitUnitFromSurplus ports _admit_unit_from_surplus verbatim: pull a
// not-yet-due budget deferral forward into THIS pass. The whole write is
// available_at (plus updated_at) -- status stays RETRYING and every
// episode column is left alone; see admitSurplusRetries' doc comment for
// why. The CAS re-asserts the exact predicate that made the unit a surplus
// candidate (retrying, and still not due), so a concurrent pass that
// already promoted or terminalized it wins and this returns false.
func admitUnitFromSurplus(ctx context.Context, tx pgx.Tx, logger *slog.Logger, syncRunID string, unit budgetUnit, now time.Time) (bool, error) {
	if logger == nil {
		logger = slog.Default()
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET available_at = $2, updated_at = $2
WHERE id = $1::uuid AND status = $3 AND available_at IS NOT NULL AND available_at > $2`,
		unit.id, now, syncRunUnitStatusRetrying)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	logger.InfoContext(ctx, "dispatch_sync_run.budget_surplus_pulled_forward",
		attrsToAny(append(unitLogAttrs(syncRunID, unit), slog.Int("budget_deferrals", unit.budgetDeferrals), slog.Time("available_at", now)))...)
	return true, nil
}

// withdrawSurplusAdmission ports _withdraw_surplus_admission verbatim: the
// exact inverse of admitUnitFromSurplus and nothing more (CHAOS-3465
// review, CRITICAL) -- available_at returns to its pre-promotion value,
// and every column any exhaustion predicate reads is left untouched, so
// from the unit's point of view this pass never happened. updated_at DOES
// still move: the row was written twice, and nothing keys off updated_at
// for a RETRYING unit (the staleness cutoff applies only to DISPATCHING).
//
// The CAS pins available_at to the promoted value, so if anything else
// moved the unit between the promotion and here, this leaves it alone and
// returns false.
func withdrawSurplusAdmission(ctx context.Context, tx pgx.Tx, logger *slog.Logger, syncRunID string, unit budgetUnit, promotedAvailableAt, priorAvailableAt, now time.Time) (bool, error) {
	if logger == nil {
		logger = slog.Default()
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET available_at = $2, updated_at = $3
WHERE id = $1::uuid AND status = $4 AND available_at = $5`,
		unit.id, priorAvailableAt, now, syncRunUnitStatusRetrying, promotedAvailableAt)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		logger.WarnContext(ctx, "dispatch_sync_run.budget_surplus_withdrawal_lost_race",
			attrsToAny(append(unitLogAttrs(syncRunID, unit), slog.Time("prior_available_at", priorAvailableAt)))...)
		return false, nil
	}
	logger.InfoContext(ctx, "dispatch_sync_run.budget_surplus_withdrawn",
		attrsToAny(append(unitLogAttrs(syncRunID, unit),
			slog.String("reason", "cooldown_landed_after_admission"),
			slog.Time("restored_available_at", priorAvailableAt),
			// Proof, in the log line itself, that the episode survived the
			// round trip -- this is the value the CRITICAL finding zeroed.
			slog.Int("budget_deferrals", unit.budgetDeferrals),
		))...)
	return true, nil
}

// attrsToAny flattens []slog.Attr into the ...any form InfoContext/WarnContext
// accept, so every log call site above can build its attribute list once
// with slog.Attr's type safety instead of hand-interleaving key/value pairs.
func attrsToAny(attrs []slog.Attr) []any {
	args := make([]any, len(attrs))
	for i, attr := range attrs {
		args[i] = attr
	}
	return args
}

// observationToAnyArgs turns one accumulated observation map (built for
// admitted-batch reporting) into slog attributes for its own log line.
func observationToAnyArgs(observation map[string]any) []any {
	args := make([]any, 0, len(observation)*2)
	for key, value := range observation {
		args = append(args, key, value)
	}
	return args
}
