package syncdispatchruntime

import (
	"context"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"time"

	"github.com/jackc/pgx/v5"
)

// unitLogAttrs ports _unit_log_context verbatim: the identifying fields
// every BudgetGuard log line in this family carries.
func unitLogAttrs(syncRunID string, unit budgetUnit) []synclog.Attr {
	return []synclog.Attr{
		synclog.Run(synclog.ParseID(syncRunID)),
		synclog.Unit(synclog.ParseID(unit.id)),
		synclog.Source(synclog.ParseID(unit.sourceID)),
		synclog.Text(synclog.KeyDatasetKey, synclog.ParseLabel(unit.datasetKey)),
		synclog.Provider(synclog.ParseLabel(unit.provider)),
		synclog.Text(synclog.KeyCostClass, synclog.ParseLabel(unit.costClass)),
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
	ctx context.Context, tx pgx.Tx, logger *synclog.Logger, syncRunID string,
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
	admitted = map[string]time.Time{}

	for _, unit := range candidates {
		logAttrs := unitLogAttrs(syncRunID, unit)
		estimates := estimatesByUnit[unit.id]
		if len(estimates) == 0 {
			continue
		}

		if _, found := matchingCooldownExpiry(estimates, unit.orgID, unit.provider, unit.integrationID, familyCooldowns, dimensionCooldowns); found {
			logger.Info(ctx, synclog.MsgDispatchSyncRunBudgetSurplusSkipped, append(logAttrs, synclog.Text(synclog.KeyReason, synclog.LabelCooldownActive))...)
			continue
		}

		slotKey := dispatchBucket{orgID: unit.orgID, provider: unit.provider, costClass: unit.costClass}
		if slotHeadroom[slotKey] <= 0 {
			logger.Info(ctx, synclog.MsgDispatchSyncRunBudgetSurplusSkipped, append(logAttrs, synclog.Text(synclog.KeyReason, synclog.LabelNoConcurrencySlot))...)
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
			logger.Info(ctx, synclog.MsgDispatchSyncRunBudgetSurplusSkipped, append(logAttrs, synclog.Text(synclog.KeyReason, synclog.LabelInsufficientSurplus))...)
			continue
		}

		// Captured BEFORE the promotion overwrites it: this is what a later
		// withdrawal restores. The selection query requires available_at >
		// now, so a candidate always has one; if that ever stops holding,
		// skip rather than promote a unit we could not put back.
		if unit.availableAt == nil {
			logger.Warn(ctx, synclog.MsgDispatchSyncRunBudgetSurplusSkipped, append(logAttrs, synclog.Text(synclog.KeyReason, synclog.LabelNoPriorAvailableAt))...)
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
			logger.Info(ctx, synclog.MsgDispatchSyncRunBudgetSurplusAdmitted, observationAttrs(readObservation(observation))...)
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
func admitUnitFromSurplus(ctx context.Context, tx pgx.Tx, logger *synclog.Logger, syncRunID string, unit budgetUnit, now time.Time) (bool, error) {
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
	logger.Info(ctx, synclog.MsgDispatchSyncRunBudgetSurplusPulledForward, append(unitLogAttrs(syncRunID, unit), synclog.Count(synclog.KeyBudgetDeferrals, unit.budgetDeferrals), synclog.Instant(synclog.KeyAvailableAt, now))...)
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
func withdrawSurplusAdmission(ctx context.Context, tx pgx.Tx, logger *synclog.Logger, syncRunID string, unit budgetUnit, promotedAvailableAt, priorAvailableAt, now time.Time) (bool, error) {
	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET available_at = $2, updated_at = $3
WHERE id = $1::uuid AND status = $4 AND available_at = $5`,
		unit.id, priorAvailableAt, now, syncRunUnitStatusRetrying, promotedAvailableAt)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		logger.Warn(ctx, synclog.MsgDispatchSyncRunBudgetSurplusWithdrawalLostRace, append(unitLogAttrs(syncRunID, unit), synclog.Instant(synclog.KeyPriorAvailableAt, priorAvailableAt))...)
		return false, nil
	}
	logger.Info(ctx, synclog.MsgDispatchSyncRunBudgetSurplusWithdrawn, append(unitLogAttrs(syncRunID, unit),
		synclog.Text(synclog.KeyReason, synclog.LabelCooldownLandedAfterAdmission),
		synclog.Instant(synclog.KeyRestoredAvailableAt, priorAvailableAt),
		// Proof, in the log line itself, that the episode survived the
		// round trip -- this is the value the CRITICAL finding zeroed.
		synclog.Count(synclog.KeyBudgetDeferrals, unit.budgetDeferrals),
	)...)
	return true, nil
}

// observationFields is what a budget observation line logs, as typed values (no any reaches the logger).
type observationFields struct {
	runID, unitID, sourceID, provider                                                             string
	datasetKey, costClass, decision, budgetKey, confidence, routeFamily, availableAt, suggestedAt string
	estimatedUnits, projectedUnits, budgetLimit, budgetDeferrals                                  *int
	bucket                                                                                        *bucketFields
}

type bucketFields struct{ provider, orgID, host, credentialFingerprint, dimension string }

// readObservation reads one observation map (built by this package) by key name and type: an unknown key or a value of another
// type is dropped.
func readObservation(observation map[string]any) observationFields {
	text := func(key string) string { value, _ := observation[key].(string); return value }
	number := func(key string) *int {
		if value, ok := observation[key].(int); ok {
			return &value
		}
		return nil
	}
	fields := observationFields{
		runID: text("sync_run_id"), unitID: text("unit_id"), sourceID: text("source_id"), provider: text("provider"),
		datasetKey: text("dataset_key"), costClass: text("cost_class"), decision: text("decision"), budgetKey: text("budget_key"),
		confidence: text("confidence"), routeFamily: text("route_family"), availableAt: text("available_at"), suggestedAt: text("suggested_available_at"),
		estimatedUnits: number("estimated_units"), projectedUnits: number("projected_units"), budgetLimit: number("budget_limit"), budgetDeferrals: number("budget_deferrals"),
	}
	if bucket, ok := observation["bucket"].(map[string]any); ok {
		get := func(key string) string { value, _ := bucket[key].(string); return value }
		fields.bucket = &bucketFields{provider: get("provider"), orgID: get("org_id"), host: get("host"), credentialFingerprint: get("credential_fingerprint"), dimension: get("dimension")}
	}
	return fields
}

// observationAttrs turns typed observation fields into attributes: an empty text or an absent number is not logged.
func observationAttrs(fields observationFields) []synclog.Attr {
	var attrs []synclog.Attr
	for _, field := range []struct {
		value string
		build func(synclog.ID) synclog.Attr
	}{{fields.runID, synclog.Run}, {fields.unitID, synclog.Unit}, {fields.sourceID, synclog.Source}} {
		if field.value != "" {
			attrs = append(attrs, field.build(synclog.ParseID(field.value)))
		}
	}
	if fields.provider != "" {
		attrs = append(attrs, synclog.Provider(synclog.ParseLabel(fields.provider)))
	}
	for _, field := range []struct {
		key   synclog.Key
		value string
	}{{synclog.KeyDatasetKey, fields.datasetKey}, {synclog.KeyCostClass, fields.costClass}, {synclog.KeyDecision, fields.decision}, {synclog.KeyBudgetKey, fields.budgetKey},
		{synclog.KeyConfidence, fields.confidence}, {synclog.KeyRouteFamily, fields.routeFamily}, {synclog.KeyAvailableAt, fields.availableAt}, {synclog.KeySuggestedAvailableAt, fields.suggestedAt}} {
		if field.value != "" {
			attrs = append(attrs, synclog.Text(field.key, synclog.ParseLabel(field.value)))
		}
	}
	for _, field := range []struct {
		key   synclog.Key
		value *int
	}{{synclog.KeyEstimatedUnits, fields.estimatedUnits}, {synclog.KeyProjectedUnits, fields.projectedUnits}, {synclog.KeyBudgetLimit, fields.budgetLimit}, {synclog.KeyBudgetDeferrals, fields.budgetDeferrals}} {
		if field.value != nil {
			attrs = append(attrs, synclog.Count(field.key, *field.value))
		}
	}
	if fields.bucket != nil {
		var group []synclog.Attr
		for _, field := range []struct {
			key   synclog.Key
			value string
		}{{synclog.KeyProvider, fields.bucket.provider}, {synclog.KeyOrgId, fields.bucket.orgID}, {synclog.KeyHost, fields.bucket.host}, {synclog.KeyCredentialFingerprint, fields.bucket.credentialFingerprint}, {synclog.KeyDimension, fields.bucket.dimension}} {
			if field.value != "" {
				group = append(group, synclog.Text(field.key, synclog.ParseLabel(field.value)))
			}
		}
		attrs = append(attrs, synclog.Group(synclog.KeyBucket, group...))
	}
	return attrs
}
