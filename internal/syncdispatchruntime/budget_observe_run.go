package syncdispatchruntime

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// observeRun ports BudgetGuard.observe_run verbatim: the shadow/dry-run
// telemetry pass -- what would this pass's candidates do against the
// DRY-RUN limit config, using a purely LOCAL running total seeded from
// nothing (never activeBudgetConsumption's real-world snapshot, and never
// enforceRun's own consumedByBucket). This function is READ-ONLY: it never
// writes to sync_run_units, never acquires an advisory lock, and its
// return value -- []map[string]any observations -- is not a shape
// enforceRun's admission path can consume even by accident. That is
// deliberate and structural, not a convention to remember: there is no
// consumedByBucket, no candidateUnits, no estimatesByUnit in this
// function's signature or return type for a caller to accidentally wire
// into the real admission flow.
//
// Ruled to be ported (not skipped) because it is production-reachable --
// dispatch_sync_run.py calls this unconditionally on its own hot path, and
// while its own return value is discarded there, the
// dispatch_sync_run.budget_guard_dry_run log lines it emits are the
// operator-visible shadow-measurement stream CHAOS-4189 (deriving budget
// caps from concurrent walk width) will need to evaluate candidate limits
// before enforcing them. The message name and field set below are
// parity-pinned against Python's exactly for that reason: operator
// dashboards may already parse dispatch_sync_run.budget_guard_dry_run.
func observeRun(
	ctx context.Context, tx pgx.Tx, bridge budgetEstimator, logger *slog.Logger,
	orgID, syncRunID string, cappedUnitIDs map[string]bool, now time.Time,
) ([]map[string]any, error) {
	if logger == nil {
		logger = slog.Default()
	}

	units, err := dispatchCandidateUnits(ctx, tx, syncRunID, cappedUnitIDs, now)
	if err != nil {
		return nil, err
	}
	if len(units) == 0 {
		return nil, nil
	}

	// The DRY-RUN limit source, resolved exactly as Python does: a
	// SEPARATE env var from enforceRun's real limits (SYNC_BUDGET_
	// DRY_RUN_BUCKET_LIMITS / SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT /
	// SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS, not SYNC_BUDGET_BUCKET_LIMITS /
	// SYNC_BUDGET_DEFAULT_LIMIT / SYNC_BUDGET_DEFERRAL_SECONDS) -- a shadow
	// config an operator can tune independently of what is actually
	// enforced, which is the entire point of a dry-run channel.
	limits := budgetLimits()
	defaultLimit := budgetEnvInt("SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT", 1_000_000)
	deferralSeconds := budgetEnvInt("SYNC_BUDGET_DRY_RUN_DEFERRAL_SECONDS", 60)
	consumedByBucket := map[string]int{}
	var observations []map[string]any

	unitIDs := make([]string, len(units))
	for i, unit := range units {
		unitIDs[i] = unit.id
	}
	estimatesByUnit, bridgeErr := bridge.DispatchBudgetEstimate(ctx, orgID, syncRunID, unitIDs)
	if bridgeErr != nil {
		// Same choice as enforceRun's own whole-batch-failure handling:
		// Python's per-unit try/except has no precedent for a batched
		// failure, so every unit in this pass degrades to "no estimate"
		// and is logged individually under Python's own per-unit failure
		// message name.
		for _, unit := range units {
			logger.WarnContext(ctx, "dispatch_sync_run.budget_guard_dry_run_failed",
				attrsToAny(append(unitLogAttrs(syncRunID, unit), slog.String("error", bridgeErr.Error())))...)
		}
		return nil, nil
	}

	for _, unit := range units {
		logFields := unitLogFields(syncRunID, unit)
		for _, estimate := range estimatesByUnit[unit.id] {
			// recordConsumption=true (Python's own default for this call
			// site): unlike enforceRun's dry pre-check against the REAL
			// consumedByBucket, this pass's running total is entirely its
			// own simulation -- charge it as it goes, since there is no
			// second, real admission decision downstream to keep this
			// number pristine for.
			observation := observeEstimate(estimate, logFields, consumedByBucket, limits, defaultLimit, now, deferralSeconds, true)
			observations = append(observations, observation)
			logger.InfoContext(ctx, "dispatch_sync_run.budget_guard_dry_run", observationToAnyArgs(observation)...)
		}
	}
	return observations, nil
}
