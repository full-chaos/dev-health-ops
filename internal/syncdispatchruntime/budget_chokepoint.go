package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// terminalOutcome ports TerminalOutcome verbatim: what a terminalization
// attempt actually did. REFUSED and CAS_LOST were previously both a bare
// nil in an earlier design, collapsing "the verdict's evidence doesn't
// license this claim" into "the row moved on" -- a refusal silently
// resetting the aggregate clock is exactly the invariant violation the
// separate REFUSED case exists to prevent from happening quietly.
type terminalOutcome string

const (
	terminalOutcomeTerminalized terminalOutcome = "terminalized"
	terminalOutcomeRefused      terminalOutcome = "refused"
	terminalOutcomeCASLost      terminalOutcome = "cas_lost"
)

// terminalDecision ports TerminalDecision verbatim.
type terminalDecision struct {
	outcome terminalOutcome
	at      time.Time
}

// budgetUnfitness ports BudgetUnfitness verbatim: why a unit does not fit
// its bucket, measured against a STABLE baseline. permanent is the strong
// case -- the estimate alone exceeds the whole bucket limit, so no amount
// of other work finishing can ever make room; only that case may claim
// "can never be admitted".
type budgetUnfitness struct {
	budgetKey      string
	estimatedUnits int
	budgetLimit    int
	durableUnits   int
	permanent      bool
}

// terminalVerdict ports TerminalVerdict verbatim: a proposal to terminally
// fail a unit, carrying the evidence it rests on. episode names the
// episode the verdict ASSERTS as the cause ("" for the aggregate backstop,
// which asserts no single cause). evidence becomes the persisted result
// payload.
type terminalVerdict struct {
	errorCategory string
	errorText     string
	evidence      map[string]any
	episode       string // "" means no episode asserted (the aggregate backstop)
	fitness       *budgetUnfitness
}

// episodeEvidence ports _EPISODE_EVIDENCE verbatim: which error categories
// license an assertion that a given episode is this unit's cause.
var episodeEvidence = map[string]map[string]bool{
	"rate_limit": rateLimitEpisodeErrorCategories,
	"budget":     budgetEpisodeErrorCategories,
}

// fitnessBasedCategories ports _FITNESS_BASED_CATEGORIES verbatim.
var fitnessBasedCategories = map[string]bool{
	budgetDeferralExhaustedCategory: true,
}

// claimLicences ports _CLAIM_LICENCES verbatim: an error text phrase may
// only appear when the evidence key it maps to is literally true.
var claimLicences = map[string]string{
	"can never be admitted": "permanently_oversized",
	"alternated":            "episodes_alternated",
	"kept changing":         "episodes_alternated",
}

// assertVerdictWellformed ports _assert_verdict_wellformed verbatim. A
// violation here is a DEFECT IN THIS MODULE, not a runtime data problem
// (Python: TerminalVerdictError, an AssertionError subclass) -- panic() is
// the Go idiom for exactly that class of invariant violation, verified
// safe for a River job handler: a panic here is recovered by
// internal/jobruntime.Adapter.execute as an ordinary retryable job
// failure, not a worker-process crash (CHAOS-4175,
// TestHandlerPanicIsRecoveredAsAJobFailureAndTheWorkerProcessSurvives).
func assertVerdictWellformed(verdict terminalVerdict) {
	if verdict.episode != "" {
		if _, known := episodeEvidence[verdict.episode]; !known {
			panic(fmt.Sprintf("verdict %q names unknown episode %q; register it in episodeEvidence so the evidence check can be applied to it",
				verdict.errorCategory, verdict.episode))
		}
	}
	if fitnessBasedCategories[verdict.errorCategory] && verdict.fitness == nil {
		panic(fmt.Sprintf("verdict %q makes a fitness claim but carries no budgetUnfitness measured against the durable baseline", verdict.errorCategory))
	}
	if strings.TrimSpace(verdict.errorText) == "" {
		panic(fmt.Sprintf("verdict %q has no error text; an unexplained terminal failure is the state this ticket exists to remove", verdict.errorCategory))
	}
	for phrase, evidenceKey := range claimLicences {
		if strings.Contains(verdict.errorText, phrase) {
			licensed, _ := verdict.evidence[evidenceKey].(bool)
			if !licensed {
				panic(fmt.Sprintf("verdict %q claims %q but its evidence does not license it (%s=%v); an error text may only assert what its own evidence supports",
					verdict.errorCategory, phrase, evidenceKey, verdict.evidence[evidenceKey]))
			}
		}
	}
}

// terminalizeUnit ports _terminalize_unit verbatim -- THE single place a
// sync unit is terminally failed by a deferral-exhaustion decision. Every
// terminal deferral stamp in this family routes through this function; no
// other code path may set status=FAILED for a budget/rate-limit/aggregate
// exhaustion reason.
//
// Error classification: a query/exec failure here is a bare execution
// failure, no different from any other write in this family -- returned
// raw, propagated to the caller, ultimately to River, which retries the
// whole dispatch pass (see the closure-sweep note in native_dispatch_sync_run.go
// once that lands: every write in this family is an idempotent CAS, so a
// whole-pass retry is always safe).
func terminalizeUnit(
	ctx context.Context, tx pgx.Tx, logger *slog.Logger, unit budgetUnit, verdict terminalVerdict, now time.Time,
) (terminalDecision, error) {
	if logger == nil {
		logger = slog.Default()
	}
	assertVerdictWellformed(verdict)
	if verdict.episode != "" {
		licensed := episodeEvidence[verdict.episode]
		lastCategory := unit.lastErrorCategory()
		if !licensed[lastCategory] {
			logger.WarnContext(ctx, "dispatch_sync_run.terminal_verdict_refused",
				slog.String("unit_id", unit.id), slog.String("error_category", verdict.errorCategory),
				slog.String("asserted_episode", verdict.episode), slog.String("unit_last_error_category", lastCategory))
			return terminalDecision{outcome: terminalOutcomeRefused}, nil
		}
	}
	evidence := make(map[string]any, len(verdict.evidence)+1)
	for key, value := range verdict.evidence {
		evidence[key] = value
	}
	evidence["error_category"] = verdict.errorCategory
	resultJSON, err := json.Marshal(evidence)
	if err != nil {
		return terminalDecision{}, fmt.Errorf("marshal terminal verdict evidence: %w", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = $2, error = $3, result = $4::json, lease_owner = NULL, lease_expires_at = NULL,
    last_heartbeat_at = $5, updated_at = $5
WHERE id = $1::uuid
  AND (
        status = $6
     OR (status = $7 AND available_at IS NOT NULL AND available_at <= $5)
     OR (status = $8 AND updated_at <= $9)
      )`,
		unit.id, syncRunUnitStatusFailed, verdict.errorText, resultJSON, now,
		syncRunUnitStatusPlanned, syncRunUnitStatusRetrying, syncRunUnitStatusDispatching, staleDispatchCutoff(now))
	if err != nil {
		return terminalDecision{}, err
	}
	if tag.RowsAffected() == 0 {
		return terminalDecision{outcome: terminalOutcomeCASLost}, nil
	}
	// CHAOS-4586: this is THE single budget/rate-limit/aggregate-deferral-
	// exhaustion terminal-fail path (three call sites: terminalizeRateLimitExhausted,
	// terminalizeDeferralTotalExhausted, terminalizeBudgetExhausted), and the run
	// commonly stays active with other units still dispatching/running --
	// recompute the parent sync_runs rollup in the SAME transaction, same
	// lock-first order as the normal per-unit path (CHAOS-4559), so
	// sync_runs.failed_units never lags a budget/rate-limit terminalization.
	if _, _, _, err := bumpSyncRunRollup(ctx, tx, unit.syncRunID); err != nil {
		return terminalDecision{}, err
	}
	recordRollupBump(ctx, rollupPathBudgetExhausted)
	logger.WarnContext(ctx, "dispatch_sync_run.unit_terminalized",
		slog.String("unit_id", unit.id), slog.String("error_category", verdict.errorCategory), slog.String("error", verdict.errorText))
	return terminalDecision{outcome: terminalOutcomeTerminalized, at: now}, nil
}

// settleOrSkip ports _settle_or_skip verbatim: for call sites where the
// unit is NOT held by an active cooldown, a refusal and a lost race have
// the same safe consequence -- leave the unit to the pass's normal
// handling. ok=false means "skip" (Python's bare None).
func settleOrSkip(decision terminalDecision) (at time.Time, terminalized bool, ok bool) {
	if decision.outcome == terminalOutcomeTerminalized {
		return decision.at, true, true
	}
	return time.Time{}, false, false
}

// settleResult ports the three-way return _settle_terminal_decision needs
// (Python: tuple[datetime, bool] | None | the _CARRY_ON sentinel). Modeled
// as an explicit enum rather than Python's sentinel-object trick, since
// Go's type system does not need one to distinguish three shapes.
type settleResult int

const (
	settleWritten settleResult = iota
	settleCASLost
	settleCarryOn
)

// settleTerminalDecision ports _settle_terminal_decision verbatim: keeps
// REFUSED distinct from CAS_LOST, unlike settleOrSkip. The cooldown path
// needs this because a REFUSED verdict there must NOT fall through to a
// claim while a live cooldown still blocks the unit -- it must try the
// next check in the ordering instead (settleCarryOn), never be silently
// dropped (settleCASLost).
func settleTerminalDecision(decision terminalDecision) (time.Time, bool, settleResult) {
	switch decision.outcome {
	case terminalOutcomeTerminalized:
		return decision.at, true, settleWritten
	case terminalOutcomeCASLost:
		return time.Time{}, false, settleCASLost
	default:
		return time.Time{}, false, settleCarryOn
	}
}

// terminalizeRateLimitExhausted ports _terminalize_rate_limit_exhausted
// verbatim: propose terminal failure for a spent RATE-LIMIT episode
// (CHAOS-2742).
func terminalizeRateLimitExhausted(ctx context.Context, tx pgx.Tx, logger *slog.Logger, unit budgetUnit, now time.Time) (terminalDecision, error) {
	return terminalizeUnit(ctx, tx, logger, unit, terminalVerdict{
		errorCategory: rateLimitCooldownExhaustedCategory,
		errorText:     "rate limit cooldown deferral budget exhausted",
		evidence:      map[string]any{"rate_limit_deferrals": unit.rateLimitDeferrals},
		episode:       "rate_limit",
	}, now)
}

// terminalizeDeferralTotalExhausted ports _terminalize_deferral_total_exhausted
// verbatim: the aggregate backstop for a unit that stayed blocked without
// any single-cause cap being reached. Names NO episode -- this decision
// deliberately asserts that no single cause explains the block.
func terminalizeDeferralTotalExhausted(ctx context.Context, tx pgx.Tx, logger *slog.Logger, unit budgetUnit, now time.Time) (terminalDecision, error) {
	budgetDeferrals := unit.budgetDeferrals
	rateLimitDeferrals := unit.rateLimitDeferrals
	var blockedSeconds int
	if unit.firstBlockedAt != nil {
		blockedSeconds = int(now.Sub(*unit.firstBlockedAt).Seconds())
	}
	alternated := budgetDeferrals > 0 && rateLimitDeferrals > 0
	cause := "It stayed blocked without any single-cause cap being reached"
	if alternated {
		cause = "The blocking reason alternated between sync budget admission and provider rate limiting, so no single-cause cap applied"
	}
	errorText := fmt.Sprintf(
		"sync unit blocked for %dh without ever running; last blocked by %s "+
			"(budget deferrals: %d, rate-limit deferrals: %d). %s. Remedies: run a scoped backfill "+
			"over a narrower window, raise this bucket's cap via SYNC_BUDGET_BUCKET_LIMITS, or reduce "+
			"concurrent load on the provider so cooldowns stop recurring.",
		blockedSeconds/3600, unit.lastEpisodeKind(), budgetDeferrals, rateLimitDeferrals, cause)
	var firstBlockedAtValue any
	if unit.firstBlockedAt != nil {
		firstBlockedAtValue = unit.firstBlockedAt.Format(time.RFC3339Nano)
	}
	return terminalizeUnit(ctx, tx, logger, unit, terminalVerdict{
		errorCategory: deferralExhaustedCategory,
		errorText:     errorText,
		evidence: map[string]any{
			"budget_deferrals":     budgetDeferrals,
			"rate_limit_deferrals": rateLimitDeferrals,
			"first_blocked_at":     firstBlockedAtValue,
			"blocked_seconds":      blockedSeconds,
			"last_episode":         unit.lastEpisodeKind(),
			"episodes_alternated":  alternated,
		},
	}, now)
}

// blockingBudgetObservation ports _blocking_budget_observation verbatim:
// the observation from THIS pass that actually blocked the unit -- falls
// back to the largest estimate when no entry carries an explicit
// would-defer decision.
func blockingBudgetObservation(observations []map[string]any) map[string]any {
	if len(observations) == 0 {
		return nil
	}
	blockingDecisions := map[string]bool{"would_defer": true, "deferred": true, "exhausted": true}
	candidates := observations
	var blocking []map[string]any
	for _, entry := range observations {
		decision, _ := entry["decision"].(string)
		if blockingDecisions[decision] {
			blocking = append(blocking, entry)
		}
	}
	if len(blocking) > 0 {
		candidates = blocking
	}
	best := candidates[0]
	bestUnits := estimatedUnitsOf(best)
	for _, entry := range candidates[1:] {
		if units := estimatedUnitsOf(entry); units > bestUnits {
			best, bestUnits = entry, units
		}
	}
	return best
}

func estimatedUnitsOf(entry map[string]any) int {
	switch value := entry["estimated_units"].(type) {
	case int:
		return value
	case float64:
		return int(value)
	default:
		return 0
	}
}

// budgetExhaustionErrorText ports _budget_exhaustion_error_text verbatim:
// operator-actionable failure text naming WHAT could not fit, BY HOW MUCH,
// over WHAT window -- the "can never be admitted" claim appears only when
// it is literally true (unfitness.permanent).
func budgetExhaustionErrorText(unit budgetUnit, deferrals int, unfitness budgetUnfitness) string {
	spanDays := unit.windowSpanDays()
	head := fmt.Sprintf(
		"sync budget deferral exhausted after %d deferrals: dataset '%s' estimates %d units against bucket '%s' whose cap is %d, over a %d-day window",
		deferrals, unit.datasetKey, unfitness.estimatedUnits, unfitness.budgetKey, unfitness.budgetLimit, spanDays)
	if unfitness.permanent {
		return head + ", so it can never be admitted and was re-deferred instead of running"
	}
	return head + fmt.Sprintf(
		", and %d units of that cap are held by sync work already running. The contention has not cleared for the whole deferral budget, so the unit was re-deferred instead of running",
		unfitness.durableUnits)
}

// terminalizeBudgetExhausted ports _terminalize_budget_exhausted verbatim:
// propose terminal failure for a spent BUDGET episode whose misfit holds
// against the durable baseline (CHAOS-3412).
func terminalizeBudgetExhausted(
	ctx context.Context, tx pgx.Tx, logger *slog.Logger, unit budgetUnit, now time.Time,
	observations []map[string]any, unfitness budgetUnfitness,
) (terminalDecision, error) {
	deferrals := unit.budgetDeferrals
	var budgetGuardEvidence []map[string]any
	if blocking := blockingBudgetObservation(observations); blocking != nil {
		budgetGuardEvidence = []map[string]any{blocking}
	}
	var firstDeferredAtValue any
	if unit.budgetFirstDeferredAt != nil {
		firstDeferredAtValue = unit.budgetFirstDeferredAt.Format(time.RFC3339Nano)
	}
	return terminalizeUnit(ctx, tx, logger, unit, terminalVerdict{
		errorCategory: budgetDeferralExhaustedCategory,
		errorText:     budgetExhaustionErrorText(unit, deferrals, unfitness),
		evidence: map[string]any{
			"budget_deferrals":         deferrals,
			"budget_key":               unfitness.budgetKey,
			"estimated_units":          unfitness.estimatedUnits,
			"budget_limit":             unfitness.budgetLimit,
			"durable_units":            unfitness.durableUnits,
			"permanently_oversized":    unfitness.permanent,
			"budget_first_deferred_at": firstDeferredAtValue,
			"budget_guard":             budgetGuardEvidence,
		},
		episode: "budget",
		fitness: &unfitness,
	}, now)
}

// baselineUnfitness ports _baseline_unfitness verbatim: the worst way this
// unit fails to fit against the DURABLE baseline (never against capacity
// taken by this pass's own optional admissions), or nil if it fits --
// meaning any deferral it just took was caused by contention with THIS
// pass's own admissions and must never be grounds for terminalizing it.
// Worst = a permanent misfit ahead of a contention misfit, then the
// largest estimate.
func baselineUnfitness(estimates []budgetEstimate, baselineConsumption map[string]int, limits map[string]int, defaultLimit int) *budgetUnfitness {
	var worst *budgetUnfitness
	for _, estimate := range estimates {
		key := budgetKeyFor(estimate.Bucket, estimate.RouteFamily)
		limit := limitForBucket(estimate.Bucket, estimate.RouteFamily, limits, defaultLimit)
		durable := baselineConsumption[key]
		if durable+estimate.EstimatedUnits <= limit {
			continue
		}
		candidate := budgetUnfitness{
			budgetKey:      key,
			estimatedUnits: estimate.EstimatedUnits,
			budgetLimit:    limit,
			durableUnits:   durable,
			permanent:      estimate.EstimatedUnits > limit,
		}
		if worst == nil || worseUnfitness(candidate, *worst) {
			worst = &candidate
		}
	}
	return worst
}

// worseUnfitness ports the ordering _baseline_unfitness's max() comparison
// implies: a permanent misfit outranks a contention misfit; within the
// same kind, the larger estimate wins.
func worseUnfitness(candidate, current budgetUnfitness) bool {
	if candidate.permanent != current.permanent {
		return candidate.permanent
	}
	return candidate.estimatedUnits > current.estimatedUnits
}
