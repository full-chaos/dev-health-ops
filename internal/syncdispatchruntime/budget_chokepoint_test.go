package syncdispatchruntime

import (
	"strings"
	"testing"
	"time"
)

func wellformedVerdict() terminalVerdict {
	return terminalVerdict{
		errorCategory: rateLimitCooldownExhaustedCategory,
		errorText:     "rate limit cooldown deferral budget exhausted",
		evidence:      map[string]any{"rate_limit_deferrals": 5},
		episode:       "rate_limit",
	}
}

func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatalf("%s: want panic, got none", name)
		}
	}()
	fn()
}

func TestAssertVerdictWellformed(t *testing.T) {
	t.Run("well-formed verdict does not panic", func(t *testing.T) {
		assertVerdictWellformed(wellformedVerdict())
	})

	t.Run("unknown episode panics", func(t *testing.T) {
		mustPanic(t, "unknown episode", func() {
			verdict := wellformedVerdict()
			verdict.episode = "not_a_real_episode"
			assertVerdictWellformed(verdict)
		})
	})

	t.Run("fitness-based category with no fitness panics", func(t *testing.T) {
		mustPanic(t, "missing fitness", func() {
			assertVerdictWellformed(terminalVerdict{
				errorCategory: budgetDeferralExhaustedCategory,
				errorText:     "some text",
				episode:       "budget",
				// fitness deliberately nil
			})
		})
	})

	t.Run("empty error text panics", func(t *testing.T) {
		mustPanic(t, "empty error text", func() {
			verdict := wellformedVerdict()
			verdict.errorText = "   "
			assertVerdictWellformed(verdict)
		})
	})

	t.Run("unlicensed claim phrase panics", func(t *testing.T) {
		mustPanic(t, "unlicensed claim", func() {
			verdict := wellformedVerdict()
			verdict.errorText = "this unit can never be admitted"
			// evidence does NOT set permanently_oversized=true
			assertVerdictWellformed(verdict)
		})
	})

	t.Run("licensed claim phrase does not panic", func(t *testing.T) {
		verdict := wellformedVerdict()
		verdict.errorText = "this unit can never be admitted"
		verdict.evidence = map[string]any{"permanently_oversized": true}
		assertVerdictWellformed(verdict)
	})
}

func TestSettleOrSkip(t *testing.T) {
	now := time.Now()
	if at, terminalized, ok := settleOrSkip(terminalDecision{outcome: terminalOutcomeTerminalized, at: now}); !ok || !terminalized || !at.Equal(now) {
		t.Fatalf("terminalized: at=%s terminalized=%v ok=%v", at, terminalized, ok)
	}
	if _, _, ok := settleOrSkip(terminalDecision{outcome: terminalOutcomeCASLost}); ok {
		t.Fatal("cas_lost: want ok=false")
	}
	if _, _, ok := settleOrSkip(terminalDecision{outcome: terminalOutcomeRefused}); ok {
		t.Fatal("refused: want ok=false (settleOrSkip collapses refused and cas_lost)")
	}
}

func TestSettleTerminalDecision(t *testing.T) {
	now := time.Now()
	if at, terminalized, result := settleTerminalDecision(terminalDecision{outcome: terminalOutcomeTerminalized, at: now}); result != settleWritten || !terminalized || !at.Equal(now) {
		t.Fatalf("terminalized: at=%s terminalized=%v result=%v", at, terminalized, result)
	}
	if _, _, result := settleTerminalDecision(terminalDecision{outcome: terminalOutcomeCASLost}); result != settleCASLost {
		t.Fatalf("cas_lost: result=%v want settleCASLost", result)
	}
	if _, _, result := settleTerminalDecision(terminalDecision{outcome: terminalOutcomeRefused}); result != settleCarryOn {
		t.Fatalf("refused: result=%v want settleCarryOn -- refused and cas_lost MUST stay distinct here", result)
	}
}

func TestBaselineUnfitness(t *testing.T) {
	bucket := budgetEstimateBucket{Provider: "github", Dimension: "rest_core"}
	limits := map[string]int{}

	t.Run("nil when everything fits", func(t *testing.T) {
		estimates := []budgetEstimate{{Bucket: bucket, RouteFamily: "work-items", EstimatedUnits: 10}}
		if got := baselineUnfitness(estimates, map[string]int{}, limits, 100); got != nil {
			t.Fatalf("got=%+v want nil", got)
		}
	})

	t.Run("permanent misfit ranks ahead of a contention misfit even with a SMALLER raw estimate", func(t *testing.T) {
		// Deliberately gives the CONTENTION misfit the LARGER raw
		// estimated_units (80 vs 20), so a naive "just take the bigger
		// estimate" comparison would pick the wrong one -- only checking
		// `permanent` FIRST (matching Python's
		// rank = (permanent, estimated_units) tuple comparison) selects
		// the right candidate here.
		estimates := []budgetEstimate{
			// Contention misfit: 80 alone fits under the 100 default limit,
			// but durable(50)+80=130 exceeds it -- not permanent.
			{Bucket: bucket, RouteFamily: "contention", EstimatedUnits: 80},
			// Permanent misfit: this bucket's OWN limit (10, via override)
			// is exceeded by the 20-unit estimate ALONE -- permanent, even
			// though 20 < 80.
			{Bucket: bucket, RouteFamily: "permanent", EstimatedUnits: 20},
		}
		baseline := map[string]int{budgetKeyFor(bucket, "contention"): 50}
		overrides := map[string]int{budgetKeyFor(bucket, "permanent"): 10}
		got := baselineUnfitness(estimates, baseline, overrides, 100)
		if got == nil || !got.permanent || got.budgetKey != budgetKeyFor(bucket, "permanent") {
			t.Fatalf("got=%+v want the permanent misfit (20 units > its own 10-unit limit), ranked ahead of the larger 80-unit contention misfit", got)
		}
	})

	t.Run("within the same kind, the larger estimate wins", func(t *testing.T) {
		estimates := []budgetEstimate{
			{Bucket: bucket, RouteFamily: "small", EstimatedUnits: 150},
			{Bucket: bucket, RouteFamily: "large", EstimatedUnits: 500},
		}
		got := baselineUnfitness(estimates, map[string]int{}, limits, 100)
		if got == nil || got.budgetKey != budgetKeyFor(bucket, "large") {
			t.Fatalf("got=%+v want the larger (500-unit) permanent misfit", got)
		}
	})
}

func TestBudgetExhaustionErrorText(t *testing.T) {
	unit := budgetUnit{datasetKey: "commits"}

	permanent := budgetExhaustionErrorText(unit, 10, budgetUnfitness{
		budgetKey: "github:rest_core", estimatedUnits: 500, budgetLimit: 100, permanent: true,
	})
	if !strings.Contains(permanent, "can never be admitted") {
		t.Fatalf("permanent text=%q, want the 'can never be admitted' claim", permanent)
	}

	contention := budgetExhaustionErrorText(unit, 10, budgetUnfitness{
		budgetKey: "github:rest_core", estimatedUnits: 50, budgetLimit: 100, durableUnits: 80, permanent: false,
	})
	if strings.Contains(contention, "can never be admitted") {
		t.Fatalf("contention text=%q must NOT claim permanence -- that claim requires unfitness.permanent", contention)
	}
	if !strings.Contains(contention, "already running") {
		t.Fatalf("contention text=%q, want it to name durable consumption as the cause", contention)
	}
}

func TestBlockingBudgetObservation(t *testing.T) {
	if got := blockingBudgetObservation(nil); got != nil {
		t.Fatalf("empty input: got=%+v want nil", got)
	}
	observations := []map[string]any{
		{"decision": "allowed", "estimated_units": 999}, // not a blocking decision, must be ignored
		{"decision": "would_defer", "estimated_units": 10},
		{"decision": "would_defer", "estimated_units": 50},
	}
	got := blockingBudgetObservation(observations)
	if got == nil || estimatedUnitsOf(got) != 50 {
		t.Fatalf("got=%+v want the largest would_defer entry (50 units), ignoring the non-blocking 999-unit entry", got)
	}
}
