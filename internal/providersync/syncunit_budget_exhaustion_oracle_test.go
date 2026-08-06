package providersync

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// budgetExhaustionDecision is the typed row this pair compares. A concrete
// struct rather than a map, for the reason the comparator's own doc gives:
// a struct return type makes "silently expose only some of the decision" a
// type error instead of a runtime choice.
type budgetExhaustionDecision struct {
	Exhausted bool `json:"exhausted"`
}

// Defaults mirrored from sync/budget_guard.py (BUDGET_MAX_DEFERRALS_DEFAULT,
// BUDGET_DEFERRAL_WALL_CLOCK_SECONDS_DEFAULT). The oracle runs with both env
// vars unset on both sides, so these are the values in play.
const (
	budgetMaxDeferralsDefault           = 10
	budgetDeferralWallClockSecondsDefau = 6 * 60 * 60
	budgetDeferredCategory              = "budget_deferred"
)

// budgetEpisodeSnapshot is the unit state the predicate reads. These three
// values are exactly the ones Go's stamps write, which is why this pair is
// the Go side's business at all.
type budgetEpisodeSnapshot struct {
	Deferrals       int
	FirstDeferredAt *time.Time
	ErrorCategory   string
}

// budgetDeferralExhausted is a HAND-WRITTEN Go mirror of
// sync/budget_guard.py's _budget_deferral_exhausted, clause for clause and in
// the same order:
//
//  1. no budget history at all -> never exhausted;
//  2. defence in depth -- the unit's MOST RECENTLY recorded error_category
//     must be the budget guard's own deferral category;
//  3. the count cap;
//  4. the wall-clock cap, measured from the first deferral of THIS episode.
//
// It is NOT a production function driven under test, and calling it one would
// be the inaccurate coverage claim. Go owns no budget-admission decision
// today: the only budget references in this repository's non-test Go source
// are the two SQL clears (repository_postgres.go's completeUnitSQL and
// syncreconciler's markExpiredLeaseRetryingSQL), so there is no production Go
// producer to drive here instead of this mirror. Adding one would be unwired
// code pretending to be a second implementation.
//
// What the mirror IS for is the differential against the live Python
// authority: the pair below fails the moment the two readings of a shared
// sync_run_units row disagree, so when the Go runtime does take over unit
// admission it inherits an executable specification instead of a prose one.
func budgetDeferralExhausted(state budgetEpisodeSnapshot, now time.Time) bool {
	if state.Deferrals <= 0 && state.FirstDeferredAt == nil {
		return false
	}
	if state.ErrorCategory != budgetDeferredCategory {
		return false
	}
	if state.Deferrals >= budgetMaxDeferralsDefault {
		return true
	}
	if state.FirstDeferredAt == nil {
		return false
	}
	elapsed := now.Sub(state.FirstDeferredAt.UTC()).Seconds()
	return elapsed >= budgetDeferralWallClockSecondsDefau
}

func buildBudgetExhaustionDecisionForOracle(
	t *testing.T, input map[string]any,
) budgetExhaustionDecision {
	t.Helper()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	// The case Input map reaches the Go builder as authored (ints stay ints)
	// but reaches Python through JSON; accept both so the two paths cannot
	// disagree about a value that is numerically identical.
	number := func(raw any) int64 {
		switch value := raw.(type) {
		case int:
			return int64(value)
		case int64:
			return value
		case float64:
			return int64(value)
		default:
			t.Fatalf("case value %#v is not a number", raw)
			return 0
		}
	}
	state := budgetEpisodeSnapshot{Deferrals: int(number(input["budget_deferrals"]))}
	if raw := input["first_deferred_seconds_ago"]; raw != nil {
		at := now.Add(-time.Duration(number(raw)) * time.Second)
		state.FirstDeferredAt = &at
	}
	if raw := input["error_category"]; raw != nil {
		state.ErrorCategory = raw.(string)
	}
	return budgetExhaustionDecision{Exhausted: budgetDeferralExhausted(state, now)}
}

// stampedErrorCategory extracts the literal error_category a Go result stamp
// writes, DERIVED from the SQL constant rather than restated here. That is
// what makes the go_stamp_* cases below a guard on the real statements: edit
// the stamp's category and the case that runs through the live Python
// predicate changes with it.
func stampedErrorCategory(t *testing.T, sql string) string {
	t.Helper()
	matches := regexp.MustCompile(`'error_category',\s*'([a-z_]+)'`).FindStringSubmatch(sql)
	if len(matches) != 2 {
		t.Fatalf("could not derive the stamped error_category from:\n%s", sql)
	}
	return matches[1]
}

func budgetExhaustionOracleCases(t *testing.T) []oracleCase {
	t.Helper()
	snapshot := func(deferrals int, secondsAgo any, category any) map[string]any {
		return map[string]any{
			"budget_deferrals":           deferrals,
			"first_deferred_seconds_ago": secondsAgo,
			"error_category":             category,
		}
	}
	wallClock := budgetDeferralWallClockSecondsDefau
	cases := []oracleCase{
		// Clause 1: no history at all. This is the state Go's episode clears
		// produce, and the reason they are sufficient.
		{ID: "fresh_unit_is_never_exhausted", Input: snapshot(0, nil, nil)},
		{ID: "cleared_unit_with_a_budget_category_is_not_exhausted",
			Input: snapshot(0, nil, budgetDeferredCategory)},
		// The count cap, on both sides of the boundary.
		{ID: "one_below_the_count_cap",
			Input: snapshot(budgetMaxDeferralsDefault-1, 60, budgetDeferredCategory)},
		{ID: "exactly_at_the_count_cap",
			Input: snapshot(budgetMaxDeferralsDefault, 60, budgetDeferredCategory)},
		{ID: "past_the_count_cap",
			Input: snapshot(budgetMaxDeferralsDefault+3, 60, budgetDeferredCategory)},
		// The wall-clock cap, on both sides of the boundary.
		{ID: "one_second_below_the_wall_clock_cap",
			Input: snapshot(1, wallClock-1, budgetDeferredCategory)},
		{ID: "exactly_at_the_wall_clock_cap",
			Input: snapshot(1, wallClock, budgetDeferredCategory)},
		{ID: "past_the_wall_clock_cap",
			Input: snapshot(1, wallClock+3600, budgetDeferredCategory)},
		// A counter with no first-deferred stamp: history exists, but the
		// wall clock cannot be measured, so only the count cap can fire.
		{ID: "counter_without_a_first_deferred_stamp",
			Input: snapshot(2, nil, budgetDeferredCategory)},
		// Clause 2, the defence-in-depth gate, at its most dangerous input:
		// a unit far past BOTH caps whose category is not the budget one.
		{ID: "unrelated_category_is_refused_despite_both_caps",
			Input: snapshot(budgetMaxDeferralsDefault+5, wallClock*2, "rate_limit_deferred")},
		{ID: "absent_result_document_is_refused",
			Input: snapshot(budgetMaxDeferralsDefault+5, wallClock*2, nil)},
	}
	// The go_stamp_* cases: a unit deep into a budget episode that a Go stamp
	// then re-stamps. The stale counters survive (Go's stamps other than the
	// clears do not reset them), so the ONLY thing standing between this unit
	// and a spurious terminalization is the category the stamp wrote --
	// derived from the live SQL constants, not restated. If a stamp is ever
	// changed to PRESERVE the prior category, the derived value here becomes
	// 'budget_deferred' and the live Python predicate answers "exhausted" for
	// a unit that is merely being retried.
	for name, sql := range map[string]string{
		"release_for_retry": releaseForRetrySQL,
	} {
		cases = append(cases, oracleCase{
			ID: "go_stamp_" + name + "_is_not_terminalizable",
			Input: snapshot(
				budgetMaxDeferralsDefault+5, wallClock*2, stampedErrorCategory(t, sql),
			),
		})
	}
	return cases
}

// TestBudgetExhaustionPredicateMatchesLivePython is a PREDICATE-PARITY check,
// and the name says so because the earlier one ("generic oracle ... for budget
// exhaustion") read like a state-machine oracle and was reported as one.
//
// WHAT IT MEASURES. One side is the live, unmodified
// sync/budget_guard.py::_budget_deferral_exhausted; the other is the
// hand-written Go mirror above. Given the same unit-state snapshot, the two
// must return the same verdict. Two of the case inputs are DERIVED from the
// production Go SQL constants (the go_stamp_* cases read the stamped
// error_category straight out of releaseForRetrySQL), so those cases do bind
// real Go statements to the live predicate -- but the rest compare two
// predicates, not two producers.
//
// WHAT IT DOES NOT MEASURE, and what covers that instead:
//
//   - That the Go stamps write the episode columns this predicate reads. The
//     SQL-string guards own that: TestCompleteUnitSQLClearsEveryEpisodeColumn
//     and TestReleaseForRetrySQLDoesNotClearEpisodeState here,
//     TestExpiredLeaseRetryStampClearsEveryEpisodeColumn and
//     TestExpiredLeaseRetryStampLeavesTheAggregateClockAlone in
//     internal/syncreconciler.
//   - That those statements actually REACH that state in a database. The two
//     integration suites own that, against real PostgreSQL:
//     internal/providersync/budget_episode_integration_test.go and
//     internal/syncreconciler/lease_repair_integration_test.go (both
//     integration-tagged, both run by `ci/check_go.sh integration`).
//   - Real state-machine TRANSITIONS (deferral -> retry -> success). Nothing
//     here drives those; the integration suites above are the closest thing,
//     and Python's own tests/test_budget_guard_cooldown.py owns the admission
//     loop itself.
func TestBudgetExhaustionPredicateMatchesLivePython(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "syncunit/budget/exhaustion", budgetExhaustionOracleCases(t),
		buildBudgetExhaustionDecisionForOracle, nil,
	)
}

// TestGoStampCategoriesAreNotBudgetDeferred states the go_stamp_* cases'
// premise as its own assertion, so a stamp that started writing the budget
// category cannot slip through by making that case merely agree with Python
// (both sides would then say "exhausted" and the differential would pass
// while the behaviour was wrong).
func TestGoStampCategoriesAreNotBudgetDeferred(t *testing.T) {
	stamps := map[string]string{
		"releaseForRetrySQL": releaseForRetrySQL,
	}
	for name, sql := range stamps {
		if !strings.Contains(sql, "error_category") {
			t.Fatalf("%s no longer writes error_category; the go_stamp_* oracle "+
				"cases derive their input from it and have gone vacuous", name)
		}
		if got := stampedErrorCategory(t, sql); got == budgetDeferredCategory {
			t.Errorf("%s stamps %q. A Go stamp claiming the budget guard's own "+
				"deferral category makes stale budget counters "+
				"terminalization-eligible.", name, got)
		}
	}
}
