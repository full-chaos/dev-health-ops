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
// today. Go now owns short-lived HTTP reservation contention, but that is a
// distinct provider_budget_contention episode: it clears the intrinsic pair
// and River snoozes the job. It still does not estimate a unit or decide
// whether it fits the planner budget, so there is no production Go intrinsic-
// budget producer to drive here instead of this mirror.
//
// "No admission" is too broad and is narrowed here deliberately: Go owns no
// unit-DISPATCH admission. It does own PLAN-TIME validation of the same
// limits -- internal/scheduler/sync/materializer.go loads max_sync_units and
// rejects a plan whose unit count exceeds the total cap. That is a different
// decision at a different moment (before units exist, not before they are
// queued), and it reads none of the columns this predicate reads, which is
// why it changes nothing here. It is named so the narrower claim is the one
// on record.
//
// CHAOS-3465 re-tested that claim rather than inheriting it, because surplus
// retry is new budget-ADMISSION behaviour and admission is the thing Go would
// have to mirror if it owned any. It does not: the surplus phase lives
// entirely in sync/budget_guard.py, the Go scheduler mirrors Python's PLANNER
// (windows and the HEAVY ratchet) and not unit-DISPATCH admission, and the Go
// budget footprint is still exactly the two clears above -- providerfoundation
// /budget.go's SyncBudgetKey and PostgresBudgetLocker are prepared but have no
// caller outside tests, so they are not a third. The predicate this mirror
// tracks, _budget_deferral_exhausted, is byte-identical across that commit.
// So the CHAOS-3465 mirror is deliberately no Go behaviour change -- plus the
// two half_cleared_* cases below, and the surplus write-set probe in the pair
// file, which is what stops the new Python writers from invalidating this
// corpus unobserved.
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
		// A HALF-CLEARED episode: budget_deferrals back to zero while
		// budget_first_deferred_at survives. Clause 1 short-circuits only when
		// BOTH are empty, so this row falls through to the wall clock and the
		// stale stamp decides -- which is why Go's clears must zero the pair
		// in ONE statement, and why clearing "the counter" is not the same
		// thing as clearing the episode.
		//
		// The SQL-string guards already assert that both columns appear in
		// completeUnitSQL and in the reconciler's retry stamp. What they
		// cannot say is what a half-clear would COST, because they never ask
		// the predicate. These two cases do, and they are the only inputs in
		// the corpus pairing a zero counter with a live stamp: every
		// pre-existing zero-counter case has a nil stamp, so a Go mirror that
		// dropped clause 1's `&& FirstDeferredAt == nil` conjunct agreed with
		// Python on all of them. Verified by planting exactly that mutation by
		// hand: it survives the old corpus.
		//
		// Which of the two kills it, stated exactly rather than as "these
		// cases": only the PAST-the-wall-clock one. Under the mutant, Go
		// short-circuits on the zero counter and answers "not exhausted" while
		// live Python answers "exhausted" -- the disagreement. The
		// inside-the-wall-clock case AGREES with the mutant (both say "not
		// exhausted"), so it kills nothing on its own. It is the boundary
		// companion: it proves the past-the-cap case is decided by the elapsed
		// time and not merely by the stamp being present, which is what stops
		// the killer from passing for the wrong reason.
		{ID: "half_cleared_episode_past_the_wall_clock_is_still_terminalizable",
			Input: snapshot(0, wallClock+3600, budgetDeferredCategory)},
		{ID: "half_cleared_episode_inside_the_wall_clock_is_not",
			Input: snapshot(0, 60, budgetDeferredCategory)},
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
	// Request-reservation contention is a real Go budget-related stamp, but it
	// starts a distinct episode and clears the intrinsic pair. Build the exact
	// post-stamp field shape, with its category derived from production SQL, so
	// live Python proves it can never call that state intrinsic unfitness.
	cases = append(cases, oracleCase{
		ID: "go_stamp_provider_budget_contention_is_not_intrinsic_exhaustion",
		Input: snapshot(
			0, nil, stampedErrorCategory(t, deferForBudgetContentionSQL),
		),
	})
	return cases
}

// TestBudgetExhaustionPredicateMatchesLivePython is a PREDICATE-PARITY check,
// and the name says so because the earlier one ("generic oracle ... for budget
// exhaustion") read like a state-machine oracle and was reported as one.
//
// WHAT IT MEASURES. One side is the live, unmodified
// sync/budget_guard.py::_budget_deferral_exhausted; the other is the
// hand-written Go mirror above. Given the same unit-state snapshot, the two
// must return the same verdict. ONE case input is DERIVED from a production
// Go SQL constant (the single go_stamp_* case reads the stamped
// error_category straight out of releaseForRetrySQL), so that one case does
// bind a real Go statement to the live predicate -- the rest compare two
// predicates, not two producers. It said "two" while the loop below built
// one, which overstates how much of this corpus is anchored to real SQL.
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
//   - CHAOS-3465's surplus retry as BEHAVIOUR. No case drives it, and none
//     could: surplus writes only available_at, which this predicate does not
//     read, so it produces no new input here. What the pair does own is the
//     premise that makes that sentence true -- the pair file's
//     _assert_surplus_writes_leave_the_episode_alone reads the live surplus
//     write paths and fails this oracle if either ever touches an episode
//     column. tests/test_chaos_3465_budget_surplus_retry.py owns the phase's
//     own behaviour.
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
		"releaseForRetrySQL":          releaseForRetrySQL,
		"deferForBudgetContentionSQL": deferForBudgetContentionSQL,
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
