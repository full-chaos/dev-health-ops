package providersync

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// episodeClearColumns is the per-episode + aggregate deferral bookkeeping a
// non-terminal SUCCESS stamp must reset. It is deliberately stated once, here,
// and asserted against BOTH Go clear sites (completeUnitSQL below,
// markExpiredLeaseRetryingSQL in internal/syncreconciler) so a third episode
// pair added later cannot be half-wired the way CHAOS-3412's budget pair was.
//
// Python derives its equivalent set from the live SyncRunUnit model
// (test_deferral_lifecycle_columns_are_classified_and_stamped_correctly). That
// derivation CANNOT see this SQL -- it parses Python source only -- so the Go
// half is owed here.
var episodeClearColumns = []string{
	"rate_limit_deferrals = 0",
	"rate_limit_first_seen_at = NULL",
	"budget_deferrals = 0",
	"budget_first_deferred_at = NULL",
}

// TestCompleteUnitSQLClearsEveryEpisodeColumn pins the SUCCESS stamp's episode
// symmetry (CHAOS-3427 obligation #1).
//
// A unit budget-deferred by Python and later completed by Go must come back
// with a ZEROED budget pair: SUCCESS proves the unit is not permanently
// oversized, so its next budget deferral has to start a fresh count and a
// fresh wall clock. Leaving the pair set hands
// `sync/budget_guard.py::_budget_deferral_exhausted` a resolved episode's
// counters and terminalizes a healthy unit.
func TestCompleteUnitSQLClearsEveryEpisodeColumn(t *testing.T) {
	for _, column := range episodeClearColumns {
		if !strings.Contains(completeUnitSQL, column) {
			t.Errorf("completeUnitSQL does not clear %q:\n%s", column, completeUnitSQL)
		}
	}
	// The AGGREGATE blocked clock stops at SUCCESS too -- the unit got
	// through, so it is not "going nowhere" any more. Python clears it at
	// exactly three sites (SUCCESS plus the two dispatch-claim UPDATEs); Go
	// owns the SUCCESS one.
	if !strings.Contains(completeUnitSQL, "first_blocked_at = NULL") {
		t.Errorf("completeUnitSQL does not clear first_blocked_at:\n%s", completeUnitSQL)
	}
}

// TestReleaseForRetrySQLDoesNotClearEpisodeState pins the deliberate ASYMMETRY
// (CHAOS-3427 obligation #1, second half). A release for retry is not an
// episode boundary: 'provider_unit_retryable' is outside
// _RATE_LIMIT_EPISODE_ERROR_CATEGORIES and outside the budget episode
// categories, so resetting a still-running episode here would erase real
// deferral history and make the exhaustion paths unreachable.
//
// This is a POSITIVE assertion of the current contract, not an oversight
// waiting to be fixed: "make it symmetric with SUCCESS" is the change this
// test exists to reject.
func TestReleaseForRetrySQLDoesNotClearEpisodeState(t *testing.T) {
	for _, column := range append([]string{"first_blocked_at"}, episodeClearColumns...) {
		name := strings.SplitN(column, " ", 2)[0]
		if strings.Contains(releaseForRetrySQL, name) {
			t.Errorf("releaseForRetrySQL touches %q; a release for retry is not an "+
				"episode boundary and must leave episode state alone:\n%s",
				name, releaseForRetrySQL)
		}
	}
}

func TestProviderBudgetContentionStampIsDistinctAndDurable(t *testing.T) {
	for _, required := range []string{
		"status = 'dispatching'",
		"attempts = GREATEST(unit.attempts - 1, 0)",
		"available_at = $4",
		"COALESCE(unit.result::jsonb, jsonb_build_object()) ||",
		"'error_category', 'provider_budget_contention'",
		"'provider_budget_contention_deferrals'",
		"first_blocked_at = COALESCE(unit.first_blocked_at, $3)",
		"last_retry_reason = 'provider_budget_contention'",
	} {
		if !strings.Contains(deferForBudgetContentionSQL, required) {
			t.Errorf("deferForBudgetContentionSQL missing %q:\n%s", required, deferForBudgetContentionSQL)
		}
	}
	if strings.Contains(deferForBudgetContentionSQL, "budget_deferrals = unit.budget_deferrals + 1") {
		t.Fatalf("request-slot contention was charged to the intrinsic planner budget episode:\n%s",
			deferForBudgetContentionSQL)
	}
	for _, column := range episodeClearColumns {
		name := strings.SplitN(column, " ", 2)[0]
		if strings.Contains(deferForBudgetContentionSQL, name+" =") {
			t.Fatalf("contention stamp changes existing %q episode state:\n%s",
				name, deferForBudgetContentionSQL)
		}
	}
	if preservingErrorCategory(deferForBudgetContentionSQL) {
		t.Fatalf("contention stamp preserves a prior category; Python could treat it as intrinsic unfitness:\n%s",
			deferForBudgetContentionSQL)
	}
}

func TestClaimHonorsDurableProviderBudgetContentionFence(t *testing.T) {
	for _, required := range []string{
		"unit.status = 'dispatching'",
		"unit.available_at IS NULL OR unit.available_at <= $3",
	} {
		if !strings.Contains(claimUnitSQL, required) {
			t.Errorf("claimUnitSQL missing %q; a restarted worker could bypass available_at:\n%s",
				required, claimUnitSQL)
		}
	}
}

// preservingErrorCategory reports whether a jsonb result stamp could leave a
// PRIOR result document's 'error_category' in place. See
// sqlshape.PreservesPriorResultCategory for the two forbidden shapes and why
// the merge-direction half is a parenthesis-aware scan rather than a regexp:
// the regexp it replaced spanned jsonb_build_object's arguments with `[^)]*`
// and therefore missed every preserving merge whose build_object nests a call
// -- which is the shape both lease-repair stamps have today.
//
// The detector is shared with internal/syncreconciler rather than duplicated,
// because the two hand-copied regexps carried the SAME false negative and a
// single fix to one copy would have left the other silently wrong.
func preservingErrorCategory(sql string) bool {
	return sqlshape.PreservesPriorResultCategory(sql)
}

// TestNoUnitStampPreservesAPriorErrorCategory is the FORBIDDEN PATTERN guard
// (CHAOS-3427 obligation #2).
//
// Python's `_budget_deferral_exhausted` has a defence-in-depth gate: it
// terminalizes only when the unit's most recently recorded
// `result.error_category` is the budget guard's own deferral category. That
// gate is what makes the Python-only shape of CHAOS-3412 safe today, because
// BOTH Go stamps OVERWRITE the category with their own cause
// (`worker_lost`, `provider_unit_retryable`) rather than preserving the
// previous one.
//
// A Go stamp that PRESERVED a prior error_category -- e.g. a jsonb merge that
// keeps the old value the way the CHAOS-3122 pattern does for other keys --
// would make stale budget counters terminalization-eligible again and fail
// healthy units. The obligation-#1 clears above remove the DEPENDENCE on that
// backstop, but the backstop must not be dismantled either, so the pattern
// stays forbidden.
//
// Note that releaseForRetrySQL DOES use a jsonb merge: it must, to keep the
// go_effect_ledger_v1 key alive across a retry. What makes it safe is the
// DIRECTION -- jsonb_build_object sits on the right of `||`, so its
// error_category wins. This test measures that direction, not the presence of
// a merge.
func TestNoUnitStampPreservesAPriorErrorCategory(t *testing.T) {
	stamps := map[string]string{
		"completeUnitSQL":             completeUnitSQL,
		"releaseForRetrySQL":          releaseForRetrySQL,
		"deferForBudgetContentionSQL": deferForBudgetContentionSQL,
		"failUnitSQL":                 failUnitSQL,
	}
	measured := 0
	for name, sql := range stamps {
		if !strings.Contains(sql, "result") {
			continue
		}
		measured++
		if preservingErrorCategory(sql) {
			t.Errorf("%s preserves a prior error_category. Python's exhaustion "+
				"predicate accepts only its own deferral category, so a preserved "+
				"stale category makes stale counters terminalization-eligible:\n%s",
				name, sql)
		}
	}
	// A derivation that measures nothing must FAIL, not read as "all clean".
	if measured < len(stamps) {
		t.Fatalf("only %d of %d unit result stamps were measured; a stamp that "+
			"stopped writing `result` silently left this guard vacuous",
			measured, len(stamps))
	}
}

// TestPreservingErrorCategoryDetectorCatchesBothShapes is the guard's own
// negative control. Without it, TestNoUnitStampPreservesAPriorErrorCategory
// would pass identically if `preservingErrorCategory` always returned false --
// which is exactly what "a test that cannot fail" looks like.
func TestPreservingErrorCategoryDetectorCatchesBothShapes(t *testing.T) {
	for name, sql := range map[string]string{
		"reads the prior value back": `
			UPDATE public.sync_run_units AS unit
			SET result = jsonb_build_object(
				'error_category',
				COALESCE(unit.result->>'error_category', 'worker_lost')
			)`,
		"existing document wins the merge": `
			UPDATE public.sync_run_units AS unit
			SET result = (
				jsonb_build_object('error_category', 'worker_lost') ||
				COALESCE(unit.result::jsonb, '{}'::jsonb)
			)`,
		// The shape the LEASE-REPAIR stamps actually have: a
		// jsonb_build_object carrying NESTED calls. Any detector that scans
		// the build_object's argument list with a non-nesting `[^)]*` stops
		// at the first inner `)` and never reaches the `||`, so this
		// preserving merge reads as clean. That is a false NEGATIVE on the
		// exact statement shape this repository writes today.
		"existing document wins a merge whose build_object nests calls": `
			UPDATE public.sync_run_units AS unit
			SET result = (
				jsonb_build_object(
					'error_category', 'worker_lost',
					'retry_count', unit.expired_lease_retry_count + 1,
					'next_retry_at', to_jsonb($4::timestamptz),
					'retry_surfaces', to_jsonb($5::text[])
				) || COALESCE(unit.result::jsonb, '{}'::jsonb)
			)`,
		// No COALESCE at all -- the bare prior document on the right of the
		// merge preserves every key it carries, error_category included.
		"bare prior document on the right of the merge": `
			UPDATE public.sync_run_units AS unit
			SET result = jsonb_build_object('error_category', 'worker_lost') || unit.result`,
	} {
		if !preservingErrorCategory(sql) {
			t.Errorf("detector missed a preserving stamp (%s):\n%s", name, sql)
		}
	}
	for name, sql := range map[string]string{
		"production release-for-retry": releaseForRetrySQL,
		"production complete":          completeUnitSQL,
		// The SAFE direction, nested, so a detector "fixed" into flagging any
		// statement that merely contains both a merge and a COALESCE over the
		// prior document fails here instead of reading as a stricter guard.
		"safe direction with a nested build_object": `
			UPDATE public.sync_run_units AS unit
			SET result = (
				COALESCE(unit.result::jsonb, '{}'::jsonb) ||
				jsonb_build_object(
					'error_category', 'worker_lost',
					'next_retry_at', to_jsonb($4::timestamptz)
				)
			)`,
	} {
		if preservingErrorCategory(sql) {
			t.Errorf("detector false-positives on %s:\n%s", name, sql)
		}
	}
}

// TestUpsertWatermarkSQLHealsOnlyProvablyCorruptFutureValues pins clause
// C10(b). The write stays monotonic (CHAOS-2578) except for one narrow case:
// a STORED value ahead of `now` with a sane incoming one. Widening that to
// "any lower value wins" is a defect -- Python pinned it by mutation.
func TestUpsertWatermarkSQLHealsOnlyProvablyCorruptFutureValues(t *testing.T) {
	for _, required := range []string{
		"public.sync_watermarks.last_synced_at > $6::timestamptz",
		"EXCLUDED.last_synced_at <= $6::timestamptz",
		"GREATEST(",
	} {
		if !strings.Contains(upsertWatermarkSQL, required) {
			t.Errorf("upsertWatermarkSQL missing %q:\n%s", required, upsertWatermarkSQL)
		}
	}
	// Both halves of the guard, or the exception is not narrow. A CASE with
	// only the stored-is-future test would let a legitimately lower (also
	// future) incoming value roll a watermark backwards.
	if strings.Count(upsertWatermarkSQL, "$6::timestamptz") != 2 {
		t.Errorf("the future-heal exception must test BOTH the stored and the "+
			"incoming value against the write instant:\n%s", upsertWatermarkSQL)
	}
}
