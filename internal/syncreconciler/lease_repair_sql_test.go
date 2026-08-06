package syncreconciler

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// TestExpiredLeaseRetryStampClearsEveryEpisodeColumn pins the lease-repair
// half of CHAOS-3427 obligation #1.
//
// An expired lease is neither a rate-limit episode nor a budget episode, so
// this non-terminal RETRYING stamp must reset BOTH per-episode pairs, exactly
// as its Python analogue (workers/sync_reconciler.py's RETRYING stamp) does.
// Before CHAOS-3427 it cleared only the rate-limit pair, so a unit that Python
// had budget-deferred came back from lease repair with stale nonzero budget
// counters -- refused terminalization only by the defence-in-depth
// error-category gate, which is a backstop and not the contract.
//
// lease_repair_test.go already asserts "rate_limit_deferrals = 0" against the
// executed statement; this is the same discipline stated over the whole
// episode column set so a THIRD pair added later cannot be half-wired.
func TestExpiredLeaseRetryStampClearsEveryEpisodeColumn(t *testing.T) {
	for _, column := range []string{
		"rate_limit_deferrals = 0",
		"rate_limit_first_seen_at = NULL",
		"budget_deferrals = 0",
		"budget_first_deferred_at = NULL",
	} {
		if !strings.Contains(markExpiredLeaseRetryingSQL, column) {
			t.Errorf("markExpiredLeaseRetryingSQL does not clear %q:\n%s",
				column, markExpiredLeaseRetryingSQL)
		}
	}
}

// TestExpiredLeaseRetryStampLeavesTheAggregateClockAlone pins the deliberate
// exception. `first_blocked_at` is the AGGREGATE blocked clock: started
// set-if-null by real DEFERRAL stamps, cleared only by SUCCESS and the
// dispatch claim. Clearing it on every worker-churn retry would reset the
// outer bound that makes the budget/rate-limit ALTERNATION terminalizable --
// which is the exact F2 defect CHAOS-3412's review round 2 found.
func TestExpiredLeaseRetryStampLeavesTheAggregateClockAlone(t *testing.T) {
	if strings.Contains(markExpiredLeaseRetryingSQL, "first_blocked_at") {
		t.Errorf("markExpiredLeaseRetryingSQL touches first_blocked_at; only "+
			"deferral stamps start it and only SUCCESS/claim clear it:\n%s",
			markExpiredLeaseRetryingSQL)
	}
}

// preservingErrorCategory is the SAME detector internal/providersync uses --
// sqlshape.PreservesPriorResultCategory -- not a second copy of it. The two
// hand-copied regexps this replaced shared one false negative: they spanned
// jsonb_build_object's argument list with `[^)]*`, which stops at the first
// nested `)`, and BOTH stamps in this file nest `to_jsonb(...)` calls. A
// preserving merge written on these statements therefore read as clean, and
// fixing one copy would have left the other silently wrong.
func preservingErrorCategory(sql string) bool {
	return sqlshape.PreservesPriorResultCategory(sql)
}

// TestNoLeaseRepairStampPreservesAPriorErrorCategory is the FORBIDDEN PATTERN
// guard (CHAOS-3427 obligation #2) for this package's stamps. See the
// providersync twin for the full rationale: Python's exhaustion predicate
// accepts only its own deferral category, so a stamp that PRESERVED a prior
// category would make stale counters terminalization-eligible.
func TestNoLeaseRepairStampPreservesAPriorErrorCategory(t *testing.T) {
	stamps := map[string]string{
		"markExpiredLeaseRetryingSQL": markExpiredLeaseRetryingSQL,
		"markExpiredLeaseFailedSQL":   markExpiredLeaseFailedSQL,
	}
	measured := 0
	for name, sql := range stamps {
		if !strings.Contains(sql, "error_category") {
			continue
		}
		measured++
		if preservingErrorCategory(sql) {
			t.Errorf("%s preserves a prior error_category:\n%s", name, sql)
		}
	}
	// An unmeasured guard must FAIL loudly rather than read as coverage.
	if measured != len(stamps) {
		t.Fatalf("only %d of %d lease-repair stamps write error_category; this "+
			"guard went vacuous", measured, len(stamps))
	}
}

// TestPreservingErrorCategoryDetectorIsNotVacuous is this copy's own negative
// control -- without it the guard above would pass identically with a detector
// that always returned false.
func TestPreservingErrorCategoryDetectorIsNotVacuous(t *testing.T) {
	for name, preserving := range map[string]string{
		"reads the prior value back": `
			SET result = jsonb_build_object(
				'error_category',
				COALESCE(unit.result->>'error_category', 'worker_lost')
			)`,
		// THIS package's stamp shape, merged in the preserving direction. The
		// nested to_jsonb() calls are the point: the regexp detector this
		// replaced scanned the build_object's arguments with `[^)]*`, stopped
		// at `to_jsonb(`'s closing paren, never reached the `||`, and passed
		// this construct as clean.
		"existing document wins a merge whose build_object nests calls": `
			SET result = (
				jsonb_build_object(
					'error_category', 'worker_lost',
					'retry_count', unit.expired_lease_retry_count + 1,
					'next_retry_at', to_jsonb($4::timestamptz),
					'last_lease_expired_at', to_jsonb($3::timestamptz)
				) || COALESCE(unit.result::jsonb, '{}'::jsonb)
			)`,
		"bare prior document on the right of the merge": `
			SET result = jsonb_build_object('error_category', 'worker_lost') || unit.result`,
	} {
		if !preservingErrorCategory(preserving) {
			t.Errorf("detector missed a preserving stamp (%s):\n%s", name, preserving)
		}
	}
	for name, clean := range map[string]string{
		"production retry stamp": markExpiredLeaseRetryingSQL,
		"production fail stamp":  markExpiredLeaseFailedSQL,
		// The SAFE merge direction, nested, so a detector "fixed" into
		// flagging any statement carrying both a merge and a COALESCE over
		// the prior document fails here rather than reading as stricter.
		"safe direction with a nested build_object": `
			SET result = (
				COALESCE(unit.result::jsonb, '{}'::jsonb) ||
				jsonb_build_object(
					'error_category', 'worker_lost',
					'next_retry_at', to_jsonb($4::timestamptz)
				)
			)`,
	} {
		if preservingErrorCategory(clean) {
			t.Errorf("detector false-positives on %s:\n%s", name, clean)
		}
	}
}
