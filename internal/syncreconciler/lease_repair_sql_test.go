package syncreconciler

import (
	"regexp"
	"strings"
	"testing"
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

// preservingErrorCategory mirrors the detector in
// internal/providersync/repository_postgres_sql_test.go; see its comment for
// why the two forbidden shapes are what they are. It is duplicated rather than
// exported because both copies are test-only and a shared production helper
// for a test-only rule would be worse.
func preservingErrorCategory(sql string) bool {
	if regexp.MustCompile(`result\s*->>?\s*'error_category'`).MatchString(sql) {
		return true
	}
	return regexp.MustCompile(`jsonb_build_object\([^)]*\)\s*\|\|\s*COALESCE\(\s*\w+\.result`).
		MatchString(sql)
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
	preserving := `
		SET result = jsonb_build_object(
			'error_category',
			COALESCE(unit.result->>'error_category', 'worker_lost')
		)`
	if !preservingErrorCategory(preserving) {
		t.Fatalf("detector missed a preserving stamp:\n%s", preserving)
	}
	if preservingErrorCategory(markExpiredLeaseRetryingSQL) {
		t.Fatalf("detector false-positives on the production retry stamp")
	}
}
