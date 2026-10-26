package pgmigrate

import (
	"strings"
	"testing"
	"time"
)

func TestFailureStreakDropBudgetIsNamedAndIgnoresLockAndRetryDelay(t *testing.T) {
	want := failureStreakDropBudget(IndexBuildOptions{})
	if want != DefaultIndexCleanupBudget || want > 5*time.Minute {
		t.Fatalf("default budget %s, want the %s cap", want, DefaultIndexCleanupBudget)
	}
	if got := failureStreakDropBudget(IndexBuildOptions{LockTimeout: time.Minute, RetryDelay: 20 * time.Minute}); got != want {
		t.Fatalf("budget depends on LockTimeout or RetryDelay: %s vs %s", got, want)
	}
	if got := failureStreakDropBudget(IndexBuildOptions{CleanupBudget: 3 * time.Second}); got != 3*time.Second {
		t.Fatalf("explicit budget ignored: %s", got)
	}
}

// The Job's whole run (startup, the build window, the cleanup) fits its activeDeadlineSeconds with the chart default of 3600.
func TestDefaultBuildWindowAndCleanupFitTheJobDeadline(t *testing.T) {
	const startup = time.Minute
	if total := startup + DefaultIndexBuildDeadline + DefaultIndexCleanupBudget; total > 3600*time.Second {
		t.Fatalf("default window %s + cleanup %s + startup %s = %s exceeds the 3600 s Job deadline", DefaultIndexBuildDeadline, DefaultIndexCleanupBudget, startup, total)
	}
}

// IF NOT EXISTS over an INVALID index is a silent no-op that reports success.
func TestFailureStreakBuildSQLHasNoIfNotExists(t *testing.T) {
	if strings.Contains(failureStreakBuildSQL, "IF NOT EXISTS") {
		t.Fatalf("build SQL %q must not use IF NOT EXISTS", failureStreakBuildSQL)
	}
	if !strings.Contains(failureStreakBuildSQL, "CREATE INDEX CONCURRENTLY ") || !strings.Contains(failureStreakDropSQL, "DROP INDEX CONCURRENTLY IF EXISTS") {
		t.Fatalf("build/drop SQL not concurrent: %q / %q", failureStreakBuildSQL, failureStreakDropSQL)
	}
}

func TestIndexInfoState(t *testing.T) {
	for want, info := range map[string]indexInfo{
		"none":      {},
		"valid":     {exists: true, valid: true, isReady: true},
		"invalid":   {exists: true, isReady: true},
		"not_ready": {exists: true, valid: true},
	} {
		if got := info.state(); got != want {
			t.Errorf("state() = %s, want %s", got, want)
		}
	}
}
