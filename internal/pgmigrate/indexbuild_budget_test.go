package pgmigrate

import (
	"testing"
	"time"
)

func TestFailureStreakDropBudgetIsCappedAndIgnoresRetryDelay(t *testing.T) {
	long := failureStreakDropBudget(IndexBuildOptions{LockTimeout: time.Minute, RetryDelay: 20 * time.Minute})
	if long > 5*time.Minute {
		t.Fatalf("budget %s exceeds the 5m cap", long)
	}
	if got := failureStreakDropBudget(IndexBuildOptions{LockTimeout: time.Minute}); got != long {
		t.Fatalf("budget depends on RetryDelay: %s vs %s", got, long)
	}
	if got := failureStreakDropBudget(IndexBuildOptions{LockTimeout: 5 * time.Second, RetryDelay: 15 * time.Second}); got > 90*time.Second {
		t.Fatalf("default budget %s exceeds 90s", got)
	}
}
