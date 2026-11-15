package testops

import (
	"slices"
	"testing"
)

// CompletedRunStatuses is exactly the statuses the compute counts as succeeded or failed, so a ClickHouse reader that
// asks "did a run complete" agrees with the rates.
func TestCompletedRunStatusesArePinnedToNormalizeStatus(t *testing.T) {
	completed := CompletedRunStatuses()
	if len(completed) == 0 {
		t.Fatal("no completed statuses")
	}
	for _, status := range completed {
		if class := NormalizeStatus(status); class != "success" && class != "failure" {
			t.Errorf("%q is listed as completed but normalizes to %q", status, class)
		}
		if NormalizeStatus("  "+status+"  ") != NormalizeStatus(status) {
			t.Errorf("%q: padding changes its class", status)
		}
	}
	for _, status := range []string{
		"cancelled", "canceled", "cancel", "skipped", "skip", "running", "queued", "pending", "in_progress",
		"neutral", "stale", "action_required", "startup_failure", "manual", "",
	} {
		if slices.Contains(completed, status) {
			t.Errorf("%q is not a completed run but is listed", status)
		}
		if class := NormalizeStatus(status); class == "success" || class == "failure" {
			t.Errorf("%q normalizes to %q but is not listed as completed", status, class)
		}
	}
	// The list is a copy: a caller cannot change what the compute counts.
	completed[0] = "mutated"
	if CompletedRunStatuses()[0] == "mutated" {
		t.Error("CompletedRunStatuses returns the shared slice")
	}
}
