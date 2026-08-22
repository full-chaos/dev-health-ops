package syncreconciler

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobrescue"
	"github.com/riverqueue/river"
)

// The sentinel this package matches on is a STRING in a SQL predicate, and the
// error it has to equal is produced two packages away. Nothing about that
// linkage is checked by the compiler, and a drift would be invisible in
// production: a sentinel that no longer matches simply stops recovering
// anything, reporting a healthy zero forever. That is the same silence
// CHAOS-4093 sat inside for twenty-two hours.
//
// So the string is reconstructed here from its two actual sources -- River's
// JobCancelError wrapper and jobrescue's own error -- and compared. The import
// lives in a test file rather than in the production file on purpose: this
// package has no runtime need for jobrescue, and adding one to protect a
// constant would be a worse trade than checking it here.
func TestRescueOnlyCancelSentinelMatchesJobRescue(t *testing.T) {
	wrapped := river.JobCancel(jobrescue.ErrRescueOnlyWorkerExecuted)
	if wrapped == nil {
		t.Fatal("river.JobCancel returned nil")
	}
	if got := wrapped.Error(); got != riverRescueOnlyCancelError {
		t.Fatalf("rescue-only cancel sentinel = %q, want %q.\n"+
			"internal/jobrescue's error text or River's JobCancelError wrapper "+
			"changed, so the terminal-delivery repair now matches nothing and "+
			"will report zero recoveries whatever happens in production.",
			riverRescueOnlyCancelError, got)
	}
}
