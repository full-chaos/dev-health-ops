package daily

import (
	"context"
	"testing"
)

// A terminal exit with no known reason is stored under a bounded value, never an
// unbounded or empty one.
func TestPartitionTerminalWriteWithNoReasonUsesTerminalUnclassified(t *testing.T) {
	store := terminalPartitionStore()
	failPartitionTerminally(context.Background(), executionAtAttempt(1, 5), store, *store.partitionClaim, "")
	if store.permanentFailures != 1 || store.permanentFailureReason != "terminal_unclassified" {
		t.Fatalf("permanent=%d reason=%q, want 1/terminal_unclassified", store.permanentFailures, store.permanentFailureReason)
	}
}
