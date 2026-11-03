package delegationend

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func TestChildrenAreTheRowsOutsideTheIDs(t *testing.T) {
	rows := []Row{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	got := Children(rows, []string{"b"})
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "c" {
		t.Fatalf("Children = %+v", got)
	}
	if got := Children(nil, []string{"b"}); len(got) != 0 {
		t.Fatalf("Children of nothing = %+v", got)
	}
}

func TestEndOfNoRowsWritesNothing(t *testing.T) {
	if err := End(context.Background(), audit.TxOps{}, `"auth"`, nil, ReasonBaseEnded); err != nil {
		t.Fatalf("End of no rows: %v", err)
	}
}

// A TxOps with no queue has nowhere to put the row, and the caller hears it.
func TestRecordFailsWithNoAuditQueue(t *testing.T) {
	err := Record(context.Background(), audit.TxOps{}, []Row{{ID: "a", Target: "t", Actor: "x", Organization: "o"}}, EndPublicRows)
	if !errors.Is(err, audit.ErrMutationFailed) {
		t.Fatalf("Record = %v, want ErrMutationFailed", err)
	}
}
