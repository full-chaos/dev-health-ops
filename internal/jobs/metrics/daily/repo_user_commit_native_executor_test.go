package daily

import (
	"errors"
	"testing"
)

// TestRepoUserCommitPartialWriteGuardPinsBothDirections is the codex sweep
// red-first proof (CHAOS-5190 r3 follow-up, team-lead-requested): before
// this fix, repouser.Writer.WriteResult's own correctly-threaded partial
// counts (repoRows on a userMetrics failure, repoRows+userRows on a
// commitMetrics failure) were discarded by ComputeFamily's bare
// `return 0, err` -- exactly the class already fixed in
// work_item_state/work_item/work_item_estimate/work_graph_edges/
// ai_governance. Mirrors those tests' exact shape: wrap only when
// something already landed, never when nothing did.
func TestRepoUserCommitPartialWriteGuardPinsBothDirections(t *testing.T) {
	cause := errors.New("simulated ClickHouse send failure")

	t.Run("failure after rows landed is a partial write", func(t *testing.T) {
		rows, err := wrapRepoUserCommitPartialWrite(5, cause)
		if !errors.Is(err, ErrPartialWrite) {
			t.Errorf("a failure after 5 rows landed must wrap ErrPartialWrite; got %v", err)
		}
		if !errors.Is(err, cause) {
			t.Errorf("the original cause must survive wrapping; got %v", err)
		}
		if rows != 5 {
			t.Errorf("the TRUE rows-written count must be reported, got %d, want 5", rows)
		}
	})

	t.Run("failure with nothing written is an ordinary failure", func(t *testing.T) {
		rows, err := wrapRepoUserCommitPartialWrite(0, cause)
		if errors.Is(err, ErrPartialWrite) {
			t.Error("a failure with nothing written must NOT wrap ErrPartialWrite")
		}
		if !errors.Is(err, cause) {
			t.Errorf("the original cause must be returned unchanged; got %v", err)
		}
		if rows != 0 {
			t.Errorf("rows=%d, want 0", rows)
		}
	})
}
