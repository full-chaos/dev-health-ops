package daily

import (
	"errors"
	"testing"
)

// TestTestopsRiskPartialWriteGuardPinsBothDirections is the codex sweep
// red-first proof (CHAOS-5190 r3 follow-up, team-lead-requested): before
// this fix, a writeTestopsRisk failure on a LATER table (quality_drag or
// pipeline_stability) after an EARLIER table already landed rows was
// reported `return 0, err` -- exactly the class already fixed in
// work_item_state/work_item/work_item_estimate/work_graph_edges/
// ai_governance/repo_user_commit. Mirrors those tests' exact shape.
//
// CHAOS-5245 deleted this file's original contents (buildTestopsFixture,
// releaseConfidenceAsMap/qualityDragAsMap/pipelineStabilityAsMap,
// normalizeOracleTeamService, assertJSONEqual, and
// TestTestopsRiskComputeMatchesLivePythonProduction -- the live-Python-oracle
// rot guard for compute_testops_risk.py, which no longer exists) alongside
// compute_testops_risk.py itself. This test is unrelated to that deletion --
// it exercises wrapTestopsRiskPartialWrite, a pure-Go helper in
// testops_risk_native_executor.go with no Python counterpart -- and survives.
func TestTestopsRiskPartialWriteGuardPinsBothDirections(t *testing.T) {
	cause := errors.New("simulated ClickHouse send failure")

	t.Run("failure after rows landed is a partial write", func(t *testing.T) {
		rows, err := wrapTestopsRiskPartialWrite(5, cause)
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
		rows, err := wrapTestopsRiskPartialWrite(0, cause)
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
