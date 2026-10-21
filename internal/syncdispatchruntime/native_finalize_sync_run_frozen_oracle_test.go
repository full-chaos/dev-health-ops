package syncdispatchruntime

import (
	"encoding/json"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

type oracleAggregateCase struct {
	Total   int    `json:"total"`
	Success int    `json:"success"`
	Failed  int    `json:"failed"`
	Status  string `json:"status"`
}

type oracleReasonCase struct {
	PlannerResult map[string]any `json:"planner_result"`
	ReasonOut     string         `json:"reason_out"`
}

type finalizeZeroUnitOracle struct {
	Aggregate []oracleAggregateCase `json:"aggregate"`
	Reason    []oracleReasonCase    `json:"reason"`
}

// TestAggregateRunStatusAndZeroUnitReasonMatchFrozenPython is the CHAOS-4175
// frozen-Python oracle: it compares a script answer,
// whose frozen answer (executed once on the pinned build) comes from the REAL, unmodified
// dev_health_ops.workers.sync_units._aggregate_run_status and
// ._zero_unit_reason, and diffs Go's aggregateRunStatus/zeroUnitReasonFrom
// against every case the script produced. This is what caught (before this
// test even existed -- by hand-running the oracle script first) that
// zeroUnitReasonFrom needs a whitespace-trimming blank check while two
// sibling checks in Finalize deliberately do NOT trim, matching three
// distinct predicates in the same Python function family.
func TestAggregateRunStatusAndZeroUnitReasonMatchFrozenPython(t *testing.T) {
	output := []byte(frozenPython(t, "finalize-zero-unit.golden.json",
		programoracle.Program{Name: "finalize zero unit oracle", Stdin: nil})[0])
	var want finalizeZeroUnitOracle
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode Python zero-unit oracle: %v: %s", err, output)
	}
	if len(want.Aggregate) == 0 || len(want.Reason) == 0 {
		t.Fatalf("oracle produced no cases: %s", output)
	}

	for _, oracleCase := range want.Aggregate {
		got := aggregateRunStatus(oracleCase.Total, oracleCase.Success, oracleCase.Failed)
		if got != oracleCase.Status {
			t.Errorf("aggregateRunStatus(%d,%d,%d)=%q want=%q (Python _aggregate_run_status)",
				oracleCase.Total, oracleCase.Success, oracleCase.Failed, got, oracleCase.Status)
		}
	}
	for _, oracleCase := range want.Reason {
		got := zeroUnitReasonFrom(oracleCase.PlannerResult)
		if got != oracleCase.ReasonOut {
			t.Errorf("zeroUnitReasonFrom(%#v)=%q want=%q (Python _zero_unit_reason)",
				oracleCase.PlannerResult, got, oracleCase.ReasonOut)
		}
	}

}
