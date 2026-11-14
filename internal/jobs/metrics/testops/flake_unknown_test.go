package testops

import (
	"testing"

	"github.com/google/uuid"
)

// A day whose case rows were all left out of the flake measures is unknown, not
// "no flakes"; one case row folded in, or no mark at all, leaves the record as
// it was.
func TestTestAccumulatorMarksFlakeUnknownOnlyWhenNoCaseRowWasFolded(t *testing.T) {
	suite := SuiteRow{RunID: "r1", SuiteID: "s1", TotalCount: 2, PassedCount: 2}
	for name, test := range map[string]struct {
		mark, foldCase bool
		want           bool
	}{
		"marked, no case folded":    {mark: true, want: true},
		"marked, a case folded":     {mark: true, foldCase: true},
		"not marked, no case":       {},
		"not marked, a case folded": {foldCase: true},
	} {
		accumulator := NewTestAccumulator(uuid.New(), "", nil)
		accumulator.AddSuite(suite)
		if test.mark {
			accumulator.MarkCasesOffDefaultBranchOnly()
		}
		if test.foldCase {
			accumulator.AddCaseGroup(CaseGroup{CaseName: "c", Statuses: []string{"passed"}})
		}
		metrics := accumulator.Finish(nil)
		if len(metrics) != 1 || metrics[0].FlakeUnknown != test.want {
			t.Errorf("%s: FlakeUnknown=%v, want %v (%+v)", name, metrics[0].FlakeUnknown, test.want, metrics)
		}
	}
}
