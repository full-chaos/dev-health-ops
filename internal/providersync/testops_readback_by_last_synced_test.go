package providersync

import (
	"errors"
	"testing"
	"time"
)

func TestInspectTestOpsRowsByLastSyncedDecidesEachKeyThenTheBatch(t *testing.T) {
	batchAt := time.Date(2026, 11, 20, 19, 46, 0, 0, time.UTC)
	row := func(run, name string, at time.Time) githubTestsJobRow {
		return githubTestsJobRow{OrgID: "org-acme", RepoID: "repo", RunID: run, JobID: "11", JobName: name, LastSynced: at}
	}
	type stored struct {
		at   time.Time
		name string
	}
	older, newer := &stored{batchAt.Add(-time.Hour), "before"}, &stored{batchAt.Add(time.Hour), "after"}
	same, other := &stored{batchAt, "batch"}, &stored{batchAt, "before"}
	cases := []struct {
		name   string
		stored []*stored
		want   EffectInspection
	}{
		{"empty batch", nil, EffectAbsent},
		{"no row", []*stored{nil}, EffectAbsent},
		{"older", []*stored{older}, EffectAbsent},
		{"same version same row", []*stored{same}, EffectExact},
		{"same version other row", []*stored{other}, EffectConflict},
		{"newer", []*stored{newer}, EffectExact},
		{"older and newer", []*stored{older, newer}, EffectAbsent},
		{"newer and older", []*stored{newer, older}, EffectAbsent},
		{"same other and newer", []*stored{other, newer}, EffectConflict},
		{"older and same other", []*stored{older, other}, EffectConflict},
		{"same row and no row", []*stored{same, nil}, EffectAbsent},
		{"same row and newer", []*stored{same, newer}, EffectExact},
	}
	for _, cell := range cases {
		t.Run(cell.name, func(t *testing.T) {
			expected := make([]githubTestsJobRow, len(cell.stored))
			byRun := map[string]*stored{}
			for index, value := range cell.stored {
				run := string(rune('a' + index))
				expected[index] = row(run, "batch", batchAt)
				byRun[run] = value
			}
			got, err := inspectTestOpsRowsByLastSynced(expected, func(want githubTestsJobRow) (githubTestsJobRow, bool, error) {
				value := byRun[want.RunID]
				if value == nil {
					return githubTestsJobRow{}, false, nil
				}
				return row(want.RunID, value.name, value.at), true, nil
			})
			if err != nil || got != cell.want {
				t.Fatalf("inspection=%s err=%v, want %s", got, err, cell.want)
			}
		})
	}
	loadErr := errors.New("read failed")
	got, err := inspectTestOpsRowsByLastSynced([]githubTestsJobRow{row("a", "batch", batchAt)}, func(githubTestsJobRow) (githubTestsJobRow, bool, error) {
		return githubTestsJobRow{}, false, loadErr
	})
	if !errors.Is(err, loadErr) || got != EffectConflict {
		t.Fatalf("load error inspection=%s err=%v", got, err)
	}
}
