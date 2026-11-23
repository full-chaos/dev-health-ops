package complexitytimeseries

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-14515: a caller repoIds list over the effective limit must not be cut
// with no notice. A 200-day DAY window with no limit gives an effective limit
// of 5 (MaxTimeseriesPoints 1000 / 200 buckets), so 6 repoIds bind 5. Resolve
// refuses the list with an error, or serves a field outside the fields of
// today (Points, TotalScope) that names the cut.
func TestRepoIdsOverTheEffectiveLimitIsRefusedOrDisclosed(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := since.AddDate(0, 0, 199)
	repoIDs := []string{"r1", "r2", "r3", "r4", "r5", "r6"}
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{}}}}
	got, err := Resolve(context.Background(), client, "org-1", since, until,
		model.TimeGranularityDay, model.ComplexityScopeRepo, repoIDs, nil)
	if err != nil {
		return
	}
	v := reflect.ValueOf(*got)
	for i := 0; i < v.NumField(); i++ {
		if name := v.Type().Field(i).Name; name != "Points" && name != "TotalScope" && !v.Field(i).IsZero() {
			return
		}
	}
	bound := -1
	for _, b := range client.bindings[0] {
		if b.Name == "repo_ids" {
			bound = len(b.Value.([]string))
		}
	}
	t.Fatalf("%d repoIds over a 200-day DAY window: no error and no served field names the cut; repo_ids bound %d, points=%d; want an error or a served cut field",
		len(repoIDs), bound, len(got.Points))
}
