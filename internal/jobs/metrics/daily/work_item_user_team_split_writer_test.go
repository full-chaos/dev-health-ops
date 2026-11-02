package daily

import (
	"context"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// The person's row carries its team split in the four team_split_* columns
// (CHAOS-12535), on each view row; a row of zeros carries none.
func TestTheWorkItemUserWriterStoresTheTeamSplit(t *testing.T) {
	conn := &liveLegacyConn{
		batch:   &countingBatch{},
		live:    [][]any{{"github", "s", "u", "", rowViewsStored}},
		carried: [][]any{{"github", "s", "u", "", "T1", "Team One"}},
	}
	row := workitemmetrics.UserMetricsDailyRow{
		Day: rowViewsDay, Provider: "github", WorkScopeID: "s", UserIdentity: "u", TeamID: "T2", TeamName: "Team Two",
		ItemsCompleted: 2, WIPCountEndOfDay: 1,
		TeamSplit: []workitemmetrics.UserTeamCount{
			{TeamID: "T2", IsPrimary: 1, ItemsCompleted: 1, WIPCountEndOfDay: 1},
			{TeamID: "T3", IsPrimary: 2, ItemsCompleted: 1},
			{TeamID: "unassigned", IsPrimary: 1, ItemsCompleted: 1},
		},
	}
	if _, err := WriteWorkItemUserMetricsDaily(context.Background(), conn, "org-a", rowViewsDay,
		[]workitemmetrics.UserMetricsDailyRow{row}, rowViewsComputedAt); err != nil {
		t.Fatal(err)
	}
	appended := conn.batch.appended
	if len(appended) != 3 {
		t.Fatalf("appended %d rows, want two view rows and a row of zeros", len(appended))
	}
	split := func(values []any) []any { return values[len(values)-6 : len(values)-2] }
	want := []any{
		[]string{"T2", "T3", "unassigned"}, []uint8{1, 2, 1}, []uint32{1, 1, 1}, []uint32{1, 0, 0},
	}
	for index, values := range appended[:2] {
		if got := split(values); !reflect.DeepEqual(got, want) {
			t.Errorf("view row %d split = %v, want %v", index, got, want)
		}
	}
	empty := []any{[]string{}, []uint8{}, []uint32{}, []uint32{}}
	if got := split(appended[2]); !reflect.DeepEqual(got, empty) {
		t.Errorf("row of zeros split = %v, want empty arrays", got)
	}
}
