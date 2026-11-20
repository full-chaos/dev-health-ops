package operatingreview

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
)

// The work_items read picks a key's version by rowview.WorkItemMetricsNewestOrder, the one order of
// work_item_metrics_daily, in the org form and in the one-team source (CHAOS-14379, CHAOS-14406). The order is the
// same expression the read had before: computed_at, then the hash of its 19 picked measures, in the same order.
func TestWorkItemsReadTakesTheSharedVersionOrder(t *testing.T) {
	const before = `(work_item_metrics_daily.computed_at, cityHash64(tuple(
                work_item_metrics_daily.items_started, work_item_metrics_daily.items_completed,
                work_item_metrics_daily.items_started_unassigned,
                work_item_metrics_daily.items_completed_unassigned,
                work_item_metrics_daily.wip_count_end_of_day,
                work_item_metrics_daily.wip_unassigned_end_of_day, work_item_metrics_daily.bug_completed_ratio,
                work_item_metrics_daily.story_points_completed, work_item_metrics_daily.new_bugs_count,
                work_item_metrics_daily.new_items_count, work_item_metrics_daily.defect_intro_rate,
                work_item_metrics_daily.wip_congestion_ratio, work_item_metrics_daily.predictability_score,
                work_item_metrics_daily.cycle_time_p50_hours, work_item_metrics_daily.cycle_time_p90_hours,
                work_item_metrics_daily.lead_time_p50_hours, work_item_metrics_daily.lead_time_p90_hours,
                work_item_metrics_daily.wip_age_p50_hours, work_item_metrics_daily.wip_age_p90_hours)))`
	if got, want := strings.Join(strings.Fields(rowview.WorkItemMetricsNewestOrder), " "), strings.Join(strings.Fields(before), " "); got != want {
		t.Fatalf("rowview.WorkItemMetricsNewestOrder is not the order of the work_items read:\n%s\nwant\n%s", got, want)
	}
	start := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		teams teamSelection
		want  int
	}{{nil, 1}, {teamSelection{"P", "Q"}, 1}, {teamSelection{"P"}, 2}} {
		client := &fakeClient{responses: []*fakeRowScanner{{}}}
		if _, err := fetchWorkItems(context.Background(), client, "org-1", c.teams, start, start.AddDate(0, 0, 7)); err != nil {
			t.Fatalf("%v: %v", c.teams, err)
		}
		// The org form's argMax takes the order once; one team's served source takes it once more.
		if got := strings.Count(client.statements[0], rowview.WorkItemMetricsNewestOrder); got != c.want {
			t.Errorf("%v: the work_items statement holds the shared order %d times, want %d:\n%s", c.teams, got, c.want, client.statements[0])
		}
	}
}
