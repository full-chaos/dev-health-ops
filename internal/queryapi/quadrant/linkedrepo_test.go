package quadrant

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

func dayOf(value string) time.Time {
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		panic(err)
	}
	return parsed
}

// The linked scope of a request: its rows are the daily compute's, read once.
func linkedScopeWithRows(rows map[time.Time][]workitemmetrics.MetricsDailyRow) *linkedRepoScope {
	scope := &linkedRepoScope{repoIDs: []string{"r1"}}
	scope.once.Do(func() { scope.rows = rows })
	return scope
}

func TestFetchLinkedTeamMetricAggregatesAsTheDailyTableDoes(t *testing.T) {
	rows := map[time.Time][]workitemmetrics.MetricsDailyRow{
		dayOf("2026-09-14"): {
			{Day: dayOf("2026-09-14"), TeamID: "a", TeamName: "Alpha", ItemsCompleted: 2, WIPCountEndOfDay: 4},
			{Day: dayOf("2026-09-14"), TeamID: "b", TeamName: "Beta", ItemsCompleted: 1, WIPCountEndOfDay: 1},
			{Day: dayOf("2026-09-14"), TeamID: "", ItemsCompleted: 9, WIPCountEndOfDay: 9},
		},
		dayOf("2026-09-15"): {{Day: dayOf("2026-09-15"), TeamID: "a", TeamName: "Alpha", ItemsCompleted: 1, WIPCountEndOfDay: 2}},
		dayOf("2026-09-21"): {{Day: dayOf("2026-09-21"), TeamID: "a", TeamName: "Alpha", ItemsCompleted: 5, WIPCountEndOfDay: 5}},
		dayOf("2026-09-30"): {{Day: dayOf("2026-09-30"), TeamID: "a", TeamName: "Alpha", ItemsCompleted: 7, WIPCountEndOfDay: 7}},
	}
	scope := linkedScopeWithRows(rows)
	start, end := dayOf("2026-09-01"), dayOf("2026-09-28")
	ctx := context.Background()

	throughput, err := fetchLinkedTeamMetric(ctx, nil, scope, "throughput", start, end, bucketWindow, "org", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []metricRow{
		{Bucket: start, EntityID: "a", EntityLabel: "Alpha", Value: 8},
		{Bucket: start, EntityID: "b", EntityLabel: "Beta", Value: 1},
	}
	if !reflect.DeepEqual(throughput, want) {
		t.Errorf("window throughput = %+v, want %+v (a team without an id is skipped, a day outside the window is not read)", throughput, want)
	}

	wip, err := fetchLinkedTeamMetric(ctx, nil, scope, "wip", start, end, "week", "org", []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	wantWIP := []metricRow{
		{Bucket: dayOf("2026-09-13"), EntityID: "a", EntityLabel: "Alpha", Value: 3},
		{Bucket: dayOf("2026-09-20"), EntityID: "a", EntityLabel: "Alpha", Value: 5},
	}
	if !reflect.DeepEqual(wip, wantWIP) {
		t.Errorf("weekly wip of the selected team = %+v, want %+v (the mean of the rows of a bucket, the week starts on Sunday)", wip, wantWIP)
	}

	month, err := fetchLinkedTeamMetric(ctx, nil, scope, "throughput", start, end, "month", "org", []string{"b"})
	if err != nil || len(month) != 1 || month[0].Bucket != dayOf("2026-09-01") || month[0].Value != 1 {
		t.Errorf("monthly throughput = %+v, %v", month, err)
	}
	if _, err := fetchLinkedTeamMetric(ctx, nil, scope, "cycle_time", start, end, bucketWindow, "org", nil); err == nil {
		t.Error("an unsupported metric gave no error")
	}
}

func TestLinkedConditionsNarrowTheReadsByTheRepositoriesWork(t *testing.T) {
	scope := &linkedRepoScope{repoIDs: []string{"r1", "r2"}}

	asOf := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	query, _, err := workItemTeamQuadrantQuery("throughput", "week", "org-1", asOf, scope.linkedItemCondition("wct.work_item_id"))
	if err != nil {
		t.Fatal(err)
	}
	const linked = "wct.work_item_id IN (SELECT work_item_id FROM work_graph_issue_pr FINAL"
	idx := strings.Index(query, linked)
	if idx < 0 {
		t.Fatalf("the team read lacks the link condition %q:\n%s", linked, query)
	}
	if strings.Index(query, "OVER (PARTITION BY w.work_item_id)") > idx {
		t.Error("the repository narrows the items before the item weights are computed")
	}
	depths := sqlshape.Depths(query)
	orgIdx := strings.Index(query[idx:], "org_id = {org_id:String}")
	if orgIdx < 0 || depths[idx+orgIdx] != depths[idx+strings.Index(query[idx:], "FROM work_graph_issue_pr FINAL")] {
		t.Errorf("org_id does not scope the link table at its own depth:\n%s", query[idx:])
	}
	if strings.Contains(query, "wct.repo_id") {
		t.Error("an item's own repository column is the relation")
	}

	owner, ownerScope := ownerTeamRead(TeamMetrics["review_load"], "org-1", nil, asOf)
	text := quadrantMetricQuery(owner, "week", "", joinScopes(ownerScope, scope.ownerRepoCondition()))
	if !strings.Contains(text, "toString(m.repo_id) IN {scope_repo_ids:Array(String)}") ||
		strings.Index(text, "toString(m.repo_id) IN") < strings.Index(text, "org_id = {org_id:String}") {
		t.Errorf("the review read is not narrowed by the repository after the org predicate:\n%s", text)
	}
	joined := joinScopes(ownerScope, scope.ownerRepoCondition())
	if len(joined.Bindings) != len(ownerScope.Bindings)+1 {
		t.Errorf("bindings of the joined scope = %d, want %d", len(joined.Bindings), len(ownerScope.Bindings)+1)
	}
}

func TestBucketStartMatchesClickHouse(t *testing.T) {
	start := dayOf("2026-09-01")
	for _, tc := range []struct{ bucket, day, want string }{
		{"week", "2026-09-14", "2026-09-13"}, // Monday -> the Sunday before
		{"week", "2026-09-13", "2026-09-13"}, // Sunday is its own week start
		{"month", "2026-09-30", "2026-09-01"},
		{bucketWindow, "2026-09-30", "2026-09-01"},
	} {
		if got := bucketStart(tc.bucket, dayOf(tc.day), start); !got.Equal(dayOf(tc.want)) {
			t.Errorf("bucketStart(%s, %s) = %s, want %s", tc.bucket, tc.day, got.Format("2006-01-02"), tc.want)
		}
	}
}

func TestFetchLinkedTeamMetricSkipsRetractionRowsAsTheLiveRowRuleDoes(t *testing.T) {
	rows := map[time.Time][]workitemmetrics.MetricsDailyRow{
		dayOf("2026-09-14"): {{Day: dayOf("2026-09-14"), TeamID: "a", TeamName: "Alpha", ItemsCompleted: 1, WIPCountEndOfDay: 4}},
		dayOf("2026-09-15"): {{Day: dayOf("2026-09-15"), TeamID: "a", TeamName: "Alpha"}},
		dayOf("2026-09-16"): {{Day: dayOf("2026-09-16"), TeamID: "a", TeamName: "Alpha", NewItemsCount: 1}},
	}
	wip, err := fetchLinkedTeamMetric(context.Background(), nil, linkedScopeWithRows(rows), "wip", dayOf("2026-09-01"), dayOf("2026-09-28"), bucketWindow, "org", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []metricRow{{Bucket: dayOf("2026-09-01"), EntityID: "a", EntityLabel: "Alpha", Value: 2}}
	if !reflect.DeepEqual(wip, want) {
		t.Errorf("wip = %+v, want %+v (an all-zero row is no sample of the mean; a row with any measure is)", wip, want)
	}
}
