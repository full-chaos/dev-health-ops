package quadrant

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// windowCase is one quadrant type at one grain. xMarker and yMarker pick the
// reader of each axis out of the statement text.
type windowCase struct {
	name         string
	quadrantType string
	scope        string
	xMarker      string
	yMarker      string
	xDiv         float64 // the axis transform of the x metric (cycle time is hours / 24)
}

var windowCases = []windowCase{
	{"wip x throughput, team grain", "wip_throughput", "team", "avg(m.wip_count_end_of_day)", "sum(m.items_completed)", 1},
	{"cycle x throughput, team grain, attributed items", "cycle_throughput", "team", "sum(cycle_time_hours * weight)", "sum(item_weight)", 24},
	{"review load x latency, team grain, owner share", "review_load_latency", "team", "toFloat64(m.reviews_given)", "sum(m.pr_first_review_p50_hours * o.owner_share)", 1},
	{"churn x throughput, team grain", "churn_throughput", "service", "toFloat64(m.loc_touched)", "sum(m.items_completed)", 1},
	{"review load x latency, repository grain", "review_load_latency", "repo", "sum(m.reviews_given)", "avg(m.pr_first_review_p50_hours)", 1},
}

// windowReads builds the fake: a bucketed read answers the weekly rows, the
// whole-window read answers the window's own aggregate. The two differ on
// purpose, so a point taken from the last bucket (or from a mean of the
// buckets) cannot equal the window value.
func windowReads(c windowCase, weekly, window map[string][]float64, entity string) fakeQueryClient {
	return fakeQueryClient{handler: func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "SELECT toString(id) AS team_id") {
			return &fixtureRowScanner{}, nil
		}
		axis := ""
		switch {
		case strings.Contains(query, c.xMarker):
			axis = "x"
		case strings.Contains(query, c.yMarker):
			axis = "y"
		default:
			t.Fatalf("unexpected query for %s:\n%s", c.name, query)
		}
		if isWindowRead(query) {
			var rows [][]any
			for _, v := range window[axis] {
				rows = append(rows, []any{day(2024, 1, 1), entity, entity, v})
			}
			return &fixtureRowScanner{rows: rows}, nil
		}
		var rows [][]any
		for i, v := range weekly[axis] {
			rows = append(rows, []any{day(2024, 1, 1+7*i), entity, entity, v})
		}
		return &fixtureRowScanner{rows: rows}, nil
	}}
}

func buildWindow(t *testing.T, client fakeQueryClient, c windowCase, bucket string) *Response {
	t.Helper()
	client.t = t
	resp, err := BuildResponse(context.Background(), client, "org-1", Params{
		Type: c.quadrantType, ScopeType: c.scope, RangeDays: 30, Bucket: bucket,
		StartDate: timePtr(day(2024, 1, 1)), EndDate: timePtr(day(2024, 1, 21)),
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	return resp
}

func TestThePointIsTheWholeWindowAndTheTrajectoryKeepsTheBuckets(t *testing.T) {
	for _, c := range windowCases {
		for _, bucket := range []string{"week", "month"} {
			t.Run(c.name+" "+bucket, func(t *testing.T) {
				weekly := map[string][]float64{"x": {10, 20, 30}, "y": {1, 2, 9}}
				window := map[string][]float64{"x": {60}, "y": {12}}
				resp := buildWindow(t, windowReads(c, weekly, window, "e1"), c, bucket)
				if len(resp.Points) != 1 {
					t.Fatalf("points = %+v, want one", resp.Points)
				}
				p := resp.Points[0]
				if *p.X != 60/c.xDiv || *p.Y != 12 {
					t.Errorf("point (x, y) = (%v, %v), want the window's (%v, 12), not the last bucket's (%v, 9)", *p.X, *p.Y, 60/c.xDiv, 30/c.xDiv)
				}
				if p.WindowStart != "2024-01-01" || p.WindowEnd != "2024-01-22" {
					t.Errorf("window = %s..%s, want the requested 2024-01-01..2024-01-22", p.WindowStart, p.WindowEnd)
				}
				if len(p.Trajectory) != 3 || p.Trajectory[2].Y != 9 || p.Trajectory[0].Y != 1 {
					t.Errorf("trajectory = %+v, want the three weekly values", p.Trajectory)
				}
			})
		}
	}
}

func TestAOneBucketWindowHasAPointAndNoTrajectory(t *testing.T) {
	for _, c := range windowCases {
		t.Run(c.name, func(t *testing.T) {
			weekly := map[string][]float64{"x": {10}, "y": {4}}
			window := map[string][]float64{"x": {10}, "y": {4}}
			resp := buildWindow(t, windowReads(c, weekly, window, "e1"), c, "week")
			if len(resp.Points) != 1 {
				t.Fatalf("points = %+v, want one", resp.Points)
			}
			if p := resp.Points[0]; *p.X != 10/c.xDiv || *p.Y != 4 || p.Trajectory != nil {
				t.Errorf("point = %+v, want (%v, 4) and no trajectory", p, 10/c.xDiv)
			}
		})
	}
}

func TestAnEmptyWindowHasNoPoints(t *testing.T) {
	for _, c := range windowCases {
		t.Run(c.name, func(t *testing.T) {
			resp := buildWindow(t, windowReads(c, nil, nil, "e1"), c, "week")
			if resp.Points == nil || len(resp.Points) != 0 {
				t.Errorf("points = %#v, want an empty, non-null list", resp.Points)
			}
		})
	}
}

// A team whose two axes fall in different buckets has a window value on each
// axis: the point exists (no trajectory), where the last-bucket join dropped it.
func TestAPointNeedsBothAxesInTheWindowNotInOneBucket(t *testing.T) {
	c := windowCases[0]
	weekly := map[string][]float64{"x": {10}, "y": {}}
	window := map[string][]float64{"x": {10}, "y": {7}}
	resp := buildWindow(t, windowReads(c, weekly, window, "e1"), c, "week")
	if len(resp.Points) != 1 || *resp.Points[0].Y != 7 || resp.Points[0].Trajectory != nil {
		t.Errorf("points = %+v, want one window point without a trajectory", resp.Points)
	}
}

func TestTheWindowReadHasOneBucketForTheWholeWindow(t *testing.T) {
	for _, bucket := range []string{"week", "month"} {
		query := quadrantMetricQuery(TeamMetrics["throughput"], bucketWindow, "")
		if strings.Contains(query, "toStartOf") {
			t.Errorf("window read groups by a calendar bucket:\n%s", query)
		}
		if !strings.Contains(query, "toDate({start_day:Date}) AS bucket") {
			t.Errorf("window read does not carry the window's first day as its bucket:\n%s", query)
		}
		if got := quadrantMetricQuery(TeamMetrics["throughput"], bucket, ""); !strings.Contains(got, "toStartOf") {
			t.Errorf("%s read lost its calendar bucket", bucket)
		}
	}
}
