//go:build integration

package quadrant

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestQuadrantMetricsGiveRetractionRowsNoWeight reads every quadrant metric
// that comes from work_item_metrics_daily, by team and by repository, for two
// organizations that hold the same measurements. One of them also holds the
// old rows of the retired team ids and the retraction row over each (package
// retractionseed). The points must be the same: a retired team id is not a
// point, and a retraction row is not a sample of a repository's mean WIP.
func TestQuadrantMetricsGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	start, end := store.Days[1], store.Days[len(store.Days)-1].AddDate(0, 0, 1)
	read := func(org string) map[string][]metricRow {
		t.Helper()
		answers := map[string][]metricRow{}
		for scope, specs := range map[string]map[string]MetricSpec{"team": TeamMetrics, "repo": RepoMetrics} {
			for name, spec := range specs {
				if !strings.HasPrefix(spec.Table, "work_item_metrics_daily") {
					continue
				}
				rows, err := fetchQuadrantMetric(ctx, client, spec, start, end, "month", org, "")
				if err != nil {
					t.Fatalf("%s %s %s: %v", org, scope, name, err)
				}
				sort.Slice(rows, func(i, j int) bool {
					if !rows[i].Bucket.Equal(rows[j].Bucket) {
						return rows[i].Bucket.Before(rows[j].Bucket)
					}
					return rows[i].EntityID < rows[j].EntityID
				})
				answers[scope+"/"+name] = rows
			}
		}
		return answers
	}

	control := read(retractionseed.ControlOrg)

	// The control points, from the seed: the WIP of the four teams is 3, 4, 5
	// and 6 on every day, so that is each team's mean in every bucket, and
	// the mean of the one team of each repository.
	wantWIP := map[string]float64{
		"jira:ENG": 3, "github:platform": 4, "gitlab:ops": 5, "linear:core": 6,
		"ENGPROJ": 3, "acme/platform": 4, "acme/ops": 5, "CORE": 6,
	}
	for _, key := range []string{"team/wip", "repo/wip"} {
		rows := control[key]
		if len(rows) == 0 || len(rows)%4 != 0 {
			t.Fatalf("control %s = %+v, want four entities in each bucket", key, rows)
		}
		for _, row := range rows {
			want, ok := wantWIP[row.EntityID]
			if !ok || row.Value != want {
				t.Fatalf("control %s row %+v, want one of %v", key, row, wantWIP)
			}
		}
	}
	if len(control) != 4 {
		t.Fatalf("control covers %d work item metrics, want 4 (team throughput, cycle_time, wip; repo wip): %v", len(control), control)
	}

	retracted := read(retractionseed.RetractedOrg)
	if !reflect.DeepEqual(control, retracted) {
		t.Fatalf("the retraction rows changed a point:\n control   %+v\n retracted %+v", control, retracted)
	}
}
