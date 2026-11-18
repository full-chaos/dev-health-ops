//go:build integration

package report

import (
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/operatingreview"
)

// The operating review mttr_hours tile is the pooled MTTR the report charts, not a mean of the stored daily values.
func TestOperatingReviewMTTRTileEqualsPooled(t *testing.T) {
	e := startPooledEnv(t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: e.uri})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	tile := func(org string) (float64, bool) {
		review, err := operatingreview.Resolve(e.ctx, client, org, nil, graphqldate.New(deliveryDay1))
		if err != nil {
			t.Fatal(err)
		}
		for _, section := range review.Sections {
			for _, metric := range section.Metrics {
				if metric.Key == "mttr_hours" {
					return metric.Value, metric.HasData
				}
			}
		}
		t.Fatal("no mttr_hours tile")
		return 0, false
	}
	at := func(day time.Time, i int) time.Time { return day.Add(time.Duration(i+1) * 30 * time.Minute) }

	// A: bug work items only. Day 1: 1 h and 3 h; day 2: eight of 10 h. Stored 2 and 10, pooled 8.4.
	orgA := e.newOrg()
	const repoA = "14026000-0000-4000-8000-0000000000e1"
	e.insertRepo(orgA, repoA)
	for i, hours := range []int{1, 3} {
		e.insertBug(orgA, repoA, "b1-"+string(rune('a'+i)), "bug", at(deliveryDay1, i), at(deliveryDay1, i).Add(time.Duration(hours)*time.Hour))
	}
	e.insertPullRequest(orgA, repoA, 1, at(deliveryDay1, 0), 5, 5)
	for i := 0; i < 8; i++ {
		e.insertBug(orgA, repoA, "b2-"+string(rune('a'+i)), "bug", at(deliveryDay2, i), at(deliveryDay2, i).Add(10*time.Hour))
	}
	e.insertPullRequest(orgA, repoA, 2, at(deliveryDay2, 0), 5, 5)
	e.computeDelivery(orgA, []string{repoA}, deliveryDay1.Format(time.DateOnly), deliveryDay2.Format(time.DateOnly))
	valueA, hasA := tile(orgA)
	pooledA := e.deliveryChart(orgA, "mttr_hours", "", "scorecard", "2026-02-02", "2026-02-08")["total"]
	if !hasA || !near(pooledA, 8.4) || !near(valueA, pooledA) {
		t.Errorf("A (bugs only): tile %v (hasData %v), pooled %v, want both 8.4", valueA, hasA, pooledA)
	}

	// B: incidents and bugs. Stored incident medians 2 and 10, pooled median 10, pooled mean 8.4: the tile is the median.
	orgB := e.newOrg()
	const repoB = "14026000-0000-4000-8000-0000000000e2"
	seedDeliveryCell(e, orgB, repoB, "svc-14026")
	e.computeDelivery(orgB, []string{repoB}, deliveryDay1.Format(time.DateOnly), deliveryDay2.Format(time.DateOnly))
	valueB, hasB := tile(orgB)
	pooledP50 := e.deliveryChart(orgB, "mttr_p50_hours", "", "scorecard", "2026-02-02", "2026-02-08")["total"]
	pooledMean := e.deliveryChart(orgB, "mttr_hours", "", "scorecard", "2026-02-02", "2026-02-08")["total"]
	if !hasB || !near(pooledP50, 10) || !near(pooledMean, 8.4) || !near(valueB, pooledP50) {
		t.Errorf("B (incidents + bugs): tile %v (hasData %v), pooled p50 %v, pooled mean %v, want tile = p50 = 10, mean 8.4", valueB, hasB, pooledP50, pooledMean)
	}
}
