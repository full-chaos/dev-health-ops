//go:build integration

package report

import (
	"fmt"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/operatingreview"
)

// mttrTile resolves the operating review of the week of deliveryDay1 and returns its mttr_hours tile.
func mttrTile(e *pooledEnv, client *dhclickhouse.Client, org string) (float64, bool) {
	e.t.Helper()
	review, err := operatingreview.Resolve(e.ctx, client, org, nil, graphqldate.New(deliveryDay1))
	if err != nil {
		e.t.Fatal(err)
	}
	for _, section := range review.Sections {
		for _, metric := range section.Metrics {
			if metric.Key == "mttr_hours" {
				return metric.Value, metric.HasData
			}
		}
	}
	e.t.Fatal("no mttr_hours tile")
	return 0, false
}

func mttrTileClient(e *pooledEnv) *dhclickhouse.Client {
	e.t.Helper()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: e.uri})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = client.Close() })
	return client
}

// The operating review mttr_hours tile is the pooled MTTR the report charts, not a mean of the stored daily values.
func TestOperatingReviewMTTRTileEqualsPooled(t *testing.T) {
	e := startPooledEnv(t)
	client := mttrTileClient(e)
	tile := func(org string) (float64, bool) { return mttrTile(e, client, org) }
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

// The availability of the tile is that of the pooled result: a sample the daily writer has not seen is data, and a stored
// value whose sample is gone is no data, not a measured zero.
func TestOperatingReviewMTTRTileDataFollowsThePool(t *testing.T) {
	e := startPooledEnv(t)
	client := mttrTileClient(e)
	at := func(day time.Time, i int) time.Time { return day.Add(time.Duration(i+1) * 30 * time.Minute) }

	// a: the writer ran with no bug (stored MTTR NULL); a bug completed that day is synced after it. Pooled 4, data.
	orgA := e.newOrg()
	const repoA = "14026000-0000-4000-8000-0000000000f1"
	e.insertRepo(orgA, repoA)
	e.insertPullRequest(orgA, repoA, 1, at(deliveryDay1, 0), 5, 5)
	e.computeDelivery(orgA, []string{repoA}, deliveryDay1.Format(time.DateOnly))
	e.insertBug(orgA, repoA, "late", "bug", at(deliveryDay1, 1), at(deliveryDay1, 1).Add(4*time.Hour))
	valueA, hasA := mttrTile(e, client, orgA)
	pooledA := e.deliveryChart(orgA, "mttr_hours", "", "scorecard", "2026-02-02", "2026-02-08")["total"]
	if !hasA || !near(valueA, 4) || !near(pooledA, 4) {
		t.Errorf("a (pooled sample, stored NULL): tile %v (hasData %v), pooled %v, want 4 with data", valueA, hasA, pooledA)
	}

	// b: the writer stored 2 h; the work item is then corrected to a story, so the pool is empty. No data, not 0.
	orgB := e.newOrg()
	const repoB = "14026000-0000-4000-8000-0000000000f2"
	e.insertRepo(orgB, repoB)
	e.insertBug(orgB, repoB, "gone", "bug", at(deliveryDay1, 0), at(deliveryDay1, 0).Add(2*time.Hour))
	e.insertPullRequest(orgB, repoB, 1, at(deliveryDay1, 0), 5, 5)
	e.computeDelivery(orgB, []string{repoB}, deliveryDay1.Format(time.DateOnly))
	stored := e.storedDelivery(orgB, repoB, "repo_metrics_daily", "mttr_hours")
	e.exec(`INSERT INTO work_items (repo_id, work_item_id, provider, type, created_at, updated_at, started_at, completed_at, last_synced, org_id)
VALUES (?, 'gone', 'jira', 'story', ?, ?, ?, ?, '2026-02-03 00:00:00.000', ?)`,
		repoB, at(deliveryDay1, 0).Format(deliveryTimeFormat), at(deliveryDay1, 0).Format(deliveryTimeFormat), at(deliveryDay1, 0).Format(deliveryTimeFormat),
		at(deliveryDay1, 0).Add(2*time.Hour).Format(deliveryTimeFormat), orgB)
	valueB, hasB := mttrTile(e, client, orgB)
	if !near(stored["2026-02-02"], 2) || hasB || valueB != 0 {
		t.Errorf("b (empty pool, stored 2): stored %v, tile %v (hasData %v), want stored 2 and no data", stored, valueB, hasB)
	}
}

// A mapping that became valid after the week ended but before the request counts for the tile as for the report: both
// read the incident mappings at the request instant.
func TestOperatingReviewMTTRTileReadsMappingsAtRequestTime(t *testing.T) {
	e := startPooledEnv(t)
	client := mttrTileClient(e)
	org := e.newOrg()
	const repo = "14026000-0000-4000-8000-0000000000a1"
	e.insertRepo(org, repo)
	e.exec(`INSERT INTO operational_service_repository_mappings
    (org_id, provider, provider_instance_id, source_entity_type, external_id,
     source_version_at, source_revision, source_conflict_key, ingest_revision, ordering_contract,
     id, observed_at, last_synced, service_id, repo_id, mapping_kind, valid_from, valid_to, is_active)
VALUES (?, 'pagerduty', 'pd', 'repository_mapping', 'map-late', now64(6), 1, '', 1, 2, 'map-late', now64(6), now64(6), 'svc-late', toUUID(?), 'repository_derived', '2026-03-01 00:00:00.000000', NULL, 1)`,
		org, repo)
	at := func(day time.Time, i int) time.Time { return day.Add(time.Duration(i+1) * 30 * time.Minute) }
	for i, hours := range []float64{1, 3} {
		e.insertIncident(org, "svc-late", fmt.Sprintf("i1-%d", i), at(deliveryDay1, i), at(deliveryDay1, i).Add(time.Duration(hours)*time.Hour), 0)
	}
	for i := 0; i < 8; i++ {
		e.insertIncident(org, "svc-late", fmt.Sprintf("i2-%d", i), at(deliveryDay2, i), at(deliveryDay2, i).Add(10*time.Hour), 0)
	}
	e.insertBug(org, repo, "b-1", "bug", at(deliveryDay1, 0), at(deliveryDay1, 0).Add(2*time.Hour))
	e.insertPullRequest(org, repo, 1, at(deliveryDay1, 0), 5, 5)
	e.computeDelivery(org, []string{repo}, deliveryDay1.Format(time.DateOnly), deliveryDay2.Format(time.DateOnly))
	value, has := mttrTile(e, client, org)
	pooledP50 := e.deliveryChart(org, "mttr_p50_hours", "", "scorecard", "2026-02-02", "2026-02-08")["total"]
	if !has || !near(pooledP50, 10) || !near(value, pooledP50) {
		t.Errorf("mapping valid after the week: tile %v (hasData %v), report p50 %v, want both 10", value, has, pooledP50)
	}
}
