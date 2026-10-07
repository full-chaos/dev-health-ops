//go:build integration

package home

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestFetchReworkThemeAllocationKeepsEachRepositoryOfOneDay holds the reader against three repository
// partitions of one day whose other key columns are equal: two repositories
// and the nil repository (the work items that have none). The daily job
// computes a day one repository partition at a time (CHAOS-8813), so each
// partition has its own newest row and none of them replaces another.
//
//	repository A    10:00 100   11:05   1
//	repository B    10:00 200   11:06  20
//	nil repository  10:00 400   11:07 300
//
// The reader must return 321: the newest row of each repository. A reader
// whose key has no repo_id returns 300 (the newest row of the day), a read
// with no dedupe returns 1021.
func TestFetchReworkThemeAllocationKeepsEachRepositoryOfOneDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-8813-rework-theme"
	const repoA = "11111111-1111-4111-8111-111111111111"
	const repoB = "22222222-2222-4222-8222-222222222222"
	const noRepo = "00000000-0000-0000-0000-000000000000"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	at := func(hour, minute int) time.Time {
		return day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
	}
	insert := `INSERT INTO investment_metrics_daily
		(repo_id, day, team_id, investment_area, project_stream, delivery_units,
		 work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
		VALUES (?, ?, '', 'quality', 'general', ?, ?, 0, 0, 0, ?, ?)`
	for _, row := range []struct {
		repo       string
		value      uint32
		computedAt time.Time
	}{
		{repoA, 100, at(10, 0)}, {repoA, 1, at(11, 5)},
		{repoB, 200, at(10, 0)}, {repoB, 20, at(11, 6)},
		{noRepo, 400, at(10, 0)}, {noRepo, 300, at(11, 7)},
	} {
		if err := conn.Exec(ctx, insert, row.repo, day, row.value, row.value, row.computedAt, org); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}

	got, err := fetchReworkThemeAllocation(ctx, client, day, day.AddDate(0, 0, 1), "", nil, "", nil, org)
	if err != nil {
		t.Fatalf("fetchReworkThemeAllocation: %v", err)
	}
	if len(got) != 1 || got[0].Theme != "quality" {
		t.Fatalf("fetchReworkThemeAllocation = %+v, want one row of the quality theme", got)
	}
	if got[0].Allocation != 321 {
		t.Fatalf("quality allocation = %v, want 321: the newest row of each repository (1 + 20 + 300). 300 is a key with no repo_id, 1021 is every row",
			got[0].Allocation)
	}
}
