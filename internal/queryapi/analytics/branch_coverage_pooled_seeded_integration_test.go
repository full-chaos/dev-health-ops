//go:build integration

package analytics

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-14219: COVERAGE_BRANCH_PCT is the branches covered over the branches total of the stored counts, never the mean of
// the stored per-repository-day percents. A row with no counts is in neither sum.
func TestCoverageBranchPct_IsPooledOverStoredCounts_RealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	chschema.Apply(ctx, t, inst)
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}

	const orgID = "org-14219"
	const big, small, noCounts, onlyNoCounts = "14219000-0000-0000-0000-000000000001", "14219000-0000-0000-0000-000000000002", "14219000-0000-0000-0000-000000000003", "14219000-0000-0000-0000-000000000004"
	// team-a: 900 of 1,000 branches (90) and 10 of 100 (10), plus a row with a percent (99) and no counts.
	// Mean of the stored percents 66.33; pooled 910 / 1,100. team-b: one row with a percent and no counts: no value.
	exec(fmt.Sprintf("INSERT INTO testops_coverage_metrics_daily (repo_id, day, branch_coverage_pct, branches_total, branches_covered, team_id, org_id, computed_at) VALUES ('%s', '2026-01-02', 90, 1000, 900, 'team-a', '%s', now()), ('%s', '2026-01-02', 10, 100, 10, 'team-a', '%s', now()), ('%s', '2026-01-02', 99, NULL, NULL, 'team-a', '%s', now()), ('%s', '2026-01-02', 70, NULL, NULL, 'team-b', '%s', now())",
		big, orgID, small, orgID, noCounts, orgID, onlyNoCounts, orgID))
	for _, o := range []struct{ repo, team string }{{big, "team-a"}, {small, "team-a"}, {noCounts, "team-a"}, {onlyNoCounts, "team-b"}} {
		for _, q := range ownerSeedSQL(orgID, o.repo, o.team) {
			exec(q)
		}
	}
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	got, err := resolveOneTimeseries(ctx, client, orgID, tsInput(model.DimensionInputTeam, model.MeasureInputCoverageBranchPct), false, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	teams := map[string]*float64{}
	for _, s := range got {
		if s.Scope != model.TimeseriesScopeOrg && len(s.Buckets) == 1 {
			teams[s.DimensionValue] = s.Buckets[0].Value
		}
	}
	show := func(p *float64) any {
		if p == nil {
			return nil
		}
		return *p
	}
	t.Logf("BRANCHCELL analytics team-a=%v team-b=%v (pooled want %v)", show(teams["team-a"]), show(teams["team-b"]), 910.0/1100*100)
	if a := teams["team-a"]; a == nil || math.Abs(*a-910.0/1100*100) > 1e-9 {
		t.Errorf("COVERAGE_BRANCH_PCT team-a = %v, want %v (pooled over the stored counts)", show(a), 910.0/1100*100)
	}
	if b := teams["team-b"]; b != nil {
		t.Errorf("COVERAGE_BRANCH_PCT team-b = %v, want NULL (no row carries counts)", *b)
	}
}
