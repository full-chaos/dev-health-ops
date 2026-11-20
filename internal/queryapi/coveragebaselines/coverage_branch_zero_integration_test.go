//go:build integration

package coveragebaselines

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A repo-day that measured zero covered branches stores branch_coverage_pct 0.0 (CHAOS-14296) and is a day with a value in the
// branch baseline; a day whose branch counts are unknown stores NULL and is not. A second org with the same shape is not read.
func TestBaselinesCountAMeasuredZeroBranchDayApartFromAnUnknownOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const (
		org      = "org-14296"
		zeroHit  = "14296000-0000-4000-8000-00000000000a"
		otherOrg = "14296000-0000-4000-8000-00000000000b"
		unknown  = "14296000-0000-4000-8000-00000000000c"
	)
	end := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	at := time.Date(2026, 9, 2, 1, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for repo, name := range map[string]string{zeroHit: "acme/zero", otherOrg: "acme/other", unknown: "acme/unknown"} {
		repoOrg := org
		if repo == otherOrg {
			repoOrg = "org-14296-other"
		}
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, repo, name, repoOrg, at, at)
	}
	for back := 1; back <= 8; back++ {
		day := end.AddDate(0, 0, -back)
		insert := `INSERT INTO testops_coverage_metrics_daily
			(repo_id, day, branch_coverage_pct, branches_covered, branches_total, uncovered_files_count, coverage_regression_count, org_id, computed_at)
			VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)`
		exec(insert, zeroHit, day, 0.0, uint32(0), uint32(10), org, at)
		exec(insert, unknown, day, nil, nil, nil, org, at)
		exec(insert, otherOrg, day, 50.0, uint32(5), uint32(10), "org-14296-other", at)
	}

	rows, err := Resolve(ctx, client, org, graphqldate.New(end), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	byRepo := map[string]int{}
	for index, row := range rows {
		byRepo[row.RepoID] = index
	}
	if len(rows) != 2 {
		t.Fatalf("%d rows, want 2 (the other org is not read): %+v", len(rows), rows)
	}
	if z := rows[byRepo[zeroHit]]; z.BranchBaselinePct == nil || *z.BranchBaselinePct != 0 || z.BranchDays != 8 {
		t.Errorf("zero-branch repo branch = %v over %d days, want 0 over 8 days", show(z.BranchBaselinePct), z.BranchDays)
	}
	if u := rows[byRepo[unknown]]; u.BranchBaselinePct != nil || u.BranchDays != 0 {
		t.Errorf("unknown-branch repo branch = %v over %d days, want no value over 0 days", show(u.BranchBaselinePct), u.BranchDays)
	}
}
