//go:build integration

package home

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// The Home "driven by" lookup shares the driver expression with /explain
// (deltarule.DriverPercentSQL): a repository whose prior is a measured 0 and
// whose current is not has no percent, so it is not named; a repository at 0
// against 0 has a true 0 % and is; a measured one is. Real ClickHouse.
func TestHomeDriverLookupAtTheEdgesOfTheSharedExpression(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	const org = "home-driver-zero-edges"
	fromZero, zeroZero, measured, currentOnly := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	current := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prior := current.AddDate(0, 0, -1)
	for repo, rows := range map[uuid.UUID]map[time.Time]uint32{
		fromZero:    {current: 5, prior: 0},
		zeroZero:    {current: 0, prior: 0},
		measured:    {current: 5, prior: 10},
		currentOnly: {current: 9},
	} {
		for day, churn := range rows {
			crossorg.Exec(ctx, t, admin, `INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
				repo, day, churn, current.AddDate(0, 0, 3), org)
		}
	}
	rows, err := fetchMetricDriverDelta(ctx, client, "repo_metrics_daily", "total_loc_touched", "repo_id",
		current, current.AddDate(0, 0, 1), prior, current, "", nil, org, 10)
	if err != nil {
		t.Fatal(err)
	}
	named := map[string]bool{}
	for _, row := range rows {
		named[row.ID] = true
	}
	if named[fromZero.String()] || named[currentOnly.String()] {
		t.Errorf("drivers %v name a repository with no percent (against a measured zero, or without a comparison row)", named)
	}
	if !named[zeroZero.String()] || !named[measured.String()] {
		t.Errorf("drivers %v, want the repository at 0 against 0 (a true 0 %%) and the measured one", named)
	}
}

// CHAOS-9158 (D5856): a repository with a stored value in the comparison window
// and NO row in the current window is the same state as one whose current value
// is NULL. The "driven by" lookup names a repository only for a percent, and a
// percent needs a stored value on both sides, so neither is ever named: the
// lookup starting from the current window leaves out nothing it could name.
func TestHomeDriverLookupTreatsAPriorOnlyRepositoryLikeANullCurrentOne(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	const org = "home-driver-prior-only"
	measured, priorOnly, nullCurrent := uuid.New(), uuid.New(), uuid.New()
	current := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prior := current.AddDate(0, 0, -1)
	ins := func(repo uuid.UUID, day time.Time, hours any) {
		t.Helper()
		crossorg.Exec(ctx, t, admin, `INSERT INTO repo_metrics_daily (repo_id, day, pr_first_review_p50_hours, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
			repo, day, hours, current.AddDate(0, 0, 3), org)
	}
	ins(measured, current, 5.0)
	ins(measured, prior, 10.0)
	ins(priorOnly, prior, 8.0)     // no row in the current window
	ins(nullCurrent, current, nil) // a current row holding no value
	ins(nullCurrent, prior, 8.0)
	rows, err := fetchMetricDriverDelta(ctx, client, "repo_metrics_daily", "pr_first_review_p50_hours", "repo_id",
		current, current.AddDate(0, 0, 1), prior, current, "", nil, org, 10)
	if err != nil {
		t.Fatal(err)
	}
	named := map[string]bool{}
	for _, row := range rows {
		named[row.ID] = true
	}
	if !named[measured.String()] || named[priorOnly.String()] || named[nullCurrent.String()] || len(named) != 1 {
		t.Errorf("drivers %v, want only the measured repository %s (a prior-only and a null-current repository are the same state: neither has a percent)", named, measured)
	}
}
