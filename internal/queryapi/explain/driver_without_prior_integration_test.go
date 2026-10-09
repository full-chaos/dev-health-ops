//go:build integration

package explain

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A driver's delta_pct is a statement about two measured values: a driver with
// rows in the current window and none in the comparison window serves null with
// has_prior_data false (it was 0.0, the LEFT JOIN default), is ranked after the
// drivers that have a delta, and a contributor row, read for the current window
// only, serves none either (CHAOS-9063).
func TestExplainDriverWithoutAPriorRowServesNoDelta(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	const org = "explain-driver-no-prior"
	repoBoth, repoCurrentOnly := uuid.New(), uuid.New()
	current := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prior := current.AddDate(0, 0, -1)
	computedAt := current.AddDate(0, 0, 3)
	for repo, rows := range map[uuid.UUID]map[time.Time]uint32{
		repoBoth:        {current: 5, prior: 10},
		repoCurrentOnly: {current: 7},
	} {
		for day, churn := range rows {
			if err := admin.Exec(ctx,
				`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
				repo, day, churn, computedAt, org); err != nil {
				t.Fatal(err)
			}
		}
	}
	got, err := BuildExplainResponse(ctx, reader, org, Params{
		Metric: "churn", StartDay: current, EndDay: current.AddDate(0, 0, 1),
		CompareStart: prior, CompareEnd: current,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Drivers) != 2 {
		t.Fatalf("drivers = %+v, want 2", got.Drivers)
	}
	first, second := got.Drivers[0], got.Drivers[1]
	if first.ID != repoBoth.String() || first.DeltaPct == nil || !near(*first.DeltaPct, -50) || !first.HasData || !first.HasPriorData {
		t.Errorf("first driver = %+v (delta %v), want the repository with both windows at -50 with both flags", first, first.DeltaPct)
	}
	if second.ID != repoCurrentOnly.String() || second.DeltaPct != nil || !second.HasData || second.HasPriorData {
		t.Errorf("second driver = %+v (delta %v), want the current-only repository with delta null, has_data true and has_prior_data false", second, second.DeltaPct)
	}
	if len(got.Contributors) == 0 {
		t.Fatal("no contributors")
	}
	for _, contributor := range got.Contributors {
		if contributor.DeltaPct != nil || !contributor.HasData || contributor.HasPriorData {
			t.Errorf("contributor %+v (delta %v), want delta null, has_data true, has_prior_data false", contributor, contributor.DeltaPct)
		}
	}
}
