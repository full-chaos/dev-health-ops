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
	repoBoth, repoCurrentOnly, repoCurrentZero := uuid.New(), uuid.New(), uuid.New()
	current := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prior := current.AddDate(0, 0, -1)
	computedAt := current.AddDate(0, 0, 3)
	for repo, rows := range map[uuid.UUID]map[time.Time]uint32{
		repoBoth:        {current: 5, prior: 10},
		repoCurrentOnly: {current: 7},
		// A stored 0 now and no comparison row: still no delta (the LEFT JOIN default is 0, and 0 against 0 would read as a true 0 %).
		repoCurrentZero: {current: 0},
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
	if len(got.Drivers) != 3 {
		t.Fatalf("drivers = %+v, want 3", got.Drivers)
	}
	first := got.Drivers[0]
	for _, driver := range got.Drivers[1:] {
		if driver.ID != repoCurrentOnly.String() && driver.ID != repoCurrentZero.String() {
			t.Errorf("driver %s ranks after the measured one but has no comparison row", driver.ID)
		}
		if driver.DeltaPct != nil || !driver.HasData || driver.HasPriorData {
			t.Errorf("driver %+v (delta %v), want delta null, has_data true, has_prior_data false", driver, driver.DeltaPct)
		}
	}
	second := got.Drivers[1]
	if first.ID != repoBoth.String() || first.DeltaPct == nil || !near(*first.DeltaPct, -50) || !first.HasData || !first.HasPriorData {
		t.Errorf("first driver = %+v (delta %v), want the repository with both windows at -50 with both flags", first, first.DeltaPct)
	}
	_ = second
	if len(got.Contributors) == 0 {
		t.Fatal("no contributors")
	}
	for _, contributor := range got.Contributors {
		if contributor.DeltaPct != nil || !contributor.HasData || contributor.HasPriorData {
			t.Errorf("contributor %+v (delta %v), want delta null, has_data true, has_prior_data false", contributor, contributor.DeltaPct)
		}
	}
}

// The shared driver expression (deltarule.DriverPercentSQL) at its two edges on
// a real ClickHouse: a driver whose prior is a measured 0 and whose current is
// not has NO delta (null, both flags true: the percent against zero is
// undefined), and a driver at 0 against 0 has a true 0 delta (not null).
func TestExplainDriverAgainstAMeasuredZeroHasNoDeltaAndZeroAgainstZeroIsZero(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	const org = "explain-driver-zero-edges"
	fromZero, zeroZero, measured := uuid.New(), uuid.New(), uuid.New()
	current := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prior := current.AddDate(0, 0, -1)
	for repo, rows := range map[uuid.UUID]map[time.Time]uint32{
		fromZero: {current: 5, prior: 0},
		zeroZero: {current: 0, prior: 0},
		measured: {current: 5, prior: 10},
	} {
		for day, churn := range rows {
			if err := admin.Exec(ctx,
				`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
				repo, day, churn, current.AddDate(0, 0, 3), org); err != nil {
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
	byID := map[string]Contributor{}
	for _, driver := range got.Drivers {
		byID[driver.ID] = driver
	}
	if d := byID[fromZero.String()]; d.DeltaPct != nil || !d.HasData || !d.HasPriorData {
		t.Errorf("driver against a measured zero = %+v (delta %v), want delta null with both flags true", d, d.DeltaPct)
	}
	if d := byID[zeroZero.String()]; d.DeltaPct == nil || *d.DeltaPct != 0 || !d.HasData || !d.HasPriorData {
		t.Errorf("driver at 0 against 0 = %+v (delta %v), want a delta of 0 with both flags true", d, d.DeltaPct)
	}
	if d := byID[measured.String()]; d.DeltaPct == nil || !near(*d.DeltaPct, -50) {
		t.Errorf("measured driver = %+v (delta %v), want -50", d, d.DeltaPct)
	}
	if len(got.Drivers) == 3 && (got.Drivers[0].ID != measured.String() && got.Drivers[0].ID != zeroZero.String()) {
		t.Errorf("a driver without a delta ranks first: %+v", got.Drivers)
	}
}
