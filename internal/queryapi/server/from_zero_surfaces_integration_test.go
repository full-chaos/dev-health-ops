//go:build integration

package server

import (
	"context"
	"crypto/md5"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
)

// A percent change against a measured zero is undefined (deltarule.KindFromZero):
// the prior window holds a stored 0 and the current one holds 5. Every REST
// surface that serves a delta serves null for it, with both presence flags
// true, never a 0 % and never a placeholder; and a true 0 % (a measured zero in
// both windows) stays a 0. One row of this table per surface.
func TestEverySurfaceServesAPercentAgainstAMeasuredZeroAsNull(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", filepath.Join(t.TempDir(), "missing.yaml"))
	conn, client := startTeamScopeClickHouse(t)
	ctx := context.Background()

	// Home and /explain: repo_metrics_daily.total_loc_touched is the churn metric.
	current, prior := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	for org, rows := range map[string]map[time.Time]uint32{
		"from-zero-surfaces": {current: 5, prior: 0},
		"zero-zero-surfaces": {current: 0, prior: 0},
	} {
		repo := uuid.New()
		for day, churn := range rows {
			if err := conn.Exec(ctx, `INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
				repo, day, churn, time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), org); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("REST Home", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /api/v1/home", newHomeGetHandler(client, nil))
		const target = "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25"
		fromZero := restSummaryDeltas(t, mux, "from-zero-surfaces", target)["churn"]
		if fromZero.DeltaPct != nil || fromZero.HasData == nil || !*fromZero.HasData || fromZero.HasPriorData == nil || !*fromZero.HasPriorData || fromZero.Value != 5 {
			t.Errorf("churn from a measured zero = value %v delta_pct %s has_data %s has_prior_data %s, want 5, null, true, true", fromZero.Value, showDelta(fromZero.DeltaPct), flag(fromZero.HasData), flag(fromZero.HasPriorData))
		}
		zero := restSummaryDeltas(t, mux, "zero-zero-surfaces", target)["churn"]
		if zero.DeltaPct == nil || *zero.DeltaPct != 0 {
			t.Errorf("churn 0 -> 0 delta_pct = %s, want a true 0", showDelta(zero.DeltaPct))
		}
	})

	t.Run("explain", func(t *testing.T) {
		reader, err := explain.NewReader(client)
		if err != nil {
			t.Fatal(err)
		}
		params := explain.Params{Metric: "churn", StartDay: current, EndDay: current.AddDate(0, 0, 1), CompareStart: prior, CompareEnd: prior.AddDate(0, 0, 1)}
		got, err := explain.BuildExplainResponse(ctx, reader, "from-zero-surfaces", params)
		if err != nil {
			t.Fatal(err)
		}
		if got.DeltaPct != nil || !got.HasData || !got.HasPriorData || got.Value != 5 {
			t.Errorf("explain churn from a measured zero = value %v delta_pct %v has_data %v has_prior_data %v, want 5, null, true, true", got.Value, got.DeltaPct, got.HasData, got.HasPriorData)
		}
		if len(got.Drivers) != 1 || got.Drivers[0].DeltaPct != nil || !got.Drivers[0].HasData || !got.Drivers[0].HasPriorData {
			t.Errorf("explain drivers from a measured zero = %+v, want one driver with delta_pct null and both flags true", got.Drivers)
		}
		zero, err := explain.BuildExplainResponse(ctx, reader, "zero-zero-surfaces", params)
		if err != nil {
			t.Fatal(err)
		}
		if zero.DeltaPct == nil || *zero.DeltaPct != 0 {
			t.Errorf("explain churn 0 -> 0 delta_pct = %v, want a true 0", zero.DeltaPct)
		}
		if len(zero.Drivers) != 1 || zero.Drivers[0].DeltaPct == nil || *zero.Drivers[0].DeltaPct != 0 {
			t.Errorf("explain drivers 0 -> 0 = %+v, want delta_pct 0", zero.Drivers)
		}
	})

	t.Run("person summary", func(t *testing.T) {
		reader, err := people.NewReader(client)
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /api/v1/people/{person_id}/summary", newPeopleSummaryHandler(reader))
		now := time.Now().UTC()
		day := func(daysAgo int) time.Time {
			return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -daysAgo)
		}
		const identity = "alice@example.com"
		personID := fmt.Sprintf("%x", md5.Sum([]byte(identity)))
		for org, rows := range map[string]map[int]uint32{
			"from-zero-surfaces": {3: 5, 20: 0},
			"zero-zero-surfaces": {3: 0, 20: 0},
		} {
			repo := uuid.New()
			for daysAgo, loc := range rows {
				if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily (repo_id, day, identity_id, author_email, loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
					repo, day(daysAgo), identity, identity, loc, now, org); err != nil {
					t.Fatal(err)
				}
			}
		}
		target := "/api/v1/people/" + personID + "/summary?range_days=14&compare_days=14"
		fromZero := restSummaryDeltas(t, mux, "from-zero-surfaces", target)["churn"]
		if fromZero.DeltaPct != nil || fromZero.HasData == nil || !*fromZero.HasData || fromZero.HasPriorData == nil || !*fromZero.HasPriorData || fromZero.Value != 5 {
			t.Errorf("person churn from a measured zero = value %v delta_pct %s has_data %s has_prior_data %s, want 5, null, true, true", fromZero.Value, showDelta(fromZero.DeltaPct), flag(fromZero.HasData), flag(fromZero.HasPriorData))
		}
		zero := restSummaryDeltas(t, mux, "zero-zero-surfaces", target)["churn"]
		if zero.DeltaPct == nil || *zero.DeltaPct != 0 {
			t.Errorf("person churn 0 -> 0 delta_pct = %s, want a true 0", showDelta(zero.DeltaPct))
		}
	})
}
