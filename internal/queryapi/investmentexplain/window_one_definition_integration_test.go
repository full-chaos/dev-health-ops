//go:build integration

package investmentexplain

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/timewindow"
)

// subcategoryOf is the subcategory of the unit that ends offset days from today (offset 0 to -12).
func subcategoryOf(offset int) string { return units.SortedSubcategories[-offset] }

// TestExplainAndTheInvestmentPageReadOneWindowForANamedEnd pins CHAOS-10118 / D6101 against a real ClickHouse: for the
// same named end date, the explain reader (the window of timewindow.Compute, the REST window) and the GraphQL
// breakdown (the page's dateRange) read the SAME work units. A unit whose end day is the named end counts in both; a unit
// of the day after does not; and an end of today (or later) reads the trailing window, the last day yesterday, in both,
// with a unit that ended today in neither.
func TestExplainAndTheInvestmentPageReadOneWindowForANamedEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)
	const org = "investmentexplain-one-window-org"

	today := time.Now().UTC().Truncate(24 * time.Hour)
	day := func(offset int) time.Time { return today.AddDate(0, 0, offset) }
	computed := today.Add(-time.Hour)
	// One unit per end day, each in its own subcategory so a boundary unit is seen on its own: the unit that ends on day
	// -k has subcategory number k of the taxonomy.
	type seeded struct {
		id    string
		endAt int // day offset of its to_ts, from today
	}
	var seededUnits []seeded
	for offset := -12; offset <= 0; offset++ {
		seededUnits = append(seededUnits, seeded{fmt.Sprintf("wu-%+03d", offset), offset})
	}
	for _, u := range seededUnits {
		sub := subcategoryOf(u.endAt)
		theme := units.ThemeOf(sub)
		to := day(u.endAt).Add(6 * time.Hour)
		if err := admin.Exec(ctx, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, ?, 'github', 'churn_loc', 10, ?, ?, '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			u.id, to.Add(-time.Hour), to, uuid.New(), map[string]float64{theme: 1}, map[string]float64{sub: 1}, computed, org); err != nil {
			t.Fatalf("seed %s: %v", u.id, err)
		}
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	start := day(-10)
	useInvestment := true
	for _, c := range []struct {
		name string
		end  time.Time
	}{
		{"a past end counts its own day", day(-4)},
		{"yesterday", day(-1)},
		{"today is not a day of the window", day(0)},
		{"a future end is clipped", day(5)},
	} {
		// The REST window of the same end (the explain's own).
		last := c.end
		window, err := timewindow.Compute(14, 14, &start, &last, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		rows, err := reader.FetchInvestmentBreakdown(ctx, BreakdownFilters{
			OrgID: org, StartTS: window.StartDay, EndTS: window.EndDay, DatedByEnd: true, Measure: MeasureCount,
		})
		if err != nil {
			t.Fatalf("%s: explain read: %v", c.name, err)
		}
		explainSubs := map[string]float64{}
		for _, row := range rows {
			explainSubs[row.Subcategory] += row.Value
		}

		result, err := analytics.Resolve(ctx, client, org, model.AnalyticsRequestInput{
			UseInvestment: &useInvestment,
			Breakdowns: []model.BreakdownRequestInput{{
				Dimension: model.DimensionInputSubcategory, Measure: model.MeasureInputCount, TopN: 100,
				DateRange: &model.DateRangeInput{StartDate: graphqldate.New(start), EndDate: graphqldate.New(c.end)},
			}},
		})
		if err != nil {
			t.Fatalf("%s: page read: %v", c.name, err)
		}
		pageSubs := map[string]float64{}
		for _, item := range result.Breakdowns[0].Items {
			if item.Value != nil {
				pageSubs[item.Key] = *item.Value
			}
		}

		// The units the window holds: those whose end day is in [start, last final day or the named end].
		lastCounted := c.end
		if yesterday := day(-1); lastCounted.After(yesterday) {
			lastCounted = yesterday
		}
		want := map[string]float64{}
		for _, u := range seededUnits {
			d := day(u.endAt)
			if !d.Before(start) && !d.After(lastCounted) {
				want[subcategoryOf(u.endAt)] = 1
			}
		}
		if len(want) < 3 {
			t.Fatalf("%s: the seeded window holds %d units, too few to prove anything", c.name, len(want))
		}
		for _, got := range []struct {
			reader string
			themes map[string]float64
		}{{"explain", explainSubs}, {"page", pageSubs}} {
			if len(got.themes) != len(want) {
				t.Errorf("%s: %s reads %d subcategories, the window of %s..%s holds %d\n got %v\nwant %v", c.name, got.reader, len(got.themes), start.Format(time.DateOnly), lastCounted.Format(time.DateOnly), len(want), got.themes, want)
				continue
			}
			for key, value := range want {
				if got.themes[key] != value {
					t.Errorf("%s: %s reads %s = %v, want %v", c.name, got.reader, key, got.themes[key], value)
				}
			}
		}
	}
}
