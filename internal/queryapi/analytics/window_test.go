package analytics

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/timewindow"
)

func withWindowToday(t *testing.T, day string) time.Time {
	t.Helper()
	today, err := time.Parse(time.DateOnly, day)
	if err != nil {
		t.Fatal(err)
	}
	prior := windowToday
	windowToday = func() time.Time { return today.Add(9 * time.Hour) }
	t.Cleanup(func() { windowToday = prior })
	return today
}

// TestGraphQLDateRangeBindsTheEndEveryOtherReaderBinds pins CHAOS-10118 / D6101: for every named end, the exclusive end a
// GraphQL request binds is the EndDay of the REST window (timewindow.Compute) for the same end, in all four request
// kinds and the evidence-quality window. Today is 2026-10-18.
func TestGraphQLDateRangeBindsTheEndEveryOtherReaderBinds(t *testing.T) {
	today := withWindowToday(t, "2026-10-18")
	start := today.AddDate(0, 0, -20)
	for _, endText := range []string{"2026-10-10", "2026-10-16", "2026-10-17", "2026-10-18", "2026-10-19", "2026-12-31"} {
		end, err := time.Parse(time.DateOnly, endText)
		if err != nil {
			t.Fatal(err)
		}
		rest, err := timewindow.Compute(14, 14, &start, &end, today)
		if err != nil {
			t.Fatal(err)
		}
		dr := &model.DateRangeInput{StartDate: graphqldate.New(start), EndDate: graphqldate.New(end)}
		bound := map[string]time.Time{}
		b, err := BreakdownRequestFromInput(model.BreakdownRequestInput{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dr, TopN: 10})
		if err != nil {
			t.Fatal(err)
		}
		bound["breakdown"] = b.EndDate.Time()
		ts, err := TimeseriesRequestFromInput(model.TimeseriesRequestInput{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dr, Interval: model.BucketIntervalInputDay})
		if err != nil {
			t.Fatal(err)
		}
		bound["timeseries"] = ts.EndDate.Time()
		s, err := SankeyRequestFromInput(model.SankeyRequestInput{Path: []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme}, Measure: model.MeasureInputCount, DateRange: dr, MaxNodes: 10, MaxEdges: 10})
		if err != nil {
			t.Fatal(err)
		}
		bound["sankey"] = s.EndDate.Time()
		f, err := FlowMatrixRequestFromInput(model.FlowMatrixRequestInput{Dimension: model.DimensionInputTeam, Measure: model.MeasureInputCount, DateRange: dr, MaxNodes: 10, MaxEdges: 10})
		if err != nil {
			t.Fatal(err)
		}
		bound["flowmatrix"] = f.EndDate.Time()
		_, qEnd, ok, err := analyticsQualityWindow(model.AnalyticsRequestInput{Breakdowns: []model.BreakdownRequestInput{{DateRange: dr}}})
		if err != nil || !ok {
			t.Fatalf("quality window: ok=%v err=%v", ok, err)
		}
		bound["evidence-quality"] = qEnd.Time()
		for kind, got := range bound {
			if !got.Equal(rest.EndDay) {
				t.Errorf("end %s: %s binds %s, the REST window's exclusive end is %s", endText, kind, got.Format(time.DateOnly), rest.EndDay.Format(time.DateOnly))
			}
			if got.After(today) {
				t.Errorf("end %s: %s binds %s, after today %s: a day that is not over is in the window", endText, kind, got.Format(time.DateOnly), today.Format(time.DateOnly))
			}
		}
	}
}

// TestEveryWindowedReadBindsOneExclusiveEnd pins that the work-unit reads and the daily reads read the same bound the
// same way: `< end` for both, never `<= end` against the end the request binds.
func TestEveryWindowedReadBindsOneExclusiveEnd(t *testing.T) {
	for _, text := range []string{InvestmentWindowClause(true), InvestmentWindowClause(false)} {
		if strings.Contains(text, "<= {end_date") {
			t.Errorf("investment window clause reads the end inclusively: %s", text)
		}
	}
	_, _, daily := nonInvestmentSourceAndDateFilter(MeasureCount)
	if !strings.Contains(daily, "day < {end_date:Date}") || strings.Contains(daily, "<= {end_date") {
		t.Errorf("daily window filter = %q, want `day < {end_date:Date}`", daily)
	}
}

// TestADateRangeThatEndsTodayOrLaterIsTheTrailingWindow: the web's dateRange [today-N, today] and an end in the future
// both read [today-N, today): the same rows as the default window of every REST reader.
func TestADateRangeThatEndsTodayOrLaterIsTheTrailingWindow(t *testing.T) {
	today := withWindowToday(t, "2026-10-18")
	start := graphqldate.New(today.AddDate(0, 0, -30))
	for _, end := range []time.Time{today, today.AddDate(0, 0, 1), today.AddDate(0, 0, 400)} {
		req, err := BreakdownRequestFromInput(model.BreakdownRequestInput{
			Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, TopN: 10,
			DateRange: &model.DateRangeInput{StartDate: start, EndDate: graphqldate.New(end)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := req.EndDate.Time().Format(time.DateOnly); got != "2026-10-18" {
			t.Errorf("end %s binds %s, want 2026-10-18 (exclusive; the last day read is yesterday)", end.Format(time.DateOnly), got)
		}
	}
}
