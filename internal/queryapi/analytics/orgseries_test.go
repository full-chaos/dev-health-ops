package analytics

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

func orgSeriesClient(dimRows, orgRows [][]any) *routingFakeClient {
	return (&routingFakeClient{}).
		on("AS numerator", &fakeRowScanner{rows: orgRows}).
		on("AS dimension_value", &fakeRowScanner{rows: dimRows})
}

func TestResolveOneTimeseries_OrgSeriesIsTheCountRateNotTheMeanOfTeams(t *testing.T) {
	// team-a 1/1 = 100%, team-b 1/9 = 11.1%: the mean of the team rates is 55.6%, the org rate 2/10 = 20%.
	client := orgSeriesClient(
		[][]any{
			{mustTime("2026-01-01"), "team-a", 100.0},
			{mustTime("2026-01-01"), "team-b", 11.1},
		},
		[][]any{{mustTime("2026-01-01"), 2.0, 10.0}},
	)
	got, err := resolveOneTimeseries(context.Background(), client, "org-1", tsInput(model.DimensionInputTeam, model.MeasureInputPipelineSuccessRate), false, nil)
	if err != nil {
		t.Fatalf("resolveOneTimeseries: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d results, want 2 team series + 1 org series: %+v", len(got), got)
	}
	for _, r := range got[:2] {
		if r.Scope != model.TimeseriesScopeDimension {
			t.Errorf("team series %q scope = %q, want DIMENSION", r.DimensionValue, r.Scope)
		}
	}
	org := got[2]
	if org.Scope != model.TimeseriesScopeOrg || org.DimensionValue != "org" || len(org.Buckets) != 1 || org.Buckets[0].Value == nil {
		t.Fatalf("org series = %+v", org)
	}
	if v := *org.Buckets[0].Value; math.Abs(v-20) > 1e-9 {
		t.Fatalf("org value = %v, want 20 (2/10), not the team mean 55.6", v)
	}
}

func TestResolveOneTimeseries_OrgSeriesNilWhenNoDenominator(t *testing.T) {
	client := orgSeriesClient(nil, [][]any{
		{mustTime("2026-01-01"), 0.0, 0.0},
		{mustTime("2026-01-02"), nil, nil},
	})
	got, err := resolveOneTimeseries(context.Background(), client, "org-1", tsInput(model.DimensionInputTeam, model.MeasureInputCoverageLinePct), false, nil)
	if err != nil {
		t.Fatalf("resolveOneTimeseries: %v", err)
	}
	if len(got) != 1 || got[0].Scope != model.TimeseriesScopeOrg || len(got[0].Buckets) != 2 {
		t.Fatalf("got %+v", got)
	}
	for _, b := range got[0].Buckets {
		if b.Value != nil {
			t.Errorf("bucket %v value = %v, want nil (no measurement is not a rate of 0)", b.Date, *b.Value)
		}
	}
}

func TestResolveOneTimeseries_NoOrgSeriesForOtherMeasures(t *testing.T) {
	for _, m := range []model.MeasureInput{model.MeasureInputTestFlakeRate, model.MeasureInputTestPassRate, model.MeasureInputPipelineDurationP95} {
		client := orgSeriesClient([][]any{{mustTime("2026-01-01"), "team-a", 1.0}}, nil)
		got, err := resolveOneTimeseries(context.Background(), client, "org-1", tsInput(model.DimensionInputTeam, m), false, nil)
		if err != nil {
			t.Fatalf("%s: %v", m, err)
		}
		if len(got) != 1 || got[0].Scope != model.TimeseriesScopeDimension {
			t.Errorf("%s: got %+v, want the one DIMENSION series only", m, got)
		}
		for _, c := range client.calls {
			if c == "AS numerator" {
				t.Errorf("%s: an org query was sent", m)
			}
		}
	}
}

func TestCompileOrgTimeseries_UsesCountsAndIgnoresDimension(t *testing.T) {
	req, err := TimeseriesRequestFromInput(tsInput(model.DimensionInputRepo, model.MeasureInputCoverageLinePct))
	if err != nil {
		t.Fatal(err)
	}
	q, ok, err := CompileOrgTimeseries(req, "org-1", 30, false, nil)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	for _, want := range []string{"lines_covered", "lines_total", "testops_coverage_metrics_daily", "GROUP BY bucket\n"} {
		if !strings.Contains(q.sql, want) {
			t.Errorf("sql lacks %q:\n%s", want, q.sql)
		}
	}
	if strings.Contains(q.sql, "AVG(") || strings.Contains(q.sql, "dimension_value") {
		t.Errorf("org sql must be count-based and not grouped by dimension:\n%s", q.sql)
	}
	if _, ok, _ := CompileOrgTimeseries(req, "org-1", 30, true, nil); ok {
		t.Error("investment path must not compile an org series")
	}
}
