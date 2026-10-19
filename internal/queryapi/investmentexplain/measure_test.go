package investmentexplain

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// CHAOS-9809 (D5999, D6027): a surface reads ISSUE_SIZE only when it carries the sizing coverage in the same answer
// (REST /investment, the explanation); the others stay on COUNT until they do.
func TestSurfaceMeasuresServeIssueSizeOnlyWithCoverage(t *testing.T) {
	for name, tc := range map[string]struct{ got, want string }{
		"REST /investment":    {RESTInvestmentMeasure, MeasureIssueSize},
		"explanation default": {ExplanationDefaultMeasure, MeasureIssueSize},
		"Home tile":           {HomeTileMeasure, MeasureCount},
		"Home tile (sized)":   {HomeTileSizedMeasure, MeasureIssueSize},
		"sunburst":            {SunburstMeasure, MeasureCount},
		"Operating Review":    {OperatingReviewMeasure, MeasureCount},
	} {
		if tc.got != tc.want {
			t.Errorf("%s measure = %q, want %q", name, tc.got, tc.want)
		}
	}
}

// CHAOS-9622: the one SQL definition of a measure. COUNT never reads effort_value (the changed lines of
// code). CHAOS-9695: there is no lines-of-code measure to ask for.
func TestMeasureValueSQL(t *testing.T) {
	count, err := MeasureValueSQL(MeasureCount, "allocation_weight", "owner_share")
	if err != nil {
		t.Fatal(err)
	}
	if count != "sum(subcategory_kv.2 * allocation_weight * owner_share)" {
		t.Errorf("COUNT = %q", count)
	}
	if strings.Contains(count, "effort_value") {
		t.Errorf("COUNT reads effort_value: %q", count)
	}
	for _, name := range []string{"", "EFFORT", "effort_value", "SIZE", "CHURN_LOC", "churn_loc"} {
		if _, err := MeasureValueSQL(name, "1.0", "1.0"); !errors.Is(err, ErrMeasureRequired) {
			t.Errorf("MeasureValueSQL(%q) error = %v, want ErrMeasureRequired: there is no default weight", name, err)
		}
	}
}

// FetchInvestmentBreakdown reads MeasureValueSQL, the one definition: its query holds the shared expression of the
// measure it is given, scoped and not.
func TestBreakdownReadsTheSharedMeasureSQL(t *testing.T) {
	for _, measure := range []string{MeasureCount} {
		client := &capturingClient{}
		reader, err := NewReader(client)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.FetchInvestmentBreakdown(context.Background(), BreakdownFilters{
			OrgID: "org-1", StartTS: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), Measure: measure,
		}); err != nil {
			t.Fatalf("%s: %v", measure, err)
		}
		want, err := MeasureValueSQL(measure, "1.0", "1.0")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(client.lastQuery, want+" AS value") {
			t.Errorf("%s: the breakdown query lacks %q:\n%s", measure, want, client.lastQuery)
		}
	}
}

// No read can fall back to a weight: a read that names no measure is refused before any query.
func TestReadsWithoutAMeasureAreRefused(t *testing.T) {
	client := &capturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	filters := BreakdownFilters{OrgID: "org-1", StartTS: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)}
	if _, err := reader.FetchInvestmentBreakdown(context.Background(), filters); !errors.Is(err, ErrMeasureRequired) {
		t.Errorf("FetchInvestmentBreakdown error = %v, want ErrMeasureRequired", err)
	}
	if _, err := reader.FetchInvestmentThemeEffort(context.Background(), filters); !errors.Is(err, ErrMeasureRequired) {
		t.Errorf("FetchInvestmentThemeEffort error = %v, want ErrMeasureRequired", err)
	}
	if client.calls != 0 {
		t.Errorf("%d queries ran for a read with no measure, want none", client.calls)
	}
}

// The theme read weighs by the measure it is given and by nothing else.
func TestThemeEffortReadWeighsByTheNamedMeasure(t *testing.T) {
	for _, tc := range []struct {
		measure, want, notWant string
	}{
		{MeasureCount, "toFloat64(sum(subcategory_kv.2 * 1.0 * 1.0)) AS value", "toFloat64(sum(subcategory_kv.2 * effort_value"},
	} {
		client := &capturingClient{}
		reader, err := NewReader(client)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.FetchInvestmentThemeEffort(context.Background(), BreakdownFilters{
			OrgID: "org-1", StartTS: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), Measure: tc.measure,
		}); err != nil {
			t.Fatalf("%s: %v", tc.measure, err)
		}
		if !strings.Contains(client.lastQuery, tc.want) {
			t.Errorf("%s: query lacks %q:\n%s", tc.measure, tc.want, client.lastQuery)
		}
		if tc.notWant != "" && strings.Contains(client.lastQuery, tc.notWant) {
			t.Errorf("%s: query weighs by effort_value:\n%s", tc.measure, client.lastQuery)
		}
	}
}
