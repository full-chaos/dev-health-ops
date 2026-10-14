package investmentexplain

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestInvestmentShareMeasureIsCount(t *testing.T) {
	if InvestmentShareMeasure != MeasureCount {
		t.Fatalf("InvestmentShareMeasure = %q, want %q (the Investment page's measure, D5999)", InvestmentShareMeasure, MeasureCount)
	}
}

// CHAOS-9622: the one SQL definition of a measure. COUNT never reads effort_value (the changed lines of
// code); CHURN_LOC reads it only for churn_loc units and only when asked.
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
	lines, err := MeasureValueSQL(MeasureChurnLOC, "allocation_weight", "owner_share")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lines, "effort_metric = 'churn_loc'") || !strings.Contains(lines, "effort_value") {
		t.Errorf("CHURN_LOC = %q, want the explicit lines-of-code expression", lines)
	}
	for _, name := range []string{"", "EFFORT", "effort_value", "SIZE"} {
		if _, err := MeasureValueSQL(name, "1.0", "1.0"); !errors.Is(err, ErrMeasureRequired) {
			t.Errorf("MeasureValueSQL(%q) error = %v, want ErrMeasureRequired: there is no default weight", name, err)
		}
	}
}

// The measure FetchInvestmentBreakdown reads (valueExpr) and the one the theme and sunburst reads take
// (MeasureValueSQL) are one definition.
func TestBreakdownValueExprIsTheSharedMeasureSQL(t *testing.T) {
	for _, measure := range []string{MeasureCount, MeasureChurnLOC} {
		for _, scoped := range []bool{false, true} {
			scopeSQL, weight := "", "1.0"
			if scoped {
				scopeSQL, weight = " AND repo_id IN {scope_ids:Array(String)}", "allocation_weight"
			}
			filters := BreakdownFilters{Measure: measure}
			want, err := MeasureValueSQL(measure, weight, "owner_share")
			if err != nil {
				t.Fatal(err)
			}
			if got := filters.valueExpr(scopeSQL, "owner_share"); got != want {
				t.Errorf("%s scoped=%v: valueExpr = %q, MeasureValueSQL = %q", measure, scoped, got, want)
			}
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
		{MeasureChurnLOC, "toFloat64(sum(if(effort_metric = 'churn_loc', subcategory_kv.2 * effort_value, 0) * 1.0)) AS value", ""},
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
