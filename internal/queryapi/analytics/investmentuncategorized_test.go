package analytics

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

func uncategorizedBatch() model.AnalyticsRequestInput {
	return model.AnalyticsRequestInput{
		Breakdowns:    []model.BreakdownRequestInput{bdInput(model.DimensionInputTheme, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
	}
}

func TestCompileInvestmentQualityStats_CountsUncategorizedUnits(t *testing.T) {
	q := compileInvestmentQualityStats("org-1", mustDate(t, "2026-01-01"), mustDate(t, "2026-01-31"), "", nil, nil, nil)
	want := "countIf(work_unit_investments.categorization_status IN ('insufficient_evidence', 'no_text_sources')) AS uncategorized_count"
	if !strings.Contains(q.sql, want) {
		t.Errorf("quality stats SQL lacks the uncategorized count, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "count() AS total,") {
		t.Errorf("total must keep counting every unit, got: %s", q.sql)
	}
}

func TestCompileInvestmentQualityStats_CountsSizedUnits(t *testing.T) {
	q := compileInvestmentQualityStats("org-1", mustDate(t, "2026-01-01"), mustDate(t, "2026-01-31"), "", nil, nil, nil)
	want := "countIf(work_unit_investments.categorization_status NOT IN ('insufficient_evidence', 'no_text_sources') AND unit_issue_size > 0) AS sized_unit_count"
	if !strings.Contains(q.sql, want) {
		t.Errorf("quality stats SQL lacks the sized count, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "unit_issue_size") || !strings.Contains(q.sql, "work_unit_issue_size") {
		t.Errorf("quality stats source is not wrapped by the issue-size source, got: %s", q.sql)
	}
}

func TestResolveEvidenceQualityStats_SizedUnitCount(t *testing.T) {
	cases := []struct {
		name  string
		total uint64
		sized uint64
		want  *int
	}{
		{"some sized", 10, 3, intPtr(3)},
		{"none sized is zero, not unknown", 10, 0, intPtr(0)},
		{"no units is unknown", 0, 0, nil},
	}
	for _, c := range cases {
		client := &routingFakeClient{}
		client.on("quality_known_count", &fakeRowScanner{
			rows: [][]any{{c.total, uint64(0), 0.0, 0.0, uint64(0), uint64(0), uint64(0), uint64(0), c.total, uint64(0), c.sized}},
		})
		got, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", uncategorizedBatch(), true, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		switch {
		case c.want == nil && got.SizedUnitCount != nil:
			t.Errorf("%s: SizedUnitCount = %d, want nil", c.name, *got.SizedUnitCount)
		case c.want != nil && (got.SizedUnitCount == nil || *got.SizedUnitCount != *c.want):
			t.Errorf("%s: SizedUnitCount = %v, want %d", c.name, got.SizedUnitCount, *c.want)
		}
	}
}

func TestResolveEvidenceQualityStats_UncategorizedCount(t *testing.T) {
	cases := []struct {
		name  string
		total uint64
		unc   uint64
		want  *int
	}{
		{"some gated", 10, 4, intPtr(4)},
		{"none gated is zero, not unknown", 10, 0, intPtr(0)},
		{"no units is unknown", 0, 0, nil},
	}
	for _, c := range cases {
		client := &routingFakeClient{}
		client.on("quality_known_count", &fakeRowScanner{
			rows: [][]any{{c.total, uint64(0), 0.0, 0.0, uint64(0), uint64(0), uint64(0), uint64(0), c.total, c.unc, uint64(0)}},
		})
		got, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", uncategorizedBatch(), true, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		switch {
		case c.want == nil && got.UncategorizedCount != nil:
			t.Errorf("%s: UncategorizedCount = %d, want nil", c.name, *got.UncategorizedCount)
		case c.want != nil && (got.UncategorizedCount == nil || *got.UncategorizedCount != *c.want):
			t.Errorf("%s: UncategorizedCount = %v, want %d", c.name, got.UncategorizedCount, *c.want)
		}
	}
}

func TestResolveEvidenceQualityStats_NoRowsLeavesUncategorizedUnknown(t *testing.T) {
	client := &routingFakeClient{}
	client.on("quality_known_count", &fakeRowScanner{rows: nil})
	got, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", uncategorizedBatch(), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.UncategorizedCount != nil {
		t.Errorf("UncategorizedCount = %d on an empty answer, want nil", *got.UncategorizedCount)
	}
}

func intPtr(v int) *int { return &v }
