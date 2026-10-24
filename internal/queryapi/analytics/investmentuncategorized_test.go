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
	if strings.Contains(q.sql, "categorization_status NOT IN") {
		t.Errorf("total must keep counting every unit, got: %s", q.sql)
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
			rows: [][]any{{c.total, uint64(0), 0.0, 0.0, uint64(0), uint64(0), uint64(0), uint64(0), c.total, c.unc}},
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
