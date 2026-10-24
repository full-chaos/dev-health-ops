package investment

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func TestComputeQualityStats_UncategorizedCount(t *testing.T) {
	zero, four := 0, 4
	cases := []struct {
		name  string
		row   QualityStatsRow
		found bool
		want  *int
	}{
		{"some gated", QualityStatsRow{Total: 10, UnknownCount: 10, UncategorizedCount: 4}, true, &four},
		{"none gated is zero", QualityStatsRow{Total: 10, UnknownCount: 10}, true, &zero},
		{"no units is unknown", QualityStatsRow{}, true, nil},
		{"no row is unknown", QualityStatsRow{UncategorizedCount: 4}, false, nil},
	}
	for _, c := range cases {
		got := computeQualityStats(c.row, c.found).UncategorizedCount
		switch {
		case c.want == nil && got != nil:
			t.Errorf("%s: got %d, want nil", c.name, *got)
		case c.want != nil && (got == nil || *got != *c.want):
			t.Errorf("%s: got %v, want %d", c.name, got, *c.want)
		}
	}
}

func TestFetchInvestmentQualityStats_SelectsAndScansUncategorizedCount(t *testing.T) {
	var seen string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		seen = query
		return &fixtureRowScanner{rows: [][]any{{uint64(10), uint64(0), 0.0, 0.0, uint64(0), uint64(0), uint64(0), uint64(0), uint64(10), uint64(4)}}}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	row, found, err := reader.FetchInvestmentQualityStats(context.Background(), QualityStatsFilters{OrgID: "org-1", StartTS: day(2026, 1, 1), EndTS: day(2026, 1, 31)})
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if row.UncategorizedCount != 4 {
		t.Errorf("UncategorizedCount = %d, want 4", row.UncategorizedCount)
	}
	if !strings.Contains(seen, "countIf(work_unit_investments.categorization_status IN ('insufficient_evidence', 'no_text_sources')) AS uncategorized_count") {
		t.Errorf("REST quality stats SQL lacks the uncategorized count: %s", seen)
	}
}
