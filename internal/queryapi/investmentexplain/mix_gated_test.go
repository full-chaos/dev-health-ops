package investmentexplain

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
)

func gatedFilters() BreakdownFilters {
	return BreakdownFilters{
		OrgID:   "org-1",
		StartTS: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTS:   time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC),
		Measure: MeasureCount,
	}
}

func TestMixReadsLeaveOutEvidenceGatedUnits(t *testing.T) {
	reads := map[string]func(*Reader) error{
		"breakdown": func(r *Reader) error {
			_, err := r.FetchInvestmentBreakdown(context.Background(), gatedFilters())
			return err
		},
		"theme effort": func(r *Reader) error {
			_, err := r.FetchInvestmentThemeEffort(context.Background(), gatedFilters())
			return err
		},
		"sizing coverage": func(r *Reader) error {
			_, err := r.FetchSizingCoverage(context.Background(), gatedFilters())
			return err
		},
	}
	for name, run := range reads {
		client := &capturingClient{}
		reader, err := NewReader(client)
		if err != nil {
			t.Fatal(err)
		}
		_ = run(reader)
		if client.calls == 0 {
			t.Fatalf("%s: no query captured", name)
		}
		if !strings.Contains(client.lastQuery, analytics.CategorizedOnlySQL) {
			t.Errorf("%s does not exclude gated units:\n%s", name, client.lastQuery)
		}
	}
}

func TestMockFixtureCountKeepsEveryUnit(t *testing.T) {
	client := &capturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.FetchMockFixtureInvestmentRowCount(context.Background(), gatedFilters()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(client.lastQuery, analytics.CategorizedOnlySQL) {
		t.Errorf("the provenance count must see every unit:\n%s", client.lastQuery)
	}
}
