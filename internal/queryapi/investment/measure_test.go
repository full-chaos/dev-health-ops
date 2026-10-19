package investment

import (
	"context"
	"errors"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// CHAOS-9809 (D5999, D6027, D6031): the REST investment breakdown reads the issue size (1 when unsized), never
// effort_value, and the response names the measure, the sum its shares are shares of, and the sizing coverage with
// the "mostly unsized" sentence. The sunburst, which carries no coverage, stays on COUNT.
func TestRestInvestmentReadsTheIssueSizeMeasure(t *testing.T) {
	var breakdown, sunburst string
	client := fakeQueryClient{t: t, handler: tablePresentHandler(func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "GROUP BY subcategory, theme"):
			breakdown = query
			return &fixtureRowScanner{rows: [][]any{
				{"maintenance.debt", "maintenance", 1.5},
				{"feature_delivery.roadmap", "feature_delivery", 1.0},
				{"risk.security", "risk", 0.5},
			}}, nil
		case strings.Contains(query, "GROUP BY theme, subcategory, scope"):
			sunburst = query
			return &fixtureRowScanner{}, nil
		case strings.Contains(query, "SELECT count() AS count"):
			return &fixtureRowScanner{rows: [][]any{{uint64(0)}}}, nil
		case strings.Contains(query, "AS sized_units"):
			return &fixtureRowScanner{rows: [][]any{{uint64(1), uint64(4)}}}, nil
		default:
			return &fixtureRowScanner{}, nil
		}
	})}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildResponse(context.Background(), reader, "org-1", Params{StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 8), ScopeLevel: "org"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildSunburstResponse(context.Background(), reader, "org-1", SunburstParams{StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 8), ScopeLevel: "org", Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(breakdown, "sum(subcategory_kv.2 * 1.0 * 1.0 * unit_size_weight) AS value") {
		t.Errorf("breakdown query does not sum the issue size:\n%s", breakdown)
	}
	if !strings.Contains(sunburst, "sum(subcategory_kv.2 * 1.0 * 1.0) AS value") || strings.Contains(sunburst, "unit_size_weight") {
		t.Errorf("sunburst query does not sum the COUNT:\n%s", sunburst)
	}
	for name, query := range map[string]string{"breakdown": breakdown, "sunburst": sunburst} {
		if strings.Contains(query, "effort_value *") {
			t.Errorf("%s query weighs by effort_value:\n%s", name, query)
		}
	}
	if got.Measure != "ISSUE_SIZE" || got.Denominator == nil || *got.Denominator != 3 {
		t.Errorf("measure = %q denominator = %v, want ISSUE_SIZE and 3 (the sum of the themes)", got.Measure, got.Denominator)
	}
	if got.SizingCoverage == nil || *got.SizingCoverage != 0.25 || got.UnsizedUnitCount == nil || *got.UnsizedUnitCount != 3 {
		t.Errorf("sizing = (%v, %v), want (0.25, 3)", got.SizingCoverage, got.UnsizedUnitCount)
	}
	if want := "Mostly unsized: 1 of 4 work units have an issue size. The other 3 have no size, so each counts as 1 work unit."; got.SizingNote != want {
		t.Errorf("sizing note = %q, want %q", got.SizingNote, want)
	}
}

func TestSunburstWithoutAMeasureIsRefused(t *testing.T) {
	client := &capturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.FetchInvestmentSunburst(context.Background(), SunburstFilters{OrgID: "org-1", StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 8)})
	if !errors.Is(err, investmentexplain.ErrMeasureRequired) {
		t.Fatalf("error = %v, want ErrMeasureRequired", err)
	}
	if client.lastQuery != "" {
		t.Errorf("a query ran for a sunburst with no measure:\n%s", client.lastQuery)
	}
}

// CHAOS-9809 (D6027): an ISSUE_SIZE answer always carries its coverage. A window with no unit says 0 unsized and no
// sizing share (there is no unit to take a share of), never no coverage at all.
func TestRestInvestmentOverAnEmptyWindowStillCarriesItsCoverage(t *testing.T) {
	client := fakeQueryClient{t: t, handler: tablePresentHandler(func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "SELECT count() AS count"):
			return &fixtureRowScanner{rows: [][]any{{uint64(0)}}}, nil
		case strings.Contains(query, "AS sized_units"):
			return &fixtureRowScanner{rows: [][]any{{uint64(0), uint64(0)}}}, nil
		default:
			return &fixtureRowScanner{}, nil
		}
	})}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildResponse(context.Background(), reader, "org-1", Params{StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 8), ScopeLevel: "org"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Measure != "ISSUE_SIZE" || got.UnsizedUnitCount == nil || *got.UnsizedUnitCount != 0 || got.SizingCoverage != nil || got.SizingNote != "" {
		t.Errorf("empty window: measure %q, unsized %v, coverage %v, note %q; want ISSUE_SIZE, 0, no share, no sentence", got.Measure, got.UnsizedUnitCount, got.SizingCoverage, got.SizingNote)
	}
}
