package investment

import (
	"context"
	"errors"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// CHAOS-9622 (D5999): the REST investment breakdown and the sunburst read the COUNT, never effort_value, and the
// response names the measure and the sum its shares are shares of.
func TestRestInvestmentReadsTheCountMeasure(t *testing.T) {
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
	for name, query := range map[string]string{"breakdown": breakdown, "sunburst": sunburst} {
		if !strings.Contains(query, "sum(subcategory_kv.2 * 1.0 * 1.0) AS value") {
			t.Errorf("%s query does not sum the COUNT:\n%s", name, query)
		}
		if strings.Contains(query, "effort_value *") {
			t.Errorf("%s query weighs by effort_value:\n%s", name, query)
		}
	}
	if got.Measure != "COUNT" || got.Denominator == nil || *got.Denominator != 3 {
		t.Errorf("measure = %q denominator = %v, want COUNT and 3 (the sum of the themes)", got.Measure, got.Denominator)
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
