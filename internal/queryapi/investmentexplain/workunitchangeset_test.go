package investmentexplain

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type workUnitRowsClient struct{ rows [][]any }

func (c *workUnitRowsClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(query, "AS work_unit_count") {
		return &reflectScanner{rows: c.rows}, nil
	}
	return &reflectScanner{}, nil
}

func TestChangeSetOfIsTheStoredChurnAndNothingElse(t *testing.T) {
	str := func(s string) *string { return &s }
	num := func(f float64) *float64 { return &f }
	if got := changeSetOf(str("churn_loc"), num(12.5)); got == nil || got.Total != 13 || got.Additions != nil || got.Deletions != nil {
		t.Errorf("churn_loc 12.5 = %+v, want total 13 and no split", got)
	}
	for name, got := range map[string]*WorkUnitChangeSet{
		"hours":     changeSetOf(str("active_hours"), num(40)),
		"zero":      changeSetOf(str("churn_loc"), num(0)),
		"negative":  changeSetOf(str("churn_loc"), num(-3)),
		"no value":  changeSetOf(str("churn_loc"), nil),
		"no metric": changeSetOf(nil, num(9)),
	} {
		if got != nil {
			t.Errorf("%s = %+v, want no change set", name, got)
		}
	}
}

// The change set is a fact: the effort value and the order of the list are the work-unit count, whatever the unit's
// lines of code are (CHAOS-9695, D5999).
func TestWorkUnitEffortAndOrderDoNotDependOnTheChangeSet(t *testing.T) {
	end := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	row := func(id string, churn, count float64) []any {
		return []any{
			id, "work_unit", "Unit", end.AddDate(0, 0, -5), end, nil, nil, "churn_loc", churn, count,
			[]string{"feature_delivery"}, []float64{1}, []string{"feature_delivery.customer"}, []float64{1},
			"{}", 0.8, "high", "ok", "v1", "run-1", end,
		}
	}
	for name, rows := range map[string][][]any{
		"huge churn on the smaller count": {row("wu-big", 90000, 0.5), row("wu-small", 1, 1.0)},
		"the same counts, churn swapped":  {row("wu-big", 1, 0.5), row("wu-small", 90000, 1.0)},
	} {
		reader, err := NewReader(&workUnitRowsClient{rows: rows})
		if err != nil {
			t.Fatal(err)
		}
		units, err := reader.BuildWorkUnitInvestments(context.Background(), BuildWorkUnitInvestmentsOptions{OrgID: "o", StartTS: end.AddDate(0, 0, -7), EndTS: end, Limit: 10})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(units) != 2 || units[0].WorkUnitID != "wu-small" || units[1].WorkUnitID != "wu-big" {
			t.Fatalf("%s: order = %v, want wu-small (count 1) then wu-big (count 0.5)", name, units)
		}
		if units[0].Effort.Metric != "work_units" || units[0].Effort.Value != 1 || units[1].Effort.Value != 0.5 {
			t.Errorf("%s: effort = %+v / %+v, want work_units 1 and 0.5", name, units[0].Effort, units[1].Effort)
		}
		if units[0].ChangeSet == nil || units[1].ChangeSet == nil {
			t.Errorf("%s: a unit with churn has no change set", name)
		}
	}
}
