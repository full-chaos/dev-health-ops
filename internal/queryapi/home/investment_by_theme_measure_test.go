package home

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type measureCapturingClient struct{ statements []string }

func (c *measureCapturingClient) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	if strings.Contains(statement, "AS sized_units") {
		return &measureOneRow{}, nil
	}
	return measureEmptyRows{}, nil
}

type measureEmptyRows struct{}

func (measureEmptyRows) Next() bool        { return false }
func (measureEmptyRows) Scan(...any) error { return nil }
func (measureEmptyRows) Err() error        { return nil }
func (measureEmptyRows) Close() error      { return nil }

// measureOneRow is the one all-zero row of an aggregate read over no unit.
type measureOneRow struct{ done bool }

func (r *measureOneRow) Next() bool {
	if r.done {
		return false
	}
	r.done = true
	return true
}
func (*measureOneRow) Scan(...any) error { return nil }
func (*measureOneRow) Err() error        { return nil }
func (*measureOneRow) Close() error      { return nil }

// CHAOS-9622, CHAOS-9624 (D5999): the tile's allocation is the issue size of the units (1 when unsized), never their
// effort_value; the churn_loc line stays the explicit lines-of-code figure.
func TestInvestmentByThemeReadsTheIssueSizeMeasureWhenAsked(t *testing.T) {
	client := &measureCapturingClient{}
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	if _, err := fetchInvestmentThemeAllocation(context.Background(), client, Filters{TileMeasure: "ISSUE_SIZE"}, "org-1", day, day.AddDate(0, 0, 7), day); err != nil {
		t.Fatal(err)
	}
	var theme string
	for _, statement := range client.statements {
		if strings.Contains(statement, "AS churn_loc") {
			theme = statement
		}
	}
	if theme == "" {
		t.Fatalf("no theme read ran: %v", client.statements)
	}
	if !strings.Contains(theme, "toFloat64(sum(subcategory_kv.2 * 1.0 * 1.0 * unit_size_weight)) AS value") {
		t.Errorf("the tile does not sum the issue size:\n%s", theme)
	}
	if strings.Contains(theme, "toFloat64(sum(subcategory_kv.2 * effort_value") {
		t.Errorf("the tile weighs by effort_value:\n%s", theme)
	}
	if !strings.Contains(theme, "AS churn_loc") {
		t.Errorf("the explicit churn_loc line is gone:\n%s", theme)
	}
}

// CHAOS-9624: by default the tile sums the work-unit COUNT; a request that does not ask for the sized tile never
// reads a unit size.
func TestInvestmentByThemeReadsTheCountMeasureByDefault(t *testing.T) {
	client := &measureCapturingClient{}
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	if _, err := fetchInvestmentThemeAllocation(context.Background(), client, Filters{}, "org-1", day, day.AddDate(0, 0, 7), day); err != nil {
		t.Fatal(err)
	}
	var theme string
	for _, statement := range client.statements {
		if strings.Contains(statement, "AS churn_loc") {
			theme = statement
		}
		if strings.Contains(statement, "AS sized_units") {
			t.Errorf("the default tile read the sizing coverage:\n%s", statement)
		}
	}
	if !strings.Contains(theme, "toFloat64(sum(subcategory_kv.2 * 1.0 * 1.0)) AS value") || strings.Contains(theme, "unit_size_weight") {
		t.Errorf("the default tile does not sum the COUNT:\n%s", theme)
	}
}
