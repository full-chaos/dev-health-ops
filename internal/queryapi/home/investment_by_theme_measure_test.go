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
	return measureEmptyRows{}, nil
}

type measureEmptyRows struct{}

func (measureEmptyRows) Next() bool        { return false }
func (measureEmptyRows) Scan(...any) error { return nil }
func (measureEmptyRows) Err() error        { return nil }
func (measureEmptyRows) Close() error      { return nil }

// CHAOS-9622 (D5999): the tile's allocation is the COUNT of the units, never their effort_value. CHAOS-9695: the
// tile carries no lines-of-code figure.
func TestInvestmentByThemeReadsTheCountMeasure(t *testing.T) {
	client := &measureCapturingClient{}
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	if _, err := fetchInvestmentThemeAllocation(context.Background(), client, Filters{}, "org-1", day, day.AddDate(0, 0, 7), day); err != nil {
		t.Fatal(err)
	}
	var theme string
	for _, statement := range client.statements {
		if strings.Contains(statement, "AS prs") {
			theme = statement
		}
	}
	if theme == "" {
		t.Fatalf("no theme read ran: %v", client.statements)
	}
	if !strings.Contains(theme, "toFloat64(sum(subcategory_kv.2 * 1.0 * 1.0)) AS value") {
		t.Errorf("the tile does not sum the COUNT:\n%s", theme)
	}
	if strings.Contains(theme, "toFloat64(sum(subcategory_kv.2 * effort_value") {
		t.Errorf("the tile weighs by effort_value:\n%s", theme)
	}
	if strings.Contains(theme, "churn_loc") {
		t.Errorf("the tile reads churn_loc:\n%s", theme)
	}
}
