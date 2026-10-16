package aianalytics

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

type nullRowsClient struct{ rows [][]any }

func (c nullRowsClient) Query(context.Context, string, []clickhouse.Binding) (clickhouse.RowScanner, error) {
	return &scriptedRows{rows: c.rows}, nil
}

// CHAOS-9865: the engagement day column is typed Nullable(Date) by the server. A NULL day is
// no data: the row is skipped, it is never read as 0001-01-01 and folded into one bogus day.
func TestLoadEngagementSkipsANullDay(t *testing.T) {
	day := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	client := nullRowsClient{rows: [][]any{
		{"human", nil, uint64(1), nil, int64(2), int64(3)},
		{"human", day, uint64(4), nil, int64(5), int64(6)},
	}}
	got, err := loadEngagement(context.Background(), client, "org", day, day, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Day != "2026-08-02" || got[0].PrsWithFirstReview != 4 {
		t.Fatalf("rows = %+v, want only the row of 2026-08-02", got)
	}
}
