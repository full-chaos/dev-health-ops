package heatmap

import (
	"context"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type nullRowsClient struct{ rows [][]any }

func (c nullRowsClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return &fixtureRowScanner{rows: c.rows}, nil
}

// CHAOS-9865: the review-wait value is typed Nullable(Float64) by the server. A NULL value is no data:
// the cell is absent, never a 0-hour cell.
func TestFetchReviewWaitDensitySkipsANullValue(t *testing.T) {
	client := nullRowsClient{rows: [][]any{
		{int32(1), int32(9), nil},
		{int32(2), int32(10), float64(3.5)},
	}}
	got, err := fetchReviewWaitDensity(context.Background(), client, time.Time{}, time.Time{}, "", nil, "org")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Weekday != 2 || got[0].Value != 3.5 {
		t.Fatalf("rows = %+v, want only the cell of weekday 2", got)
	}
}

// CHAOS-9865: first_review_at is typed Nullable(DateTime64). A NULL is no data: the evidence item is
// absent, never an item that serializes first_review_at as 0001-01-01.
func TestFetchReviewWaitEvidenceSkipsANullFirstReviewAt(t *testing.T) {
	created := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	first := created.Add(2 * time.Hour)
	client := nullRowsClient{rows: [][]any{
		{"repo-a", uint32(1), "no review yet", created, nil, nil, nil},
		{"repo-a", uint32(2), "reviewed", created, first, nil, nil},
	}}
	got, err := fetchReviewWaitEvidence(context.Background(), client, time.Time{}, time.Time{}, 1, 9, "", nil, 10, "org")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Number != 2 || !got[0].FirstReviewAt.Equal(first) {
		t.Fatalf("items = %+v, want only PR 2", got)
	}
}
