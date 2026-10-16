package security

import (
	"context"
	"testing"
	"time"
)

// CHAOS-9865: the trend day column is typed Nullable(Date) by the server (the fixed_at branch of the
// UNION). A NULL day is no data: the point is skipped, never plotted at 0001-01-01.
func TestReadTrendSkipsANullDay(t *testing.T) {
	day := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	client := &recordingClient{responses: [][][]any{{
		{nil, uint64(9), uint64(9)},
		{day, uint64(2), uint64(1)},
	}}}
	got, err := readTrend(context.Background(), client, filterClause{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Opened != 2 || got[0].Fixed != 1 {
		t.Fatalf("points = %+v, want only the point of 2026-08-02", got)
	}
}
