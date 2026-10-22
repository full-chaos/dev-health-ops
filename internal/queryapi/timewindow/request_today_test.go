package timewindow

import (
	"context"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
)

func TestRequestToday_OneReadingPerOperation(t *testing.T) {
	start := time.Date(2026, 10, 22, 23, 59, 59, 0, time.UTC)
	ctx := graphql.WithOperationContext(context.Background(), &graphql.OperationContext{Stats: graphql.Stats{OperationStart: start}})
	later := func() time.Time { return start.Add(2 * time.Second) } // the next UTC day
	if got := RequestToday(ctx, later); !got.Equal(start) {
		t.Fatalf("inside an operation the clock is the operation start, got %s", got)
	}
	if got := RequestToday(context.Background(), later); !got.Equal(later()) {
		t.Fatalf("outside an operation the fallback is read, got %s", got)
	}
}
