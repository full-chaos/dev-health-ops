package analytics

import (
	"context"
	"log/slog"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// A timeseries or breakdown statement of a batch that the client's cancel
// ended reports the cancel in its own phase (one INFO line, one cancel
// report). On a live request or one whose deadline passed the same error is
// no cancel. Either way Resolve still returns the error.
func TestATimeseriesOrBreakdownTheClientCancelledIsReportedAsACancel(t *testing.T) {
	cancelledByClient := func() error {
		return &fakeOperationError{operation: "query", cause: &clickhousedriver.Exception{
			Code: 735, Name: "QUERY_WAS_CANCELLED_BY_CLIENT", Message: "Query was cancelled",
		}}
	}
	phases := []struct {
		phase string
		match string
		batch model.AnalyticsRequestInput
	}{
		{"timeseries", "repo_id AS dimension_value", model.AnalyticsRequestInput{
			Timeseries: []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
		}},
		{"breakdown", "LIMIT {top_n:UInt32}", model.AnalyticsRequestInput{
			Breakdowns: []model.BreakdownRequestInput{bdInput(model.DimensionInputRepo, model.MeasureInputCount)},
		}},
	}
	contexts := []struct {
		name       string
		make       func(t *testing.T) context.Context
		wantCancel bool
	}{
		{"the client closed the request", func(*testing.T) context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}, true},
		{"the request is alive", func(*testing.T) context.Context { return context.Background() }, false},
	}
	for _, phase := range phases {
		for _, request := range contexts {
			t.Run(phase.phase+"/"+request.name, func(t *testing.T) {
				records := captureSlog(t)
				var cancels []string
				origCancel := recordClientCancel
				recordClientCancel = func(ctx context.Context, name string, attrs ...any) {
					cancels = append(cancels, name)
					origCancel(ctx, name, attrs...)
				}
				t.Cleanup(func() { recordClientCancel = origCancel })

				client := &routingFakeClient{}
				client.onErr(phase.match, cancelledByClient())
				if _, err := Resolve(request.make(t), client, "org-1", phase.batch); err == nil {
					t.Fatal("Resolve returned no error")
				}

				cancelLines := 0
				for _, record := range *records {
					if record.msg == "analytics: query cancelled by the client" && record.attrs["phase"] == phase.phase {
						if record.level != slog.LevelInfo {
							t.Errorf("the cancel line is at %v, want INFO", record.level)
						}
						cancelLines++
					}
				}
				want := 0
				if request.wantCancel {
					want = 1
				}
				if len(cancels) != want || cancelLines != want {
					t.Errorf("cancel reports = %v, cancel lines = %d; want %d of each", cancels, cancelLines, want)
				}
			})
		}
	}
}
