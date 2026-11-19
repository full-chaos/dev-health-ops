package analytics

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// Each timeseries or breakdown statement of a batch that the client's cancel
// ended reports the cancel in its own phase (one INFO line, one cancel
// report), not only the one whose error Resolve returns; the returned error
// carries the mark, so ReportedAsClientCancel (the presenter, a REST route)
// does not report it again. On a live request the same error is no cancel.
// Either way Resolve still returns the error.
func TestATimeseriesOrBreakdownTheClientCancelledIsReportedAsACancel(t *testing.T) {
	cancelledByClient := func() error {
		return &fakeOperationError{operation: "query", cause: &clickhousedriver.Exception{
			Code: 735, Name: "QUERY_WAS_CANCELLED_BY_CLIENT", Message: "Query was cancelled",
		}}
	}
	phases := []struct {
		name   string
		phases []string
		match  []string
		batch  model.AnalyticsRequestInput
	}{
		{"timeseries", []string{"timeseries"}, []string{"repo_id AS dimension_value"}, model.AnalyticsRequestInput{
			Timeseries: []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
		}},
		{"breakdown", []string{"breakdown"}, []string{"LIMIT {top_n:UInt32}"}, model.AnalyticsRequestInput{
			Breakdowns: []model.BreakdownRequestInput{bdInput(model.DimensionInputRepo, model.MeasureInputCount)},
		}},
		{"timeseries and breakdown", []string{"timeseries", "breakdown"}, []string{"LIMIT {top_n:UInt32}", "repo_id AS dimension_value"}, model.AnalyticsRequestInput{
			Timeseries: []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
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
			t.Run(phase.name+"/"+request.name, func(t *testing.T) {
				records := captureSlog(t)
				var cancels []string
				origCancel := recordClientCancel
				recordClientCancel = func(ctx context.Context, name string, attrs ...any) {
					cancels = append(cancels, name)
					origCancel(ctx, name, attrs...)
				}
				t.Cleanup(func() { recordClientCancel = origCancel })

				client := &routingFakeClient{}
				for _, match := range phase.match {
					client.onErr(match, cancelledByClient())
				}
				ctx := request.make(t)
				_, err := Resolve(ctx, client, "org-1", phase.batch)
				if err == nil {
					t.Fatal("Resolve returned no error")
				}
				if got := ReportedAsClientCancel(ctx, err, "caller"); got != request.wantCancel {
					t.Errorf("ReportedAsClientCancel of the returned error = %v, want %v", got, request.wantCancel)
				}

				var lines []string
				for _, record := range *records {
					if record.msg == "analytics: query cancelled by the client" {
						if record.level != slog.LevelInfo {
							t.Errorf("the cancel line is at %v, want INFO", record.level)
						}
						name, _ := record.attrs["phase"].(string)
						lines = append(lines, name)
					}
				}
				var want []string
				if request.wantCancel {
					want = phase.phases
				}
				if !sameStrings(cancels, want) || !sameStrings(lines, want) {
					t.Errorf("cancel reports = %v, cancel lines = %v; want %v of each (none for the caller)", cancels, lines, want)
				}
			})
		}
	}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	sorted := func(in []string) []string {
		out := append([]string(nil), in...)
		sort.Strings(out)
		return out
	}
	g, w := sorted(got), sorted(want)
	for i := range g {
		if g[i] != w[i] {
			return false
		}
	}
	return true
}

// A server cancel (394) of one read on a live request stays a failure when the
// client leaves during the next read: each read's end is judged when it ended.
func TestAServerCancelIsNotACancelWhenTheClientLeavesLater(t *testing.T) {
	records := captureSlog(t)
	var cancels []string
	origCancel, origEnd := recordClientCancel, contextErrAtEnd
	var captured sync.Once
	tsCaptured := make(chan struct{})
	recordClientCancel = func(ctx context.Context, name string, attrs ...any) {
		cancels = append(cancels, name)
		origCancel(ctx, name, attrs...)
	}
	contextErrAtEnd = func(ctx context.Context) error {
		err := origEnd(ctx)
		if err == nil {
			captured.Do(func() { close(tsCaptured) })
		}
		return err
	}
	t.Cleanup(func() { recordClientCancel, contextErrAtEnd = origCancel, origEnd })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &leavingClient{
		inner:   &routingFakeClient{},
		leaveOn: "LIMIT {top_n:UInt32}", wait: tsCaptured, leave: cancel,
	}
	client.inner.onErr("repo_id AS dimension_value", &fakeOperationError{operation: "query", cause: &clickhousedriver.Exception{Code: 394, Name: "QUERY_WAS_CANCELLED"}})
	client.inner.onErr("LIMIT {top_n:UInt32}", &fakeOperationError{operation: "query", cause: &clickhousedriver.Exception{Code: 735, Name: "QUERY_WAS_CANCELLED_BY_CLIENT"}})

	_, err := Resolve(ctx, client, "org-1", model.AnalyticsRequestInput{
		Timeseries: []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
		Breakdowns: []model.BreakdownRequestInput{bdInput(model.DimensionInputRepo, model.MeasureInputCount)},
	})
	if err == nil {
		t.Fatal("Resolve returned no error")
	}
	if !sameStrings(cancels, []string{"breakdown"}) {
		t.Errorf("cancel reports = %v, want only the breakdown", cancels)
	}
	lines := 0
	for _, record := range *records {
		if record.msg == "analytics: query cancelled by the client" {
			lines++
		}
	}
	if lines != 1 {
		t.Errorf("cancel lines = %d, want 1", lines)
	}
	if ctx.Err() == nil {
		t.Fatal("the client did not leave")
	}
	if ClientCancelled(ctx, err) || ReportedAsClientCancel(ctx, err, "caller") {
		t.Errorf("the returned timeseries error (394 on a live request) is judged a client cancel after the client left: %v", err)
	}
}

// leavingClient makes the client leave during the read of one statement, after
// the other read's end was judged.
type leavingClient struct {
	inner   *routingFakeClient
	leaveOn string
	wait    <-chan struct{}
	leave   func()
}

func (l *leavingClient) Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if strings.Contains(statement, l.leaveOn) {
		<-l.wait
		l.leave()
	}
	return l.inner.Query(ctx, statement, bindings)
}
