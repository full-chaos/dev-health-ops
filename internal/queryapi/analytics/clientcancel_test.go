package analytics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// A statement that ends because the client closed the request is a cancel,
// not a failed query: ONE line at INFO, the cancel report, and no failure
// report (no WARN or ERROR line, no failure counter). The same error on a
// request that is still alive, and a request whose deadline passed, stay
// failures: a statement that someone else cancelled, or that ran out of this
// service's own budget, is a defect to look at.
//
// One case for each of the three reads of the investment page that report a
// failure: the coverage sums, the sankey and the flow matrix.
func TestAStatementTheClientCancelledIsReportedAsACancel(t *testing.T) {
	cancelledByClient := func() error {
		return &fakeOperationError{operation: "query", cause: &clickhousedriver.Exception{
			Code: 735, Name: "QUERY_WAS_CANCELLED_BY_CLIENT", Message: "Query was cancelled",
		}}
	}
	contexts := []struct {
		name       string
		make       func(t *testing.T) context.Context
		wantCancel bool
	}{
		{"the client closed the request", func(t *testing.T) context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}, true},
		{"the request is alive", func(*testing.T) context.Context { return context.Background() }, false},
		{"the deadline of the request passed", func(t *testing.T) context.Context {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			t.Cleanup(cancel)
			return ctx
		}, false},
	}
	flowUsesInvestment := true
	phases := []struct {
		phase string
		// run plants the error for the phase's statement and runs the read.
		run func(ctx context.Context, client *routingFakeClient)
		// failureLine is a part of the message of the phase's failure line.
		failureLine string
	}{
		{"investment_coverage", func(ctx context.Context, client *routingFakeClient) {
			client.onErr("AS assigned_team", cancelledByClient())
			resolveSankeyCoverage(ctx, client, "org-1", SankeyRequest{
				Measure: MeasureCount, StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08"),
			}, 30, false, nil)
		}, "investment_coverage.query_failed"},
		{"sankey", func(ctx context.Context, client *routingFakeClient) {
			client.onErr("AS grouping_set,", cancelledByClient())
			_, _ = Resolve(ctx, client, "org-1", sankeyFailureBatch())
		}, "sankey query failed"},
		{"flowMatrix", func(ctx context.Context, client *routingFakeClient) {
			client.onErr("work_unit_investments", cancelledByClient())
			_, _ = Resolve(ctx, client, "org-1", model.AnalyticsRequestInput{
				UseInvestment: boolPtr(true),
				FlowMatrix: &model.FlowMatrixRequestInput{
					Dimension: model.DimensionInputTeam, Measure: model.MeasureInputCount,
					DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
					MaxNodes:  50, MaxEdges: 200, UseInvestment: &flowUsesInvestment,
				},
			})
		}, "flowMatrix query failed"},
	}
	for _, phase := range phases {
		for _, request := range contexts {
			t.Run(phase.phase+"/"+request.name, func(t *testing.T) {
				records := captureSlog(t)
				var cancels, failures []string
				origCancel, origDegradation, origCoverage := recordClientCancel, recordDegradation, recordInvestmentCoverageFailure
				recordClientCancel = func(ctx context.Context, name string, attrs ...any) {
					cancels = append(cancels, name)
					origCancel(ctx, name, attrs...)
				}
				recordDegradation = func(_ context.Context, name string, _ error) { failures = append(failures, name) }
				recordInvestmentCoverageFailure = func(context.Context, string, Measure, bool, coverageFailureStage, string, error) {
					failures = append(failures, "investment_coverage")
				}
				t.Cleanup(func() {
					recordClientCancel, recordDegradation, recordInvestmentCoverageFailure = origCancel, origDegradation, origCoverage
				})

				phase.run(request.make(t), &routingFakeClient{})

				of := func(names []string) int {
					count := 0
					for _, name := range names {
						if name == phase.phase {
							count++
						}
					}
					return count
				}
				var cancelLines, failureLines int
				for _, record := range *records {
					switch {
					case record.msg == "analytics: query cancelled by the client" && record.attrs["phase"] == phase.phase:
						if record.level != slog.LevelInfo {
							t.Errorf("the cancel line is at %v, want INFO", record.level)
						}
						cancelLines++
					case strings.Contains(record.msg, phase.failureLine):
						failureLines++
					}
				}
				if request.wantCancel {
					if of(cancels) != 1 || cancelLines != 1 {
						t.Errorf("cancel reports = %d, cancel lines = %d; want one of each", of(cancels), cancelLines)
					}
					if of(failures) != 0 || failureLines != 0 {
						t.Errorf("a cancel was also reported as a failure: %d failure reports, %d failure lines", of(failures), failureLines)
					}
					return
				}
				if of(cancels) != 0 || cancelLines != 0 {
					t.Errorf("a failure was reported as a client cancel: %d cancel reports, %d cancel lines", of(cancels), cancelLines)
				}
				// The coverage failure line is written by the report this test
				// replaced, so its report is the measure there.
				if of(failures) != 1 || (phase.phase != "investment_coverage" && failureLines != 1) {
					t.Errorf("failure reports = %d, failure lines = %d; want the failure reported as before", of(failures), failureLines)
				}
			})
		}
	}
}

// The compile stage of the coverage read never sends a statement, so it is
// never a client cancel, also on a request the client closed.
func TestACompileFailureOfTheCoverageIsNeverAClientCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var cancels, failures int
	origCancel, origCoverage := recordClientCancel, recordInvestmentCoverageFailure
	recordClientCancel = func(context.Context, string, ...any) { cancels++ }
	recordInvestmentCoverageFailure = func(context.Context, string, Measure, bool, coverageFailureStage, string, error) { failures++ }
	t.Cleanup(func() { recordClientCancel, recordInvestmentCoverageFailure = origCancel, origCoverage })
	reportInvestmentCoverageFailure(ctx, "org-1", MeasureCount, true, coverageStageCompile, "", context.Canceled)
	if cancels != 0 || failures != 1 {
		t.Fatalf("a compile failure on a closed request: %d cancel reports, %d failure reports; want 0 and 1", cancels, failures)
	}
}

// The two halves of the rule, by the form of the error, on a request the
// client closed and on a live one. A statement that failed for its own reason
// is a failure also after the client left: the cancel did not cause it.
func TestOnlyAnErrorThatSaysCancelledIsAClientCancel(t *testing.T) {
	closed, cancel := context.WithCancel(context.Background())
	cancel()
	exception := func(code int32, name string) error {
		return fmt.Errorf("query: %w", &fakeOperationError{operation: "query", cause: &clickhousedriver.Exception{Code: code, Name: name}})
	}
	for _, test := range []struct {
		name string
		err  error
		want bool // on the closed request; on a live request it is never a cancel
	}{
		{"ClickHouse: cancelled by the client (735)", exception(735, "QUERY_WAS_CANCELLED_BY_CLIENT"), true},
		{"ClickHouse: cancelled (394)", exception(394, "QUERY_WAS_CANCELLED"), true},
		{"the context's own cancel error, wrapped", fmt.Errorf("rows: %w", &fakeOperationError{operation: "rows", cause: context.Canceled}), true},
		{"ClickHouse: unknown table (60)", exception(60, "UNKNOWN_TABLE"), false},
		{"ClickHouse: time limit (159)", exception(159, "TIMEOUT_EXCEEDED"), false},
		{"a statement refused before it was sent", fmt.Errorf("query: %w", clickhouse.ErrUnsafeBindingValue), false},
		{"another error", errors.New("connection reset"), false},
		{"a deadline error", fmt.Errorf("query: %w", context.DeadlineExceeded), false},
		{"no error", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := clientCancelled(closed, test.err); got != test.want {
				t.Errorf("on a request the client closed: client cancel = %v, want %v", got, test.want)
			}
			if clientCancelled(context.Background(), test.err) {
				t.Errorf("on a live request: reported as a client cancel")
			}
		})
	}
}

// A real failure of the coverage statement on a request the client closed
// since is reported as the failure it is, through the real failure sites: an
// unknown table, and a statement the ClickHouse client refused before it sent
// it (the query stage, not the compile stage).
func TestARealCoverageFailureOnAClosedRequestStaysAFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"unknown table", &fakeOperationError{operation: "query", cause: &clickhousedriver.Exception{Code: 60, Name: "UNKNOWN_TABLE"}}},
		{"refused before it was sent", clickhouse.ErrUnsafeBindingValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var cancels, failures int
			origCancel, origCoverage := recordClientCancel, recordInvestmentCoverageFailure
			recordClientCancel = func(context.Context, string, ...any) { cancels++ }
			recordInvestmentCoverageFailure = func(context.Context, string, Measure, bool, coverageFailureStage, string, error) { failures++ }
			t.Cleanup(func() { recordClientCancel, recordInvestmentCoverageFailure = origCancel, origCoverage })
			client := &routingFakeClient{}
			client.onErr("AS assigned_team", test.err)
			resolveSankeyCoverage(ctx, client, "org-1", SankeyRequest{
				Measure: MeasureCount, StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08"),
			}, 30, false, nil)
			if cancels != 0 || failures != 1 {
				t.Errorf("cancel reports = %d, failure reports = %d; want 0 and 1", cancels, failures)
			}
		})
	}
}

// What the report itself records, through the real meter and a real span (the
// tests above replace the report, so they say nothing of it): one count of
// the phase, one span event with the phase, one INFO line.
func TestTheClientCancelReportRecordsACountASpanEventAndALine(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("test").Start(context.Background(), "resolve")
	records := captureSlog(t)
	const phase = "phase_of_the_report_test"
	count := func() int64 {
		t.Helper()
		var collected metricdata.ResourceMetrics
		if err := realMeterReader.Collect(context.Background(), &collected); err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, scope := range collected.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != "devhealth_query_api_analytics_client_cancelled_total" {
					continue
				}
				sum, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					t.Fatalf("counter data shape = %+v", m.Data)
				}
				for _, point := range sum.DataPoints {
					if value, _ := point.Attributes.Value("phase"); value.AsString() == phase {
						total += point.Value
					}
				}
			}
		}
		return total
	}
	before := count()
	defaultRecordClientCancel(ctx, phase, "org_id", "org-1")
	span.End()
	if got := count() - before; got != 1 {
		t.Errorf("the counter of the phase moved by %d, want 1", got)
	}
	ended := recorder.Ended()
	if len(ended) != 1 || len(ended[0].Events()) != 1 || ended[0].Events()[0].Name != "analytics.client_cancelled" {
		t.Fatalf("span events = %+v, want one analytics.client_cancelled", ended)
	}
	if value := ended[0].Events()[0].Attributes; len(value) != 1 || string(value[0].Key) != "phase" || value[0].Value.AsString() != phase {
		t.Errorf("the span event's attributes = %+v, want the phase only", value)
	}
	lines := 0
	for _, record := range *records {
		if record.msg == "analytics: query cancelled by the client" && record.level == slog.LevelInfo && record.attrs["phase"] == phase && record.attrs["org_id"] == "org-1" {
			lines++
		}
	}
	if lines != 1 {
		t.Errorf("INFO lines of the report = %d, want 1", lines)
	}
}

// The flow matrix runs two statements. When the client's cancel ends one and
// the other fails by itself, the read is a failure, whichever of the two the
// real failure is in; when both end by the cancel, it is a cancel.
func TestAFlowMatrixWithOneRealFailureIsAFailure(t *testing.T) {
	cancelled := func() error {
		return &fakeOperationError{operation: "query", cause: &clickhousedriver.Exception{Code: 735, Name: "QUERY_WAS_CANCELLED_BY_CLIENT"}}
	}
	unknownTable := func() error {
		return &fakeOperationError{operation: "query", cause: &clickhousedriver.Exception{Code: 60, Name: "UNKNOWN_TABLE"}}
	}
	const nodesStatement, edgesStatement = "LIMIT {limit_per_dim:UInt32}", "LIMIT {max_edges:UInt32}"
	flowUsesInvestment := true
	for _, test := range []struct {
		name         string
		nodes, edges error
		wantCancel   bool
	}{
		{"the nodes statement is cancelled, the edges statement fails by itself", cancelled(), unknownTable(), false},
		{"the nodes statement fails by itself, the edges statement is cancelled", unknownTable(), cancelled(), false},
		{"both statements are cancelled", cancelled(), cancelled(), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var cancels, failures int
			origCancel, origDegradation := recordClientCancel, recordDegradation
			recordClientCancel = func(context.Context, string, ...any) { cancels++ }
			recordDegradation = func(context.Context, string, error) { failures++ }
			t.Cleanup(func() { recordClientCancel, recordDegradation = origCancel, origDegradation })
			client := &routingFakeClient{}
			client.onErr(nodesStatement, test.nodes)
			client.onErr(edgesStatement, test.edges)
			_, _ = Resolve(ctx, client, "org-1", model.AnalyticsRequestInput{
				UseInvestment: boolPtr(true),
				FlowMatrix: &model.FlowMatrixRequestInput{
					Dimension: model.DimensionInputTeam, Measure: model.MeasureInputCount,
					DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
					MaxNodes:  50, MaxEdges: 200, UseInvestment: &flowUsesInvestment,
				},
			})
			if len(client.calls) != 2 {
				t.Fatalf("statements the scripted client matched = %v, want the nodes and the edges statement", client.calls)
			}
			wantCancels, wantFailures := 0, 1
			if test.wantCancel {
				wantCancels, wantFailures = 1, 0
			}
			if cancels != wantCancels || failures != wantFailures {
				t.Errorf("cancel reports = %d, failure reports = %d; want %d and %d", cancels, failures, wantCancels, wantFailures)
			}
		})
	}
}
