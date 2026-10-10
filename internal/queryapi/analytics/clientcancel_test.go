package analytics

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"

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
