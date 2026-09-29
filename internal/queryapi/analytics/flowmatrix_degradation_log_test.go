package analytics

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-7176: a flowMatrix execution failure is swallowed to an empty result
// (analytics.py:959-961, parity-declared), reported through recordDegradation
// (counter + span event, telemetry.go) and -- the line this test adds -- logged
// at WARN, as the sankey twin has always been. All three, and the degraded
// response, are asserted from ONE planted ClickHouse error.
func TestResolve_FlowMatrixExecutionFailure_LogsWarnReportsAndStillDegrades(t *testing.T) {
	driverErr := errors.New("code: 60, message: Table default.work_unit_investments does not exist")
	boom := &fakeOperationError{operation: "query", cause: driverErr}
	client := &routingFakeClient{}
	client.onErr("work_unit_investments", boom)

	records := captureSlog(t)
	var reports []string
	orig := recordDegradation
	recordDegradation = func(_ context.Context, phase string, _ error) { reports = append(reports, phase) }
	t.Cleanup(func() { recordDegradation = orig })

	fmUseInvestment := true
	result, err := Resolve(context.Background(), client, "org-1", model.AnalyticsRequestInput{
		UseInvestment: boolPtr(true),
		FlowMatrix: &model.FlowMatrixRequestInput{
			Dimension:     model.DimensionInputTeam,
			Measure:       model.MeasureInputCount,
			DateRange:     &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			MaxNodes:      50,
			MaxEdges:      200,
			UseInvestment: &fmUseInvestment,
		},
	})
	// Parity: the swallow still swallows, with the additive disclosure.
	if err != nil || result.FlowMatrix == nil || len(result.FlowMatrix.Nodes) != 0 || len(result.FlowMatrix.Edges) != 0 {
		t.Fatalf("degraded response changed: err=%v result=%+v", err, result.FlowMatrix)
	}
	if result.FlowMatrix.DegradedReason == nil || *result.FlowMatrix.DegradedReason != "FLOW_MATRIX_EXECUTION_FAILED" {
		t.Fatalf("DegradedReason = %v", result.FlowMatrix.DegradedReason)
	}
	// Telemetry report (already existed): exactly one, phase flowMatrix.
	if len(reports) != 1 || reports[0] != "flowMatrix" {
		t.Fatalf("degradation reports = %v, want exactly [flowMatrix]", reports)
	}
	// The log line.
	var warns []capturedLogRecord
	for _, record := range *records {
		if record.level == slog.LevelWarn && strings.Contains(record.msg, "flowMatrix query failed") {
			warns = append(warns, record)
		}
	}
	if len(warns) != 1 {
		t.Fatalf("want exactly one flowMatrix WARN log, got %d of %d records", len(warns), len(*records))
	}
	attrs := warns[0].attrs
	if attrs["org_id"] != "org-1" || attrs["use_investment"] != true {
		t.Fatalf("WARN attrs = %v", attrs)
	}
	if cause, _ := attrs["error_cause"].(string); !strings.Contains(cause, "code: 60") {
		t.Fatalf("error_cause = %q, want the driver text (the fixed client error omits it)", cause)
	}
	if strings.Contains(warns[0].msg, "code: 60") {
		t.Fatal("the fake must not leak the driver text into the message")
	}
}
