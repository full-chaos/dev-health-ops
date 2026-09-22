package analytics

import (
	"context"
	"errors"
	"fmt"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestResolveSankeyCoverage_QueryFailure_ReportsClickHouseCodeAndQueryID is
// the wiring-level half of CHAOS-6121: the 09-20 STEP 142 incident logged
// investment_coverage.query_failed 15x with only an opaque errorString --
// the ClickHouse code (735, QUERY_WAS_CANCELLED_BY_CLIENT) was visible only
// server-side, forcing a manual system.query_log search to find the
// matching row. This proves resolveSankeyCoverage itself, not just the
// recorder function in isolation, mints a query id and hands the failure
// (still wrapped in a real ClickHouse server exception) to the recorder.
//
// The fake error models the REAL client's shape, not a convenient one:
// dev-health-go/clickhouse wraps every driver failure as its unexported
// *operationError (fixed Error(), cause reachable only via Unwrap()) --
// fakeOperationError (resolve_test.go) reproduces exactly that, and
// resolveSankeyCoverage layers its own fmt.Errorf("query: %w", ...) on
// top, the same double wrap production has.
func TestResolveSankeyCoverage_QueryFailure_ReportsClickHouseCodeAndQueryID(t *testing.T) {
	exception := &clickhousedriver.Exception{
		Code:    735,
		Name:    "QUERY_WAS_CANCELLED_BY_CLIENT",
		Message: "Query was cancelled",
	}
	client := &routingFakeClient{}
	client.onErr("AS assigned_team", &fakeOperationError{operation: "query", cause: exception})

	type report struct {
		stage   coverageFailureStage
		queryID string
		err     error
	}
	var got []report
	orig := recordInvestmentCoverageFailure
	recordInvestmentCoverageFailure = func(_ context.Context, _ string, _ Measure, _ bool, stage coverageFailureStage, queryID string, err error) {
		got = append(got, report{stage: stage, queryID: queryID, err: err})
	}
	t.Cleanup(func() { recordInvestmentCoverageFailure = orig })

	req := SankeyRequest{
		Measure:   MeasureCount,
		StartDate: mustGraphQLDate("2026-01-01"),
		EndDate:   mustGraphQLDate("2026-01-08"),
	}
	coverage := resolveSankeyCoverage(context.Background(), client, "org-1", req, 30, false, nil)

	// Parity first: a failed coverage query degrades to nil, never an error.
	if coverage != nil {
		t.Fatalf("expected nil coverage on a failed query, got %+v", coverage)
	}

	if len(got) != 1 {
		t.Fatalf("expected exactly 1 failure report, got %d", len(got))
	}
	if got[0].stage != coverageStageQuery {
		t.Fatalf("stage = %q, want %q", got[0].stage, coverageStageQuery)
	}
	if got[0].queryID == "" {
		t.Fatal("queryID is empty -- resolveSankeyCoverage must mint one and thread it through, or the log line cannot be matched to system.query_log")
	}
	if _, err := uuid.Parse(got[0].queryID); err != nil {
		t.Fatalf("queryID = %q is not a valid UUID: %v", got[0].queryID, err)
	}

	var gotException *clickhousedriver.Exception
	if !errors.As(got[0].err, &gotException) {
		t.Fatalf("reported error does not unwrap to a ClickHouse exception: %v", got[0].err)
	}
	if gotException.Code != 735 || gotException.Name != "QUERY_WAS_CANCELLED_BY_CLIENT" {
		t.Fatalf("reported exception = %+v, want code 735 / QUERY_WAS_CANCELLED_BY_CLIENT", gotException)
	}
}

// TestResolveSankeyCoverage_LocalValidationFailure_ReportsNoQueryID pins a
// CHAOS-6121 finding reproduced live against a real ClickHouse container:
// a bad binding value (`clickhouse.ErrInvalidBinding`)
// fails INSIDE dev-health-go's Client.Query before it ever calls the
// driver (client.go:111-133, translateBindings runs before
// c.connection.Query) -- so the minted query id was never sent, and
// system.query_log has zero rows for it. Before the fix, resolveSankeyCoverage
// reported that id anyway, sending on-call searching for a row that can
// never exist. `client.onErr` here returns the RAW sentinel-wrapped error
// exactly as dev-health-go's Client.Query does for this class (no
// operationError wrapping -- see client.go:115-118): the failure never
// reaches c.connection.Query, so there is no driver error to wrap.
func TestResolveSankeyCoverage_LocalValidationFailure_ReportsNoQueryID(t *testing.T) {
	client := &routingFakeClient{}
	client.onErr("AS assigned_team", fmt.Errorf("binding value: %w", clickhouse.ErrInvalidBinding))

	var got []string
	orig := recordInvestmentCoverageFailure
	recordInvestmentCoverageFailure = func(_ context.Context, _ string, _ Measure, _ bool, stage coverageFailureStage, queryID string, _ error) {
		got = append(got, queryID)
		if stage != coverageStageQuery {
			t.Fatalf("stage = %q, want %q", stage, coverageStageQuery)
		}
	}
	t.Cleanup(func() { recordInvestmentCoverageFailure = orig })

	req := SankeyRequest{
		Measure:   MeasureCount,
		StartDate: mustGraphQLDate("2026-01-01"),
		EndDate:   mustGraphQLDate("2026-01-08"),
	}
	resolveSankeyCoverage(context.Background(), client, "org-1", req, 30, false, nil)

	if len(got) != 1 {
		t.Fatalf("expected exactly 1 failure report, got %d", len(got))
	}
	if got[0] != "" {
		t.Fatalf("queryID = %q, want empty -- this request was never dispatched to ClickHouse, so no id can appear in system.query_log", got[0])
	}
}

// TestIsLocalValidationFailure is the unit-level pin for the guard
// resolveSankeyCoverage's fix relies on: it must match BOTH of
// dev-health-go's pre-dispatch sentinels (wrapped or bare, the two shapes
// Client.Query actually returns them in) and reject a real driver/network
// failure -- a false positive on the latter would suppress query_id for
// failures that DID reach ClickHouse, regressing the P1 this PR exists to
// fix in the first place.
func TestIsLocalValidationFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"wrapped ErrInvalidBinding", fmt.Errorf("binding value: %w", clickhouse.ErrInvalidBinding), true},
		{"bare ErrUnsafeStatement", clickhouse.ErrUnsafeStatement, true},
		// CHAOS-6121: dev-health-go@v0.8.0 (the pinned version -- see
		// go.mod) has FOUR local sentinels. ErrUnsupportedBinding
		// (clickHouseParameter, a slice/array shape with no literal
		// encoding) and ErrUnsafeBindingValue (clickHouseQuotedString, a
		// []string element containing a backslash) are exactly as
		// pre-dispatch as ErrInvalidBinding/ErrUnsafeStatement -- both
		// fail inside translateBindings before c.connection.Query is
		// ever called.
		{"wrapped ErrUnsupportedBinding", fmt.Errorf("binding value: %w", clickhouse.ErrUnsupportedBinding), true},
		{"wrapped ErrUnsafeBindingValue", fmt.Errorf("binding value: %w", clickhouse.ErrUnsafeBindingValue), true},
		{"ClickHouse server exception", &clickhousedriver.Exception{Code: 60, Name: "UNKNOWN_TABLE"}, false},
		{"context cancellation", context.Canceled, false},
		{"unrelated error", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isLocalValidationFailure(tc.err); got != tc.want {
				t.Errorf("isLocalValidationFailure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestDefaultRecordInvestmentCoverageFailure_ClickHouseException asserts
// the span event AND the log line -- the two places CHAOS-6121 names --
// carry clickhouse_code, clickhouse_exception and query_id when the
// failure is a ClickHouse server exception. Calling
// defaultRecordInvestmentCoverageFailure directly (rather than through the
// spy used above) is deliberate: the spy REPLACES this function, so it
// proves nothing about what the function itself records -- the same
// layer-masking shape TestDefaultRecordDegradation_RecordsDriverCause's
// doc comment names.
func TestDefaultRecordInvestmentCoverageFailure_ClickHouseException(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("test").Start(context.Background(), "resolve")
	records := captureSlog(t)

	exception := &clickhousedriver.Exception{
		Code:    735,
		Name:    "QUERY_WAS_CANCELLED_BY_CLIENT",
		Message: "Query was cancelled",
	}
	wrapped := fmt.Errorf("query: %w", &fakeOperationError{operation: "query", cause: exception})
	const queryID = "11111111-2222-3333-4444-555555555555"

	defaultRecordInvestmentCoverageFailure(ctx, "org-1", MeasureCount, true, coverageStageQuery, queryID, wrapped)
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 recorded span, got %d", len(ended))
	}
	events := ended[0].Events()
	if len(events) != 1 || events[0].Name != "investment_coverage.query_failed" {
		t.Fatalf("expected one investment_coverage.query_failed event, got %+v", events)
	}
	spanAttrs := map[string]string{}
	for _, a := range events[0].Attributes {
		spanAttrs[string(a.Key)] = a.Value.Emit()
	}
	if spanAttrs["query_id"] != queryID {
		t.Fatalf("span query_id = %q, want %q", spanAttrs["query_id"], queryID)
	}
	if spanAttrs["clickhouse_code"] != "735" {
		t.Fatalf("span clickhouse_code = %q, want \"735\"", spanAttrs["clickhouse_code"])
	}
	if spanAttrs["clickhouse_exception"] != "QUERY_WAS_CANCELLED_BY_CLIENT" {
		t.Fatalf("span clickhouse_exception = %q, want QUERY_WAS_CANCELLED_BY_CLIENT", spanAttrs["clickhouse_exception"])
	}
	// The message itself never changes -- existing log filters on it must
	// keep matching (card constraint).
	if spanAttrs["error"] == "" {
		t.Fatal("existing error attribute must still be present")
	}

	if len(*records) != 1 {
		t.Fatalf("expected 1 log record, got %d", len(*records))
	}
	rec := (*records)[0]
	if rec.msg != "investment_coverage.query_failed" {
		t.Fatalf("log msg = %q, want investment_coverage.query_failed (existing filters must keep matching)", rec.msg)
	}
	if fmt.Sprint(rec.attrs["query_id"]) != queryID {
		t.Fatalf("log query_id = %v, want %q", rec.attrs["query_id"], queryID)
	}
	if fmt.Sprint(rec.attrs["clickhouse_code"]) != "735" {
		t.Fatalf("log clickhouse_code = %v, want 735", rec.attrs["clickhouse_code"])
	}
	if rec.attrs["clickhouse_exception"] != "QUERY_WAS_CANCELLED_BY_CLIENT" {
		t.Fatalf("log clickhouse_exception = %v, want QUERY_WAS_CANCELLED_BY_CLIENT", rec.attrs["clickhouse_exception"])
	}
}

// TestDefaultRecordInvestmentCoverageFailure_ContextCancellation is the
// card's second required case: a failure that is NOT a ClickHouse server
// exception (context cancellation, a transport error) must still carry
// query_id on every failure, and must NOT fabricate a clickhouse_code/
// clickhouse_exception it never observed -- AGENTS.md's "an inaccurate
// coverage claim is worse than an admitted gap".
func TestDefaultRecordInvestmentCoverageFailure_ContextCancellation(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("test").Start(context.Background(), "resolve")
	records := captureSlog(t)

	wrapped := fmt.Errorf("query: %w", &fakeOperationError{operation: "query", cause: context.Canceled})
	const queryID = "66666666-7777-8888-9999-000000000000"

	defaultRecordInvestmentCoverageFailure(ctx, "org-1", MeasureCount, false, coverageStageQuery, queryID, wrapped)
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 recorded span, got %d", len(ended))
	}
	spanAttrs := map[string]string{}
	for _, a := range ended[0].Events()[0].Attributes {
		spanAttrs[string(a.Key)] = a.Value.Emit()
	}
	if spanAttrs["query_id"] != queryID {
		t.Fatalf("span query_id = %q, want %q -- query_id must be reported on every failure, cancellation included", spanAttrs["query_id"], queryID)
	}
	if _, ok := spanAttrs["clickhouse_code"]; ok {
		t.Fatalf("span carries clickhouse_code = %q for a non-ClickHouse-exception error -- a fabricated code is worse than an absent one", spanAttrs["clickhouse_code"])
	}
	if _, ok := spanAttrs["clickhouse_exception"]; ok {
		t.Fatalf("span carries clickhouse_exception = %q for a non-ClickHouse-exception error", spanAttrs["clickhouse_exception"])
	}

	if len(*records) != 1 {
		t.Fatalf("expected 1 log record, got %d", len(*records))
	}
	rec := (*records)[0]
	if fmt.Sprint(rec.attrs["query_id"]) != queryID {
		t.Fatalf("log query_id = %v, want %q", rec.attrs["query_id"], queryID)
	}
	if _, ok := rec.attrs["clickhouse_code"]; ok {
		t.Fatalf("log carries clickhouse_code for a non-ClickHouse-exception error: %v", rec.attrs["clickhouse_code"])
	}
}

// TestDefaultRecordInvestmentCoverageFailure_CompileStageHasNoQueryID
// pins coverageStageCompile's empty queryID: compileSankeyCoverage fails
// before any statement is ever sent, so there is no query to match in
// system.query_log -- the attribute must be absent, not an empty string
// masquerading as one (missing is not healthy, but it is not zero either;
// AGENTS.md North Star check 12).
func TestDefaultRecordInvestmentCoverageFailure_CompileStageHasNoQueryID(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("test").Start(context.Background(), "resolve")

	defaultRecordInvestmentCoverageFailure(ctx, "org-1", MeasureCount, true, coverageStageCompile, "", errors.New("compile: boom"))
	span.End()

	ended := recorder.Ended()
	spanAttrs := map[string]string{}
	for _, a := range ended[0].Events()[0].Attributes {
		spanAttrs[string(a.Key)] = a.Value.Emit()
	}
	if _, ok := spanAttrs["query_id"]; ok {
		t.Fatalf("compile-stage failure carries a query_id (%q) but no statement was ever sent", spanAttrs["query_id"])
	}
}
