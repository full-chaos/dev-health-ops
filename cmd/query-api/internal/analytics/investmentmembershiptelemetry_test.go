package analytics

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestDefaultRecordStaleInvestmentMembershipScope_RecordsToRealMeter is a
// codex round-1 P3 finding fix (2026-08-30): before this test,
// RecordStaleInvestmentMembershipScope's own package doc comment (this
// file's sibling, investmentmembershiptelemetry.go) explicitly says the
// injectable `recordStaleInvestmentMembershipScope` package var exists
// "so a test must be able to substitute a spy here" -- but nothing in
// this package had ever actually substituted one, and nothing exercised
// defaultRecordStaleInvestmentMembershipScope (the function BEHIND that
// seam) at all. That is the exact layer-masking shape
// TestDefaultRecordDegradation_RecordsDriverCause (telemetry_test.go)
// documents having already bitten this package once: "the injection seam
// that makes one behaviour testable makes the behaviour BEHIND it
// untestable, so it needs a test at its own level." This test is that
// level for the investment-membership-scope hook, mirroring
// TestDefaultRecordDegradation_RecordsDriverCause's shape but for a
// metric instrument rather than a span.
//
// Uses the package's ONE real go.opentelemetry.io/otel/sdk/metric.ManualReader
// (main_test.go's TestMain, shared with every other "RecordsToRealMeter"
// test in this package -- see that file's doc comment for why a second,
// independent SetMeterProvider+ManualReader pair here would silently
// lose the process-wide one-time delegation), not a spy -- per root
// AGENTS.md's verification rules ("sink-level tests assert on the
// production sink's real output bytes") and the brief's own instruction
// ("verify something CONSUMES it -- never merely that the value exists
// and is populated"). If this test fails to observe anything, that
// delegation is the first thing to re-check, not a red herring.
func TestDefaultRecordStaleInvestmentMembershipScope_RecordsToRealMeter(t *testing.T) {
	ctx := context.Background()
	state := InvestmentMembershipScopeState{ScopeMode: "scoped_projection_lag", LagSeconds: 4321, RunID: "run-1"}
	// The reader is process-wide and cumulative: seeded tests in this
	// package that read through a lagging projection record on the same
	// counter, so this test asserts its own increment, not the total.
	before := staleScopeCounterValue(t, ctx)
	defaultRecordStaleInvestmentMembershipScope(ctx, "org-1", state)

	var rm metricdata.ResourceMetrics
	if err := realMeterReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect error = %v", err)
	}

	var sawCounter, sawGauge bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "devhealth_query_api_investment_membership_scope_stale_total":
				sawCounter = true
				data, ok := m.Data.(metricdata.Sum[int64])
				if !ok || len(data.DataPoints) != 1 {
					t.Fatalf("counter data shape = %+v, want one int64 sum data point", m.Data)
				}
				dp := data.DataPoints[0]
				if dp.Value-before != 1 {
					t.Errorf("counter increment = %d, want 1", dp.Value-before)
				}
				if got, ok := dp.Attributes.Value("scope_mode"); !ok || got.AsString() != "scoped_projection_lag" {
					t.Errorf("counter scope_mode attribute = %v (present=%v), want scoped_projection_lag", got, ok)
				}
			case "devhealth_query_api_investment_membership_scope_lag_seconds":
				sawGauge = true
				data, ok := m.Data.(metricdata.Gauge[int64])
				if !ok || len(data.DataPoints) != 1 {
					t.Fatalf("gauge data shape = %+v, want one int64 gauge data point", m.Data)
				}
				dp := data.DataPoints[0]
				if dp.Value != 4321 {
					t.Errorf("gauge value = %d, want 4321", dp.Value)
				}
				if got, ok := dp.Attributes.Value("scope_mode"); !ok || got.AsString() != "scoped_projection_lag" {
					t.Errorf("gauge scope_mode attribute = %v (present=%v), want scoped_projection_lag", got, ok)
				}
			}
		}
	}
	if !sawCounter {
		t.Error("devhealth_query_api_investment_membership_scope_stale_total was never emitted to the real meter")
	}
	if !sawGauge {
		t.Error("devhealth_query_api_investment_membership_scope_lag_seconds was never emitted to the real meter")
	}
}

// TestRecordStaleInvestmentMembershipScope_OnlyFiresOnProjectionLag
// pins RecordStaleInvestmentMembershipScope's own decision logic (the
// exported wrapper, not the recorder it calls) via the injectable
// package-var seam -- the same shape TestResolve_FlowMatrixDegradation_IsReported
// already uses for recordDegradation. Removing the
// `if state.ScopeMode != scopeModeProjectionLag { return }` guard in
// RecordStaleInvestmentMembershipScope must turn this red.
func TestRecordStaleInvestmentMembershipScope_OnlyFiresOnProjectionLag(t *testing.T) {
	for _, tc := range []struct {
		name       string
		scopeMode  string
		runID      string
		wantRecord bool
	}{
		{"scoped_does_not_fire", "scoped", "run-1", false},
		{"unscoped_no_marker_does_not_fire", "unscoped_no_marker", "", false},
		{"projection_lag_fires", "scoped_projection_lag", "run-1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var captured *InvestmentMembershipScopeState
			orig := recordStaleInvestmentMembershipScope
			recordStaleInvestmentMembershipScope = func(_ context.Context, _ string, state InvestmentMembershipScopeState) {
				c := state
				captured = &c
			}
			t.Cleanup(func() { recordStaleInvestmentMembershipScope = orig })

			client := &routingFakeClient{}
			// int64: lag_seconds is a toInt64(...) SQL expression
			// (investmentmembershipscope.go), the only *int64 scan
			// destination in this whole port -- fakeRowScanner's Scan
			// (flowmatrix_test.go) had never been taught this case
			// before this test, which surfaced when the
			// wantRecord=false cases could not distinguish a genuine
			// "did not fire" from a swallowed scan error.
			client.on("SELECT scope_mode, lag_seconds, latest_run_id", &fakeRowScanner{rows: [][]any{
				{tc.scopeMode, int64(99), tc.runID},
			}})

			RecordStaleInvestmentMembershipScope(context.Background(), client, "org-1", 30)

			if tc.wantRecord && captured == nil {
				t.Fatalf("scope_mode=%q: expected the recorder to fire, it did not", tc.scopeMode)
			}
			if !tc.wantRecord && captured != nil {
				t.Fatalf("scope_mode=%q: expected the recorder NOT to fire, got %+v", tc.scopeMode, *captured)
			}
			if tc.wantRecord && (captured.ScopeMode != tc.scopeMode || captured.LagSeconds != 99 || captured.RunID != tc.runID) {
				t.Fatalf("captured = %+v, want {%s 99 %s}", *captured, tc.scopeMode, tc.runID)
			}
		})
	}
}

// staleScopeCounterValue reads the stale-scope counter's current cumulative
// value from the shared reader (0 before its first increment).
func staleScopeCounterValue(t *testing.T, ctx context.Context) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := realMeterReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect error = %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "devhealth_query_api_investment_membership_scope_stale_total" {
				continue
			}
			if data, ok := m.Data.(metricdata.Sum[int64]); ok && len(data.DataPoints) == 1 {
				return data.DataPoints[0].Value
			}
		}
	}
	return 0
}

// TestDefaultRecordStaleInvestmentMembershipScope_LogsLagAndRun: the warn
// line names the organisation, the lag and the membership run the reads
// stayed on.
func TestDefaultRecordStaleInvestmentMembershipScope_LogsLagAndRun(t *testing.T) {
	records := captureSlog(t)
	defaultRecordStaleInvestmentMembershipScope(context.Background(), "org-1", InvestmentMembershipScopeState{
		ScopeMode: "scoped_projection_lag", LagSeconds: 6, RunID: "run-1",
	})
	for _, record := range *records {
		if record.level != slog.LevelWarn {
			continue
		}
		if record.attrs["org_id"] != "org-1" || record.attrs["lag_seconds"] != int64(6) || record.attrs["membership_run_id"] != "run-1" || record.attrs["scope_mode"] != "scoped_projection_lag" {
			t.Fatalf("warn attrs = %v, want org_id=org-1 lag_seconds=6 membership_run_id=run-1 scope_mode=scoped_projection_lag", record.attrs)
		}
		return
	}
	t.Fatal("no warn line recorded for a lagging projection")
}
