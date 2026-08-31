package analytics

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
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
// Uses a REAL go.opentelemetry.io/otel/sdk/metric.ManualReader, not a
// spy -- per root AGENTS.md's verification rules ("sink-level tests
// assert on the production sink's real output bytes") and the brief's
// own instruction ("verify something CONSUMES it -- never merely that
// the value exists and is populated"). otel.SetMeterProvider redirects
// the package-level membershipScopeStaleCounter/membershipScopeLagGauge
// instruments (created once at package-init time via the GLOBAL
// delegating otel.Meter(...) proxy) to this test's real SDK provider --
// that delegation is exactly what makes a package-level var safe to use
// as an OTel instrument at all; if this test fails to observe anything,
// that assumption itself is the first thing to re-check, not a red
// herring.
func TestDefaultRecordStaleInvestmentMembershipScope_RecordsToRealMeter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prior := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(prior) })

	ctx := context.Background()
	state := InvestmentMembershipScopeState{ScopeMode: "unscoped_fallback", LagSeconds: 4321}
	defaultRecordStaleInvestmentMembershipScope(ctx, state)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
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
				if dp.Value != 1 {
					t.Errorf("counter value = %d, want 1", dp.Value)
				}
				if got, ok := dp.Attributes.Value("scope_mode"); !ok || got.AsString() != "unscoped_fallback" {
					t.Errorf("counter scope_mode attribute = %v (present=%v), want unscoped_fallback", got, ok)
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
				if got, ok := dp.Attributes.Value("scope_mode"); !ok || got.AsString() != "unscoped_fallback" {
					t.Errorf("gauge scope_mode attribute = %v (present=%v), want unscoped_fallback", got, ok)
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

// TestRecordStaleInvestmentMembershipScope_OnlyFiresOnUnscopedFallback
// pins RecordStaleInvestmentMembershipScope's own decision logic (the
// exported wrapper, not the recorder it calls) via the injectable
// package-var seam -- the same shape TestResolve_FlowMatrixDegradation_IsReported
// already uses for recordDegradation. Removing the
// `if state.ScopeMode != "unscoped_fallback" { return }` guard in
// RecordStaleInvestmentMembershipScope must turn this red.
func TestRecordStaleInvestmentMembershipScope_OnlyFiresOnUnscopedFallback(t *testing.T) {
	for _, tc := range []struct {
		name       string
		scopeMode  string
		wantRecord bool
	}{
		{"scoped_does_not_fire", "scoped", false},
		{"unscoped_no_marker_does_not_fire", "unscoped_no_marker", false},
		{"unscoped_fallback_fires", "unscoped_fallback", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var captured *InvestmentMembershipScopeState
			orig := recordStaleInvestmentMembershipScope
			recordStaleInvestmentMembershipScope = func(_ context.Context, state InvestmentMembershipScopeState) {
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
			client.on("SELECT scope_mode, lag_seconds", &fakeRowScanner{rows: [][]any{
				{tc.scopeMode, int64(99)},
			}})

			RecordStaleInvestmentMembershipScope(context.Background(), client, "org-1", 30)

			if tc.wantRecord && captured == nil {
				t.Fatalf("scope_mode=%q: expected the recorder to fire, it did not", tc.scopeMode)
			}
			if !tc.wantRecord && captured != nil {
				t.Fatalf("scope_mode=%q: expected the recorder NOT to fire, got %+v", tc.scopeMode, *captured)
			}
			if tc.wantRecord && captured.ScopeMode != tc.scopeMode {
				t.Fatalf("captured ScopeMode = %q, want %q", captured.ScopeMode, tc.scopeMode)
			}
		})
	}
}
