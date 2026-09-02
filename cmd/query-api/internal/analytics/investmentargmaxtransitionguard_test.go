package analytics

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// resetArgMaxNullTransitionGate clears the package-level cooldown map so
// tests do not leak state into each other or into a later real request.
func resetArgMaxNullTransitionGate(t *testing.T) {
	t.Helper()
	argMaxNullTransitionGate.mu.Lock()
	argMaxNullTransitionGate.lastChecked = make(map[string]time.Time)
	argMaxNullTransitionGate.mu.Unlock()
	origClock := argMaxNullTransitionGateClock
	t.Cleanup(func() {
		argMaxNullTransitionGate.mu.Lock()
		argMaxNullTransitionGate.lastChecked = make(map[string]time.Time)
		argMaxNullTransitionGate.mu.Unlock()
		argMaxNullTransitionGateClock = origClock
	})
}

// TestFetchArgMaxNullTransitionState_ScansRow pins the scan order against
// the query's own column order -- a reordering of either side that the
// other does not follow must turn this red rather than silently
// transposing two counts.
func TestFetchArgMaxNullTransitionState_ScansRow(t *testing.T) {
	client := &routingFakeClient{}
	client.on("HAVING count() > 1", &fakeRowScanner{rows: [][]any{
		{int64(1), int64(2), int64(3), int64(4), int64(203)},
	}})

	got, err := FetchArgMaxNullTransitionState(context.Background(), client, "org-1", 30)
	if err != nil {
		t.Fatalf("FetchArgMaxNullTransitionState error = %v", err)
	}
	want := ArgMaxNullTransitionState{RepoID: 1, Provider: 2, WorkUnitType: 3, WorkUnitName: 4, MultiGenerationUnits: 203}
	if got != want {
		t.Fatalf("state = %+v, want %+v", got, want)
	}
	if !got.Diverged() {
		t.Fatalf("Diverged() = false, want true for %+v", got)
	}
}

// TestArgMaxNullTransitionState_Diverged pins Diverged()'s decision logic
// directly: any single column > 0 must report diverged, and the CHAOS-4759
// baseline (all-zero, 203 multi-generation units) must not.
func TestArgMaxNullTransitionState_Diverged(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state ArgMaxNullTransitionState
		want  bool
	}{
		{"baseline_zero", ArgMaxNullTransitionState{MultiGenerationUnits: 203}, false},
		{"repo_id_diverged", ArgMaxNullTransitionState{RepoID: 1}, true},
		{"provider_diverged", ArgMaxNullTransitionState{Provider: 1}, true},
		{"work_unit_type_diverged", ArgMaxNullTransitionState{WorkUnitType: 1}, true},
		{"work_unit_name_diverged", ArgMaxNullTransitionState{WorkUnitName: 1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.state.Diverged(); got != tc.want {
				t.Errorf("Diverged() = %v, want %v for %+v", got, tc.want, tc.state)
			}
		})
	}
}

// TestDefaultRecordArgMaxNullTransition_RecordsToRealMeter is this file's
// level for the injection seam behind recordArgMaxNullTransition -- the
// SAME shape TestDefaultRecordStaleInvestmentMembershipScope_RecordsToRealMeter
// (investmentmembershiptelemetry_test.go) uses, for the same reason: the
// seam being testable would otherwise make the function BEHIND it
// untested. Reads from the package's ONE real
// go.opentelemetry.io/otel/sdk/metric.ManualReader (main_test.go's
// TestMain) rather than installing a second, independent
// SetMeterProvider+ManualReader pair here -- see that file's doc comment
// for why a second pair silently loses the process-wide one-time
// delegation (the exact failure this test surfaced against
// investmentmembershiptelemetry_test.go's own real-meter test before
// this fix).
func TestDefaultRecordArgMaxNullTransition_RecordsToRealMeter(t *testing.T) {
	ctx := context.Background()
	state := ArgMaxNullTransitionState{RepoID: 3, Provider: 0, WorkUnitType: 1, WorkUnitName: 0, MultiGenerationUnits: 250}
	defaultRecordArgMaxNullTransition(ctx, "org-1", state)

	var rm metricdata.ResourceMetrics
	if err := realMeterReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect error = %v", err)
	}

	gotColumns := map[string]int64{}
	var sawGauge bool
	var gaugeValue int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "devhealth_query_api_investment_argmax_null_transition_total":
				data, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					t.Fatalf("counter data shape = %+v, want Sum[int64]", m.Data)
				}
				for _, dp := range data.DataPoints {
					column, ok := dp.Attributes.Value("column")
					if !ok {
						t.Fatalf("counter data point missing column attribute: %+v", dp)
					}
					gotColumns[column.AsString()] = dp.Value
				}
			case "devhealth_query_api_investment_argmax_null_transition_multi_generation_units":
				sawGauge = true
				data, ok := m.Data.(metricdata.Gauge[int64])
				if !ok || len(data.DataPoints) != 1 {
					t.Fatalf("gauge data shape = %+v, want one int64 gauge data point", m.Data)
				}
				gaugeValue = data.DataPoints[0].Value
			}
		}
	}

	// Only the two NON-ZERO columns fire -- a zero-count column must not
	// appear as its own zero-valued data point (that would make "any
	// series present" indistinguishable from "this column diverged").
	wantColumns := map[string]int64{"repo_id": 3, "work_unit_type": 1}
	if len(gotColumns) != len(wantColumns) {
		t.Fatalf("counter columns = %+v, want %+v", gotColumns, wantColumns)
	}
	for column, want := range wantColumns {
		if got := gotColumns[column]; got != want {
			t.Errorf("counter[%s] = %d, want %d", column, got, want)
		}
	}
	if _, present := gotColumns["provider"]; present {
		t.Errorf("counter recorded a data point for provider, which did not diverge")
	}
	if _, present := gotColumns["work_unit_name"]; present {
		t.Errorf("counter recorded a data point for work_unit_name, which did not diverge")
	}
	if !sawGauge {
		t.Fatal("devhealth_query_api_investment_argmax_null_transition_multi_generation_units was never emitted to the real meter")
	}
	if gaugeValue != 250 {
		t.Errorf("gauge value = %d, want 250", gaugeValue)
	}
}

// TestRecordArgMaxNullTransitionGuard_OnlyFiresWhenDiverged pins
// RecordArgMaxNullTransitionGuard's own decision logic (the exported
// wrapper, not the recorder behind it) via the injectable package-var
// seam, mirroring
// TestRecordStaleInvestmentMembershipScope_OnlyFiresOnUnscopedFallback's
// shape. Removing the `if !state.Diverged() { return }` guard in
// RecordArgMaxNullTransitionGuard must turn this red.
func TestRecordArgMaxNullTransitionGuard_OnlyFiresWhenDiverged(t *testing.T) {
	for _, tc := range []struct {
		name       string
		row        []any
		wantRecord bool
	}{
		{"all_zero_does_not_fire", []any{int64(0), int64(0), int64(0), int64(0), int64(203)}, false},
		{"repo_id_diverged_fires", []any{int64(1), int64(0), int64(0), int64(0), int64(203)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetArgMaxNullTransitionGate(t)

			var captured *ArgMaxNullTransitionState
			origRecord := recordArgMaxNullTransition
			recordArgMaxNullTransition = func(_ context.Context, _ string, state ArgMaxNullTransitionState) {
				c := state
				captured = &c
			}
			t.Cleanup(func() { recordArgMaxNullTransition = origRecord })

			client := &routingFakeClient{}
			client.on("HAVING count() > 1", &fakeRowScanner{rows: [][]any{tc.row}})

			RecordArgMaxNullTransitionGuard(context.Background(), client, "org-1", 30)

			if tc.wantRecord && captured == nil {
				t.Fatalf("expected the recorder to fire, it did not")
			}
			if !tc.wantRecord && captured != nil {
				t.Fatalf("expected the recorder NOT to fire, got %+v", *captured)
			}
		})
	}
}

// TestRecordArgMaxNullTransitionGuard_EmptyOrgIDNeverQueries guards the
// same "no orgID, no query" contract RecordStaleInvestmentMembershipScope
// has -- a fake client with NO registered rule fires
// routingFakeClient's "no rule matches" error if the guard ever queries,
// which would turn this red.
func TestRecordArgMaxNullTransitionGuard_EmptyOrgIDNeverQueries(t *testing.T) {
	resetArgMaxNullTransitionGate(t)
	client := &routingFakeClient{}
	RecordArgMaxNullTransitionGuard(context.Background(), client, "", 30)
	if len(client.calls) != 0 {
		t.Fatalf("expected no query for an empty orgID, got calls = %v", client.calls)
	}
}

// TestRecordArgMaxNullTransitionGuard_CooldownSuppressesRepeatQueries is
// the cost-control contract's own test: within
// argMaxNullTransitionCheckCooldown, a second call for the SAME org must
// not query again (double-scanning work_unit_investments on every
// investment request would double this package's ClickHouse load for a
// signal that only needs bounded-latency detection). After the cooldown
// elapses, the next call must query again.
func TestRecordArgMaxNullTransitionGuard_CooldownSuppressesRepeatQueries(t *testing.T) {
	resetArgMaxNullTransitionGate(t)

	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	argMaxNullTransitionGateClock = func() time.Time { return now }

	client := &routingFakeClient{}
	client.on("HAVING count() > 1", &fakeRowScanner{rows: [][]any{
		{int64(0), int64(0), int64(0), int64(0), int64(203)},
	}})

	RecordArgMaxNullTransitionGuard(context.Background(), client, "org-1", 30)
	if got := len(client.calls); got != 1 {
		t.Fatalf("first call: expected exactly one query, got %d", got)
	}

	// Still within the cooldown: must NOT query again. fakeRowScanner is
	// single-use (its cursor does not reset), so a second real query
	// against the same rule would itself fail loudly -- this assertion
	// is belt-and-braces on top of that.
	RecordArgMaxNullTransitionGuard(context.Background(), client, "org-1", 30)
	if got := len(client.calls); got != 1 {
		t.Fatalf("second call inside cooldown: expected still exactly one query, got %d", got)
	}

	// Advance past the cooldown and re-arm the fixture (the first row was
	// already consumed).
	now = now.Add(argMaxNullTransitionCheckCooldown + time.Second)
	client.rules = nil
	client.on("HAVING count() > 1", &fakeRowScanner{rows: [][]any{
		{int64(0), int64(0), int64(0), int64(0), int64(203)},
	}})

	client.mu.Lock()
	client.calls = nil
	client.mu.Unlock()

	RecordArgMaxNullTransitionGuard(context.Background(), client, "org-1", 30)
	if got := len(client.calls); got != 1 {
		t.Fatalf("call after cooldown elapsed: expected exactly one NEW query, got %d", got)
	}
}

// TestRecordArgMaxNullTransitionGuard_CooldownIsPerOrg proves the
// cooldown does not cross-suppress a DIFFERENT org's check -- a shared
// key (rather than one keyed by orgID) would silently blind every org
// but the first one to query on a shared process.
func TestRecordArgMaxNullTransitionGuard_CooldownIsPerOrg(t *testing.T) {
	resetArgMaxNullTransitionGate(t)

	client := &routingFakeClient{}
	client.on("HAVING count() > 1", &fakeRowScanner{rows: [][]any{
		{int64(0), int64(0), int64(0), int64(0), int64(10)},
	}})
	RecordArgMaxNullTransitionGuard(context.Background(), client, "org-a", 30)

	client2 := &routingFakeClient{}
	client2.on("HAVING count() > 1", &fakeRowScanner{rows: [][]any{
		{int64(0), int64(0), int64(0), int64(0), int64(20)},
	}})
	RecordArgMaxNullTransitionGuard(context.Background(), client2, "org-b", 30)

	if got := len(client.calls); got != 1 {
		t.Errorf("org-a: expected exactly one query, got %d", got)
	}
	if got := len(client2.calls); got != 1 {
		t.Errorf("org-b: expected exactly one query (not suppressed by org-a's cooldown), got %d", got)
	}
}
