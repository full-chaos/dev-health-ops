package analytics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// scopePinRecordingClient records every statement and binding list it receives and
// answers with an empty result.
type scopePinRecordingClient struct {
	mu    sync.Mutex
	calls []recordedQuery
}

type recordedQuery struct {
	statement string
	bindings  []clickhouse.Binding
}

func (c *scopePinRecordingClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, recordedQuery{statement: statement, bindings: append([]clickhouse.Binding(nil), bindings...)})
	return &fakeRowScanner{}, nil
}

func (c *scopePinRecordingClient) last(t *testing.T) recordedQuery {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.calls) == 0 {
		t.Fatal("no query reached the wrapped client")
	}
	return c.calls[len(c.calls)-1]
}

// stubScopeResolution replaces the state fetch with a function answering
// one fixed state per organisation, and counts the fetches.
func stubScopeResolution(t *testing.T, byOrg map[string]InvestmentMembershipScopeState, err error) *atomic.Int64 {
	t.Helper()
	var fetches atomic.Int64
	orig := fetchInvestmentMembershipScopeState
	fetchInvestmentMembershipScopeState = func(_ context.Context, _ QueryClient, orgID string, _ int) (InvestmentMembershipScopeState, error) {
		fetches.Add(1)
		if err != nil {
			return InvestmentMembershipScopeState{ScopeMode: scopeModeUnscopedNoRuns}, err
		}
		return byOrg[orgID], nil
	}
	t.Cleanup(func() { fetchInvestmentMembershipScopeState = orig })
	return &fetches
}

func investmentStatement() string {
	return "SELECT count() FROM " + LatestWorkUnitInvestmentsSource() + " AS work_unit_investments"
}

func orgBindings(orgID string) []clickhouse.Binding {
	return []clickhouse.Binding{{Name: "org_id", Value: orgID}, {Name: "start_date", Value: "2026-01-01"}}
}

func bindingValue(bindings []clickhouse.Binding, name string) (any, bool) {
	for _, binding := range bindings {
		if binding.Name == name {
			return binding.Value, true
		}
	}
	return nil, false
}

func TestPinInvestmentMembershipScope_BindsTheResolvedRun(t *testing.T) {
	stubScopeResolution(t, map[string]InvestmentMembershipScopeState{
		"org-a": {ScopeMode: scopeModeProjectionLag, LagSeconds: 6, RunID: "run-1"},
	}, nil)
	inner := &scopePinRecordingClient{}
	bindings := orgBindings("org-a")

	if _, err := PinInvestmentMembershipScope(inner).Query(WithInvestmentMembershipScopeRequest(context.Background()), investmentStatement(), bindings); err != nil {
		t.Fatalf("Query: %v", err)
	}

	got := inner.last(t)
	if strings.Contains(got.statement, investmentScopeRunIDSQL()) {
		t.Errorf("pinned statement still evaluates its own membership run:\n%s", got.statement)
	}
	// The filter names its run five times: the no-run test, and the
	// membership subquery's real-run branch (three tests) and legacy
	// branch (one). Every one must read the pinned parameter.
	if n := strings.Count(got.statement, investmentScopeRunIDPlaceholder()); n != 5 {
		t.Errorf("placeholder occurrences = %d, want 5:\n%s", n, got.statement)
	}
	if value, ok := bindingValue(got.bindings, investmentScopeRunIDParam); !ok || value != "run-1" {
		t.Errorf("%s binding = %v (present=%v), want run-1", investmentScopeRunIDParam, value, ok)
	}
	if value, ok := bindingValue(got.bindings, "start_date"); !ok || value != "2026-01-01" {
		t.Errorf("caller binding start_date = %v (present=%v), want it passed through", value, ok)
	}
	if len(bindings) != 2 {
		t.Errorf("caller's binding slice changed length to %d", len(bindings))
	}
}

func TestPinInvestmentMembershipScope_NoCompleteRunBindsEmptyRun(t *testing.T) {
	stubScopeResolution(t, map[string]InvestmentMembershipScopeState{
		"org-a": {ScopeMode: scopeModeUnscopedNoRuns},
	}, nil)
	inner := &scopePinRecordingClient{}

	if _, err := PinInvestmentMembershipScope(inner).Query(context.Background(), investmentStatement(), orgBindings("org-a")); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if value, ok := bindingValue(inner.last(t).bindings, investmentScopeRunIDParam); !ok || value != "" {
		t.Errorf("%s binding = %v (present=%v), want the empty run (unscoped read)", investmentScopeRunIDParam, value, ok)
	}
}

func TestPinInvestmentMembershipScope_OneResolutionPerRequest(t *testing.T) {
	fetches := stubScopeResolution(t, map[string]InvestmentMembershipScopeState{
		"org-a": {ScopeMode: scopeModeScoped, RunID: "run-1"},
	}, nil)
	client := PinInvestmentMembershipScope(&scopePinRecordingClient{})

	request := WithInvestmentMembershipScopeRequest(context.Background())
	for i := 0; i < 3; i++ {
		if _, err := client.Query(request, investmentStatement(), orgBindings("org-a")); err != nil {
			t.Fatalf("Query: %v", err)
		}
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("resolutions for three queries of one request = %d, want 1", got)
	}

	if _, err := client.Query(WithInvestmentMembershipScopeRequest(context.Background()), investmentStatement(), orgBindings("org-a")); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("resolutions after a second request = %d, want 2", got)
	}

	// Outside a request each query resolves for itself.
	for i := 0; i < 2; i++ {
		if _, err := client.Query(context.Background(), investmentStatement(), orgBindings("org-a")); err != nil {
			t.Fatalf("Query: %v", err)
		}
	}
	if got := fetches.Load(); got != 4 {
		t.Fatalf("resolutions after two queries without a request = %d, want 4", got)
	}
}

func TestPinInvestmentMembershipScope_ConcurrentQueriesShareOneResolution(t *testing.T) {
	fetches := stubScopeResolution(t, map[string]InvestmentMembershipScopeState{
		"org-a": {ScopeMode: scopeModeScoped, RunID: "run-1"},
	}, nil)
	inner := &scopePinRecordingClient{}
	client := PinInvestmentMembershipScope(inner)
	request := WithInvestmentMembershipScopeRequest(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client.Query(request, investmentStatement(), orgBindings("org-a"))
		}()
	}
	wg.Wait()
	if got := fetches.Load(); got != 1 {
		t.Fatalf("resolutions for 16 concurrent queries of one request = %d, want 1", got)
	}
	for _, call := range inner.calls {
		if value, _ := bindingValue(call.bindings, investmentScopeRunIDParam); value != "run-1" {
			t.Fatalf("a concurrent query bound %v, want run-1", value)
		}
	}
}

func TestPinInvestmentMembershipScope_ResolvesPerOrganisation(t *testing.T) {
	fetches := stubScopeResolution(t, map[string]InvestmentMembershipScopeState{
		"org-a": {ScopeMode: scopeModeScoped, RunID: "run-a"},
		"org-b": {ScopeMode: scopeModeScoped, RunID: "run-b"},
	}, nil)
	inner := &scopePinRecordingClient{}
	client := PinInvestmentMembershipScope(inner)
	request := WithInvestmentMembershipScopeRequest(context.Background())

	for _, org := range []string{"org-a", "org-b"} {
		if _, err := client.Query(request, investmentStatement(), orgBindings(org)); err != nil {
			t.Fatalf("Query: %v", err)
		}
		if value, _ := bindingValue(inner.last(t).bindings, investmentScopeRunIDParam); value != "run-"+strings.TrimPrefix(org, "org-") {
			t.Fatalf("%s bound run %v", org, value)
		}
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("resolutions for two organisations = %d, want 2", got)
	}
}

func TestPinInvestmentMembershipScope_PassesThroughStatementsWithoutTheFilter(t *testing.T) {
	fetches := stubScopeResolution(t, nil, nil)
	inner := &scopePinRecordingClient{}
	const statement = "SELECT count() FROM repos WHERE org_id = {org_id:String}"

	if _, err := PinInvestmentMembershipScope(inner).Query(context.Background(), statement, orgBindings("org-a")); err != nil {
		t.Fatalf("Query: %v", err)
	}
	got := inner.last(t)
	if got.statement != statement || len(got.bindings) != 2 || fetches.Load() != 0 {
		t.Fatalf("statement without the scope filter was changed or resolved: %+v, fetches=%d", got, fetches.Load())
	}
}

// A statement whose org_id binding is absent, empty or not a string cannot
// be resolved for an organisation; it passes through unchanged (ClickHouse
// then rejects or answers it exactly as it would unwrapped).
func TestPinInvestmentMembershipScope_UnusableOrgBindingPassesThrough(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bindings []clickhouse.Binding
	}{
		{"absent", nil},
		{"other_bindings_only", []clickhouse.Binding{{Name: "start_date", Value: "2026-01-01"}}},
		{"empty_string", []clickhouse.Binding{{Name: "org_id", Value: ""}}},
		{"wrong_scalar_type", []clickhouse.Binding{{Name: "org_id", Value: 42}}},
		{"nil_value", []clickhouse.Binding{{Name: "org_id", Value: nil}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fetches := stubScopeResolution(t, nil, nil)
			inner := &scopePinRecordingClient{}
			if _, err := PinInvestmentMembershipScope(inner).Query(WithInvestmentMembershipScopeRequest(context.Background()), investmentStatement(), tc.bindings); err != nil {
				t.Fatalf("Query: %v", err)
			}
			got := inner.last(t)
			if got.statement != investmentStatement() || len(got.bindings) != len(tc.bindings) || fetches.Load() != 0 {
				t.Fatalf("statement was changed or resolved (bindings=%v, fetches=%d)", got.bindings, fetches.Load())
			}
		})
	}
}

// Duplicate org_id bindings: the first one names the organisation.
func TestPinInvestmentMembershipScope_FirstOrgBindingWins(t *testing.T) {
	stubScopeResolution(t, map[string]InvestmentMembershipScopeState{
		"org-a": {ScopeMode: scopeModeScoped, RunID: "run-a"},
		"org-b": {ScopeMode: scopeModeScoped, RunID: "run-b"},
	}, nil)
	inner := &scopePinRecordingClient{}
	bindings := []clickhouse.Binding{{Name: "org_id", Value: "org-a"}, {Name: "org_id", Value: "org-b"}}
	if _, err := PinInvestmentMembershipScope(inner).Query(context.Background(), investmentStatement(), bindings); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if value, _ := bindingValue(inner.last(t).bindings, investmentScopeRunIDParam); value != "run-a" {
		t.Fatalf("bound run %v, want run-a", value)
	}
}

func TestPinInvestmentMembershipScope_ResolutionFailureFailsTheRequestsScopedQueries(t *testing.T) {
	records := captureSlog(t)
	fetches := stubScopeResolution(t, nil, errors.New("state query failed"))
	inner := &scopePinRecordingClient{}
	client := PinInvestmentMembershipScope(inner)
	request := WithInvestmentMembershipScopeRequest(context.Background())

	for i := 0; i < 2; i++ {
		if _, err := client.Query(request, investmentStatement(), orgBindings("org-a")); err == nil || !strings.Contains(err.Error(), "state query failed") {
			t.Fatalf("scope-filtered query %d error = %v, want the resolution failure", i, err)
		}
	}
	if len(inner.calls) != 0 {
		t.Fatalf("%d scope-filtered statements reached ClickHouse without a pinned run", len(inner.calls))
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("resolutions after a failure within one request = %d, want 1", got)
	}
	// Statements without the scope filter are not the resolution's to fail.
	if _, err := client.Query(request, "SELECT 1 FROM repos WHERE org_id = {org_id:String}", orgBindings("org-a")); err != nil {
		t.Fatalf("statement without the scope filter failed: %v", err)
	}
	warned := 0
	for _, record := range *records {
		if record.msg == "investment membership scope unresolved; failing the scope-filtered query" && record.attrs["org_id"] == "org-a" {
			warned++
		}
	}
	if warned != 2 {
		t.Fatalf("warn lines for two failed queries = %d, want 2", warned)
	}
}

func TestPinInvestmentMembershipScope_RecordsLagOncePerRequest(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state InvestmentMembershipScopeState
		want  int
	}{
		{"projection_lag", InvestmentMembershipScopeState{ScopeMode: scopeModeProjectionLag, LagSeconds: 6, RunID: "run-1"}, 1},
		{"scoped", InvestmentMembershipScopeState{ScopeMode: scopeModeScoped, RunID: "run-1"}, 0},
		{"no_marker", InvestmentMembershipScopeState{ScopeMode: scopeModeUnscopedNoRuns}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubScopeResolution(t, map[string]InvestmentMembershipScopeState{"org-a": tc.state}, nil)
			var recorded []InvestmentMembershipScopeState
			orig := recordStaleInvestmentMembershipScope
			recordStaleInvestmentMembershipScope = func(_ context.Context, _ string, state InvestmentMembershipScopeState) {
				recorded = append(recorded, state)
			}
			t.Cleanup(func() { recordStaleInvestmentMembershipScope = orig })

			client := PinInvestmentMembershipScope(&scopePinRecordingClient{})
			request := WithInvestmentMembershipScopeRequest(context.Background())
			for i := 0; i < 3; i++ {
				if _, err := client.Query(request, investmentStatement(), orgBindings("org-a")); err != nil {
					t.Fatalf("Query: %v", err)
				}
			}
			if len(recorded) != tc.want {
				t.Fatalf("stale-projection records for three pinned queries of one request = %d, want %d", len(recorded), tc.want)
			}
			if tc.want == 1 && recorded[0] != tc.state {
				t.Fatalf("recorded %+v, want %+v", recorded[0], tc.state)
			}
		})
	}
}

func TestRecordStaleInvestmentMembershipScope_SharesTheRequestResolution(t *testing.T) {
	fetches := stubScopeResolution(t, map[string]InvestmentMembershipScopeState{
		"org-a": {ScopeMode: scopeModeProjectionLag, LagSeconds: 6, RunID: "run-1"},
	}, nil)
	var recorded []InvestmentMembershipScopeState
	orig := recordStaleInvestmentMembershipScope
	recordStaleInvestmentMembershipScope = func(_ context.Context, _ string, state InvestmentMembershipScopeState) {
		recorded = append(recorded, state)
	}
	t.Cleanup(func() { recordStaleInvestmentMembershipScope = orig })

	request := WithInvestmentMembershipScopeRequest(context.Background())
	RecordStaleInvestmentMembershipScope(request, &scopePinRecordingClient{}, "org-a", 30)
	if _, err := PinInvestmentMembershipScope(&scopePinRecordingClient{}).Query(request, investmentStatement(), orgBindings("org-a")); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("resolutions for telemetry plus one query of a request = %d, want 1", got)
	}
	if len(recorded) != 1 || recorded[0].RunID != "run-1" || recorded[0].LagSeconds != 6 {
		t.Fatalf("recorded = %+v, want one lagging state naming run-1 with lag 6", recorded)
	}
}

func TestInvestmentMembershipScopeRequestMiddleware_GivesEachRequestItsOwnResolution(t *testing.T) {
	var seen []*investmentScopeRequest
	handler := InvestmentMembershipScopeRequestMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		request, ok := r.Context().Value(investmentScopeRequestKey{}).(*investmentScopeRequest)
		if !ok {
			t.Fatal("request context carries no scope resolution")
		}
		seen = append(seen, request)
	}))
	for i := 0; i < 2; i++ {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/investment", nil))
	}
	if len(seen) != 2 || seen[0] == seen[1] {
		t.Fatalf("two requests shared one scope resolution holder: %v", seen)
	}
}

func TestFetchInvestmentMembershipScopeState_RunIDContract(t *testing.T) {
	for _, tc := range []struct {
		name    string
		row     []any
		want    InvestmentMembershipScopeState
		wantErr bool
	}{
		{"scoped", []any{"scoped", int64(0), "run-1"}, InvestmentMembershipScopeState{ScopeMode: "scoped", RunID: "run-1"}, false},
		{"projection_lag", []any{"scoped_projection_lag", int64(6), "run-1"}, InvestmentMembershipScopeState{ScopeMode: "scoped_projection_lag", LagSeconds: 6, RunID: "run-1"}, false},
		{"no_marker_carries_no_run", []any{"unscoped_no_marker", int64(0), "run-stray"}, InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, false},
		{"unknown_mode_is_an_error", []any{"other", int64(0), "run-1"}, InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, true},
		{"scoped_without_run_is_an_error", []any{"scoped", int64(0), ""}, InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, true},
		{"lag_without_run_is_an_error", []any{"scoped_projection_lag", int64(6), ""}, InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, true},
		{"empty_mode_is_an_error", []any{"", int64(0), "run-1"}, InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, true},
		{"wrong_scalar_type_is_an_error", []any{"scoped", "six", "run-1"}, InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := (&routingFakeClient{}).on("SELECT scope_mode, lag_seconds, latest_run_id", &fakeRowScanner{rows: [][]any{tc.row}})
			got, err := FetchInvestmentMembershipScopeState(context.Background(), client, "org-a", 30)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("state = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestFetchInvestmentMembershipScopeState_NoRowIsAnError(t *testing.T) {
	client := (&routingFakeClient{}).on("SELECT scope_mode, lag_seconds, latest_run_id", &fakeRowScanner{})
	if _, err := FetchInvestmentMembershipScopeState(context.Background(), client, "org-a", 30); err == nil {
		t.Fatal("a state query that answers no row must be an error, not an unscoped read")
	}
}

// An unreadable state fails the scope-filtered query instead of reading
// every stored generation.
func TestPinInvestmentMembershipScope_UnreadableStateFailsTheQuery(t *testing.T) {
	for _, row := range [][]any{{"other", int64(0), "run-1"}, {"", int64(0), ""}, {"scoped", int64(0), ""}} {
		inner := (&routingFakeClient{}).
			on("SELECT scope_mode, lag_seconds, latest_run_id", &fakeRowScanner{rows: [][]any{row}}).
			on("SELECT count()", &fakeRowScanner{})
		_, err := PinInvestmentMembershipScope(inner).Query(WithInvestmentMembershipScopeRequest(context.Background()), investmentStatement(), orgBindings("org-a"))
		if err == nil {
			t.Fatalf("state row %v: scope-filtered query succeeded, want the resolution error", row)
		}
		for _, call := range inner.calls {
			if call == "SELECT count()" {
				t.Fatalf("state row %v: the scope-filtered statement reached ClickHouse", row)
			}
		}
	}
}

// TestResolve_ScopeResolutionFailure_DegradesChartsWithoutUnscopedRows: on
// the GraphQL chart sections (sankey, flowMatrix) a failed scope resolution
// takes the sections' degradation path -- empty section, degradation
// telemetry, the resolution warn line -- and no investment statement runs,
// so no unscoped row can reach the response.
func TestResolve_ScopeResolutionFailure_DegradesChartsWithoutUnscopedRows(t *testing.T) {
	records := captureSlog(t)
	var degraded []string
	origDegradation := recordDegradation
	recordDegradation = func(_ context.Context, phase string, err error) {
		if strings.Contains(err.Error(), "resolve investment membership scope") {
			degraded = append(degraded, phase)
		}
	}
	t.Cleanup(func() { recordDegradation = origDegradation })

	inner := (&routingFakeClient{}).
		onErr("SELECT scope_mode, lag_seconds, latest_run_id", errors.New("state query failed")).
		on(investmentScopeRunIDPlaceholder(), &fakeRowScanner{rows: [][]any{{"feature_delivery", "feature_delivery.roadmap", float64(99)}}}).
		on(investmentScopeRunIDSQL(), &fakeRowScanner{rows: [][]any{{"feature_delivery", "feature_delivery.roadmap", float64(99)}}})
	dateRange := &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")}
	result, err := Resolve(WithInvestmentMembershipScopeRequest(context.Background()), PinInvestmentMembershipScope(inner), "org-a", model.AnalyticsRequestInput{
		UseInvestment: boolPtr(true),
		Sankey:        &model.SankeyRequestInput{Path: []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputSubcategory}, Measure: model.MeasureInputCount, DateRange: dateRange, MaxNodes: 20, MaxEdges: 20, UseInvestment: boolPtr(true)},
		FlowMatrix:    &model.FlowMatrixRequestInput{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dateRange, MaxNodes: 20, MaxEdges: 20, UseInvestment: boolPtr(true)},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	for _, call := range inner.calls {
		if call == investmentScopeRunIDPlaceholder() || call == investmentScopeRunIDSQL() {
			t.Fatal("a scope-filtered investment statement ran without a resolved scope")
		}
	}
	if result.Sankey == nil || len(result.Sankey.Nodes) != 0 || len(result.Sankey.Edges) != 0 {
		t.Fatalf("sankey = %+v, want an empty section", result.Sankey)
	}
	if result.FlowMatrix == nil || len(result.FlowMatrix.Nodes) != 0 || len(result.FlowMatrix.Edges) != 0 {
		t.Fatalf("flowMatrix = %+v, want an empty section", result.FlowMatrix)
	}
	if !slices.Contains(degraded, "sankey") || !slices.Contains(degraded, "flowMatrix") {
		t.Fatalf("degradation phases carrying the resolution error = %v, want sankey and flowMatrix", degraded)
	}
	warned := 0
	for _, record := range *records {
		if record.msg == "investment membership scope unresolved; failing the scope-filtered query" {
			warned++
		}
	}
	if warned == 0 {
		t.Fatal("no resolution warn line")
	}
}
