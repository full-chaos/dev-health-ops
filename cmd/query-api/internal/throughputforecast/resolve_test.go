package throughputforecast

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// Fake row scanner and client, same shape as the sibling operation packages'.

type fakeRowScanner struct {
	rows   [][]any
	cursor int
	err    error
}

func (f *fakeRowScanner) Next() bool {
	if f.err != nil {
		return false
	}
	return f.cursor < len(f.rows)
}

func (f *fakeRowScanner) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	if len(dest) != len(row) {
		return errors.New("throughputforecast test: scan arity mismatch")
	}
	for index, destination := range dest {
		value := row[index]
		switch pointer := destination.(type) {
		case *time.Time:
			*pointer = value.(time.Time)
		case *uint64:
			*pointer = value.(uint64)
		case *float64:
			*pointer = value.(float64)
		case **float64:
			if value == nil {
				*pointer = nil
			} else {
				local := value.(float64)
				*pointer = &local
			}
		default:
			return errors.New("throughputforecast test: unsupported scan destination")
		}
	}
	return nil
}

func (f *fakeRowScanner) Err() error   { return f.err }
func (f *fakeRowScanner) Close() error { return nil }

type fakeClient struct {
	responses  []*fakeRowScanner
	errs       []error
	calls      int
	statements []string
	bindings   [][]clickhouse.Binding
}

func (f *fakeClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	index := f.calls
	f.calls++
	f.statements = append(f.statements, statement)
	f.bindings = append(f.bindings, bindings)
	if index < len(f.errs) && f.errs[index] != nil {
		return nil, f.errs[index]
	}
	if index >= len(f.responses) {
		return nil, errors.New("throughputforecast test: unexpected extra query")
	}
	return f.responses[index], nil
}

func mustDay(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

func historyRows(t *testing.T, days int, perDay uint64) *fakeRowScanner {
	t.Helper()
	rows := make([][]any, 0, days)
	for index := range days {
		rows = append(rows, []any{mustDay(t, "2026-06-01").AddDate(0, 0, index), perDay})
	}
	return &fakeRowScanner{rows: rows}
}

func oneRow(values ...any) *fakeRowScanner {
	return &fakeRowScanner{rows: [][]any{values}}
}

// fullResponseSet returns the seven scanners a complete, history-bearing call
// consumes, in the exact order Resolve issues them. Ordering IS the contract
// here: Python loads history, then backlog, then coverage, then the four
// overlays, and a reordered port would read a different table into a different
// field with no type error to catch it.
func fullResponseSet(t *testing.T, days int, perDay uint64) []*fakeRowScanner {
	t.Helper()
	return []*fakeRowScanner{
		historyRows(t, days, perDay),                   // 1 throughput history
		oneRow(uint64(500)),                            // 2 backlog
		oneRow(uint64(40), uint64(60), uint64(100)),    // 3 estimate coverage
		oneRow(12.5, uint64(20)),                       // 4 work item overlay
		oneRow(floatPointer(30.0), floatPointer(90.0)), // 5 stale wip
		oneRow(floatPointer(12.0)),                     // 6 review overlay
		oneRow(2.5),                                    // 7 incident overlay
	}
}

func floatPointer(value float64) any { return value }

func bindingValue(bindings []clickhouse.Binding, name string) (any, bool) {
	for _, binding := range bindings {
		if binding.Name == name {
			return binding.Value, true
		}
	}
	return nil, false
}

// requireOrgScoped replaces the Python suite's five
// `..._passes_org_id_in_sql_params` tests and its cross-org isolation test, and
// strengthens them: it checks EVERY statement the call issued rather than one
// hand-picked query, so a newly added read cannot skip the predicate unnoticed.
func requireOrgScoped(t *testing.T, client *fakeClient, orgID string) {
	t.Helper()
	if client.calls == 0 {
		t.Fatal("no query was issued, so org scoping was not exercised at all")
	}
	for index, statement := range client.statements {
		if !strings.Contains(statement, "org_id = {org_id:String}") {
			t.Errorf("statement %d carries no org_id predicate:\n%s", index, statement)
		}
		value, ok := bindingValue(client.bindings[index], "org_id")
		if !ok {
			t.Errorf("statement %d binds no org_id", index)
			continue
		}
		if value != orgID {
			t.Errorf("statement %d bound org_id %v, want the authorized org %q", index, value, orgID)
		}
	}
}

func TestResolveRejectsANonPositiveHistoryWindowBeforeQuerying(t *testing.T) {
	client := &fakeClient{}
	_, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{HistoryWeeks: 0}, mustDay(t, "2026-09-01"))
	if err == nil {
		t.Fatal("got nil error, want a rejection for history_weeks=0")
	}
	// Checked before any read, so a client error costs the org nothing.
	if client.calls != 0 {
		t.Errorf("issued %d queries before rejecting the input, want 0", client.calls)
	}
}

func TestResolveScopesEveryReadToTheAuthorizedOrg(t *testing.T) {
	client := &fakeClient{responses: fullResponseSet(t, 120, 7)}
	if _, err := Resolve(context.Background(), client, "org-authorized",
		model.ThroughputForecastInput{HistoryWeeks: 12}, mustDay(t, "2026-09-01"),
	); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if client.calls != 7 {
		t.Fatalf("issued %d queries, want all 7 reads", client.calls)
	}
	requireOrgScoped(t, client, "org-authorized")
}

func TestResolveTeamFilterSwitchesShapeOnCardinality(t *testing.T) {
	cases := []struct {
		name        string
		teamIDs     []string
		wantClause  string
		wantBinding string
	}{
		{
			name:       "no teams means org-wide, with no predicate at all",
			teamIDs:    nil,
			wantClause: "",
		},
		{
			// `=` rather than a one-element IN. Python's _team_filter branches
			// on cardinality, and the two forms bind DIFFERENT parameter names
			// (team_id vs team_ids), so collapsing them breaks the binding.
			name:        "one team uses equality and binds team_id",
			teamIDs:     []string{"team-a"},
			wantClause:  "team_id = {team_id:String}",
			wantBinding: "team_id",
		},
		{
			name:        "several teams use IN and bind team_ids",
			teamIDs:     []string{"team-a", "team-b"},
			wantClause:  "team_id IN {team_ids:Array(String)}",
			wantBinding: "team_ids",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			client := &fakeClient{responses: fullResponseSet(t, 120, 7)}
			if _, err := Resolve(context.Background(), client, "org-1",
				model.ThroughputForecastInput{TeamIds: testCase.teamIDs, HistoryWeeks: 12},
				mustDay(t, "2026-09-01"),
			); err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			statement := client.statements[0]
			if testCase.wantClause == "" {
				if strings.Contains(statement, "team_id = {") || strings.Contains(statement, "team_id IN {") {
					t.Errorf("an empty team selection produced a predicate:\n%s", statement)
				}
				if _, ok := bindingValue(client.bindings[0], "team_id"); ok {
					t.Error("an empty team selection bound team_id")
				}
				return
			}
			if !strings.Contains(statement, testCase.wantClause) {
				t.Errorf("statement is missing %q:\n%s", testCase.wantClause, statement)
			}
			if _, ok := bindingValue(client.bindings[0], testCase.wantBinding); !ok {
				t.Errorf("statement binds no %s", testCase.wantBinding)
			}
		})
	}
}

func TestResolveLabelsTheTeamOnlyForASingleTeamScope(t *testing.T) {
	cases := []struct {
		name    string
		teamIDs []string
		want    *string
	}{
		{"org-wide leaves teamId null", nil, nil},
		{"one team is labelled", []string{"team-a"}, stringPointer("team-a")},
		{"several teams leave it null", []string{"team-a", "team-b"}, nil},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			client := &fakeClient{responses: fullResponseSet(t, 120, 7)}
			got, err := Resolve(context.Background(), client, "org-1",
				model.ThroughputForecastInput{TeamIds: testCase.teamIDs, HistoryWeeks: 12},
				mustDay(t, "2026-09-01"))
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			switch {
			case testCase.want == nil && got.TeamID != nil:
				t.Errorf("teamId: got %q, want null", *got.TeamID)
			case testCase.want != nil && got.TeamID == nil:
				t.Errorf("teamId: got null, want %q", *testCase.want)
			case testCase.want != nil && *got.TeamID != *testCase.want:
				t.Errorf("teamId: got %q, want %q", *got.TeamID, *testCase.want)
			}
		})
	}
}

func stringPointer(value string) *string { return &value }
func intPointer(value int) *int          { return &value }

func TestResolveCallerSuppliedBacklogSkipsTheBacklogRead(t *testing.T) {
	responses := fullResponseSet(t, 120, 7)
	// Drop the backlog scanner: the call must not consume it.
	responses = append(responses[:1], responses[2:]...)
	client := &fakeClient{responses: responses}

	got, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{BacklogSize: intPointer(250), HistoryWeeks: 12},
		mustDay(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.BacklogSize != 250 {
		t.Errorf("backlogSize: got %d, want the supplied 250", got.BacklogSize)
	}
	if client.calls != 6 {
		t.Errorf("issued %d queries, want 6 (the backlog read skipped)", client.calls)
	}
}

func TestResolveAnExplicitZeroBacklogIsSuppliedNotAbsent(t *testing.T) {
	responses := fullResponseSet(t, 120, 7)
	// No backlog read AND no estimate-coverage read: the backlog is zero, and
	// coverage OF an empty backlog is not a meaningful ratio.
	responses = append(responses[:1], responses[3:]...)
	client := &fakeClient{responses: responses}

	got, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{BacklogSize: intPointer(0), HistoryWeeks: 12},
		mustDay(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.BacklogSize != 0 {
		t.Errorf("backlogSize: got %d, want the supplied 0", got.BacklogSize)
	}
	// The nil check is on the POINTER: an explicit 0 must NOT fall through to
	// the derived backlog, which is exactly what a `backlogSize != 0` check
	// would do.
	if client.calls != 5 {
		t.Errorf("issued %d queries, want 5 (backlog and coverage both skipped)", client.calls)
	}
	if got.EstimateCoverage != nil {
		t.Error("estimateCoverage: got a value, want it absent for an empty backlog")
	}
	// A zero backlog finishes in zero weeks -- not "unknown".
	if got.P50Weeks == nil || *got.P50Weeks != 0 {
		t.Errorf("p50Weeks: got %v, want 0", got.P50Weeks)
	}
}

func TestResolveRejectsANegativeSuppliedBacklog(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{historyRows(t, 120, 7)}}
	_, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{BacklogSize: intPointer(-1), HistoryWeeks: 12},
		mustDay(t, "2026-09-01"))
	if err == nil {
		t.Fatal("got nil error, want a rejection for a negative backlog")
	}
}

func TestResolveEmptyScopeReturnsTheStructuredNoHistoryPayload(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{
		{},                  // 1 no throughput rows
		oneRow(uint64(500)), // 2 backlog
		oneRow(uint64(40), uint64(60), uint64(100)), // 3 estimate coverage
	}}

	got, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{HistoryWeeks: 12}, mustDay(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	// NOT null. "No data yet" and "the query failed" are different answers, and
	// a null would make a brand-new team indistinguishable from a broken one.
	if got == nil {
		t.Fatal("got nil, want the structured no-history payload")
	}
	if got.ForecastID != "no-history" {
		t.Errorf("forecastId: got %q, want the no-history sentinel", got.ForecastID)
	}
	if got.P50Weeks != nil || got.P75Weeks != nil || got.P90Weeks != nil {
		t.Error("weeks: want all three null on the no-history path")
	}
	if !got.InsufficientHistory {
		t.Error("insufficientHistory: want true")
	}
	if len(got.RollingWindows) != 3 {
		t.Fatalf("rollingWindows: got %d, want all three still reported", len(got.RollingWindows))
	}
	for _, window := range got.RollingWindows {
		if !window.InsufficientHistory || window.SampleCount != 0 {
			t.Errorf("window %dw: got %d samples / insufficient=%v, want 0 / true",
				window.WindowWeeks, window.SampleCount, window.InsufficientHistory)
		}
	}
	if got.PrimaryRisk == nil || got.PrimaryRisk.Kind != riskKindNone {
		t.Errorf("primaryRisk: got %v, want the neutral overlay", got.PrimaryRisk)
	}
	// Coverage IS carried on this path; staleWip is NOT even queried, because
	// Python passes only estimate_coverage into its empty payload.
	if got.EstimateCoverage == nil {
		t.Error("estimateCoverage: want it present on the no-history payload")
	}
	if got.StaleWip != nil {
		t.Error("staleWip: want it absent on the no-history payload")
	}
	if client.calls != 3 {
		t.Errorf("issued %d queries, want 3 (the four overlays are never read)", client.calls)
	}
}

func TestResolveStaleWIPIsAbsentWhenBothAgesAreNull(t *testing.T) {
	responses := fullResponseSet(t, 120, 7)
	responses[4] = oneRow(nil, nil)
	client := &fakeClient{responses: responses}

	got, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{HistoryWeeks: 12}, mustDay(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	// An object carrying two nulls is a different answer from no object, and
	// the UI renders them differently.
	if got.StaleWip != nil {
		t.Errorf("staleWip: got %+v, want nil when both ages are null", got.StaleWip)
	}
}

func TestResolveStaleWIPSurvivesOneNullAge(t *testing.T) {
	responses := fullResponseSet(t, 120, 7)
	responses[4] = oneRow(nil, floatPointer(48.0))
	client := &fakeClient{responses: responses}

	got, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{HistoryWeeks: 12}, mustDay(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.StaleWip == nil {
		t.Fatal("staleWip: got nil, want it present when ONE age is known")
	}
	if got.StaleWip.P50AgeHours != nil {
		t.Errorf("p50AgeHours: got %v, want null", got.StaleWip.P50AgeHours)
	}
	if got.StaleWip.P90AgeHours == nil || *got.StaleWip.P90AgeHours != 48 {
		t.Errorf("p90AgeHours: got %v, want 48", got.StaleWip.P90AgeHours)
	}
}

func TestResolveEstimateCoverageRatioIsNullForAZeroBacklogRow(t *testing.T) {
	responses := fullResponseSet(t, 120, 7)
	// A non-empty request backlog, but the coverage table reports zero.
	responses[2] = oneRow(uint64(0), uint64(0), uint64(0))
	client := &fakeClient{responses: responses}

	got, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{HistoryWeeks: 12}, mustDay(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.EstimateCoverage == nil {
		t.Fatal("estimateCoverage: got nil, want the counts reported")
	}
	// The counts are still reported: "the backlog is empty" is a fact, and only
	// the RATIO is undefined.
	if got.EstimateCoverage.Ratio != nil {
		t.Errorf("ratio: got %v, want null rather than a division by zero", got.EstimateCoverage.Ratio)
	}
	if got.EstimateCoverage.BacklogSize != 0 {
		t.Errorf("coverage backlogSize: got %d, want 0", got.EstimateCoverage.BacklogSize)
	}
}

func TestResolveReadWindowsAreDerivedFromTheInjectedClock(t *testing.T) {
	client := &fakeClient{responses: fullResponseSet(t, 120, 7)}
	if _, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{HistoryWeeks: 4}, mustDay(t, "2026-09-01"),
	); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	// 2026-09-01 minus 4 weeks. Bound, not interpolated -- unlike the capacity
	// path next door, which interpolates because ITS Python does.
	value, ok := bindingValue(client.bindings[0], "start_date")
	if !ok {
		t.Fatal("the throughput history read binds no start_date")
	}
	if value != "2026-08-04" {
		t.Errorf("start_date: got %v, want 2026-08-04", value)
	}
}

func TestResolvePropagatesAQueryFailure(t *testing.T) {
	boom := errors.New("clickhouse unavailable")
	client := &fakeClient{errs: []error{boom}}
	_, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{HistoryWeeks: 12}, mustDay(t, "2026-09-01"))
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want the ClickHouse failure to propagate", err)
	}
}

func TestResolveWorkItemOverlayReadsBothColumnsFromOneRow(t *testing.T) {
	responses := fullResponseSet(t, 120, 7)
	// average_wip first, current_wip second -- the SELECT's own order. Swapping
	// them changes which value the 1.25 threshold is compared against and
	// nothing else in the response reveals it.
	responses[3] = oneRow(10.0, uint64(20))
	client := &fakeClient{responses: responses}

	got, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{HistoryWeeks: 12}, mustDay(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	// ratio = current/average = 20/10 = 2.0, score = 2.0/1.25 = 1.6.
	if got.WipCongestion == nil {
		t.Fatal("wipCongestion: got nil")
	}
	if got.WipCongestion.Value != 2.0 {
		t.Errorf("wip ratio: got %v, want 2.0 (current 20 / average 10)", got.WipCongestion.Value)
	}
	if !got.WipCongestion.Active {
		t.Error("wipCongestion: want active at a ratio of 2.0 over the 1.25 threshold")
	}
}

// TestToModelRendersComputedAtAsRFC3339 replaces
// TestIsoFormatUTCMatchesPythonIsoformat, which pinned this package's own
// local isoFormatUTC helper. CHAOS-5450 / R55 made RFC 3339 with an
// explicit "+00:00" the canonical form for every String-typed timestamp
// field, and the three resolvers now share graphqldate.RFC3339UTC -- so
// this asserts the resolver's OUTPUT against literals, and the identical
// literals appear in the capacity resolvers' own tests. That is what makes
// "all three agree byte for byte on the same instant" checkable without
// wiring three packages together in one test.
//
// This resolver's rendering does NOT change: it already emitted this form.
// The point of keeping the pin is that the shared helper is now the only
// thing standing between all three fields and a silent divergence.
func TestToModelRendersComputedAtAsRFC3339(t *testing.T) {
	cases := []struct {
		name   string
		moment time.Time
		want   string
	}{
		{
			name:   "T separator and a six-digit fraction",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 678901000, time.UTC),
			want:   "2026-09-07T01:23:45.678901+00:00",
		},
		{
			name:   "trailing zeros survive",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 123000000, time.UTC),
			want:   "2026-09-07T01:23:45.123000+00:00",
		},
		{
			name:   "a zero microsecond drops the fraction entirely",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 0, time.UTC),
			want:   "2026-09-07T01:23:45+00:00",
		},
		{
			name:   "a non-UTC input is normalised",
			moment: time.Date(2026, 9, 7, 3, 23, 45, 0, time.FixedZone("CEST", 2*3600)),
			want:   "2026-09-07T01:23:45+00:00",
		},
		{
			// Sub-microsecond precision is truncated, never rounded up into
			// a different microsecond.
			name:   "nanoseconds below a microsecond are truncated",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 678901999, time.UTC),
			want:   "2026-09-07T01:23:45.678901+00:00",
		},
		{
			// A zone BEHIND UTC, so normalisation is exercised in both
			// directions and a sign error cannot pass.
			name:   "a negative offset is normalised too",
			moment: time.Date(2026, 9, 6, 21, 23, 45, 0, time.FixedZone("EDT", -4*3600)),
			want:   "2026-09-07T01:23:45+00:00",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := toModel("forecast-1", testCase.moment, forecastResult{}, nil, nil, nil)
			if got.ComputedAt != testCase.want {
				t.Errorf("computedAt: got %q, want %q", got.ComputedAt, testCase.want)
			}
		})
	}
}
