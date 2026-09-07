package capacityforecast

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// A fake row scanner and client, same shape as hotspots_test.go's, per this
// epic's convention that each operation package carries its own doubles.
//
// The scan destinations here are wider than hotspots's because the persisted
// capacity_forecasts row genuinely spans five column types plus Nullables of
// four of them -- the widths are migration 023's, not a convenience.

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
		return errors.New("capacityforecast test: scan arity mismatch")
	}
	for index, destination := range dest {
		value := row[index]
		switch pointer := destination.(type) {
		case *string:
			*pointer = value.(string)
		case *time.Time:
			*pointer = value.(time.Time)
		case *uint8:
			*pointer = value.(uint8)
		case *uint16:
			*pointer = value.(uint16)
		case *uint32:
			*pointer = value.(uint32)
		case *uint64:
			*pointer = value.(uint64)
		case *float64:
			*pointer = value.(float64)
		case **string:
			if value == nil {
				*pointer = nil
			} else {
				local := value.(string)
				*pointer = &local
			}
		case **uint16:
			if value == nil {
				*pointer = nil
			} else {
				local := value.(uint16)
				*pointer = &local
			}
		case **uint32:
			if value == nil {
				*pointer = nil
			} else {
				local := value.(uint32)
				*pointer = &local
			}
		case **time.Time:
			if value == nil {
				*pointer = nil
			} else {
				local := value.(time.Time)
				*pointer = &local
			}
		default:
			return errors.New("capacityforecast test: unsupported scan destination")
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
		return nil, errors.New("capacityforecast test: unexpected extra query")
	}
	return f.responses[index], nil
}

func day(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

func throughputRows(t *testing.T, counts ...uint64) *fakeRowScanner {
	t.Helper()
	rows := make([][]any, 0, len(counts))
	for index, count := range counts {
		rows = append(rows, []any{day(t, "2026-06-01").AddDate(0, 0, index), count})
	}
	return &fakeRowScanner{rows: rows}
}

func bindingValue(bindings []clickhouse.Binding, name string) (any, bool) {
	for _, binding := range bindings {
		if binding.Name == name {
			return binding.Value, true
		}
	}
	return nil, false
}

// requireOrgScoped is the tenant-isolation assertion every read in this package
// must earn. The Python suite has five `..._passes_org_id_in_sql_params` tests
// plus a cross-org isolation test; this is their replacement, applied to EVERY
// statement a call issued rather than to a hand-picked one.
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

func TestResolveTargetItemsFallsBackOnAFalsyCheck(t *testing.T) {
	zero := 0
	seven := 7
	cases := []struct {
		name        string
		targetItems *int
		backlog     int
		want        int
	}{
		{"absent target falls back to the backlog", nil, 42, 42},
		{"a target of ZERO falls back too -- the check is falsy, not nil", &zero, 42, 42},
		{"a positive target wins", &seven, 42, 7},
		{"a positive target wins even over a larger backlog", &seven, 1000, 7},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := resolveTargetItems(testCase.targetItems, testCase.backlog); got != testCase.want {
				t.Errorf("got %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestResolveForecastReturnsNilWithoutHistory(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{}}}
	got, err := ResolveForecast(context.Background(), client, "org-1", nil, day(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("ResolveForecast: %v", err)
	}
	if got != nil {
		t.Fatalf("got %+v, want nil for a scope with no throughput history", got)
	}
	// The BACKLOG query must not have run: Python returns before loading it.
	// An extra query here is not merely wasteful, it is a second chance to fail
	// a request that already has its answer.
	if client.calls != 1 {
		t.Errorf("issued %d queries, want exactly 1 (throughput only)", client.calls)
	}
	requireOrgScoped(t, client, "org-1")
}

func TestResolveForecastReturnsNilWhenTargetAndBacklogAreBothEmpty(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{
		throughputRows(t, 3, 4, 5),
		{rows: [][]any{{uint64(0)}}}, // backlog
	}}
	got, err := ResolveForecast(context.Background(), client, "org-1", nil, day(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("ResolveForecast: %v", err)
	}
	if got != nil {
		t.Fatalf("got %+v, want nil when the backlog is empty and no target was supplied", got)
	}
	requireOrgScoped(t, client, "org-1")
}

func TestResolveForecastComputesFromHistoryAndBacklog(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{
		throughputRows(t, 2, 4, 6, 8, 10),
		{rows: [][]any{{uint64(30)}}},
	}}
	got, err := ResolveForecast(context.Background(), client, "org-7", nil, day(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("ResolveForecast: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want a forecast")
	}

	if got.BacklogSize != 30 {
		t.Errorf("backlogSize: got %d, want 30", got.BacklogSize)
	}
	// items falls back to the backlog, and the response echoes what was
	// actually forecast rather than what was requested.
	if got.TargetItems == nil || *got.TargetItems != 30 {
		t.Errorf("targetItems: got %v, want the backlog 30", got.TargetItems)
	}
	// historyDays is the SAMPLE count, not the requested window.
	if got.HistoryDays != 5 {
		t.Errorf("historyDays: got %d, want the 5 sample days", got.HistoryDays)
	}
	if !got.InsufficientHistory {
		t.Error("insufficientHistory: 5 days is below the 14-day floor and must be flagged")
	}
	if got.ThroughputMean != 6 {
		t.Errorf("throughputMean: got %v, want 6", got.ThroughputMean)
	}

	// The fixed-scope branch ran, so the day percentiles and their dates are
	// populated; no targetDate was supplied, so the items percentiles are not.
	if got.P50Days == nil || got.P85Days == nil || got.P95Days == nil {
		t.Errorf("day percentiles: got %v/%v/%v, want all three populated",
			got.P50Days, got.P85Days, got.P95Days)
	}
	if got.P50Items != nil || got.P85Items != nil || got.P95Items != nil {
		t.Errorf("item percentiles: got %v/%v/%v, want all three absent without a targetDate",
			got.P50Items, got.P85Items, got.P95Items)
	}
	if got.P50Date == nil {
		t.Fatal("p50Date: got nil, want a date derived from the forecast horizon")
	}
	// The dates are anchored on the injected clock, not on the wall clock --
	// which is the property that makes this resolver testable at all.
	if p50 := got.P50Date.Time(); p50.Before(day(t, "2026-09-01")) {
		t.Errorf("p50Date %s precedes the injected today", p50)
	}
	requireOrgScoped(t, client, "org-7")
}

func TestResolveForecastRunsBothModesWhenATargetDateIsSupplied(t *testing.T) {
	targetDate := graphqldate.New(day(t, "2026-10-01"))
	targetItems := 25
	input := &model.CapacityForecastInput{
		TargetItems: &targetItems,
		TargetDate:  &targetDate,
		HistoryDays: 30,
		Simulations: 200,
	}
	client := &fakeClient{responses: []*fakeRowScanner{
		throughputRows(t, 2, 4, 6, 8, 10),
		{rows: [][]any{{uint64(30)}}},
	}}

	got, err := ResolveForecast(context.Background(), client, "org-7", input, day(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("ResolveForecast: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want a forecast")
	}
	// BOTH column sets, because Python's two modes are independent `if`
	// statements rather than an if/else. An `else` in the kernel would leave
	// one of these pairs nil and nothing else in the response would show it.
	if got.P50Days == nil {
		t.Error("p50Days: got nil, want the fixed-scope branch to have run")
	}
	if got.P50Items == nil {
		t.Error("p50Items: got nil, want the fixed-date branch to have run too")
	}
	if got.TargetItems == nil || *got.TargetItems != 25 {
		t.Errorf("targetItems: got %v, want the supplied 25", got.TargetItems)
	}
	if got.TargetDate == nil || got.TargetDate.String() != "2026-10-01" {
		t.Errorf("targetDate: got %v, want it echoed back verbatim", got.TargetDate)
	}
}

func TestResolveForecastHistoryWindowIsDerivedFromTheInjectedClock(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{}}}
	input := &model.CapacityForecastInput{HistoryDays: 90, Simulations: 10}
	if _, err := ResolveForecast(
		context.Background(), client, "org-1", input, day(t, "2026-09-01"),
	); err != nil {
		t.Fatalf("ResolveForecast: %v", err)
	}
	// 2026-09-01 minus 90 days. Interpolated into the SQL rather than bound,
	// matching capacity_queries.py -- so it is asserted against the statement
	// text, not against a binding.
	if want := "day >= '2026-06-03'"; !strings.Contains(client.statements[0], want) {
		t.Errorf("throughput window: statement does not contain %q:\n%s", want, client.statements[0])
	}
}

func TestResolveForecastPropagatesAQueryFailure(t *testing.T) {
	boom := errors.New("clickhouse unavailable")
	client := &fakeClient{errs: []error{boom}}
	_, err := ResolveForecast(context.Background(), client, "org-1", nil, day(t, "2026-09-01"))
	if err == nil {
		t.Fatal("got nil error, want the ClickHouse failure to propagate")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error does not wrap the cause: %v", err)
	}
}

func TestRandomSeedIsFreshPerCall(t *testing.T) {
	// The DECLARED divergence: Python runs this Monte Carlo unseeded, so every
	// request draws a different stream. A port that fixed the seed would make
	// every response identical while still looking like a simulation -- and
	// nothing else in the output would reveal it.
	seen := make(map[int64]struct{}, 32)
	for range 32 {
		seed, err := randomSeed()
		if err != nil {
			t.Fatalf("randomSeed: %v", err)
		}
		if seed < 0 {
			t.Fatalf("randomSeed returned %d; negative seeds alias onto their positive twin "+
				"through CPython's abs() and halve the seed space", seed)
		}
		seen[seed] = struct{}{}
	}
	if len(seen) != 32 {
		t.Errorf("32 draws produced only %d distinct seeds -- the seed is not fresh per call", len(seen))
	}
}

func TestStrDatetimeUTCMatchesPythonStr(t *testing.T) {
	cases := []struct {
		name   string
		moment time.Time
		want   string
	}{
		{
			// str(), not isoformat(): a SPACE separator. The throughputForecast
			// resolver next door renders "T" for the same kind of field.
			name:   "space separator, six fractional digits",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 678901000, time.UTC),
			want:   "2026-09-07 01:23:45.678901+00:00",
		},
		{
			// Trailing zeros are KEPT: Python prints six digits or none.
			// Go's ".999999" layout would render this as ".123".
			name:   "trailing zeros survive",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 123000000, time.UTC),
			want:   "2026-09-07 01:23:45.123000+00:00",
		},
		{
			name:   "a zero microsecond drops the fraction entirely",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 0, time.UTC),
			want:   "2026-09-07 01:23:45+00:00",
		},
		{
			name:   "a non-UTC input is normalised, not relabelled",
			moment: time.Date(2026, 9, 7, 3, 23, 45, 0, time.FixedZone("CEST", 2*3600)),
			want:   "2026-09-07 01:23:45+00:00",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := strDatetimeUTC(testCase.moment); got != testCase.want {
				t.Errorf("got %q, want %q", got, testCase.want)
			}
		})
	}
}

// TestResolveForecastRejectsANonPositiveSimulationCount is CHAOS-5349 r1 P1's
// resolver half, and a DECLARED DIVERGENCE from Python.
//
// Python answers p50=p85=p95=0 for `simulations: -1`, because `range(-1)` is
// empty. Executed against the live module during the review:
// `PY_NEGATIVE_SIMULATIONS_OK p50=0 p85=0 p95=0`. That is a defect, not a
// contract: a forecast built from zero simulations is not a conservative
// estimate, it is a fabricated one, and handing it back as an answer is worse
// than refusing the request.
//
// So this port refuses, BEFORE issuing any query -- a nonsensical request must
// not cost the org a ClickHouse read. The nil client below is the proof: if the
// guard ever stopped short-circuiting, this test would panic rather than pass.
func TestResolveForecastRejectsANonPositiveSimulationCount(t *testing.T) {
	for _, simulations := range []int{0, -1, -10000} {
		input := &model.CapacityForecastInput{HistoryDays: 90, Simulations: simulations}

		// A nil QueryClient: reaching any read at all is a nil-pointer panic.
		result, err := ResolveForecast(
			context.Background(), nil, "org-1", input, day(t, "2026-09-01"))

		if result != nil {
			t.Errorf("simulations=%d: got a forecast, want a rejection", simulations)
		}
		if err == nil {
			t.Fatalf("simulations=%d: got nil error, want a validation error", simulations)
		}
		gqlErr, ok := err.(*gqlerror.Error)
		if !ok {
			t.Fatalf("simulations=%d: error is %T, want *gqlerror.Error so the client sees a coded rejection",
				simulations, err)
		}
		if code, _ := gqlErr.Extensions["code"].(string); code != "BAD_USER_INPUT" {
			t.Errorf("simulations=%d: extensions.code = %v, want BAD_USER_INPUT",
				simulations, gqlErr.Extensions["code"])
		}
		// The offending FIELD and its VALUE are both named: a rejection that
		// does not say which input was wrong makes the caller guess.
		if field, _ := gqlErr.Extensions["field"].(string); field != "simulations" {
			t.Errorf("simulations=%d: extensions.field = %v, want \"simulations\"",
				simulations, gqlErr.Extensions["field"])
		}
		if !strings.Contains(gqlErr.Message, "simulations") ||
			!strings.Contains(gqlErr.Message, strconv.Itoa(simulations)) {
			t.Errorf("simulations=%d: message %q names neither the field nor the value",
				simulations, gqlErr.Message)
		}
	}
}

// TestResolveForecastAcceptsTheSmallestValidSimulationCount pins the boundary
// from the other side, so the guard cannot quietly become `< 2` or `<= 1`.
func TestResolveForecastAcceptsTheSmallestValidSimulationCount(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{
		throughputRows(t, 2, 4, 6),
		{rows: [][]any{{uint64(12)}}},
	}}
	input := &model.CapacityForecastInput{HistoryDays: 90, Simulations: 1}

	got, err := ResolveForecast(context.Background(), client, "org-1", input, day(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("simulations=1 was rejected: %v", err)
	}
	if got == nil {
		t.Fatal("simulations=1 produced no forecast")
	}
	if got.P50Days == nil {
		t.Error("simulations=1: the fixed-scope branch did not run")
	}
}
