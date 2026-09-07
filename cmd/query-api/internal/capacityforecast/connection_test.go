package capacityforecast

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// persistedRow builds one capacity_forecasts row in the SELECT's column order,
// with each value at the width migration 023 declares. The order is the thing
// under test as much as the values: ClickHouse binds by position, so two
// same-typed columns swapped would scan silently crossed.
func persistedRow(t *testing.T, forecastID string, computedAt time.Time) []any {
	t.Helper()
	targetItems := uint32(120)
	targetDate := day(t, "2026-10-01")
	p50Date := day(t, "2026-09-20")
	p85Date := day(t, "2026-09-25")
	p95Date := day(t, "2026-09-28")
	p50Days := uint16(19)
	p85Days := uint16(24)
	p95Days := uint16(27)
	teamID := "team-alpha"
	workScopeID := "scope-beta"
	return []any{
		forecastID, computedAt, teamID, workScopeID, uint32(200),
		&targetItems, &targetDate, &p50Date, &p85Date, &p95Date,
		&p50Days, &p85Days, &p95Days,
		nil, nil, nil, // p50/p85/p95 items -- unset on a fixed-scope forecast
		4.25, 1.5, uint16(90),
		uint8(0), uint8(1),
	}
}

// dereferenced flattens the pointer-valued cells persistedRow builds, because
// the fake scanner takes bare values and unwraps them itself.
func flatten(row []any) []any {
	out := make([]any, 0, len(row))
	for _, cell := range row {
		switch value := cell.(type) {
		case *uint32:
			out = append(out, *value)
		case *uint16:
			out = append(out, *value)
		case *time.Time:
			out = append(out, *value)
		default:
			out = append(out, cell)
		}
	}
	return out
}

func TestResolveForecastsMapsAPersistedRow(t *testing.T) {
	computedAt := time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC)
	client := &fakeClient{responses: []*fakeRowScanner{{
		rows: [][]any{flatten(persistedRow(t, "forecast-1", computedAt))},
	}}}

	got, err := ResolveForecasts(context.Background(), client, "org-1", nil)
	if err != nil {
		t.Fatalf("ResolveForecasts: %v", err)
	}
	if len(got.Edges) != 1 {
		t.Fatalf("edges: got %d, want 1", len(got.Edges))
	}
	node := got.Edges[0].Node

	if node.ForecastID != "forecast-1" {
		t.Errorf("forecastId: got %q", node.ForecastID)
	}
	if node.ComputedAt != "2026-09-01 12:30:00+00:00" {
		t.Errorf("computedAt: got %q, want Python's str(datetime) rendering", node.ComputedAt)
	}
	if node.BacklogSize != 200 {
		t.Errorf("backlogSize: got %d, want 200", node.BacklogSize)
	}
	if node.TargetItems == nil || *node.TargetItems != 120 {
		t.Errorf("targetItems: got %v, want 120", node.TargetItems)
	}
	if node.P50Days == nil || *node.P50Days != 19 {
		t.Errorf("p50Days: got %v, want 19", node.P50Days)
	}
	// The item percentiles are NULL on a fixed-scope row and must stay absent
	// rather than becoming zero -- "we did not forecast items" is not "we
	// forecast zero items".
	if node.P50Items != nil {
		t.Errorf("p50Items: got %v, want nil for a fixed-scope row", node.P50Items)
	}
	if node.TargetDate == nil || node.TargetDate.String() != "2026-10-01" {
		t.Errorf("targetDate: got %v", node.TargetDate)
	}
	if node.InsufficientHistory {
		t.Error("insufficientHistory: got true, want false from a UInt8 0")
	}
	if !node.HighVariance {
		t.Error("highVariance: got false, want true from a UInt8 1")
	}
	if node.ThroughputMean != 4.25 || node.ThroughputStddev != 1.5 {
		t.Errorf("throughput stats: got %v/%v", node.ThroughputMean, node.ThroughputStddev)
	}
	if node.HistoryDays != 90 {
		t.Errorf("historyDays: got %d, want 90", node.HistoryDays)
	}
	requireOrgScoped(t, client, "org-1")
}

func TestResolveForecastsCursorIsTheForecastIDVerbatim(t *testing.T) {
	computedAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	client := &fakeClient{responses: []*fakeRowScanner{{
		rows: [][]any{
			flatten(persistedRow(t, "forecast-a", computedAt)),
			flatten(persistedRow(t, "forecast-b", computedAt)),
		},
	}}}

	got, err := ResolveForecasts(context.Background(), client, "org-1", nil)
	if err != nil {
		t.Fatalf("ResolveForecasts: %v", err)
	}
	// NOT base64, NOT an offset. A client that stored a cursor from the Python
	// resolver must still be able to read it after the cutover.
	if got.Edges[0].Cursor != "forecast-a" || got.Edges[1].Cursor != "forecast-b" {
		t.Errorf("cursors: got %q/%q, want the forecast ids verbatim",
			got.Edges[0].Cursor, got.Edges[1].Cursor)
	}
	if got.PageInfo.StartCursor == nil || *got.PageInfo.StartCursor != "forecast-a" {
		t.Errorf("startCursor: got %v", got.PageInfo.StartCursor)
	}
	if got.PageInfo.EndCursor == nil || *got.PageInfo.EndCursor != "forecast-b" {
		t.Errorf("endCursor: got %v", got.PageInfo.EndCursor)
	}
	// totalCount is the PAGE length, not a table count. Two rows under the
	// default limit of 10, so it is 2 and hasNextPage is false.
	if got.TotalCount != 2 {
		t.Errorf("totalCount: got %d, want the page length 2", got.TotalCount)
	}
	if got.PageInfo.HasNextPage {
		t.Error("hasNextPage: got true, want false for a partial page")
	}
	if got.PageInfo.HasPreviousPage {
		t.Error("hasPreviousPage: got true, want the hardcoded false")
	}
}

func TestResolveForecastsHasNextPageIsAFullPageGuess(t *testing.T) {
	computedAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	client := &fakeClient{responses: []*fakeRowScanner{{
		rows: [][]any{
			flatten(persistedRow(t, "forecast-a", computedAt)),
			flatten(persistedRow(t, "forecast-b", computedAt)),
		},
	}}}
	filters := &model.CapacityForecastFilterInput{Limit: 2}

	got, err := ResolveForecasts(context.Background(), client, "org-1", filters)
	if err != nil {
		t.Fatalf("ResolveForecasts: %v", err)
	}
	// Exactly `limit` rows, so it reports true even though nothing beyond them
	// is known to exist. A guess, and Python's guess.
	if !got.PageInfo.HasNextPage {
		t.Error("hasNextPage: got false, want true when the page is exactly full")
	}
}

func TestResolveForecastsEmptyConnectionIsNotNull(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{}}}
	got, err := ResolveForecasts(context.Background(), client, "org-1", nil)
	if err != nil {
		t.Fatalf("ResolveForecasts: %v", err)
	}
	// edges is a non-null list on the wire. A nil slice marshals as JSON null
	// and breaks any client that iterates it without a guard.
	if got.Edges == nil {
		t.Fatal("edges: got nil, want a non-nil empty slice")
	}
	if len(got.Edges) != 0 || got.TotalCount != 0 {
		t.Errorf("got %d edges / totalCount %d, want an empty connection", len(got.Edges), got.TotalCount)
	}
	if got.PageInfo.StartCursor != nil || got.PageInfo.EndCursor != nil {
		t.Error("cursors: want both nil on an empty page")
	}
}

func TestResolveForecastsFiltersBuildTheExpectedPredicates(t *testing.T) {
	teamID := "team-alpha"
	scopeID := "scope-beta"
	from := graphqldate.New(day(t, "2026-08-01"))
	to := graphqldate.New(day(t, "2026-09-01"))
	filters := &model.CapacityForecastFilterInput{
		TeamID: &teamID, WorkScopeID: &scopeID, FromDate: &from, ToDate: &to, Limit: 5,
	}
	client := &fakeClient{responses: []*fakeRowScanner{{}}}

	if _, err := ResolveForecasts(context.Background(), client, "org-1", filters); err != nil {
		t.Fatalf("ResolveForecasts: %v", err)
	}
	statement := client.statements[0]
	for _, want := range []string{
		"team_id = {team_id:String}",
		"work_scope_id = {work_scope_id:String}",
		"toDate(computed_at) >= {from_date:Date}",
		"toDate(computed_at) <= {to_date:Date}",
		"ORDER BY computed_at DESC",
		"LIMIT 5",
	} {
		if !strings.Contains(statement, want) {
			t.Errorf("statement is missing %q:\n%s", want, statement)
		}
	}
	// Deliberately NOT FINAL: the table is a ReplacingMergeTree ordered by a
	// column that is unique per row, so nothing ever collapses and Python does
	// not pay for it either.
	if strings.Contains(statement, "FINAL") {
		t.Errorf("statement uses FINAL, which Python's query does not:\n%s", statement)
	}
	for name, want := range map[string]any{
		"team_id": teamID, "work_scope_id": scopeID,
		"from_date": "2026-08-01", "to_date": "2026-09-01",
	} {
		value, ok := bindingValue(client.bindings[0], name)
		if !ok {
			t.Errorf("no %s binding", name)
			continue
		}
		if value != want {
			t.Errorf("%s bound %v, want %v", name, value, want)
		}
	}
}

func TestResolveForecastsEmptyStringFiltersAreTreatedAsAbsent(t *testing.T) {
	empty := ""
	filters := &model.CapacityForecastFilterInput{TeamID: &empty, WorkScopeID: &empty, Limit: 10}
	client := &fakeClient{responses: []*fakeRowScanner{{}}}

	if _, err := ResolveForecasts(context.Background(), client, "org-1", filters); err != nil {
		t.Fatalf("ResolveForecasts: %v", err)
	}
	// Python's checks are falsy. team_id is Nullable(String), so a literal
	// `team_id = ''` predicate would match no row at all and silently empty
	// every response.
	//
	// Asserted against the PREDICATE text, not the bare column name: both
	// columns appear in the SELECT list on every call, so a substring test for
	// "team_id" alone would fail against a perfectly correct query.
	if strings.Contains(client.statements[0], "team_id = {team_id:String}") {
		t.Errorf("an empty team_id produced a predicate:\n%s", client.statements[0])
	}
	if strings.Contains(client.statements[0], "work_scope_id = {work_scope_id:String}") {
		t.Errorf("an empty work_scope_id produced a predicate:\n%s", client.statements[0])
	}
	// The binding is the second half of the same claim: a predicate-free query
	// that still bound a value would be harmless today and a live filter the
	// moment someone re-added the clause.
	if _, ok := bindingValue(client.bindings[0], "team_id"); ok {
		t.Error("an empty team_id was bound as a parameter")
	}
	if _, ok := bindingValue(client.bindings[0], "work_scope_id"); ok {
		t.Error("an empty work_scope_id was bound as a parameter")
	}
}

func TestResolveForecastsPropagatesAQueryFailure(t *testing.T) {
	boom := errors.New("clickhouse unavailable")
	client := &fakeClient{errs: []error{boom}}
	if _, err := ResolveForecasts(context.Background(), client, "org-1", nil); !errors.Is(err, boom) {
		t.Fatalf("got %v, want the ClickHouse failure to propagate", err)
	}
}
