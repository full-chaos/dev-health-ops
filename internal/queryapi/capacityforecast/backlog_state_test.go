package capacityforecast

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-7508: the backlog read says how its sum was found. The counts come from the read (see
// latest_day_backlog_integration_test.go for the SQL on a real engine); this pins the mapping and the bound.
func TestResolveForecastSaysHowTheBacklogWasFound(t *testing.T) {
	stale := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	fresh := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	target := 5
	for _, tc := range []struct {
		name      string
		row       []any
		wantState model.NewestDayState
		wantAsOf  string
		wantSize  int
	}{
		{"every measured key inside the window", []any{uint64(9), uint64(2), uint64(0), fresh},
			model.NewestDayStateMeasured, "2026-08-30", 9},
		{"a measured key before the window start: the sum is kept", []any{uint64(7), uint64(2), uint64(1), stale},
			model.NewestDayStateStale, "2026-05-03", 7},
		{"no measured key (minIf over nothing is the epoch)", []any{uint64(0), uint64(0), uint64(0), time.Unix(0, 0).UTC()},
			model.NewestDayStateNoRows, "<nil>", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeClient{responses: []*fakeRowScanner{
				throughputRows(t, 3, 4, 5),
				{rows: [][]any{tc.row}},
			}}
			got, err := ResolveForecast(context.Background(), client, "org-1",
				&model.CapacityForecastInput{TargetItems: &target, HistoryDays: 90, Simulations: 100}, day(t, "2026-09-01"))
			if err != nil {
				t.Fatalf("ResolveForecast: %v", err)
			}
			if got.BacklogSize != tc.wantSize {
				t.Fatalf("backlogSize = %d, want %d", got.BacklogSize, tc.wantSize)
			}
			if got.BacklogState == nil || *got.BacklogState != tc.wantState {
				t.Fatalf("backlogState = %v, want %s", got.BacklogState, tc.wantState)
			}
			asOf := "<nil>"
			if got.BacklogAsOfDay != nil {
				asOf = got.BacklogAsOfDay.String()
			}
			if asOf != tc.wantAsOf {
				t.Fatalf("backlogAsOfDay = %s, want %s", asOf, tc.wantAsOf)
			}
			if value, ok := bindingValue(client.bindings[1], "start_date"); !ok || value != "2026-06-03" {
				t.Fatalf("backlog read start_date = %v (bound %v), want 2026-06-03: the window start of 90 days before 2026-09-01", value, ok)
			}
		})
	}
}

// A stored forecast keeps no read state: both fields are null, never a guessed MEASURED.
func TestStoredCapacityForecastCarriesNoBacklogState(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{
		rows: [][]any{flatten(persistedRow(t, "forecast-1", time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC)))},
	}}}
	got, err := ResolveForecasts(context.Background(), client, "org-1", nil)
	if err != nil {
		t.Fatalf("ResolveForecasts: %v", err)
	}
	if len(got.Edges) != 1 {
		t.Fatalf("edges: got %d, want 1", len(got.Edges))
	}
	node := got.Edges[0].Node
	if node.BacklogState != nil || node.BacklogAsOfDay != nil {
		t.Fatalf("stored forecast backlogState = %v, backlogAsOfDay = %v, want both null", node.BacklogState, node.BacklogAsOfDay)
	}
}
