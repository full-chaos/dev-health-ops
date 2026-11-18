package throughputforecast

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-8167: the backlog read says how its sum was found. The counts come from the read (see
// stale_backlog_integration_test.go for the SQL on a real engine); this pins the mapping and the bound.
func TestResolveSaysHowTheBacklogWasFound(t *testing.T) {
	stale := time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC)
	fresh := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		row       *fakeRowScanner
		supplied  *int
		wantState model.ThroughputBacklogState
		wantAsOf  string
		wantSize  int
	}{
		{"every measured key inside the window", oneRow(uint64(9), uint64(2), uint64(0), fresh), nil,
			model.ThroughputBacklogStateMeasured, "2026-08-30", 9},
		{"a measured key before the window start", oneRow(uint64(0), uint64(1), uint64(1), stale), nil,
			model.ThroughputBacklogStateStale, "2025-05-03", 0},
		{"one of two keys stale: the sum is kept", oneRow(uint64(7), uint64(2), uint64(1), stale), nil,
			model.ThroughputBacklogStateStale, "2025-05-03", 7},
		{"no measured key (minIf over nothing is the epoch)", oneRow(uint64(0), uint64(0), uint64(0), time.Unix(0, 0).UTC()), nil,
			model.ThroughputBacklogStateNoRows, "", 0},
		{"a supplied backlog is not read", nil, func() *int { v := 0; return &v }(),
			model.ThroughputBacklogStateSupplied, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			responses := fullResponseSet(t, 120, 7)
			if tc.row != nil {
				responses[1] = tc.row
			} else {
				responses = append(responses[:1], responses[2:]...)
			}
			client := &fakeClient{responses: responses}
			got, err := Resolve(context.Background(), client, "org-1",
				model.ThroughputForecastInput{HistoryWeeks: 12, BacklogSize: tc.supplied}, mustDay(t, "2026-09-01"))
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got.BacklogState != tc.wantState {
				t.Fatalf("backlogState: got %s, want %s", got.BacklogState, tc.wantState)
			}
			if got.BacklogSize != tc.wantSize {
				t.Fatalf("backlogSize: got %d, want %d", got.BacklogSize, tc.wantSize)
			}
			asOf := ""
			if got.BacklogAsOfDay != nil {
				asOf = got.BacklogAsOfDay.String()
			}
			if asOf != tc.wantAsOf {
				t.Fatalf("backlogAsOfDay: got %q, want %q", asOf, tc.wantAsOf)
			}
			if tc.row == nil {
				for _, statement := range client.statements {
					if strings.Contains(statement, "AS stale_keys") {
						t.Fatal("a supplied backlog issued the backlog read")
					}
				}
				return
			}
			if value, ok := bindingValue(client.bindings[1], "start_date"); !ok || value != "2026-06-09" {
				t.Fatalf("backlog read start_date = %v (bound %v), want 2026-06-09: the window start of 12 weeks before 2026-09-01", value, ok)
			}
		})
	}
}
