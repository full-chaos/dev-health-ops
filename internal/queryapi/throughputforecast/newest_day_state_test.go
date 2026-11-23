package throughputforecast

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-7508: how a value read from each key's newest day is mapped from the counts its read returns.
func TestNewestDayReadFromCounts(t *testing.T) {
	day := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name          string
		grains, stale uint64
		oldest        time.Time
		wantState     model.NewestDayState
		wantAsOf      string
		wantGrains    int
	}{
		{"no key: the epoch of min() over nothing is not served", 0, 0, time.Unix(0, 0).UTC(), model.NewestDayStateNoRows, "<nil>", 0},
		{"every key inside the window", 3, 0, day, model.NewestDayStateMeasured, "2026-05-03", 3},
		{"one key before the window start", 3, 1, day, model.NewestDayStateStale, "2026-05-03", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := newNewestDayRead("grains", tc.grains, tc.stale, tc.oldest)
			if err != nil {
				t.Fatal(err)
			}
			asOf := "<nil>"
			if got.asOfDay != nil {
				asOf = got.asOfDay.Format("2006-01-02")
			}
			if got.state != tc.wantState || asOf != tc.wantAsOf || got.grains != tc.wantGrains {
				t.Fatalf("got %s %s %d, want %s %s %d", got.state, asOf, got.grains, tc.wantState, tc.wantAsOf, tc.wantGrains)
			}
		})
	}
}

// CHAOS-7508: the stale wip, coverage and overlay reads bind the window start of the request, and their counts reach
// the answer. Order of the reads: history, backlog, coverage, overlay, stale wip, review, incident.
func TestResolveBindsTheWindowStartOnTheNewestDayReads(t *testing.T) {
	stale := time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	responses := fullResponseSet(t, 120, 7)
	responses[2] = oneRow(uint64(40), uint64(60), uint64(100), uint64(3), uint64(1), stale) // coverage: 1 of 3 grains stale
	responses[3] = oneRow(12.5, uint64(20), uint64(2), older)                               // overlay: 2 measured keys
	responses[4] = oneRow(30.0, 90.0, uint64(4), uint64(2), stale)                          // stale wip: 2 of 4 keys stale
	client := &fakeClient{responses: responses}

	got, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{HistoryWeeks: 12}, mustDay(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	for _, index := range []int{2, 3, 4} {
		if value, ok := bindingValue(client.bindings[index], "start_date"); !ok || value != "2026-06-09" {
			t.Fatalf("read %d start_date = %v (bound %v), want 2026-06-09: the window start of 12 weeks before 2026-09-01", index, value, ok)
		}
	}
	coverage := got.EstimateCoverage
	if coverage.State != model.NewestDayStateStale || coverage.AsOfDay == nil || coverage.AsOfDay.String() != "2025-05-03" {
		t.Fatalf("estimateCoverage state %s asOf %v, want STALE 2025-05-03", coverage.State, coverage.AsOfDay)
	}
	if got.WipCongestionState != model.NewestDayStateMeasured || got.WipCongestionAsOfDay == nil ||
		got.WipCongestionAsOfDay.String() != "2026-08-20" || got.WipCongestionGrainCount != 2 {
		t.Fatalf("wipCongestion state %s asOf %v grains %d, want MEASURED 2026-08-20 2",
			got.WipCongestionState, got.WipCongestionAsOfDay, got.WipCongestionGrainCount)
	}
	if got.StaleWip == nil || got.StaleWip.State != model.NewestDayStateStale ||
		got.StaleWip.AsOfDay.String() != "2025-05-03" || got.StaleWip.GrainCount != 4 {
		t.Fatalf("staleWip = %+v, want STALE 2025-05-03 4 grains", got.StaleWip)
	}
}

// The no-history answer reads no current WIP and no stale WIP: wipCongestion is NO_ROWS with no day, and the
// coverage answer keeps the state of its own read.
func TestResolveNoHistoryAnswerReadsNoCurrentWIP(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{
		{},              // 1 no throughput rows
		backlogRow(500), // 2 backlog
		coverageRow(40, 60, 100, 3),
	}}
	got, err := Resolve(context.Background(), client, "org-1",
		model.ThroughputForecastInput{HistoryWeeks: 12}, mustDay(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.WipCongestionState != model.NewestDayStateNoRows || got.WipCongestionAsOfDay != nil || got.WipCongestionGrainCount != 0 {
		t.Fatalf("wipCongestion state %s asOf %v grains %d, want NO_ROWS, null, 0",
			got.WipCongestionState, got.WipCongestionAsOfDay, got.WipCongestionGrainCount)
	}
	if got.StaleWip != nil {
		t.Fatalf("staleWip = %+v, want absent on the no-history answer", got.StaleWip)
	}
	if got.EstimateCoverage.State != model.NewestDayStateMeasured || got.EstimateCoverage.AsOfDay == nil {
		t.Fatalf("estimateCoverage state %s asOf %v, want MEASURED with a day", got.EstimateCoverage.State, got.EstimateCoverage.AsOfDay)
	}
}
