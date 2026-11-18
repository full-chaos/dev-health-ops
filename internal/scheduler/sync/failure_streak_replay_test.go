package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

type workItemsReplayRows struct {
	pgx.Rows
	rows []struct {
		source string
		end    *time.Time
	}
	next int
}

func (rows *workItemsReplayRows) Next() bool { rows.next++; return rows.next <= len(rows.rows) }
func (rows *workItemsReplayRows) Scan(dest ...any) error {
	row := rows.rows[rows.next-1]
	*dest[0].(*string) = row.source
	*dest[1].(**time.Time) = row.end
	return nil
}
func (rows *workItemsReplayRows) Err() error { return nil }
func (rows *workItemsReplayRows) Close()     {}

type workItemsReplayReader struct {
	runExists bool
	units     *workItemsReplayRows
	errOn     string
	queries   []string
}

func (reader *workItemsReplayReader) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	reader.queries = append(reader.queries, sql)
	if sql == reader.errOn {
		return nil, errors.New("boom")
	}
	if sql == workItemsReplayRunSQL {
		rows := &workItemsReplayRows{}
		if reader.runExists {
			rows.rows = make([]struct {
				source string
				end    *time.Time
			}, 1)
		}
		return rows, nil
	}
	return reader.units, nil
}

func TestLoadWorkItemsReplayReadsTheCommittedUnitsOfTheRun(t *testing.T) {
	end := time.Date(2026, 11, 17, 12, 0, 0, 0, time.FixedZone("x", 3600))
	units := &workItemsReplayRows{rows: []struct {
		source string
		end    *time.Time
	}{{"a", &end}, {"b", nil}}}
	datasets := []PlanDataset{{Key: "work-items"}}

	got, err := loadWorkItemsReplay(context.Background(), &workItemsReplayReader{runExists: true, units: units}, "run", datasets)
	if err != nil || got == nil || len(got.Units) != 2 || !got.Units["a"].Equal(end) || got.Units["a"].Location() != time.UTC || got.Units["b"] != nil {
		t.Fatalf("replay = %+v err %v, want a=%s in UTC and b with an open end", got, err, end.UTC())
	}
	if _, has := got.committed("c"); has {
		t.Fatal("source c has no committed unit")
	}

	// The sync run row exists and holds no work-items unit: a replay, with no entry for any source.
	got, err = loadWorkItemsReplay(context.Background(), &workItemsReplayReader{runExists: true, units: &workItemsReplayRows{}}, "run", datasets)
	if err != nil || got == nil || len(got.Units) != 0 {
		t.Fatalf("replay = %+v err %v, want a replay with no unit", got, err)
	}

	// No sync run row: a first attempt, and no unit query.
	first := &workItemsReplayReader{units: units}
	if got, err = loadWorkItemsReplay(context.Background(), first, "run", datasets); err != nil || got != nil || len(first.queries) != 1 {
		t.Fatalf("first attempt = %+v err %v queries %d, want nil, one query", got, err, len(first.queries))
	}

	// A plan with no work-item family dataset reads nothing.
	none := &workItemsReplayReader{runExists: true, units: units}
	if got, err = loadWorkItemsReplay(context.Background(), none, "run", []PlanDataset{{Key: "commits"}}); err != nil || got != nil || len(none.queries) != 0 {
		t.Fatalf("no family = %+v err %v queries %d, want no read", got, err, len(none.queries))
	}
}

// A failed read fails the attempt with a plain error, never ErrInvalidPlan: the occurrence is retried, not quarantined.
func TestLoadWorkItemsReplayReadFailureIsRetryable(t *testing.T) {
	for _, failing := range []string{workItemsReplayRunSQL, workItemsReplayUnitsSQL} {
		reader := &workItemsReplayReader{runExists: true, units: &workItemsReplayRows{}, errOn: failing}
		got, err := loadWorkItemsReplay(context.Background(), reader, "run", []PlanDataset{{Key: "work-items"}})
		if err == nil || got != nil || errors.Is(err, ErrInvalidPlan) {
			t.Fatalf("read of %q: replay %+v err %v, want a plain error", failing, got, err)
		}
	}
}

func TestReplayedWorkItemsEndKeepsOnlyAnEndTheHalvingProduces(t *testing.T) {
	start := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	end := start.Add(72 * time.Hour)
	at := func(span time.Duration) *time.Time { value := start.Add(span); return &value }
	for _, test := range []struct {
		name      string
		start     *time.Time
		committed *time.Time
		keep      bool
	}{
		{"the planned end", &start, at(72 * time.Hour), true},
		{"one halving", &start, at(36 * time.Hour), true},
		{"the floor", &start, at(providersync.WindowHalvingFloor), true},
		{"an end no halving produces", &start, at(50 * time.Hour), false},
		{"an end past the planned end", &start, at(80 * time.Hour), false},
		{"an open committed end", &start, nil, false},
		{"an open start", nil, at(36 * time.Hour), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, kept := replayedWorkItemsEnd(test.start, end, test.committed)
			if kept != test.keep || (kept && !got.Equal(*test.committed)) || (!kept && !got.Equal(end)) {
				t.Fatalf("end = %s kept %v, want kept %v", got, kept, test.keep)
			}
		})
	}
}

func TestPlannerWorkItemsReplayKeepsTheCommittedDecision(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	watermark := now.Add(-72 * time.Hour)
	slowed := providersync.FailureStreak{Count: 3, Class: "rate_limited", LastAt: now.Add(-time.Hour)}
	halving := providersync.FailureStreak{Count: 1, ShrinkCount: 1, Class: "result_too_large", LastAt: now.Add(-time.Hour)}
	natural := workItemsUnit(t, backoffInput(now, watermark, providersync.FailureStreak{}))
	if natural == nil || natural.WindowEnd == nil {
		t.Fatal("no baseline unit")
	}
	committedEnd := func(span time.Duration) *time.Time { value := natural.WindowStart.Add(span); return &value }
	naturalSpan := natural.WindowEnd.Sub(*natural.WindowStart)

	for _, test := range []struct {
		name     string
		streak   providersync.FailureStreak
		replay   *WorkItemsReplay
		wantUnit bool
		wantSpan time.Duration
	}{
		{"a first attempt halves", halving, nil, true, naturalSpan / 2},
		{"a replay keeps the unplanned-halving end", providersync.FailureStreak{}, &WorkItemsReplay{Units: map[string]*time.Time{"source": committedEnd(naturalSpan / 2)}}, true, naturalSpan / 2},
		{"a replay keeps the natural end under a halving streak", halving, &WorkItemsReplay{Units: map[string]*time.Time{"source": committedEnd(naturalSpan)}}, true, naturalSpan},
		{"a replay keeps its unit under a slow-down streak", slowed, &WorkItemsReplay{Units: map[string]*time.Time{"source": committedEnd(naturalSpan)}}, true, naturalSpan},
		{"a replay with no committed unit plans none", providersync.FailureStreak{}, &WorkItemsReplay{Units: map[string]*time.Time{}}, false, 0},
		{"a committed end no halving produces is not kept", halving, &WorkItemsReplay{Units: map[string]*time.Time{"source": committedEnd(naturalSpan * 2 / 3)}}, true, naturalSpan / 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := backoffInput(now, watermark, test.streak)
			input.WorkItemsReplay = test.replay
			unit := workItemsUnit(t, input)
			if (unit != nil) != test.wantUnit {
				t.Fatalf("unit planned = %v, want %v", unit != nil, test.wantUnit)
			}
			if unit != nil && unit.WindowEnd.Sub(*unit.WindowStart) != test.wantSpan {
				t.Fatalf("span = %s, want %s", unit.WindowEnd.Sub(*unit.WindowStart), test.wantSpan)
			}
		})
	}
}
