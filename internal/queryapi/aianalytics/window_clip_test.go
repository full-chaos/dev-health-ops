package aianalytics

import (
	"context"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

type recordingClient struct{ bindings [][]clickhouse.Binding }

func (r *recordingClient) Query(_ context.Context, _ string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	r.bindings = append(r.bindings, b)
	return &scriptedRows{}, nil
}

func pinWindowToday(t *testing.T, ts string) {
	t.Helper()
	now, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t.Fatal(err)
	}
	prev := windowToday
	windowToday = func() time.Time { return now }
	t.Cleanup(func() { windowToday = prev })
}

func rangeOf(start, end string) model.AIDateRangeInput {
	return model.AIDateRangeInput{StartDate: graphqldate.New(mustDay(start)), EndDate: graphqldate.New(mustDay(end))}
}

type entry struct {
	name string
	call func(ctx context.Context, c QueryClient, dr model.AIDateRangeInput) error
}

func entries() []entry {
	return []entry{
		{"ImpactSummary", func(ctx context.Context, c QueryClient, dr model.AIDateRangeInput) error {
			_, err := ImpactSummary(ctx, c, "org-1", dr, nil)
			return err
		}},
		{"Comparison", func(ctx context.Context, c QueryClient, dr model.AIDateRangeInput) error {
			_, err := Comparison(ctx, c, "org-1", dr, nil)
			return err
		}},
		{"ReviewLoad", func(ctx context.Context, c QueryClient, dr model.AIDateRangeInput) error {
			_, err := ReviewLoad(ctx, c, "org-1", dr, nil)
			return err
		}},
		{"RiskBreakdown", func(ctx context.Context, c QueryClient, dr model.AIDateRangeInput) error {
			_, err := RiskBreakdown(ctx, c, "org-1", dr, nil)
			return err
		}},
		{"AttributedPrs", func(ctx context.Context, c QueryClient, dr model.AIDateRangeInput) error {
			_, err := AttributedPrs(ctx, c, "org-1", dr, nil, 10, 0)
			return err
		}},
		{"AttributionOverview", func(ctx context.Context, c QueryClient, dr model.AIDateRangeInput) error {
			_, err := AttributionOverview(ctx, c, "org-1", dr, nil, 10, 0)
			return err
		}},
		{"GovernanceSummary", func(ctx context.Context, c QueryClient, dr model.AIDateRangeInput) error {
			_, err := GovernanceSummary(ctx, c, "org-1", dr, nil, 10)
			return err
		}},
	}
}

// ends are every end the entry bound: end_day as a day, end as the last second of its day.
func ends(c *recordingClient) (days, stamps []string) {
	for _, bs := range c.bindings {
		for _, b := range bs {
			switch b.Name {
			case "end_day":
				days = append(days, b.Value.(string))
			case "end":
				if ts, ok := b.Value.(time.Time); ok {
					stamps = append(stamps, ts.UTC().Format("2006-01-02"))
				}
			}
		}
	}
	return days, stamps
}

func TestEntries_EndOfTodayOrLaterIsYesterday(t *testing.T) {
	for _, now := range []string{"2026-10-22T00:30:00Z", "2026-10-22T23:30:00Z"} {
		pinWindowToday(t, now)
		for _, e := range entries() {
			for _, end := range []string{"2026-10-22", "2026-10-23", "2027-01-01"} {
				c := &recordingClient{}
				if err := e.call(context.Background(), c, rangeOf("2026-10-15", end)); err != nil {
					t.Fatalf("%s: %v", e.name, err)
				}
				days, stamps := ends(c)
				if len(days)+len(stamps) == 0 {
					t.Fatalf("%s: no end was bound", e.name)
				}
				for _, d := range append(days, stamps...) {
					if d != "2026-10-21" {
						t.Errorf("%s clock %s end %s: bound end %s, want 2026-10-21", e.name, now, end, d)
					}
				}
			}
			c := &recordingClient{}
			if err := e.call(context.Background(), c, rangeOf("2026-10-10", "2026-10-18")); err != nil {
				t.Fatalf("%s: %v", e.name, err)
			}
			days, stamps := ends(c)
			for _, d := range append(days, stamps...) {
				if d != "2026-10-18" {
					t.Errorf("%s: an earlier end moved to %s", e.name, d)
				}
			}
		}
	}
}

func TestEntries_StartOfTodayIsAnEmptyWindowNotAnError(t *testing.T) {
	pinWindowToday(t, "2026-10-22T12:00:00Z")
	for _, e := range entries() {
		c := &recordingClient{}
		if err := e.call(context.Background(), c, rangeOf("2026-10-22", "2026-10-22")); err != nil {
			t.Fatalf("%s: %v", e.name, err)
		}
		days, stamps := ends(c)
		for _, d := range append(days, stamps...) {
			if d != "2026-10-21" {
				t.Errorf("%s: bound end %s, want 2026-10-21 (before the start, so no row)", e.name, d)
			}
		}
	}
}

func TestEntries_NamedEndBeforeStartStillErrors(t *testing.T) {
	pinWindowToday(t, "2026-10-22T12:00:00Z")
	for _, e := range entries() {
		if err := e.call(context.Background(), &recordingClient{}, rangeOf("2026-10-20", "2026-10-19")); err == nil {
			t.Errorf("%s: a named end before the start must still be refused", e.name)
		}
	}
}

func TestEntries_OneRequestReadsOneClock(t *testing.T) {
	start := time.Date(2026, 10, 22, 23, 59, 59, 0, time.UTC)
	ctx := graphql.WithOperationContext(context.Background(), &graphql.OperationContext{Stats: graphql.Stats{OperationStart: start}})
	prev := windowToday
	windowToday = func() time.Time { return start.Add(2 * time.Second) } // the next UTC day, as a later field would read it
	t.Cleanup(func() { windowToday = prev })
	for _, e := range entries() {
		c := &recordingClient{}
		if err := e.call(ctx, c, rangeOf("2026-10-15", "2026-10-23")); err != nil {
			t.Fatalf("%s: %v", e.name, err)
		}
		days, stamps := ends(c)
		for _, d := range append(days, stamps...) {
			if d != "2026-10-21" {
				t.Errorf("%s: the request clock is the operation start (10-22), end bound %s, want 2026-10-21", e.name, d)
			}
		}
	}
}
