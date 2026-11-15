package otlpmetrics

import (
	"bytes"
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/scheduler/fixed"
)

type oneScheduleStepper struct{}

func (oneScheduleStepper) Step(context.Context, time.Time) (fixed.WindowResult, error) {
	return fixed.WindowResult{}, nil
}

func (oneScheduleStepper) Schedules() []fixed.Schedule {
	return []fixed.Schedule{{ID: "schedule-a"}}
}

// The real fixed loop writer, through the shipped label table, must export both labels of fixed_scheduler_occurrences_total.
func TestFixedSchedulerOccurrencesExportScheduleAndResult(t *testing.T) {
	loop, err := fixed.NewLoop(oneScheduleStepper{}, fixed.DefaultLoopConfig(health.NewRegistry(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	var text bytes.Buffer
	if err := loop.WritePrometheus(&text); err != nil {
		t.Fatal(err)
	}
	policy, err := DefaultLabelPolicy(nil)
	if err != nil {
		t.Fatal(err)
	}
	points := sumPoints(t, collectExported(t, text.String(), policy)["fixed_scheduler_occurrences_total"])
	if len(points) != 4 {
		t.Fatalf("points = %d, want 4 (one per result)", len(points))
	}
	results := map[string]bool{}
	for _, p := range points {
		if schedule, _ := p.Attributes.Value(attribute.Key("schedule")); schedule.AsString() != "schedule-a" {
			t.Errorf("point attributes %v: schedule label missing or wrong", p.Attributes)
		}
		result, ok := p.Attributes.Value(attribute.Key("result"))
		if !ok {
			t.Errorf("point attributes %v: result label missing", p.Attributes)
			continue
		}
		results[result.AsString()] = true
	}
	for _, want := range []string{"claimed", "duplicate", "skipped", "failed"} {
		if !results[want] {
			t.Errorf("no point carries result=%s: %v", want, results)
		}
	}
	if got := policy.Dropped(); len(got) != 0 {
		t.Errorf("the table drops labels of the writer: %v", got)
	}
}
