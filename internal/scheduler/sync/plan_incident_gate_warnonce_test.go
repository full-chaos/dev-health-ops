package sync

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func fixedWarnClock(t *testing.T, at time.Time) *time.Time {
	t.Helper()
	current := at
	original := planIncidentSkipWarnNow
	planIncidentSkipWarnNow = func() time.Time { return current }
	globalPlanIncidentSkipWarn.resetForTest()
	t.Cleanup(func() {
		planIncidentSkipWarnNow = original
		globalPlanIncidentSkipWarn.resetForTest()
	})
	return &current
}

func TestPlanIncidentSkipWarnMemory(t *testing.T) {
	day1 := time.Date(2026, 10, 25, 23, 59, 0, 0, time.UTC)
	memory := &planIncidentSkipWarnMemory{}
	steps := []struct {
		name   string
		org    string
		integ  string
		at     time.Time
		keys   []string
		expect bool
	}{
		{"first", "o", "i", day1, []string{"incidents", "deployments"}, true},
		{"repeat same day", "o", "i", day1.Add(30 * time.Second), []string{"incidents", "deployments"}, false},
		{"reorder is not a change", "o", "i", day1, []string{"deployments", "incidents"}, false},
		{"new utc day", "o", "i", day1.Add(2 * time.Minute), []string{"incidents", "deployments"}, true},
		{"same new day again", "o", "i", day1.Add(3 * time.Minute), []string{"incidents", "deployments"}, false},
		{"set grows", "o", "i", day1.Add(3 * time.Minute), []string{"incidents", "deployments", "x"}, true},
		{"set shrinks", "o", "i", day1.Add(3 * time.Minute), []string{"incidents"}, true},
		{"other integration independent", "o", "j", day1.Add(3 * time.Minute), []string{"incidents"}, true},
		{"other org independent", "p", "i", day1.Add(3 * time.Minute), []string{"incidents"}, true},
	}
	for _, step := range steps {
		if got := memory.shouldLog(step.org, step.integ, step.at, step.keys); got != step.expect {
			t.Fatalf("%s: shouldLog = %v, want %v", step.name, got, step.expect)
		}
	}
}

func TestPlanIncidentSkipWarnMemoryPrunesPastDays(t *testing.T) {
	past := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	today := past.Add(24 * time.Hour)
	memory := &planIncidentSkipWarnMemory{}
	for index := 0; index < planIncidentSkipWarnPruneAt; index++ {
		memory.shouldLog("o", "old-"+time.Duration(index).String(), past, []string{"incidents"})
	}
	memory.shouldLog("o", "today-a", today, []string{"incidents"})
	memory.shouldLog("o", "today-b", today, []string{"incidents"})
	if len(memory.entries) != 2 {
		t.Fatalf("entries after prune = %d, want 2 (only today's)", len(memory.entries))
	}
	if memory.shouldLog("o", "today-a", today, []string{"incidents"}) {
		t.Fatal("today's entry must survive the prune")
	}
}

func TestPlanIncidentSkipWarnMemoryParallel(t *testing.T) {
	memory := &planIncidentSkipWarnMemory{}
	at := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	var wait sync.WaitGroup
	var mu sync.Mutex
	logged := 0
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if memory.shouldLog("o", "i", at, []string{"incidents"}) {
				mu.Lock()
				logged++
				mu.Unlock()
			}
		}()
	}
	wait.Wait()
	if logged != 1 {
		t.Fatalf("parallel first attempts logged %d times, want 1", logged)
	}
}

func TestEmitIncidentSkipWarnOncePerDayMetricEveryAttempt(t *testing.T) {
	clock := fixedWarnClock(t, time.Date(2026, 10, 25, 8, 0, 0, 0, time.UTC))
	var logs bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(original) })
	globalPlanGateTelemetry.resetForTest()

	attempt := func(keys ...string) {
		for _, key := range keys {
			globalPlanGateTelemetry.observe("jira", key, planGateOutcomeFeatureDisabled)
		}
		emitIncidentSkipWarn("jira", "o", "i", "disabled", keys, 1)
	}
	warns := func() int { return strings.Count(logs.String(), `"msg":"`+planIncidentDatasetsSkippedEvent+`"`) }

	for range 5 {
		attempt("incidents")
	}
	if got := warns(); got != 1 {
		t.Fatalf("5 same-day attempts wrote %d WARNs, want 1", got)
	}
	if got := globalPlanGateTelemetry.snapshotForTest("jira", "incidents", planGateOutcomeFeatureDisabled); got != 5 {
		t.Fatalf("metric = %d, want 5 (every attempt)", got)
	}
	attempt("incidents", "deployments")
	if got := warns(); got != 2 {
		t.Fatalf("changed set wrote %d WARNs total, want 2", got)
	}
	*clock = clock.Add(24 * time.Hour)
	attempt("incidents", "deployments")
	attempt("incidents", "deployments")
	if got := warns(); got != 3 {
		t.Fatalf("new day wrote %d WARNs total, want 3", got)
	}
	if got := globalPlanGateTelemetry.snapshotForTest("jira", "incidents", planGateOutcomeFeatureDisabled); got != 8 {
		t.Fatalf("metric = %d, want 8", got)
	}
}
