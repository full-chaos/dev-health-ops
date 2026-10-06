package sync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type listerStepper struct {
	configs []UnscheduledConfig
	err     error
	calls   int
}

func (stepper *listerStepper) HandoffDueResult(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
	return HandoffResult{}, nil
}

func (stepper *listerStepper) UnscheduledConfigs(context.Context) ([]UnscheduledConfig, error) {
	stepper.calls++
	return stepper.configs, stepper.err
}

func newUnscheduledTestLoop(t *testing.T, stepper HandoffStepper) (*Loop, *bytes.Buffer, *testLoopClock) {
	t.Helper()
	clock := &testLoopClock{now: time.Date(2026, 10, 6, 1, 0, 0, 0, time.UTC)}
	loop, _ := newTestLoop(t, stepper, clock)
	var buffer bytes.Buffer
	loop.config.Logger = slog.New(slog.NewTextHandler(&buffer, nil))
	return loop, &buffer, clock
}

// CHAOS-8768: an active planner-managed config without schedule_cron is
// skipped by the handoff SQL. The skip must be loud: one WARN per config per
// pass, naming provider and config hash, never the config id.
func TestLoopWarnsOncePerPassForEveryConfigWithoutSchedule(t *testing.T) {
	stepper := &listerStepper{configs: []UnscheduledConfig{
		{Provider: "jira", Hash: ConfigHash("ef7c2457-3874-4ca6-af3c-2f95b6216aef")},
		{Provider: "github", Hash: ConfigHash("a7a44bc9-94c7-41b3-84cd-9c7b896377ec")},
	}}
	loop, buffer, clock := newUnscheduledTestLoop(t, stepper)
	now := clock.Now()
	if err := loop.step(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	// Same pass: the next one-second windows add no line.
	if err := loop.step(context.Background(), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	output := buffer.String()
	if got := strings.Count(output, "sync.scheduler.config_without_schedule"); got != 2 {
		t.Fatalf("WARN lines = %d, want 2 (one per config, once per pass):\n%s", got, output)
	}
	for _, want := range []string{"level=WARN", "provider=jira", "provider=github", "config_hash=" + ConfigHash("ef7c2457-3874-4ca6-af3c-2f95b6216aef")} {
		if !strings.Contains(output, want) {
			t.Fatalf("log missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "ef7c2457") {
		t.Fatalf("log carries a config id:\n%s", output)
	}
	// Next pass an hour later: warned again.
	if err := loop.step(context.Background(), now.Add(unscheduledReportEvery)); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(buffer.String(), "sync.scheduler.config_without_schedule"); got != 4 {
		t.Fatalf("WARN lines after second pass = %d, want 4", got)
	}
	loop.mu.Lock()
	gauge := loop.unscheduledConfigs
	loop.mu.Unlock()
	if gauge != 2 {
		t.Fatalf("unscheduledConfigs gauge = %d, want 2", gauge)
	}
}

func TestLoopUnscheduledReadFailureIsLoudAndDoesNotFailTheWindow(t *testing.T) {
	stepper := &listerStepper{err: errors.New("boom")}
	loop, buffer, clock := newUnscheduledTestLoop(t, stepper)
	if err := loop.step(context.Background(), clock.Now()); err != nil {
		t.Fatalf("a diagnostic read failure failed the window: %v", err)
	}
	if !strings.Contains(buffer.String(), "sync.scheduler.unscheduled_config_read_failed") {
		t.Fatalf("read failure not logged:\n%s", buffer.String())
	}
}

func TestUnscheduledConfigsSQLSelectsExactlyWhatTheHandoffSQLSkips(t *testing.T) {
	statement := strings.ToUpper(unscheduledConfigsSQL)
	for _, want := range []string{
		"CONFIG.IS_ACTIVE = TRUE",
		"CONFIG.PLANNER_MANAGED = TRUE",
		"COALESCE(CONFIG.SYNC_OPTIONS->>'SCHEDULE_CRON', '') = ''",
	} {
		if !strings.Contains(statement, want) {
			t.Fatalf("report query missing %q", want)
		}
	}
	if !strings.Contains(strings.ToUpper(schedulerHandoffCandidatesSQL), "COALESCE(CONFIG.SYNC_OPTIONS->>'SCHEDULE_CRON', '') <> ''") {
		t.Fatal("handoff query no longer excludes configs without schedule_cron; revisit the CHAOS-8768 report")
	}
	for _, forbidden := range []string{"INSERT", "UPDATE", "DELETE", "FOR UPDATE"} {
		if strings.Contains(statement, forbidden) {
			t.Fatalf("report query must be a plain read, found %q", forbidden)
		}
	}
}
