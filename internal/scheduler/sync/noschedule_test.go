package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type listerStepper struct {
	configs []UnscheduledConfig
	total   int // 0 = len(configs)
	err     error
	calls   int
	// block makes the read wait for ctx expiry, as a hung database would.
	block bool
}

func (stepper *listerStepper) HandoffDueResult(context.Context, time.Time, int, Coordinator) (HandoffResult, error) {
	return HandoffResult{}, nil
}

func (stepper *listerStepper) UnscheduledConfigs(ctx context.Context) ([]UnscheduledConfig, int, error) {
	stepper.calls++
	if stepper.block {
		<-ctx.Done()
		return nil, 0, ctx.Err()
	}
	total := stepper.total
	if total == 0 {
		total = len(stepper.configs)
	}
	return stepper.configs, total, stepper.err
}

// ctxHonoringOccurrences fails the window the way the real reconciler does when
// the window's context is spent.
type ctxHonoringOccurrences struct{}

func (ctxHonoringOccurrences) Reconcile(ctx context.Context, _ time.Time, _ int) (OccurrenceReconcileResult, error) {
	return OccurrenceReconcileResult{}, ctx.Err()
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
	if got := strings.Count(output, "sync.scheduler.config_without_schedule "); got != 2 {
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
	if got := strings.Count(buffer.String(), "sync.scheduler.config_without_schedule "); got != 4 {
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
	if strings.Contains(buffer.String(), "boom") {
		t.Fatalf("read failure log carries the error text:\n%s", buffer.String())
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

// A wrapped Postgres error logs its innermost type and SQLSTATE, never its text.
func TestLoopUnscheduledReadFailureNamesInnermostTypeAndSQLState(t *testing.T) {
	const secret = "ERRTEXT-SENTINEL-8768"
	wrapped := fmt.Errorf("read configs without a schedule: %w",
		&pgconn.PgError{Code: "42P01", Message: secret})
	stepper := &listerStepper{err: wrapped}
	loop, buffer, clock := newUnscheduledTestLoop(t, stepper)
	if err := loop.step(context.Background(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	output := buffer.String()
	for _, want := range []string{"error_type=*pgconn.PgError", "sqlstate=42P01"} {
		if !strings.Contains(output, want) {
			t.Fatalf("log missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "wrapError") || strings.Contains(output, "ERRTEXT-SENTINEL-8768") {
		t.Fatalf("log carries the wrapper type or the error text:\n%s", output)
	}
	// A non-Postgres error: innermost type, empty SQLSTATE.
	_, state := unscheduledReadFailureCause(fmt.Errorf("outer: %w", errors.New("plain")))
	if state != "" {
		t.Fatalf("sqlstate = %q, want empty", state)
	}
}

// CHAOS-8768 r1 P1-a: a hung diagnostic read must not fail or shorten the window.
func TestLoopHungUnscheduledReadDoesNotFailTheWindow(t *testing.T) {
	old := unscheduledReadTimeout
	unscheduledReadTimeout = 150 * time.Millisecond
	defer func() { unscheduledReadTimeout = old }()
	stepper := &listerStepper{block: true}
	loop, buffer, clock := newUnscheduledTestLoop(t, stepper)
	loop.config.Occurrences = ctxHonoringOccurrences{}
	loop.config.StepTimeout = 100 * time.Millisecond // shorter than the read timeout: a shared context would be spent
	if err := loop.step(context.Background(), clock.Now()); err != nil {
		t.Fatalf("a hung diagnostic read failed the window: %v", err)
	}
	if !strings.Contains(buffer.String(), "sync.scheduler.unscheduled_config_read_failed") ||
		!strings.Contains(buffer.String(), "error_type=context.deadlineExceededError") {
		t.Fatalf("hung read not logged by type:\n%s", buffer.String())
	}
}

// CHAOS-8768 r1 P1-b: the gauge is the true total; a capped list adds one truncated WARN.
func TestLoopTruncatedListLogsTotalAndGaugeReadsTrueTotal(t *testing.T) {
	listed := []UnscheduledConfig{{Provider: "jira", Hash: "aaaaaaaaaaaa"}, {Provider: "github", Hash: "bbbbbbbbbbbb"}}
	stepper := &listerStepper{configs: listed, total: 202}
	loop, buffer, clock := newUnscheduledTestLoop(t, stepper)
	if err := loop.step(context.Background(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	output := buffer.String()
	if got := strings.Count(output, "sync.scheduler.config_without_schedule "); got != 2 {
		t.Fatalf("per-config WARN lines = %d, want 2:\n%s", got, output)
	}
	if !strings.Contains(output, "sync.scheduler.config_without_schedule_truncated") ||
		!strings.Contains(output, "total=202") || !strings.Contains(output, "listed=2") {
		t.Fatalf("truncated WARN missing or wrong:\n%s", output)
	}
	loop.mu.Lock()
	gauge := loop.unscheduledConfigs
	loop.mu.Unlock()
	if gauge != 202 {
		t.Fatalf("gauge = %d, want the true total 202", gauge)
	}
}
