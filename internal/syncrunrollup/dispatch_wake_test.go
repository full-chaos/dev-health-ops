package syncrunrollup

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type recordingExecer struct {
	tag      pgconn.CommandTag
	err      error
	deadline time.Time
	hasDead  bool
	calls    int
}

func (execer *recordingExecer) Exec(ctx context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	execer.calls++
	execer.deadline, execer.hasDead = ctx.Deadline()
	return execer.tag, execer.err
}

// TestWakeDispatchIsBoundedAndNeverFailsTheCaller pins the best-effort contract
// the unit commit path relies on: the wake statement runs under its own short
// deadline even when the caller's context has none, a failed statement is
// reported only through the counter and the warn line (WakeDispatch has no
// error to return), and each outcome lands in its own series.
//
// RED CONTROL: drop the context.WithTimeout in WakeDispatch and the deadline
// half fails; map a failed Exec to armed and the counter half fails.
func TestWakeDispatchIsBoundedAndNeverFailsTheCaller(t *testing.T) {
	run := "00000000-0000-4000-8000-00000000a001"
	started := time.Now()

	failing := &recordingExecer{err: errors.New("ctx_deadline_exceeded")}
	failedBefore := DispatchWakeCount(DispatchWakePathProviderUnit, DispatchWakeOutcomeFailed)
	if WakeDispatch(context.Background(), failing, run, started, DispatchWakePathProviderUnit) {
		t.Fatal("a failed wake statement reported armed")
	}
	if !failing.hasDead || failing.deadline.After(started.Add(DispatchWakeTimeout+time.Second)) {
		t.Fatalf("wake statement deadline = %v (set %v), want at most %s after the call", failing.deadline, failing.hasDead, DispatchWakeTimeout)
	}
	if moved := DispatchWakeCount(DispatchWakePathProviderUnit, DispatchWakeOutcomeFailed) - failedBefore; moved != 1 {
		t.Fatalf("failed counter moved %d, want 1", moved)
	}

	idle := &recordingExecer{tag: pgconn.NewCommandTag("INSERT 0 0")}
	notWaitingBefore := DispatchWakeCount(DispatchWakePathLeaseRepair, DispatchWakeOutcomeNotWaiting)
	if WakeDispatch(context.Background(), idle, run, started, DispatchWakePathLeaseRepair) {
		t.Fatal("a wake that touched no row reported armed")
	}
	if moved := DispatchWakeCount(DispatchWakePathLeaseRepair, DispatchWakeOutcomeNotWaiting) - notWaitingBefore; moved != 1 {
		t.Fatalf("not_waiting counter moved %d, want 1", moved)
	}

	armed := &recordingExecer{tag: pgconn.NewCommandTag("INSERT 0 1")}
	armedBefore := DispatchWakeCount(DispatchWakePathRouteUnavailable, DispatchWakeOutcomeArmed)
	if !WakeDispatch(context.Background(), armed, run, started, DispatchWakePathRouteUnavailable) {
		t.Fatal("a wake that armed the row reported not armed")
	}
	if moved := DispatchWakeCount(DispatchWakePathRouteUnavailable, DispatchWakeOutcomeArmed) - armedBefore; moved != 1 {
		t.Fatalf("armed counter moved %d, want 1", moved)
	}
}

// TestDispatchWakeMetricWritesEveryPathAndOutcome pins the scrape shape: one
// HELP/TYPE and every series of the closed vocabulary, zeros included, so a
// scrape tells "no wakes" from "not exposed".
func TestDispatchWakeMetricWritesEveryPathAndOutcome(t *testing.T) {
	var out bytes.Buffer
	if err := DispatchWakeMetricsSource().WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if strings.Count(text, "# TYPE "+dispatchWakeMetricName+" counter") != 1 {
		t.Fatalf("want one TYPE line:\n%s", text)
	}
	for _, path := range dispatchWakePaths {
		for _, outcome := range dispatchWakeOutcomes {
			series := dispatchWakeMetricName + `{path="` + path + `",outcome="` + outcome + `"}`
			if !strings.Contains(text, series) {
				t.Fatalf("missing series %s in:\n%s", series, text)
			}
		}
	}
}
