package reconcilerservice

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// refusalLine is the line health.Registry writes through slog's JSON handler for a refusal.
func refusalLine(check, cause string, timedOut bool) []byte {
	return []byte(fmt.Sprintf(`{"time":"2026-10-28T00:00:00Z","level":"WARN","msg":"readiness check refused","check":%q,"cause":%q,"timed_out":%v,"refused_for_ms":0,"consecutive_refusals":1}`+"\n",
		check, cause, timedOut))
}

func writeLines(t *testing.T, sink *refusalSink, lines ...[]byte) {
	t.Helper()
	for _, line := range lines {
		if _, err := sink.Write(line); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRefusalSinkWaitReturnsALineWrittenBeforeTheWait(t *testing.T) {
	sink := &refusalSink{}
	writeLines(t, sink, refusalLine("queue_postgres", "unreachable", false))

	record, err := waitForRefusalRecord(context.Background(), sink, "queue_postgres")

	if err != nil {
		t.Fatal(err)
	}
	if record.Check != "queue_postgres" || record.Cause != "unreachable" || record.TimedOut {
		t.Errorf("record = %+v, want queue_postgres, cause unreachable, timed_out false", record)
	}
}

// The waiter may park before the line arrives; the write must wake it. The context is cancelled
// after the write, so a sink that never wakes a waiter ends the wait with the context error and
// the test fails instead of hanging.
func TestRefusalSinkWaitReturnsALineWrittenAfterTheWaitStarted(t *testing.T) {
	sink := &refusalSink{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		record refusalRecord
		err    error
	}
	done := make(chan result, 1)
	go func() {
		record, err := waitForRefusalRecord(ctx, sink, "domain_postgres")
		done <- result{record, err}
	}()

	writeLines(t, sink, refusalLine("domain_postgres", "timeout", true))
	cancel()
	got := <-done

	if got.err != nil {
		t.Fatalf("the wait ended with %v, want the record written after it started", got.err)
	}
	if got.record.Cause != "timeout" || !got.record.TimedOut {
		t.Errorf("record = %+v, want cause timeout, timed_out true", got.record)
	}
}

func TestRefusalSinkWaitSkipsOtherChecksAndOtherMessages(t *testing.T) {
	sink := &refusalSink{}
	writeLines(t, sink,
		refusalLine("domain_postgres", "error", false),
		[]byte(`{"level":"INFO","msg":"something else","check":"queue_postgres","cause":"wrong"}`+"\n"),
		refusalLine("queue_postgres", "canceled", true),
	)

	record, err := waitForRefusalRecord(context.Background(), sink, "queue_postgres")

	if err != nil {
		t.Fatal(err)
	}
	if record.Check != "queue_postgres" || record.Cause != "canceled" {
		t.Errorf("record = %+v, want the queue_postgres refusal with cause canceled", record)
	}
}

func TestRefusalSinkWaitReturnsTheFirstRecordOfACheck(t *testing.T) {
	sink := &refusalSink{}
	writeLines(t, sink,
		refusalLine("river_schema", "error", false),
		refusalLine("river_schema", "timeout", true),
	)

	record, err := waitForRefusalRecord(context.Background(), sink, "river_schema")

	if err != nil {
		t.Fatal(err)
	}
	if record.Cause != "error" || record.TimedOut {
		t.Errorf("record = %+v, want the first refusal (cause error, timed_out false)", record)
	}
}

func TestRefusalSinkWaitReturnsTheContextErrorWhenNoLineArrives(t *testing.T) {
	sink := &refusalSink{}
	writeLines(t, sink, refusalLine("domain_postgres", "error", false))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	record, err := waitForRefusalRecord(ctx, sink, "queue_postgres")

	if err == nil {
		t.Fatalf("got record %+v, want an error: no refusal of queue_postgres was written", record)
	}
	if !strings.Contains(err.Error(), "queue_postgres") || !strings.Contains(err.Error(), "never arrived") {
		t.Errorf("error %q names neither the check nor that the record never arrived", err)
	}
}

// A refusal line is written only for a check that failed; for any other check no line will ever
// arrive, so the harness fails at once instead of waiting.
func TestRefusalIsExpectedOnlyForAFailedCheck(t *testing.T) {
	readiness := health.Readiness{Checks: []health.CheckStatus{
		{Name: "domain_postgres", Failed: false},
		{Name: "queue_postgres", Failed: true, Cause: "error"},
	}}
	for check, want := range map[string]bool{
		"queue_postgres":  true,
		"domain_postgres": false,
		"river_schema":    false,
	} {
		if got := refusalExpected(readiness, check); got != want {
			t.Errorf("refusalExpected(%q) = %v, want %v", check, got, want)
		}
	}
}
