package dbphase

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNoTraceOnTheContextIsANoOp(t *testing.T) {
	handle := Start(context.Background(), KindAcquire, "pool", "")
	handle.End(nil)
	var trace *Trace
	if trace.Phases() != nil || trace.AcquireWaits() != nil || trace.Summary() != "" {
		t.Fatal("a nil trace reported something")
	}
	if _, ok := trace.Culprit(); ok {
		t.Fatal("a nil trace named a culprit")
	}
}

func TestCulpritPrefersTheOpenPhaseThenTheLastErroredThenTheLongest(t *testing.T) {
	ctx, trace := With(context.Background())
	quick := Start(ctx, KindAcquire, "coordinator", "")
	quick.End(nil)
	slow := Start(ctx, KindStatement, "coordinator", "SELECT slow")
	time.Sleep(20 * time.Millisecond)
	slow.End(nil)
	if got, _ := trace.Culprit(); got.Name != "SELECT slow" {
		t.Fatalf("with no error and none open the longest phase is the culprit; got %+v", got)
	}
	failed := Start(ctx, KindStatement, "coordinator", "SELECT failing")
	failed.End(errors.New("canceled"))
	if got, _ := trace.Culprit(); got.Name != "SELECT failing" || got.Err == nil {
		t.Fatalf("the last errored phase outranks the longest; got %+v", got)
	}
	open := Start(ctx, KindAcquire, "queue_control", "")
	got, _ := trace.Culprit()
	if got.Name != "queue_control" || got.Done {
		t.Fatalf("an open phase outranks everything; got %+v", got)
	}
	open.End(nil)
}

func TestCulpritIgnoresTheDeferredRollbackOnAnExpiredBudget(t *testing.T) {
	ctx, trace := With(context.Background())
	acquire := Start(ctx, KindAcquire, "coordinator", "")
	acquire.End(nil)
	slow := Start(ctx, KindStatement, "coordinator", "-- materialize rows\nSELECT 1")
	time.Sleep(20 * time.Millisecond)
	slow.End(nil)
	rollback := Start(ctx, KindStatement, "coordinator", "rollback")
	rollback.End(context.DeadlineExceeded)
	if got, _ := trace.Culprit(); got.Name != "materialize rows" {
		t.Fatalf("the deferred rollback on the expired budget is not the culprit; the slowest statement is; got %+v", got)
	}

	ctx, trace = With(context.Background())
	failed := Start(ctx, KindStatement, "coordinator", "SELECT failing")
	failed.End(context.DeadlineExceeded)
	rollback = Start(ctx, KindStatement, "coordinator", "ROLLBACK")
	rollback.End(context.DeadlineExceeded)
	if got, _ := trace.Culprit(); got.Name != "SELECT failing" || got.Err == nil {
		t.Fatalf("the errored statement before the rollback is the culprit; got %+v", got)
	}

	ctx, trace = With(context.Background())
	only := Start(ctx, KindStatement, "coordinator", "rollback")
	only.End(context.DeadlineExceeded)
	if got, ok := trace.Culprit(); !ok || got.Name != "rollback" {
		t.Fatalf("a rollback that is the only phase is still named; got %+v ok=%v", got, ok)
	}
}

func TestAcquireWaitsIncludeAnAcquireStillInFlight(t *testing.T) {
	ctx, trace := With(context.Background())
	Start(ctx, KindAcquire, "coordinator", "")
	time.Sleep(15 * time.Millisecond)
	waits := trace.AcquireWaits()
	if len(waits) != 1 || waits[0] < 10*time.Millisecond {
		t.Fatalf("a pool that never answers must show as a long wait, got %v", waits)
	}
}

func TestStatementNamesNeverCarryMoreThanTheSummaryOfTheText(t *testing.T) {
	for statement, want := range map[string]string{
		"-- named probe\nSELECT 1":    "named probe",
		"  SELECT   a,\n\tb FROM t  ": "SELECT a, b FROM t",
		"begin":                       "begin",
		"":                            "unnamed",
		"--":                          "unnamed",
		"SELECT 'a;b=c'":              "SELECT 'a,b:c'",
	} {
		if got := SummarizeStatement(statement); got != want {
			t.Errorf("SummarizeStatement(%q) = %q, want %q", statement, got, want)
		}
	}
	if got := SummarizeStatement(strings.Repeat("x", 500)); len(got) != maxNameRunes {
		t.Fatalf("name length = %d, want it bounded to %d", len(got), maxNameRunes)
	}
}

func TestTraceIsBoundedButKeepsCountingAcquireWaits(t *testing.T) {
	ctx, trace := With(context.Background())
	for i := 0; i < maxPhases+10; i++ {
		Start(ctx, KindAcquire, "coordinator", "").End(nil)
	}
	if len(trace.Phases()) != maxPhases {
		t.Fatalf("phases = %d, want the bound %d", len(trace.Phases()), maxPhases)
	}
	if !strings.HasSuffix(trace.Summary(), ";+10more") {
		t.Fatalf("summary does not report the dropped phases: %q", trace.Summary())
	}
}
