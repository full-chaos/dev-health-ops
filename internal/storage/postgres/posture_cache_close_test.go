package postgres

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// closeSink counts the log lines written before and after the owner's Close returned. Each Write
// first sleeps on the bubble's virtual clock: the clock moves only once every goroutine is blocked,
// so a line written after Close returned is counted after markClosed, whatever the scheduler does.
type closeSink struct {
	mu     sync.Mutex
	closed bool
	before int
	after  int
	text   strings.Builder
}

func (s *closeSink) Write(p []byte) (int, error) {
	time.Sleep(time.Second)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.after++
	} else {
		s.before++
	}
	s.text.Write(p)
	return len(p), nil
}

func (s *closeSink) markClosed() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

func (s *closeSink) counts() (before, after int, text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.before, s.after, s.text.String()
}

// CHAOS-8654: the background execution ran on context.Background() and logged after its owner had
// shut down (a dho service command returned while a warmed posture check still ran). Close must
// cancel the running execution and return only after it has ended and logged.
func TestCachedPostureCheckCloseEndsTheRunAndItsLogBeforeReturning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &closeSink{}
		started := make(chan struct{})
		var ended atomic.Bool
		check := newCachedPostureCheck("api_role", func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			ended.Store(true)
			return ctx.Err()
		}, PostureCheckOptions{Logger: slog.New(slog.NewJSONHandler(sink, nil)), RunTimeout: time.Hour})
		check.Warm()
		<-started

		check.Close()
		sink.markClosed()
		if !ended.Load() {
			t.Fatal("Close returned while the background execution still ran")
		}
		time.Sleep(time.Minute) // lets a late Write finish its sleep
		synctest.Wait()
		before, after, text := sink.counts()
		if after != 0 {
			t.Fatalf("%d log line(s) written after Close returned: %s", after, text)
		}
		if before != 1 || !strings.Contains(text, `"outcome":"unanswered"`) {
			t.Fatalf("want the one cancelled execution logged before Close returned, got %d line(s): %s", before, text)
		}
	})
}

func TestCachedPostureCheckStartsNoExecutionAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runs atomic.Int32
		check := newCachedPostureCheck("api_role", func(context.Context) error {
			runs.Add(1)
			return nil
		}, PostureCheckOptions{})
		check.Close()
		check.Close()

		check.Warm()
		if err := check.CheckNoWait(); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("CheckNoWait() after Close = %v, want ErrUnavailable", err)
		}
		if err := check.Check(context.Background()); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Check() after Close = %v, want ErrUnavailable", err)
		}
		synctest.Wait()
		if got := runs.Load(); got != 0 {
			t.Fatalf("a closed check ran %d execution(s), want 0", got)
		}
	})
}

func TestLazyProbeCheckCloseJoinsTheWarmedRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		var ended atomic.Bool
		var check LazyProbeCheck
		check.Warm("queue_postgres", func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			ended.Store(true)
			return ctx.Err()
		}, PostureCheckOptions{RunTimeout: time.Hour})
		<-started

		check.Close()
		if !ended.Load() {
			t.Fatal("Close returned while the warmed execution still ran")
		}
	})
}

func TestLazyProbeCheckClosedBeforeWarmNeverRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runs atomic.Int32
		run := func(context.Context) error {
			runs.Add(1)
			return nil
		}
		var check LazyProbeCheck
		check.Close()
		check.Warm("queue_postgres", run, PostureCheckOptions{})
		if err := check.Check(context.Background(), "queue_postgres", run); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Check() after Close = %v, want ErrUnavailable", err)
		}
		synctest.Wait()
		if got := runs.Load(); got != 0 {
			t.Fatalf("a closed check ran %d execution(s), want 0", got)
		}
	})
}
