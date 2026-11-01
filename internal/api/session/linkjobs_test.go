package session

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func withQueue(t *testing.T, slots int) *sendQueue {
	t.Helper()
	saved := linkMailQueue
	linkMailQueue = newSendQueue(slots)
	t.Cleanup(func() { linkMailQueue = saved })
	return linkMailQueue
}

func jobHandlers(logs *bytes.Buffer) handlers {
	return handlers{Deps: Deps{Logger: slog.New(slog.NewTextHandler(logs, nil))}}
}

// The queue and its job run in a bubble on a virtual clock: queueLinkJob hands the work to the queue and returns at the same
// instant, while the job is still waiting for its release.
func TestQueueLinkJobReturnsBeforeTheWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := withQueue(t, 4)
		var logs bytes.Buffer
		h := jobHandlers(&logs)
		release, ran := make(chan struct{}), make(chan struct{})
		request := httptest.NewRequest(http.MethodPost, "/forgot", nil)
		start := time.Now()
		h.queueLinkJob(context.Background(), request, mailPasswordReset, true, func(context.Context, *http.Request) error {
			<-release
			close(ran)
			return nil
		})
		returnedAt := time.Now()
		if !returnedAt.Equal(start) {
			t.Fatalf("queueLinkJob returned %v of virtual time after the start: it waited for the work", returnedAt.Sub(start))
		}
		select {
		case <-ran:
			t.Fatal("queueLinkJob returned only after the work finished")
		default:
		}
		close(release)
		q.wait()
		if text := scrape(t, q); !strings.Contains(text, `purpose="password_reset",outcome="sent"} 1`) {
			t.Errorf("the finished job was not counted as sent:\n%s", text)
		}
	})
}

func TestQueueLinkJobSurvivesACancelledRequest(t *testing.T) {
	q := withQueue(t, 4)
	var logs bytes.Buffer
	h := jobHandlers(&logs)
	ctx, cancel := context.WithCancel(context.Background())
	var sawCancel atomic.Bool
	h.queueLinkJob(ctx, httptest.NewRequest(http.MethodPost, "/x", nil).WithContext(ctx), mailVerification, true, func(jobCtx context.Context, _ *http.Request) error {
		time.Sleep(50 * time.Millisecond)
		sawCancel.Store(jobCtx.Err() != nil)
		return nil
	})
	cancel()
	q.wait()
	if sawCancel.Load() {
		t.Error("the request's cancellation reached the job")
	}
}

func TestForgotPasswordJobFailureIsCountedNotAnswered(t *testing.T) {
	q := withQueue(t, 4)
	var logs bytes.Buffer
	h := jobHandlers(&logs)
	h.queueLinkJob(context.Background(), httptest.NewRequest(http.MethodPost, "/forgot", nil), mailPasswordReset, true,
		func(context.Context, *http.Request) error { return errors.New("write failed for person@example.com") })
	q.wait()
	text := scrape(t, q)
	if !strings.Contains(text, `purpose="password_reset",outcome="failed"} 1`) || !strings.Contains(text, `purpose="password_reset",outcome="sent"} 0`) {
		t.Errorf("a failed job was not counted as failed:\n%s", text)
	}
	if strings.Contains(logs.String(), "person@example.com") || !strings.Contains(logs.String(), "purpose=password_reset") {
		t.Errorf("the failure log names an address or lacks the purpose:\n%s", logs.String())
	}
}

func TestQueueLinkJobDecoyRunsNoWorkAndCountsNothing(t *testing.T) {
	q := withQueue(t, 4)
	var logs bytes.Buffer
	h := jobHandlers(&logs)
	var ran atomic.Bool
	h.queueLinkJob(context.Background(), httptest.NewRequest(http.MethodPost, "/forgot", nil), mailPasswordReset, false,
		func(context.Context, *http.Request) error { ran.Store(true); return nil })
	q.wait()
	if ran.Load() {
		t.Error("a decoy ran the job")
	}
	for _, line := range strings.Split(scrape(t, q), "\n") {
		if strings.HasPrefix(line, mailSendsMetric+"{") && !strings.HasSuffix(line, " 0") {
			t.Errorf("a decoy moved a counter: %s", line)
		}
	}
}

func TestQueueLinkJobFullQueueDropsTheRealJobAndCountsItByPurpose(t *testing.T) {
	q := withQueue(t, 1)
	var logs bytes.Buffer
	h := jobHandlers(&logs)
	release, started := make(chan struct{}), make(chan struct{})
	h.queueLinkJob(context.Background(), httptest.NewRequest(http.MethodPost, "/x", nil), mailVerification, true,
		func(context.Context, *http.Request) error { close(started); <-release; return nil })
	<-started
	var ran atomic.Bool
	h.queueLinkJob(context.Background(), httptest.NewRequest(http.MethodPost, "/forgot", nil), mailPasswordReset, true,
		func(context.Context, *http.Request) error { ran.Store(true); return nil })
	h.queueLinkJob(context.Background(), httptest.NewRequest(http.MethodPost, "/forgot", nil), mailPasswordReset, false, nil)
	close(release)
	q.wait()
	if ran.Load() {
		t.Error("a dropped job ran")
	}
	text := scrape(t, q)
	if !strings.Contains(text, `purpose="password_reset",outcome="dropped"} 1`) {
		t.Errorf("the drop was not counted by purpose:\n%s", text)
	}
	if strings.Contains(text, `purpose="email_verification",outcome="dropped"} 1`) {
		t.Errorf("a decoy drop was counted:\n%s", text)
	}
}

func TestMailQueueDrainCountsWhatIsLeft(t *testing.T) {
	q := newSendQueue(4)
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	for _, purpose := range []string{mailVerification, mailPasswordReset} {
		q.submit(purpose, func() error { started <- struct{}{}; <-release; return nil })
	}
	<-started
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if left := q.drain(ctx); left != 2 {
		t.Fatalf("drain left %d, want 2", left)
	}
	text := scrape(t, q)
	for _, want := range []string{`purpose="email_verification",outcome="dropped"} 1`, `purpose="password_reset",outcome="dropped"} 1`} {
		if !strings.Contains(text, want) {
			t.Errorf("scrape lacks %q:\n%s", want, text)
		}
	}
	close(release)
	q.wait()
	if left := q.drain(context.Background()); left != 0 {
		t.Errorf("an idle queue drained %d", left)
	}
}

func TestMailDrainWaitsForAJobWithinTheShutdownDeadline(t *testing.T) {
	q := withQueue(t, 2)
	var logs bytes.Buffer
	finished := make(chan struct{})
	q.submit(mailVerification, func() error { time.Sleep(100 * time.Millisecond); close(finished); return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := (MailDrain{Logger: slog.New(slog.NewTextHandler(&logs, nil))}).Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Error("shutdown returned before the job finished")
	}
}

type blockingWriter struct{ release chan struct{} }

func (w blockingWriter) Write(p []byte) (int, error) { <-w.release; return len(p), nil }

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *lockedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buf.String() }

// fillQueue takes every slot with a job that holds until the returned release is closed.
func fillQueue(t *testing.T, q *sendQueue) (release func()) {
	t.Helper()
	hold := make(chan struct{})
	started := make(chan struct{}, cap(q.slots))
	for range cap(q.slots) {
		q.submit(mailPasswordReset, func() error { started <- struct{}{}; <-hold; return nil })
	}
	for range cap(q.slots) {
		<-started
	}
	return func() { close(hold) }
}

// On a full queue a real request and a decoy request return within a bound while the logger blocks: neither
// waits on a log write, a lock, or a goroutine.
func TestQueueFullRequestNeverWaitsOnALogWriter(t *testing.T) {
	q := withQueue(t, sendQueueSlots)
	writerRelease := make(chan struct{})
	logger := slog.New(slog.NewTextHandler(blockingWriter{writerRelease}, nil))
	h := handlers{Deps: Deps{Logger: logger}}
	q.startReporter(logger, 10*time.Millisecond)
	release := fillQueue(t, q)
	defer func() {
		close(writerRelease)
		release()
		q.wait()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		q.drain(ctx)
	}()
	request := httptest.NewRequest(http.MethodPost, "/forgot", nil)
	for name, real := range map[string]bool{"real": true, "decoy": false} {
		done := make(chan struct{})
		go func() {
			h.queueLinkJob(context.Background(), request, mailPasswordReset, real, func(context.Context, *http.Request) error { return nil })
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("the %s request waited on the blocked log writer", name)
		}
	}
	if text := scrape(t, q); !strings.Contains(text, `purpose="password_reset",outcome="dropped"} 1`) {
		t.Errorf("the real drop was not counted:\n%s", text)
	}
}

func TestDropReporterLogsOnceWithTheCountAfterABurst(t *testing.T) {
	q := withQueue(t, 1)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	h := handlers{Deps: Deps{Logger: logger}}
	release := fillQueue(t, q)
	defer func() { release(); q.wait() }()
	q.startReporter(logger, 40*time.Millisecond)
	request := httptest.NewRequest(http.MethodPost, "/forgot", nil)
	burst := func(n int) {
		for range n {
			h.queueLinkJob(context.Background(), request, mailPasswordReset, true, func(context.Context, *http.Request) error { return nil })
		}
	}
	lines := func() []string {
		var out []string
		for _, line := range strings.Split(logs.String(), "\n") {
			if strings.Contains(line, "queue full") {
				out = append(out, line)
			}
		}
		return out
	}
	burst(5)
	time.Sleep(300 * time.Millisecond)
	if got := lines(); len(got) != 1 || !strings.Contains(got[0], "count=5") || !strings.Contains(got[0], "purpose=password_reset") || strings.Contains(got[0], "@") {
		t.Fatalf("after a burst of 5 want one line with count=5 and the purpose only, got %q", got)
	}
	time.Sleep(200 * time.Millisecond)
	if got := lines(); len(got) != 1 {
		t.Fatalf("a quiet interval logged again: %q", got)
	}
	burst(2)
	time.Sleep(300 * time.Millisecond)
	if got := lines(); len(got) != 2 || !strings.Contains(got[1], "count=2") {
		t.Fatalf("a second burst of 2 want a second line with count=2, got %q", got)
	}
}

func TestDrainStopsTheReporterAfterALastReport(t *testing.T) {
	q := withQueue(t, 1)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	h := handlers{Deps: Deps{Logger: logger}}
	release := fillQueue(t, q)
	q.startReporter(logger, time.Hour)
	drop := func() {
		h.queueLinkJob(context.Background(), httptest.NewRequest(http.MethodPost, "/x", nil), mailPasswordReset, true, func(context.Context, *http.Request) error { return nil })
	}
	drop()
	release()
	q.wait()
	// A live context: the drain returns only after its wait goroutine and the reporter's final report are done,
	// so the next fillQueue never adds to the WaitGroup while that goroutine still starts its Wait.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q.drain(ctx)
	count := func() int { return strings.Count(logs.String(), "queue full") }
	if !strings.Contains(logs.String(), "count=1") {
		t.Fatalf("the last report was not written at the stop:\n%s", logs.String())
	}
	// The reporter has stopped: a later drop is counted but never reported.
	release2 := fillQueue(t, q)
	drop()
	time.Sleep(150 * time.Millisecond)
	release2()
	q.wait()
	if got := count(); got != 1 {
		t.Errorf("the reporter kept running after the drain: %d lines", got)
	}
}

func withReportInterval(t *testing.T, every time.Duration) {
	t.Helper()
	saved := mailDropReportInterval
	mailDropReportInterval = every
	t.Cleanup(func() { mailDropReportInterval = saved })
}

func queueFullLines(logs *lockedBuffer) []string {
	var out []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "queue full") {
			out = append(out, line)
		}
	}
	return out
}

// The api starts the reporter through the component lifecycle: Start, a burst of drops, Shutdown.
func TestMailDrainLifecycleReportsOnceWithTheCount(t *testing.T) {
	q := withQueue(t, 1)
	withReportInterval(t, 40*time.Millisecond)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	h := handlers{Deps: Deps{Logger: logger}}
	component := MailDrain{Logger: logger}
	if err := component.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	release := fillQueue(t, q)
	request := httptest.NewRequest(http.MethodPost, "/forgot", nil)
	for range 4 {
		h.queueLinkJob(context.Background(), request, mailPasswordReset, true, func(context.Context, *http.Request) error { return nil })
	}
	time.Sleep(300 * time.Millisecond)
	if got := queueFullLines(logs); len(got) != 1 || !strings.Contains(got[0], "count=4") || !strings.Contains(got[0], "purpose=password_reset") {
		t.Fatalf("after Start and a burst of 4 want one line with count=4, got %q", got)
	}
	release()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := component.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := queueFullLines(logs); len(got) != 1 {
		t.Errorf("Shutdown wrote a report with nothing new: %q", got)
	}
}

func TestMailDrainStartedTwiceRunsOneReporter(t *testing.T) {
	q := withQueue(t, 1)
	withReportInterval(t, 40*time.Millisecond)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	h := handlers{Deps: Deps{Logger: logger}}
	component := MailDrain{Logger: logger}
	for range 2 {
		if err := component.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	release := fillQueue(t, q)
	request := httptest.NewRequest(http.MethodPost, "/forgot", nil)
	for range 3 {
		h.queueLinkJob(context.Background(), request, mailPasswordReset, true, func(context.Context, *http.Request) error { return nil })
	}
	time.Sleep(300 * time.Millisecond)
	release()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = component.Shutdown(ctx)
	if got := queueFullLines(logs); len(got) != 1 || !strings.Contains(got[0], "count=3") {
		t.Fatalf("a second Start must not add a reporter: want one line with count=3, got %q", got)
	}
}

// The drain waits for the reporter's final report, bounded by its own context.
func TestDrainWaitsForTheReportersFinalReport(t *testing.T) {
	q := withQueue(t, 1)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	h := handlers{Deps: Deps{Logger: logger}}
	release := fillQueue(t, q)
	q.startReporter(logger, time.Hour)
	h.queueLinkJob(context.Background(), httptest.NewRequest(http.MethodPost, "/x", nil), mailPasswordReset, true, func(context.Context, *http.Request) error { return nil })
	release()
	q.wait()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	q.drain(ctx)
	if got := queueFullLines(logs); len(got) != 1 || !strings.Contains(got[0], "count=1") {
		t.Fatalf("drain returned before the final report: %q", got)
	}
}
