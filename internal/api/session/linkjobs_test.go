package session

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
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

func TestQueueLinkJobReturnsBeforeTheWork(t *testing.T) {
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
	if time.Since(start) > time.Second {
		t.Fatal("queueLinkJob waited for the work")
	}
	select {
	case <-ran:
		t.Fatal("the job finished before it was released")
	default:
	}
	close(release)
	q.wait()
	if text := scrape(t, q); !strings.Contains(text, `purpose="password_reset",outcome="sent"} 1`) {
		t.Errorf("the finished job was not counted as sent:\n%s", text)
	}
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
