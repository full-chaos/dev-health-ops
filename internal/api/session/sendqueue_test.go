package session

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func scrape(t *testing.T, q *sendQueue) string {
	t.Helper()
	var out bytes.Buffer
	if err := q.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestSendQueueNeverBlocksAndCountsDrops(t *testing.T) {
	q := newSendQueue(2)
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	for range 2 {
		if !q.submit(mailVerification, func() error { started <- struct{}{}; <-release; return nil }) {
			t.Fatal("a free slot refused a send")
		}
	}
	<-started
	<-started
	done := make(chan bool)
	go func() {
		done <- q.submit(mailAccountExists, func() error { t.Error("a dropped send ran"); return nil })
	}()
	select {
	case accepted := <-done:
		if accepted {
			t.Fatal("a full queue accepted a send")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("submit blocked on a full queue")
	}
	close(release)
	q.wait()
	text := scrape(t, q)
	for _, want := range []string{
		`devhealth_api_session_mail_sends_total{purpose="account_exists",outcome="dropped"} 1`,
		`devhealth_api_session_mail_sends_total{purpose="email_verification",outcome="sent"} 2`,
		`devhealth_api_session_mail_sends_total{purpose="email_verification",outcome="dropped"} 0`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("scrape lacks %q:\n%s", want, text)
		}
	}
}

func TestSendQueueFailureIsCountedAsFailed(t *testing.T) {
	q := newSendQueue(1)
	q.submit(mailVerification, func() error { return errors.New("provider down") })
	q.wait()
	if text := scrape(t, q); !strings.Contains(text, `purpose="email_verification",outcome="failed"} 1`) ||
		!strings.Contains(text, `purpose="email_verification",outcome="sent"} 0`) {
		t.Errorf("a failed send was not counted as failed:\n%s", text)
	}
}

func TestMailMetricsLabelSetIsClosedAndPreRegistered(t *testing.T) {
	q := newSendQueue(1)
	q.count("somebody@example.com", outcomeSent)
	q.count(mailVerification, "other")
	var series []string
	for _, line := range strings.Split(scrape(t, q), "\n") {
		if strings.HasPrefix(line, mailSendsMetric+"{") {
			if !strings.HasSuffix(line, " 0") {
				t.Errorf("a series is not at 0 before any send: %s", line)
			}
			series = append(series, line[:strings.LastIndex(line, " ")])
		}
	}
	want := []string{
		`devhealth_api_session_mail_sends_total{purpose="email_verification",outcome="sent"}`,
		`devhealth_api_session_mail_sends_total{purpose="email_verification",outcome="failed"}`,
		`devhealth_api_session_mail_sends_total{purpose="email_verification",outcome="dropped"}`,
		`devhealth_api_session_mail_sends_total{purpose="password_reset",outcome="sent"}`,
		`devhealth_api_session_mail_sends_total{purpose="password_reset",outcome="failed"}`,
		`devhealth_api_session_mail_sends_total{purpose="password_reset",outcome="dropped"}`,
		`devhealth_api_session_mail_sends_total{purpose="account_exists",outcome="sent"}`,
		`devhealth_api_session_mail_sends_total{purpose="account_exists",outcome="failed"}`,
		`devhealth_api_session_mail_sends_total{purpose="account_exists",outcome="dropped"}`,
	}
	if strings.Join(series, "\n") != strings.Join(want, "\n") {
		t.Errorf("series\n%s\nwant\n%s", strings.Join(series, "\n"), strings.Join(want, "\n"))
	}
}

func TestDrainedJobIsNotCountedAgainWhenItReturns(t *testing.T) {
	q := newSendQueue(2)
	release, started := make(chan struct{}), make(chan struct{})
	q.submit(mailPasswordReset, func() error { close(started); <-release; return nil })
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if left := q.drain(ctx); left != 1 {
		t.Fatalf("drain left %d, want 1", left)
	}
	close(release)
	q.wait()
	text := scrape(t, q)
	if !strings.Contains(text, `purpose="password_reset",outcome="dropped"} 1`) || !strings.Contains(text, `purpose="password_reset",outcome="sent"} 0`) {
		t.Errorf("a drained job was counted twice:\n%s", text)
	}
}

func TestDrainCancelsTheContextOfAJobBoundToTheQueue(t *testing.T) {
	q := newSendQueue(2)
	started, cancelled := make(chan struct{}), make(chan struct{})
	jobCtx, stop := q.bind(context.Background(), time.Minute)
	defer stop()
	q.submit(mailVerification, func() error {
		close(started)
		<-jobCtx.Done()
		close(cancelled)
		return jobCtx.Err()
	})
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	q.drain(ctx)
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the job context was not cancelled at the drain deadline")
	}
	q.wait()
}
