package linkmail

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/mail"
)

type fakeProvider struct {
	mu   sync.Mutex
	got  []mail.Message
	gate chan struct{}
	err  error
}

func (f *fakeProvider) Name() string { return "fake" }
func (f *fakeProvider) Send(ctx context.Context, m mail.Message) error {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, m)
	return f.err
}
func (f *fakeProvider) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.got) }

func message(p local.Purpose, deliver bool) local.Message {
	name := "Pat Example"
	return local.Message{Purpose: p, To: "who@example.test", FullName: &name,
		Secret: contracts.NewSecretString("secret-value-1"), Deliver: deliver}
}

func newSender(t *testing.T, p mail.Sender, workers, queue int, logs *bytes.Buffer, rd *metric.ManualReader) *Sender {
	t.Helper()
	cfg := Config{Mail: p, Workers: workers, Queue: queue,
		Links: authmail.Config{AppBaseURL: "https://app.example.test/", AppBaseURLSet: true}}
	if logs != nil {
		cfg.Logger = slog.New(slog.NewTextHandler(logs, nil))
	}
	if rd != nil {
		cfg.Meter = metric.NewMeterProvider(metric.WithReader(rd)).Meter("t")
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func closeIn(t *testing.T, s *Sender) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDeliverSendsRenderedMessageAndDecoySendsNothing(t *testing.T) {
	for _, tc := range []struct {
		purpose         local.Purpose
		subject, suffix string
	}{
		{local.PurposeEmailVerification, "Verify your email address", "https://app.example.test/auth/verify?token=secret-value-1"},
		{local.PurposePasswordReset, "Reset your password", "https://app.example.test/auth/reset-password?token=secret-value-1"},
	} {
		p := &fakeProvider{}
		s := newSender(t, p, 1, 4, nil, nil)
		if err := s.Send(context.Background(), message(tc.purpose, false)); err != nil {
			t.Fatal(err)
		}
		if err := s.Send(context.Background(), message(tc.purpose, true)); err != nil {
			t.Fatal(err)
		}
		closeIn(t, s)
		if p.count() != 1 {
			t.Fatalf("%s: %d provider calls, want 1", tc.purpose, p.count())
		}
		m := p.got[0]
		if m.Subject != tc.subject || m.To != "who@example.test" ||
			!strings.Contains(m.HTML, tc.suffix) || !strings.Contains(m.HTML, "Pat Example") {
			t.Fatalf("%s: message does not match the go-api shape", tc.purpose)
		}
	}
}

func TestNameFallsBackToTheAddress(t *testing.T) {
	p := &fakeProvider{}
	s := newSender(t, p, 1, 4, nil, nil)
	m := message(local.PurposePasswordReset, true)
	m.FullName = nil
	if err := s.Send(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	closeIn(t, s)
	if !strings.Contains(p.got[0].HTML, "who@example.test") {
		t.Fatal("the address is not the greeting name")
	}
}

func TestSendDoesNotWaitForTheProviderForAnyAddress(t *testing.T) {
	p := &fakeProvider{gate: make(chan struct{})}
	s := newSender(t, p, 1, 4, nil, nil)
	for _, deliver := range []bool{true, false} {
		done := make(chan error, 1)
		go func() { done <- s.Send(context.Background(), message(local.PurposeEmailVerification, deliver)) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Send(deliver=%v) waited on the provider", deliver)
		}
	}
	close(p.gate)
	closeIn(t, s)
}

func TestFullPoolReturnsAtOnceAndCountsDropped(t *testing.T) {
	p := &fakeProvider{gate: make(chan struct{})}
	var logs bytes.Buffer
	rd := metric.NewManualReader()
	s := newSender(t, p, 1, 1, &logs, rd)
	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := s.Send(context.Background(), message(local.PurposePasswordReset, true)); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(start) > time.Second {
		t.Fatal("a full pool made Send wait")
	}
	close(p.gate)
	closeIn(t, s)
	counts := outcomes(t, rd)
	if counts[OutcomeDropped] < 7 || counts[OutcomeSent] < 1 || counts[OutcomeSent]+counts[OutcomeDropped] != 10 {
		t.Fatalf("outcome counts %v do not add up to 10 sends with drops", counts)
	}
}

func TestFailureLogsNoTextAndCountsFailed(t *testing.T) {
	p := &fakeProvider{err: errors.New("relay refused who@example.test token secret-value-1")}
	var logs bytes.Buffer
	rd := metric.NewManualReader()
	s := newSender(t, p, 1, 4, &logs, rd)
	if err := s.Send(context.Background(), message(local.PurposeEmailVerification, true)); err != nil {
		t.Fatal(err)
	}
	closeIn(t, s)
	out := logs.String()
	if !strings.Contains(out, "link mail not sent") || !strings.Contains(out, "error.class=") || !strings.Contains(out, "error.type=*errors.errorString") {
		t.Fatal("failure was not logged with its class and type")
	}
	if c := outcomes(t, rd); c[OutcomeFailed] != 1 || c[OutcomeSent] != 0 {
		t.Fatalf("outcome counts %v, want one failed", c)
	}
	for _, leak := range []string{"who@example.test", "secret-value-1", "app.example.test"} {
		if strings.Contains(out, leak) {
			t.Fatal("log line carries address, secret or link")
		}
	}
}

func TestNilProviderFailsTheSendNotTheCall(t *testing.T) {
	var logs bytes.Buffer
	s := newSender(t, nil, 1, 4, &logs, nil)
	if err := s.Send(context.Background(), message(local.PurposeEmailVerification, true)); err != nil {
		t.Fatal(err)
	}
	closeIn(t, s)
	if !strings.Contains(logs.String(), "link mail not sent") {
		t.Fatal("missing provider was not logged")
	}
}

func TestUnsupportedPurposeAndClosedAreRefused(t *testing.T) {
	s := newSender(t, &fakeProvider{}, 1, 4, nil, nil)
	if err := s.Send(context.Background(), message(local.Purpose("other"), true)); err == nil {
		t.Fatal("unsupported purpose accepted")
	}
	closeIn(t, s)
	for _, deliver := range []bool{true, false} {
		if err := s.Send(context.Background(), message(local.PurposeEmailVerification, deliver)); !errors.Is(err, ErrClosed) {
			t.Fatalf("send after Close (deliver=%v): %v", deliver, err)
		}
	}
	closeIn(t, s)
}

func TestCloseHonoursItsDeadline(t *testing.T) {
	p := &fakeProvider{gate: make(chan struct{})}
	s := newSender(t, p, 1, 4, nil, nil)
	if err := s.Send(context.Background(), message(local.PurposeEmailVerification, true)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want the deadline", err)
	}
	close(p.gate)
}

func TestSendOutlivesACancelledRequest(t *testing.T) {
	p := &fakeProvider{}
	s := newSender(t, p, 1, 4, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	if err := s.Send(ctx, message(local.PurposeEmailVerification, true)); err != nil {
		t.Fatal(err)
	}
	cancel()
	closeIn(t, s)
	if p.count() != 1 {
		t.Fatal("a cancelled request lost the send")
	}
}

func outcomes(t *testing.T, rd *metric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := rd.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != MetricName {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				v, _ := dp.Attributes.Value("outcome")
				counts[v.AsString()] += dp.Value
			}
		}
	}
	return counts
}

type blockingHandler struct{ gate chan struct{} }

func (blockingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h blockingHandler) Handle(context.Context, slog.Record) error {
	<-h.gate
	return nil
}
func (h blockingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h blockingHandler) WithGroup(string) slog.Handler      { return h }

func TestFullPoolDoesNotWaitOnALogHandler(t *testing.T) {
	p := &fakeProvider{gate: make(chan struct{})}
	h := blockingHandler{gate: make(chan struct{})}
	s, err := New(Config{Mail: p, Workers: 1, Queue: 1, Logger: slog.New(h),
		Links: authmail.Config{AppBaseURL: "https://app.example.test", AppBaseURLSet: true}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			_ = s.Send(context.Background(), message(local.PurposePasswordReset, true))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send waited on the log handler")
	}
	close(p.gate)
	close(h.gate)
	closeIn(t, s)
}
