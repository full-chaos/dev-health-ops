// Package linkmail is auth-service's local.Mailer: it renders the same
// templates and builds the same links as go-api (internal/mail,
// internal/api/authmail) and hands the provider call to a bounded pool.
// Response time does not include the provider send, for known and unknown
// addresses alike.
package linkmail

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

const (
	// MetricName counts real sends by purpose and outcome. Decoys are not counted.
	MetricName = "devhealth_auth_linkmail_sends_total"

	OutcomeSent    = "sent"
	OutcomeFailed  = "failed"
	OutcomeDropped = "dropped"

	defaultWorkers = 4
	defaultQueue   = 64
)

// ErrClosed is returned by Send after Close.
var ErrClosed = errors.New("linkmail: closed")

// Config configures a Sender.
type Config struct {
	// Mail is the provider, from mail.NewSenderFromEnv. Nil: every real send fails.
	Mail mail.Sender
	// Links carries APP_BASE_URL for the link; the other fields are unused.
	Links authmail.Config
	// Workers and Queue bound the pool; zero takes the defaults.
	Workers int
	Queue   int
	Logger  *slog.Logger
	Meter   metric.Meter
}

type job struct {
	purpose local.Purpose
	message mail.Message
}

// Sender implements local.Mailer.
type Sender struct {
	cfg    Config
	jobs   chan job
	sends  metric.Int64Counter
	wg     sync.WaitGroup
	mu     sync.RWMutex
	closed bool
}

var _ local.Mailer = (*Sender)(nil)

// New starts the pool.
func New(cfg Config) (*Sender, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
	}
	if cfg.Queue <= 0 {
		cfg.Queue = defaultQueue
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/linkmail")
	}
	counter, err := meter.Int64Counter(MetricName,
		metric.WithDescription("Link e-mail sends by purpose and outcome (sent, failed, dropped)."))
	if err != nil {
		return nil, err
	}
	s := &Sender{cfg: cfg, jobs: make(chan job, cfg.Queue), sends: counter}
	for i := 0; i < cfg.Workers; i++ {
		s.wg.Add(1)
		go s.work()
	}
	return s, nil
}

type template struct{ subject, name, urlField, path string }

var templates = map[local.Purpose]template{
	local.PurposeEmailVerification: {"Verify your email address", "verification", "verification_url", "/auth/verify"},
	local.PurposePasswordReset:     {"Reset your password", "password_reset", "reset_url", "/auth/reset-password"},
}

// Send renders the message and queues the provider call. A decoy
// (Deliver false) does the same work and queues nothing. It never waits for
// the provider and never blocks: a full pool drops the message.
func (s *Sender) Send(ctx context.Context, msg local.Message) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	tpl, ok := templates[msg.Purpose]
	if !ok {
		return errors.New("linkmail: unsupported purpose")
	}
	name := msg.To
	if msg.FullName != nil && *msg.FullName != "" {
		name = *msg.FullName
	}
	html, err := mail.RenderTemplate(tpl.name, map[string]string{
		"full_name": name, tpl.urlField: s.cfg.Links.Link(tpl.path, msg.Secret.Reveal()),
	})
	if err != nil {
		return err
	}
	if !msg.Deliver {
		return nil
	}
	select {
	case s.jobs <- job{purpose: msg.Purpose, message: mail.Message{To: msg.To, Subject: tpl.subject, HTML: html}}:
	default:
		s.count(ctx, msg.Purpose, OutcomeDropped)
		s.cfg.Logger.WarnContext(ctx, "linkmail: pool full, message dropped", "purpose", string(msg.Purpose))
	}
	return nil
}

func (s *Sender) work() {
	defer s.wg.Done()
	for j := range s.jobs {
		s.deliver(j)
	}
}

func (s *Sender) deliver(j job) {
	ctx, cancel := context.WithTimeout(context.Background(), authmail.SendTimeout)
	defer cancel()
	var err error
	if s.cfg.Mail == nil {
		err = errors.New("no mail sender is configured")
	} else {
		err = s.cfg.Mail.Send(ctx, j.message)
	}
	if err != nil {
		s.count(ctx, j.purpose, OutcomeFailed)
		s.cfg.Logger.WarnContext(ctx, "linkmail: link mail not sent", "purpose", string(j.purpose), logging.ErrorAttr(err))
		return
	}
	s.count(ctx, j.purpose, OutcomeSent)
}

func (s *Sender) count(ctx context.Context, p local.Purpose, outcome string) {
	s.sends.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(
		attribute.String("purpose", string(p)), attribute.String("outcome", outcome)))
}

// Close refuses new sends and waits for the queued and in-flight ones until
// ctx ends. Sends still running then are left to finish or be lost.
func (s *Sender) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.jobs)
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
