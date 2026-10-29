package revisionfeed

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
)

// SubscriberConfig is the subscriber's inputs. Backoff is the delay before reconnect attempt n (n starts at 1); the default
// doubles from 1 s to 30 s.
type SubscriberConfig struct {
	Client   Client
	Consumer *Consumer
	Logger   *slog.Logger
	Meter    metric.Meter
	Backoff  func(attempt int) time.Duration
}

// Subscriber keeps one subscription to Channel and feeds every message to the Consumer. Every subscribe (the first and every
// reconnect) calls Consumer.Resubscribed: pub/sub has no replay, so events published while it was down are lost.
type Subscriber struct {
	client   Client
	consumer *Consumer
	log      *slog.Logger
	backoff  func(int) time.Duration
	up       atomic.Int64
	attempt  atomic.Int64
}

// NewSubscriber validates its inputs and registers devhealth_auth_revision_feed_subscription_up (1 while subscribed).
func NewSubscriber(cfg SubscriberConfig) (*Subscriber, error) {
	if cfg.Client == nil || cfg.Consumer == nil {
		return nil, errors.New("revisionfeed: a client and a consumer are required")
	}
	s := &Subscriber{client: cfg.Client, consumer: cfg.Consumer, log: cfg.Logger, backoff: cfg.Backoff}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.backoff == nil {
		s.backoff = defaultBackoff
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/revisionfeed")
	}
	if _, err := meter.Int64ObservableGauge("devhealth_auth_revision_feed_subscription_up",
		metric.WithDescription("1 while this replica holds a confirmed subscription to the revision feed channel, 0 otherwise."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(s.up.Load())
			return nil
		})); err != nil {
		return nil, err
	}
	return s, nil
}

func defaultBackoff(attempt int) time.Duration {
	d := time.Second
	for i := 1; i < attempt && d < 30*time.Second; i++ {
		d *= 2
	}
	return min(d, 30*time.Second)
}

// Run subscribes and reconnects until ctx ends. It logs that the subscription was lost, never why: a connection error
// can quote the URI.
func (s *Subscriber) Run(ctx context.Context) {
	for ctx.Err() == nil {
		s.up.Store(0)
		_ = s.client.Subscribe(ctx, Channel, SubscribeHooks{
			OnSubscribed: func() {
				s.attempt.Store(0)
				s.up.Store(1)
				s.consumer.Resubscribed(ctx)
			},
			OnMessage: func(message string) { s.onMessage(ctx, message) },
		})
		s.up.Store(0)
		if ctx.Err() != nil {
			return
		}
		s.log.WarnContext(ctx, "revision feed subscription lost; reconnecting")
		wait := s.backoff(int(s.attempt.Add(1)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (s *Subscriber) onMessage(ctx context.Context, message string) {
	typ, id, revision, ok := parseMessage(message)
	if !ok {
		s.log.DebugContext(ctx, "revision feed message dropped: not <type>:<id>:<revision>")
		return
	}
	s.consumer.Handle(ctx, contracts.ProvisioningEvent{
		AggregateType: typ, AggregateID: id,
		Payload: contracts.ProvisioningPayload{Revision: contracts.Revision(revision)},
	})
}

// parseMessage splits "<type>:<id>:<revision>": the type ends at the first colon (aggregate types have none), the
// revision starts after the last (an id may hold colons).
func parseMessage(message string) (typ, id string, revision int64, ok bool) {
	first := strings.IndexByte(message, ':')
	last := strings.LastIndexByte(message, ':')
	if first <= 0 || last <= first+1 || last == len(message)-1 {
		return "", "", 0, false
	}
	revision, err := strconv.ParseInt(message[last+1:], 10, 64)
	if err != nil || revision < 1 {
		return "", "", 0, false
	}
	return message[:first], message[first+1 : last], revision, true
}

// Component runs a Subscriber as a lifecycle component named "auth-revision-feed". Start returns at once.
type Component struct {
	sub   *Subscriber
	close func()

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

var _ lifecycle.Component = (*Component)(nil)

// NewComponent wraps sub. closeClient, when set, runs at shutdown after the subscriber stopped.
func NewComponent(sub *Subscriber, closeClient func()) *Component {
	return &Component{sub: sub, close: closeClient}
}

func (*Component) Name() string { return "auth-revision-feed" }

func (c *Component) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		return errors.New("revisionfeed: the subscriber is already started")
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.cancel = cancel
	c.done = make(chan struct{})
	go func() {
		defer close(c.done)
		c.sub.Run(runCtx)
	}()
	return nil
}

func (c *Component) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	cancel, done := c.cancel, c.done
	c.cancel, c.done = nil, nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if c.close != nil {
		c.close()
	}
	return nil
}
