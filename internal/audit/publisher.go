package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrPublishFailed reports that a publisher step could not complete. Nothing is
// lost by it: an event not marked published is delivered again by a later step.
var ErrPublishFailed = errors.New("audit: publishing the outbox failed")

// Event is one outbox row as a Sink sees it.
type Event struct {
	ID             int64
	AggregateType  string
	AggregateID    string
	EventType      string
	Payload        json.RawMessage
	IdempotencyKey string
	Attempts       int
	CreatedAt      time.Time
}

// Sink receives events. DELIVERY IS AT LEAST ONCE: a process that dies after
// Deliver returns and before the row is marked published delivers the same
// event again. A Sink and its consumers therefore must be idempotent by
// IdempotencyKey, and must apply per-aggregate revisions so a delayed or
// out-of-order event cannot revert newer state (G-54). Deliver must return nil
// only once the consumer has durably accepted the event.
//
// An error is retried with backoff. If it implements DeliveryReason() string,
// that short [a-z_] token is stored in last_error; any other error is stored as
// "delivery_failed". The error text itself is never stored: a consumer's error
// can quote the payload.
type Sink interface {
	Deliver(ctx context.Context, event Event) error
}

// AllFailureReasons is the closed vocabulary of delivery-failure reasons: the
// only values stored in last_error and used as a metric attribute.
func AllFailureReasons() []string {
	return []string{"delivery_failed", "delivery_timeout", "consumer_unavailable", "consumer_rejected"}
}

func failureReason(err error) string {
	var r interface{ DeliveryReason() string }
	if errors.As(err, &r) {
		reason := r.DeliveryReason()
		for _, known := range AllFailureReasons() {
			if reason == known {
				return known
			}
		}
	}
	return "delivery_failed"
}

type timeoutError struct{}

func (timeoutError) Error() string          { return "audit: sink delivery timed out" }
func (timeoutError) DeliveryReason() string { return "delivery_timeout" }

// maxAbandonedDeliveries caps the Deliver calls that outlived DeliverTimeout
// and have not returned. Go cannot stop a goroutine, so a sink that ignores its
// context leaks one per timeout; at the cap the publisher stops starting new
// calls and the step reports the wedge instead of leaking without bound.
const maxAbandonedDeliveries = 8

// PublisherConfig bounds one publisher.
type PublisherConfig struct {
	Schema         authschema.ValidatedIdentifier
	BaseBackoff    time.Duration // first retry delay; doubles per attempt
	MaxBackoff     time.Duration
	DeliverTimeout time.Duration // bound on one Sink.Deliver call, enforced even if the sink ignores its context
	Logger         *slog.Logger
	// Meter registers the publisher's instruments; nil uses the process meter.
	Meter metric.Meter
}

const (
	defaultBaseBackoff    = 2 * time.Second
	defaultMaxBackoff     = 10 * time.Minute
	defaultDeliverTimeout = 15 * time.Second
	maxStepLimit          = 100
)

// Publisher delivers unpublished outbox events to a Sink and records that it
// did.
type Publisher struct {
	pool   *pgxpool.Pool
	sink   Sink
	config PublisherConfig

	abandoned atomic.Int64
	delivered metric.Int64Counter
	failed    metric.Int64Counter
}

// NewPublisher validates its inputs. A nil sink is refused: a publisher with
// nowhere to deliver would mark events published that nobody received.
func NewPublisher(pool *pgxpool.Pool, sink Sink, config PublisherConfig) (*Publisher, error) {
	if pool == nil || sink == nil {
		return nil, fmt.Errorf("%w: a pool and a sink are required", ErrPublishFailed)
	}
	if config.BaseBackoff <= 0 {
		config.BaseBackoff = defaultBaseBackoff
	}
	if config.MaxBackoff < config.BaseBackoff {
		config.MaxBackoff = defaultMaxBackoff
	}
	if config.DeliverTimeout <= 0 {
		config.DeliverTimeout = defaultDeliverTimeout
	}
	if config.Logger == nil {
		config.Logger = slog.New(slog.DiscardHandler)
	}
	p := &Publisher{pool: pool, sink: sink, config: config}
	meter := config.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/audit")
	}
	var err error
	if p.delivered, err = meter.Int64Counter("devhealth_auth_outbox_delivered_total",
		metric.WithDescription("Outbox events delivered to the sink and marked published.")); err != nil {
		return nil, fmt.Errorf("%w: delivered counter: %w", ErrPublishFailed, err)
	}
	if p.failed, err = meter.Int64Counter("devhealth_auth_outbox_delivery_failures_total",
		metric.WithDescription("Outbox delivery attempts that failed, by reason.")); err != nil {
		return nil, fmt.Errorf("%w: failure counter: %w", ErrPublishFailed, err)
	}
	return p, nil
}

// PublishResult counts one step.
type PublishResult struct {
	Delivered int
	Failed    int
}

// Backoff is the delay before attempt number attempts+1 is due.
func (p *Publisher) backoff(attempts int) time.Duration {
	d := p.config.BaseBackoff
	for i := 1; i < attempts && d < p.config.MaxBackoff; i++ {
		d *= 2
	}
	if d > p.config.MaxBackoff {
		d = p.config.MaxBackoff
	}
	return d
}

// Step delivers up to limit due events, oldest first, one transaction per
// event: the row is locked FOR UPDATE SKIP LOCKED, delivered, and marked, so
// the window in which a crash causes a redelivery is one Deliver call, and two
// publishers never hold the same row.
func (p *Publisher) Step(ctx context.Context, now time.Time, limit int) (PublishResult, error) {
	var result PublishResult
	if limit <= 0 || limit > maxStepLimit {
		return result, fmt.Errorf("%w: limit %d is outside 1..%d", ErrPublishFailed, limit, maxStepLimit)
	}
	for i := 0; i < limit; i++ {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		outcome, err := p.deliverOne(ctx, now)
		if err != nil {
			return result, err
		}
		switch outcome {
		case outcomeNone:
			return result, nil
		case outcomeDelivered:
			result.Delivered++
		case outcomeFailed:
			result.Failed++
		}
	}
	return result, nil
}

type outcome int

const (
	outcomeNone outcome = iota
	outcomeDelivered
	outcomeFailed
)

func (p *Publisher) table() string { return authschema.Quote(p.config.Schema) + ".auth_outbox_events" }

// deliver calls the sink with a timeout it cannot ignore: the call runs in its
// own goroutine and is abandoned (counted, capped) when DeliverTimeout passes,
// so a sink that never returns cannot hold the row lock, the step or shutdown.
func (p *Publisher) deliver(ctx context.Context, e Event) error {
	deliverCtx, cancel := context.WithTimeout(ctx, p.config.DeliverTimeout)
	result := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				result <- errors.New("audit: sink panicked")
			}
		}()
		result <- p.sink.Deliver(deliverCtx, e)
	}()
	select {
	case err := <-result:
		cancel()
		return err
	case <-deliverCtx.Done():
		p.abandoned.Add(1)
		go func() {
			defer cancel()
			<-result
			p.abandoned.Add(-1)
		}()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return timeoutError{}
	}
}

func (p *Publisher) deliverOne(ctx context.Context, now time.Time) (outcome, error) {
	if n := p.abandoned.Load(); n >= maxAbandonedDeliveries {
		return outcomeNone, fmt.Errorf("%w: %d sink deliveries are still running past their timeout; not starting another", ErrPublishFailed, n)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return outcomeNone, fmt.Errorf("%w: beginning: %w", ErrPublishFailed, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := armTx(ctx, tx); err != nil {
		return outcomeNone, err
	}

	var e Event
	var payload []byte
	err = tx.QueryRow(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, payload, idempotency_key, attempts, created_at
		FROM `+p.table()+`
		WHERE published_at IS NULL AND available_at <= $1
		ORDER BY available_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`, now.UTC()).Scan(
		&e.ID, &e.AggregateType, &e.AggregateID, &e.EventType, &payload, &e.IdempotencyKey, &e.Attempts, &e.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return outcomeNone, nil
	}
	if err != nil {
		return outcomeNone, fmt.Errorf("%w: claiming an event: %w", ErrPublishFailed, err)
	}
	e.Payload = payload

	deliverErr := p.deliver(ctx, e)
	if deliverErr != nil && ctx.Err() != nil {
		return outcomeNone, ctx.Err() // shutting down: leave the row untouched
	}

	if deliverErr == nil {
		// The mark uses the database clock so published_at is comparable with
		// the reaper's cutoff whatever the caller's clock says.
		if _, err := tx.Exec(ctx, `UPDATE `+p.table()+`
			SET published_at = now(), attempts = attempts + 1, last_error = NULL
			WHERE id = $1`, e.ID); err != nil {
			return outcomeNone, fmt.Errorf("%w: marking event %d published: %w", ErrPublishFailed, e.ID, err)
		}
		if err := tx.Commit(ctx); err != nil {
			// The sink has the event; the row will be delivered again.
			return outcomeNone, fmt.Errorf("%w: committing the publish mark of event %d (it will be redelivered): %w", ErrPublishFailed, e.ID, err)
		}
		p.delivered.Add(ctx, 1)
		p.config.Logger.DebugContext(ctx, "outbox event delivered",
			"event_id", e.ID, "event_type", e.EventType, "attempts", e.Attempts+1)
		return outcomeDelivered, nil
	}

	reason := failureReason(deliverErr)
	// Measured from the failure, not from the step's start: a slow sink must not
	// turn the backoff into "due now".
	retryAt := time.Now().UTC().Add(p.backoff(e.Attempts + 1))
	if _, err := tx.Exec(ctx, `UPDATE `+p.table()+`
		SET attempts = attempts + 1, last_error = $2, available_at = $3
		WHERE id = $1`, e.ID, reason, retryAt); err != nil {
		return outcomeNone, fmt.Errorf("%w: recording the failure of event %d: %w", ErrPublishFailed, e.ID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return outcomeNone, fmt.Errorf("%w: committing the failure of event %d: %w", ErrPublishFailed, e.ID, err)
	}
	p.failed.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
	p.config.Logger.WarnContext(ctx, "outbox event delivery failed",
		"event_id", e.ID, "event_type", e.EventType, "attempts", e.Attempts+1, "reason", reason,
		"retry_at", retryAt)
	return outcomeFailed, nil
}
