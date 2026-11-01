// Package revisionfeed is the consumer side of the auth outbox feed (CHAOS-10983): an idempotent consumer keyed by
// aggregate and revision, and the latest revision it has seen per aggregate. It holds no decision cache and no
// transport; a cache plugs in through Invalidator.
package revisionfeed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// Invalidator is what a cache gives the consumer. Invalidate evicts what the cache holds for one aggregate (a new
// revision, or a gap: a resync); InvalidateAll evicts everything. Both only evict: they are idempotent and may be
// called twice for one revision. Invalidate at revision N covers EVERY revision <= N of that aggregate: the feed is a
// high-water mark per aggregate, so a replica that reconnects reaches the latest revision and the revisions in between are
// covered by the invalidation at the latest, not replayed.
//
// Both return an error, and a returned error means the cache may still hold what it should have dropped: the consumer
// then leaves its cursor and mark where they were, so the same revision is applied (and invalidated) again. A silent
// failure would leave a stale entry that no later event repairs.
type Invalidator interface {
	Invalidate(ctx context.Context, aggregateType, aggregateID string, revision int64) error
	InvalidateAll(ctx context.Context) error
}

// Config is the consumer's inputs. Logger and Meter default to a discarding logger and the global meter.
type Config struct {
	Invalidator Invalidator
	Logger      *slog.Logger
	Meter       metric.Meter
}

// Consumer applies outbox events by aggregate revision. Revision order is per aggregate; delivery may repeat,
// overlap and reorder (audit.Sink), so every event is classified (Peek) and advanced (ResyncTo) by contracts.ConsumerCursor
// under one mutex; the invalidation runs between the two, outside it. Handles of one aggregate are serialized from the
// Peek to the advance (inflight), so a live event and a catch-up floor of one revision never both invalidate it.
type Consumer struct {
	mu       sync.Mutex
	cursor   *contracts.ConsumerCursor
	inflight map[string]chan struct{} // closed when the Handle that holds the aggregate key is done
	marks    *Watermarks
	inv      Invalidator
	log      *slog.Logger
	onWait   func(key string) // test seam: a Handle starts to wait for its aggregate

	events  metric.Int64Counter
	resyncs metric.Int64Counter
}

// NewConsumer validates its inputs and registers the metrics.
func NewConsumer(cfg Config) (*Consumer, error) {
	if cfg.Invalidator == nil {
		return nil, errors.New("revisionfeed: an invalidator is required")
	}
	c := &Consumer{cursor: contracts.NewConsumerCursor(), inflight: map[string]chan struct{}{}, marks: NewWatermarks(), inv: cfg.Invalidator, log: cfg.Logger}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/revisionfeed")
	}
	var err error
	if c.events, err = meter.Int64Counter("devhealth_auth_revision_feed_events_total",
		metric.WithDescription("Outbox events the revision feed consumer classified, by disposition (apply, ignore, gap, dropped, invalidate_failed).")); err != nil {
		return nil, fmt.Errorf("revisionfeed: events counter: %w", err)
	}
	if c.resyncs, err = meter.Int64Counter("devhealth_auth_revision_feed_resync_total",
		metric.WithDescription("Full or per-aggregate resynchronisations, by cause (gap, resubscribe).")); err != nil {
		return nil, fmt.Errorf("revisionfeed: resync counter: %w", err)
	}
	return c, nil
}

// Watermarks is the in-process read of the latest revision seen per aggregate.
func (c *Consumer) Watermarks() *Watermarks { return c.marks }

// ErrInvalidate wraps an Invalidator error. The event is not applied: the cursor and the mark did not move, so it applies
// again when it is delivered again (or when the next catch-up reaches its floor).
var ErrInvalidate = errors.New("revisionfeed: invalidation failed")

// Handle classifies one event and acts on it: Apply invalidates the aggregate; Gap invalidates it and moves the mark to
// the gap revision (ResyncTo), so the gap does not repeat for every later revision; Ignore drops it. The invalidation runs
// BEFORE the cursor and the mark advance, and outside the lock, so a slow cache never blocks another aggregate's event. An
// invalidation error is returned (wrapping ErrInvalidate) with the cursor and the mark unmoved: the event is retryable.
// A second Handle of the same aggregate waits for the first to finish and then classifies (a ctx that ends while it waits
// is returned, nothing invalidated).
func (c *Consumer) Handle(ctx context.Context, e contracts.ProvisioningEvent) (contracts.Disposition, error) {
	revision := int64(e.Payload.Revision)
	key := contracts.AggregateKey(e.AggregateType, e.AggregateID)
	done, err := c.acquire(ctx, key)
	if err != nil {
		return "", err
	}
	defer c.release(key, done)
	c.mu.Lock()
	disposition := c.cursor.Peek(e)
	c.mu.Unlock()

	if disposition == contracts.Apply || disposition == contracts.Gap {
		if err := c.inv.Invalidate(ctx, e.AggregateType, e.AggregateID, revision); err != nil {
			c.events.Add(ctx, 1, metric.WithAttributes(attribute.String("disposition", "invalidate_failed")))
			c.log.LogAttrs(ctx, slog.LevelWarn, "revision feed invalidation failed; the event stays unapplied",
				slog.String("aggregate_type", e.AggregateType), slog.Int64("revision", revision), slog.String("disposition", string(disposition)))
			return disposition, fmt.Errorf("%w: %w", ErrInvalidate, err)
		}
		c.mu.Lock()
		c.cursor.ResyncTo(key, revision)
		c.mu.Unlock()
		c.marks.record(e.AggregateType, e.AggregateID, revision)
	}

	c.events.Add(ctx, 1, metric.WithAttributes(attribute.String("disposition", string(disposition))))
	switch disposition {
	case contracts.Apply:
		c.logDisposition(ctx, slog.LevelDebug, e.AggregateType, revision, disposition)
	case contracts.Gap:
		c.resyncs.Add(ctx, 1, metric.WithAttributes(attribute.String("cause", "gap")))
		c.logDisposition(ctx, slog.LevelInfo, e.AggregateType, revision, disposition)
	default:
		c.logDisposition(ctx, slog.LevelDebug, e.AggregateType, revision, disposition)
	}
	return disposition, nil
}

// acquire takes the in-flight slot of key, waiting while another Handle holds it.
func (c *Consumer) acquire(ctx context.Context, key string) (chan struct{}, error) {
	for {
		c.mu.Lock()
		busy, held := c.inflight[key]
		if !held {
			done := make(chan struct{})
			c.inflight[key] = done
			c.mu.Unlock()
			return done, nil
		}
		c.mu.Unlock()
		if c.onWait != nil {
			c.onWait(key)
		}
		select {
		case <-busy:
		case <-ctx.Done():
		}
		// Both may be ready at once and select picks either: a waiter whose ctx ended never takes the slot.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

func (c *Consumer) release(key string, done chan struct{}) {
	c.mu.Lock()
	delete(c.inflight, key)
	c.mu.Unlock()
	close(done)
}

// HandleOutbox maps an outbox row to a provisioning event and handles it. A row that does not map is refused with the
// mapper's error and nothing is invalidated.
func (c *Consumer) HandleOutbox(ctx context.Context, e audit.Event) (contracts.Disposition, error) {
	mapped, err := FromOutbox(e)
	if err != nil {
		return "", err
	}
	return c.Handle(ctx, mapped)
}

// Resubscribed is called on every (re)subscribe: a pub/sub channel has no replay, so events published while it was down
// are lost, and everything the cache holds may be stale. The marks stay: a duplicate after a resubscribe is still ignored.
// An invalidation error is returned: the caller retries it before it reports the subscription up.
func (c *Consumer) Resubscribed(ctx context.Context) error {
	c.resyncs.Add(ctx, 1, metric.WithAttributes(attribute.String("cause", "resubscribe")))
	if err := c.inv.InvalidateAll(ctx); err != nil {
		c.log.LogAttrs(ctx, slog.LevelWarn, "revision feed resubscribe invalidation failed", slog.String("disposition", "invalidate_failed"))
		return fmt.Errorf("%w: %w", ErrInvalidate, err)
	}
	c.log.LogAttrs(ctx, slog.LevelInfo, "revision feed resubscribed", slog.String("disposition", "resubscribe"))
	return nil
}

// CatchUp feeds the published floors (the high-water hash, read AFTER the subscribe was confirmed) to the consumer: a floor
// above the mark moves it (Gap: ResyncTo) and invalidates the aggregate, a floor at or below it is ignored. It goes through
// Handle, so it is revision-idempotent with a live event that arrives before, during or after it. A field that is not
// "<type>:<id>" is skipped. Every floor is tried; the errors of those that failed are joined and returned, and the
// caller retries the whole read (a floor already applied is a no-op).
func (c *Consumer) CatchUp(ctx context.Context, floors map[string]int64) error {
	var failed error
	for field, revision := range floors {
		typ, id, ok := splitAggregateKey(field)
		if !ok || !validRevision(revision) {
			c.Dropped(ctx)
			continue
		}
		if _, err := c.Handle(ctx, contracts.ProvisioningEvent{
			AggregateType: typ, AggregateID: id,
			Payload: contracts.ProvisioningPayload{Revision: contracts.Revision(revision)},
		}); err != nil {
			failed = errors.Join(failed, err)
		}
	}
	return failed
}

// Dropped counts one message or floor that was refused before it reached the cursor (malformed, or a revision outside
// 1..MaxRevision): events_total{disposition=dropped}. The cursor never moves on it.
func (c *Consumer) Dropped(ctx context.Context) {
	c.events.Add(ctx, 1, metric.WithAttributes(attribute.String("disposition", "dropped")))
}

// logDisposition logs the aggregate type, the revision and the disposition only: never the aggregate id or the payload.
func (c *Consumer) logDisposition(ctx context.Context, level slog.Level, aggregateType string, revision int64, d contracts.Disposition) {
	c.log.LogAttrs(ctx, level, "revision feed event",
		slog.String("aggregate_type", aggregateType), slog.Int64("revision", revision), slog.String("disposition", string(d)))
}
