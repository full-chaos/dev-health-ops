// Package streamrunner owns the bounded, lifecycle-managed Redis Streams
// consumption protocol used by the dormant Go stream profiles. It deliberately
// keeps transport and durable-write concerns separate: a message is ACKed only
// after its Handler returns nil, which is the durable-write commit boundary.
package streamrunner

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig         = errors.New("invalid stream runner configuration")
	ErrAlreadyStarted        = errors.New("stream runner already started")
	ErrQuarantineUnavailable = errors.New("stream quarantine unavailable")
	ErrDiscoveryLimit        = errors.New("stream discovery limit exceeded")
)

// Message is the bounded transport representation. Raw payloads are never
// emitted as labels or metrics; handlers own validation and persistence.
type Message struct {
	Stream string
	ID     string
	Fields map[string]string
}

// Pending is the small inspection form required for reclaim. Delivery count
// comes from Redis' PEL and is never inferred locally across restarts.
type Pending struct {
	MessageID      string
	TimesDelivered int
	Idle           time.Duration
}

// StreamStats is a low-cardinality snapshot for one configured stream.
type StreamStats struct {
	Lag           int64
	Pending       int64
	OldestPending time.Duration
}

// Transport isolates Redis protocol details. Implementations must make Read
// blocking only up to the supplied context; cancellation is the shutdown
// checkpoint that prevents a runner from fetching after drain begins.
type Transport interface {
	EnsureGroup(context.Context, string, string) error
	ReadNew(context.Context, []string, string, string, int, time.Duration) ([]Message, error)
	Pending(context.Context, string, string, int, time.Duration) ([]Pending, error)
	Claim(context.Context, string, string, string, []string, time.Duration) ([]Message, error)
	Ack(context.Context, string, string, string) error
	Quarantine(context.Context, Message, string) error
	Stats(context.Context, string, string) (StreamStats, error)
	Close()
}

// Discoverer is optional because deterministic unit transports can expose a
// fixed set of streams. The production Valkey transport implements it for
// bounded wildcard discovery of per-org stream keys.
type Discoverer interface {
	Discover(context.Context, []string, int) ([]string, error)
}

// Handler must return nil only after its authoritative sink transaction or
// ClickHouse durable insert completed. Permanent errors are quarantined;
// every other error remains pending for bounded reclaim.
type Handler interface {
	Handle(context.Context, Message) error
}

// PermanentFinalizer is implemented by handlers whose authoritative status
// store must transition only after the quarantine row is durable. The runner
// invokes it between Quarantine and ACK, preserving the external-ingest
// DLQ-first/status-second/ACK-last contract.
type PermanentFinalizer interface {
	FinalizePermanent(context.Context, Message, string) error
}

// PermanentError marks a syntactically/semantically invalid message that can
// never become valid through retry. Reason is intentionally bounded by the
// caller and is safe for the quarantine stream, not for metrics labels.
type PermanentError struct{ Reason string }

func (e *PermanentError) Error() string { return e.Reason }

func IsPermanent(err error) bool {
	var permanent *PermanentError
	return errors.As(err, &permanent)
}

// Config has deliberately conservative defaults compatible with the existing
// Celery consumers. External uses one lane and a 15-minute reclaim threshold;
// internal consumers can use a shorter threshold because they are isolated.
type Config struct {
	Name               string
	Streams            []string
	Patterns           []string
	ConsumerGroup      string
	ConsumerName       string
	BatchSize          int
	DiscoveryLimit     int
	Block              time.Duration
	ReclaimEvery       time.Duration
	ReclaimIdle        time.Duration
	MaxDeliveries      int
	ShutdownDrain      time.Duration
	Singleton          bool
	ConfiguredReplicas int
}

func (c Config) validate() error {
	if c.Name == "" || (len(c.Streams) == 0 && len(c.Patterns) == 0) || c.ConsumerGroup == "" || c.ConsumerName == "" ||
		c.BatchSize < 1 || c.BatchSize > 1_000 || c.Block < 10*time.Millisecond || c.Block > time.Minute ||
		c.ReclaimEvery < 10*time.Millisecond || c.ReclaimIdle < c.Block || c.MaxDeliveries < 1 ||
		c.MaxDeliveries > 100 || c.ShutdownDrain < c.Block || c.ShutdownDrain > 5*time.Minute {
		return ErrInvalidConfig
	}
	if len(c.Patterns) > 0 && (c.DiscoveryLimit < 1 || c.DiscoveryLimit > 100_000) {
		return ErrInvalidConfig
	}
	if c.Singleton && c.ConfiguredReplicas != 1 {
		return ErrInvalidConfig
	}
	return nil
}
