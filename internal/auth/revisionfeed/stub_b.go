package revisionfeed

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// Stub of PR B: every function does nothing. The next commit replaces this file.

const (
	HighWaterKey = "dho:rl:auth_svc_rev_hwm"
	Channel      = "auth_svc_rev"
)

const highWaterScript = ""

var (
	ErrUnavailable   = errors.New("revisionfeed: valkey unavailable")
	ErrRejected      = errors.New("revisionfeed: valkey refused the command")
	ErrCorruptFloors = errors.New("revisionfeed: the published floors are corrupt")
)

type SubscribeHooks struct {
	OnSubscribed func()
	OnMessage    func(string)
}

type Client interface {
	EvalInt(ctx context.Context, script string, keys, args []string) (int64, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	ConfigGet(ctx context.Context, name string) (string, error)
	TTL(ctx context.Context, key string) (int64, error)
	Subscribe(ctx context.Context, channel string, hooks SubscribeHooks) error
}

type Sink struct{}

func NewSink(Client) (*Sink, error)                      { return &Sink{}, nil }
func (*Sink) Deliver(context.Context, audit.Event) error { return nil }

type SubscriberConfig struct {
	Client   Client
	Consumer *Consumer
	Logger   *slog.Logger
	Meter    metric.Meter
	Backoff  func(attempt int) time.Duration
}

type Subscriber struct{}

func NewSubscriber(SubscriberConfig) (*Subscriber, error) { return &Subscriber{}, nil }
func (*Subscriber) Run(context.Context)                   {}

func PublishedFloors(context.Context, Client) (map[string]int64, error) { return nil, nil }

type Status struct {
	AppendOnly      bool
	MaxMemoryPolicy string
	HashHasTTL      bool
	Usable          bool
	Reason          string
}

func ReadStatus(context.Context, Client) (Status, error) { return Status{}, nil }
func StatusReasons() []string                            { return nil }
